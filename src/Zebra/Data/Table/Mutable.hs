{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Table.Mutable (
    MTable(..)
  , MColumn(..)

  , new
  , unsafeFreeze
  , thaw

  , insertMaybeValue
  , insertValue
  , insertDefault
  , evalStateTable

  , appendTable

  , tablesOfFacts

  , MutableError(..)
  , renderMutableError
  ) where

import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.ST (runST)
import           Control.Monad.Trans (lift)
import           Control.Monad.Trans.State.Strict (StateT, runStateT, get, put)

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.Text as T
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Storable.Mutable as MStorable
import qualified Data.Vector.Unboxed.Mutable as MUnboxed
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, left, hoistMaybe)
import           X.Data.Vector.Grow (Grow)
import qualified X.Data.Vector.Grow as Grow
import           X.Data.Vector.Ref (Ref)
import qualified X.Data.Vector.Ref as Ref

import           Zebra.Data.Core
import           Zebra.Data.Fact
import           Zebra.Data.Schema (Schema, Field(..), Variant(..))
import qualified Zebra.Data.Schema as Schema
import           Zebra.Data.Table (Table(..), Column(..))


data MTable s =
  MTable {
      mtableSchema :: !Schema
    , mtableRowCount :: !(Ref MUnboxed.MVector s Int)
    , mtableColumns :: !(Boxed.Vector (MColumn s))
    } deriving (Generic, Typeable)

data MColumn s =
    MByteColumn !(Grow MStorable.MVector s Word8)
  | MIntColumn !(Grow MStorable.MVector s Int64)
  | MDoubleColumn !(Grow MStorable.MVector s Double)
  | MArrayColumn !(Grow MStorable.MVector s Int64) !(MTable s)
    deriving (Generic, Typeable)

data MutableError =
    MutableExpectedByteColumn !FoundMColumn
  | MutableExpectedIntColumn !FoundMColumn
  | MutableExpectedDoubleColumn !FoundMColumn
  | MutableExpectedArrayColumn !FoundMColumn
  | MutableNoMoreColumns
  | MutableLeftoverColumns ![FoundMColumn]
  | MutableSchemaMismatch !Value !Schema
  | MutableStructFieldsMismatch !(Boxed.Vector Value) !(Boxed.Vector Field)
  | MutableEnumVariantMismatch !Int !Value !(Boxed.Vector Variant)
  | MutableRequiredFieldMissing !Schema
  | MutableAttributeNotFound !AttributeId
    deriving (Eq, Ord, Show)

data FoundMColumn =
    FoundMByteColumn
  | FoundMIntColumn
  | FoundMDoubleColumn
  | FoundMArrayColumn ![FoundMColumn]
    deriving (Eq, Ord, Show)

renderMutableError :: MutableError -> Text
renderMutableError = \case
  MutableExpectedByteColumn column ->
    "Expected byte column, but found: " <> renderFoundMColumn column
  MutableExpectedIntColumn column ->
    "Expected int column, but found: " <> renderFoundMColumn column
  MutableExpectedDoubleColumn column ->
    "Expected double column, but found: " <> renderFoundMColumn column
  MutableExpectedArrayColumn column ->
    "Expected array column, but found: " <> renderFoundMColumn column
  MutableNoMoreColumns ->
    "Expected to find more columns in table - ran out when trying to insert."
  MutableLeftoverColumns columns ->
    "Found more columns than expected in table: " <> renderFoundMColumns columns
  MutableSchemaMismatch value schema ->
    "Schema did not match value:" <>
    "\n" <>
    "\n  schema =" <>
    "\n    " <> T.pack (show schema) <>
    "\n" <>
    "\n  value =" <>
    "\n    " <> T.pack (show value)
  MutableStructFieldsMismatch values fields ->
    "Struct schema did not match its value:" <>
    "\n" <>
    "\n  fields =" <>
    "\n    " <> T.pack (show fields) <>
    "\n" <>
    "\n  values =" <>
    "\n    " <> T.pack (show values)
  MutableEnumVariantMismatch tag value variants ->
    "Enum schema did not match its value:" <>
    "\n" <>
    "\n  variants =" <>
    "\n    " <> T.pack (show variants) <>
    "\n" <>
    "\n  tag =" <>
    "\n    " <> T.pack (show tag) <>
    "\n" <>
    "\n  value =" <>
    "\n    " <> T.pack (show value)
  MutableRequiredFieldMissing schema ->
    "Struct required field missing: " <> T.pack (show schema)
  MutableAttributeNotFound (AttributeId aid) ->
    "Attribute not found: " <> T.pack (show aid)

renderFoundMColumn :: FoundMColumn -> Text
renderFoundMColumn = \case
  FoundMByteColumn ->
    "byte"
  FoundMIntColumn ->
    "int"
  FoundMDoubleColumn ->
    "double"
  FoundMArrayColumn xs ->
    "array (" <> renderFoundMColumns xs <> ")"

renderFoundMColumns :: [FoundMColumn] -> Text
renderFoundMColumns =
  T.intercalate ", " . fmap renderFoundMColumn

------------------------------------------------------------------------

unsafeFreezeTables :: PrimMonad m => MTableBox (PrimState m) -> m (Boxed.Vector (Table Schema))
unsafeFreezeTables (MTableBox tables) =
  traverse (unsafeFreeze . snd) tables

unsafeFreeze :: PrimMonad m => MTable (PrimState m) -> m (Table Schema)
unsafeFreeze (MTable schema nref cols) = do
  Table
    <$> pure schema
    <*> Ref.readRef nref
    <*> traverse unsafeFreezeColumn cols

unsafeFreezeColumn :: PrimMonad m => MColumn (PrimState m) -> m (Column Schema)
unsafeFreezeColumn = \case
  MByteColumn g ->
    ByteColumn . unsafeToByteString <$> Grow.unsafeFreeze g
  MIntColumn g ->
    IntColumn <$> Grow.unsafeFreeze g
  MDoubleColumn g ->
    DoubleColumn <$> Grow.unsafeFreeze g
  MArrayColumn g mtable -> do
    ns <- Grow.unsafeFreeze g
    table <- unsafeFreeze mtable
    pure $ ArrayColumn ns table

thaw :: PrimMonad m => Table Schema -> m (MTable (PrimState m))
thaw (Table schema n columns) =
  MTable
    <$> pure schema
    <*> Ref.newRef n
    <*> Boxed.mapM thawColumn columns

thawColumn :: PrimMonad m => Column Schema -> m (MColumn (PrimState m))
thawColumn = \case
  ByteColumn bs -> do
    MByteColumn <$> growOfVector (unsafeFromByteString bs)
  IntColumn vs ->
    MIntColumn <$> growOfVector vs
  DoubleColumn vs ->
    MDoubleColumn <$> growOfVector vs
  ArrayColumn vs rec -> do
    ns <- growOfVector vs
    table <- thaw rec
    pure $ MArrayColumn ns table
 where
  growOfVector vv = do
    g <- Grow.new (Storable.length vv)
    Grow.append g vv
    return g

------------------------------------------------------------------------

appendTable :: PrimMonad m => MTable (PrimState m) -> Table Schema -> EitherT MutableError m ()
appendTable (MTable _ nref mfs) (Table _ n fs) = do
  Ref.modifyRef nref (+ n)
  Boxed.zipWithM_ appendColumn mfs fs

appendColumn :: PrimMonad m => MColumn (PrimState m) -> Column Schema -> EitherT MutableError m ()
appendColumn mf = \case
  ByteColumn bs
   | MByteColumn g <- mf
   -> Grow.append g (unsafeFromByteString bs)
   | otherwise
   -> left $ MutableExpectedByteColumn $ describeColumn mf

  IntColumn vs
   | MIntColumn g <- mf
   -> Grow.append g vs
   | otherwise
   -> left $ MutableExpectedIntColumn $ describeColumn mf

  DoubleColumn vs
   | MDoubleColumn g <- mf
   -> Grow.append g vs
   | otherwise
   -> left $ MutableExpectedDoubleColumn $ describeColumn mf

  ArrayColumn vs rec
   | MArrayColumn g mrec <- mf
   -> do  Grow.append g vs
          appendTable mrec rec
   | otherwise
   -> left $ MutableExpectedArrayColumn $ describeColumn mf

------------------------------------------------------------------------

newtype MTableBox s =
  MTableBox (Boxed.Vector (Schema, MTable s))

mkMTableBox :: PrimMonad m => Boxed.Vector Schema -> m (MTableBox (PrimState m))
mkMTableBox schemas = do
  xs <- traverse new schemas
  pure . MTableBox $ Boxed.zip schemas xs

insertFact ::
  PrimMonad m =>
  MTableBox (PrimState m) ->
  AttributeId ->
  Maybe' Value ->
  EitherT MutableError m ()
insertFact (MTableBox tables) aid@(AttributeId ix) mvalue = do
  case tables Boxed.!? fromIntegral ix of
    Nothing ->
      left $ MutableAttributeNotFound aid
    Just (schema, table) ->
      insertMaybeValue schema table mvalue

------------------------------------------------------------------------

tablesOfFacts ::
  Boxed.Vector Schema ->
  Boxed.Vector Fact ->
  Either MutableError (Boxed.Vector (Table Schema))
tablesOfFacts schemas facts =
  runST $ runEitherT $ do
    box <- lift $ mkMTableBox schemas

    for_ facts $ \fact ->
      insertFact box (factAttributeId fact) (factValue fact)

    lift $ unsafeFreezeTables box

insertMaybeValue ::
  PrimMonad m =>
  Schema ->
  MTable (PrimState m) ->
  Maybe' Value ->
  EitherT MutableError m ()
insertMaybeValue schema table = \case
  Nothing' -> do
    evalStateTable table $ do
      insertDefault schema
      MTable _ nref _ <- get
      Ref.modifyRef nref (+ 1)
  Just' value -> do
    evalStateTable table $ do
      insertValue schema value
      MTable _ nref _ <- get
      Ref.modifyRef nref (+ 1)

insertValue ::
  PrimMonad m =>
  Schema ->
  Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertValue schema value =
  case schema of
    Schema.Bool ->
      case value of
        Bool x ->
          insertBool x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Schema.Byte ->
      case value of
        Byte x ->
          insertByte x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Schema.Int ->
      case value of
        Int x ->
          insertInt x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Schema.Double ->
      case value of
        Double x ->
          insertDouble x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Schema.Enum variant0 variants ->
      case value of
        Enum tag x -> do
          insertInt $ fromIntegral tag
          (vs0, v1, vs2) <-
            lift . hoistMaybe (MutableEnumVariantMismatch tag x $ Boxed.cons variant0 variants) $
              focus tag variant0 variants

          traverse_ (insertDefault . variantSchema) vs0
          insertValue (variantSchema v1) x
          traverse_ (insertDefault . variantSchema) vs2

        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Schema.Struct fields ->
      case value of
        Struct values ->
          insertStruct fields values
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Schema.Array ischema ->
      case value of
        ByteArray xs ->
          case ischema of
            Schema.Byte ->
              insertByteArray xs
            _ ->
              lift . left $ MutableSchemaMismatch value schema

        Array xs ->
          popArrayColumn $ \ns -> do
            let
              n =
                fromIntegral $ Boxed.length xs

              insertElem x = do
                table <- get
                insertValue ischema x
                MTable _ _ columns <- get
                if Boxed.null columns then do
                  put table
                else
                  lift . left . MutableLeftoverColumns . Boxed.toList $
                    fmap describeColumn columns

            Grow.add ns n
            traverse_ insertElem xs

            MTable _ nref _ <- get
            Ref.modifyRef nref (+ Boxed.length xs)

            put =<< newEmpty

        _ ->
          lift . left $ MutableSchemaMismatch value schema


focus :: Int -> a -> Boxed.Vector a -> Maybe (Boxed.Vector a, a, Boxed.Vector a)
focus i x0 xs =
  case i of
    0 ->
      Just (Boxed.empty, x0, xs)
    _ -> do
      let !j = i - 1
      x <- xs Boxed.!? j
      pure (Boxed.cons x0 $ Boxed.take j xs, x, Boxed.drop (j + 1) xs)

insertStruct ::
  PrimMonad m =>
  Boxed.Vector Field ->
  Boxed.Vector Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertStruct fields values =
  if Boxed.length fields /= Boxed.length values then
    lift . left $ MutableStructFieldsMismatch values fields
  else
    Boxed.zipWithM_ insertValue (fmap fieldSchema fields) values

------------------------------------------------------------------------

evalStateTable ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) a ->
  EitherT MutableError m a
evalStateTable table0 m = do
  (result, MTable _ _ columns) <- runStateT m table0
  if Boxed.null columns then
    pure result
  else
    left . MutableLeftoverColumns . Boxed.toList $ fmap describeColumn columns

popColumn ::
  PrimMonad m =>
  StateT (MTable (PrimState m)) (EitherT MutableError m) (MColumn (PrimState m))
popColumn = do
  MTable schema nref xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . MTable schema nref $ Boxed.drop 1 xs
      pure x
    Nothing ->
      lift $ left MutableNoMoreColumns

popByteColumn ::
  PrimMonad m =>
  StateT
    (MTable (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Word8)
popByteColumn =
  popColumn >>= \case
    MByteColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedByteColumn $ describeColumn x

popIntColumn ::
  PrimMonad m =>
  StateT
    (MTable (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Int64)
popIntColumn =
  popColumn >>= \case
    MIntColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedIntColumn $ describeColumn x

popDoubleColumn ::
  PrimMonad m =>
  StateT
    (MTable (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Double)
popDoubleColumn =
  popColumn >>= \case
    MDoubleColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedDoubleColumn $ describeColumn x

popArrayColumn ::
  PrimMonad m =>
  (Grow MStorable.MVector (PrimState m) Int64 ->
    StateT (MTable (PrimState m)) (EitherT MutableError m) a) ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) a
popArrayColumn f =
  popColumn >>= \case
    MArrayColumn ns table0 ->
      lift . evalStateTable table0 $ do
        x <- f ns

        n <- fromIntegral . Storable.sum <$> Grow.unsafeFreeze ns
        MTable _ nref _ <- get
        Ref.writeRef nref n

        pure x
    x ->
      lift . left . MutableExpectedArrayColumn $ describeColumn x

describeColumn :: MColumn s -> FoundMColumn
describeColumn = \case
  MByteColumn _ ->
    FoundMByteColumn
  MIntColumn _ ->
    FoundMIntColumn
  MDoubleColumn _ ->
    FoundMDoubleColumn
  MArrayColumn _ (MTable _ _ xs) ->
    FoundMArrayColumn . Boxed.toList $ fmap describeColumn xs

------------------------------------------------------------------------

insertBool ::
  PrimMonad m =>
  Bool ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertBool x = do
  xs <- popIntColumn
  Grow.add xs $ if x then 1 else 0

insertByte ::
  PrimMonad m =>
  Word8 ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertByte x = do
  xs <- popByteColumn
  Grow.add xs x

insertInt ::
  PrimMonad m =>
  Int64 ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertInt x = do
  xs <- popIntColumn
  Grow.add xs x

insertDouble ::
  PrimMonad m =>
  Double ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDouble x = do
  xs <- popDoubleColumn
  Grow.add xs x

insertByteArray ::
  PrimMonad m =>
  ByteString ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertByteArray bs = do
  popArrayColumn $ \ns -> do
    Grow.add ns (fromIntegral $ B.length bs)
    xs <- popByteColumn
    Grow.append xs (unsafeFromByteString bs)

insertDefault ::
  PrimMonad m =>
  Schema ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDefault = \case
  Schema.Bool ->
    insertBool False
  Schema.Byte ->
    insertByte 0
  Schema.Int ->
    insertInt 0
  Schema.Double ->
    insertDouble 0
  Schema.Struct fields ->
    traverse_ (insertDefault . fieldSchema) fields
  Schema.Enum variant0 variants -> do
    insertInt 0
    traverse_ (insertDefault . variantSchema) (Boxed.cons variant0 variants)
  Schema.Array _schema ->
    popArrayColumn $ \ns -> do
      Grow.add ns 0
      MTable schema nref _ <- get
      put $ MTable schema nref Boxed.empty

------------------------------------------------------------------------

newEmpty :: PrimMonad m => m (MTable (PrimState m))
newEmpty =
  MTable
    <$> pure (Schema.Struct Boxed.empty)
    <*> Ref.newRef 0
    <*> pure Boxed.empty

newBool :: PrimMonad m => m (MTable (PrimState m))
newBool =
  MTable
    <$> pure Schema.Bool
    <*> Ref.newRef 0
    <*> (Boxed.singleton . MIntColumn <$> Grow.new 4)

newByte :: PrimMonad m => m (MTable (PrimState m))
newByte =
  MTable
    <$> pure Schema.Byte
    <*> Ref.newRef 0
    <*> (Boxed.singleton . MByteColumn <$> Grow.new 4)

newInt :: PrimMonad m => m (MTable (PrimState m))
newInt =
  MTable
    <$> pure Schema.Int
    <*> Ref.newRef 0
    <*> (Boxed.singleton . MIntColumn <$> Grow.new 4)

newDouble :: PrimMonad m => m (MTable (PrimState m))
newDouble =
  MTable
    <$> pure Schema.Double
    <*> Ref.newRef 0
    <*> (Boxed.singleton . MDoubleColumn <$> Grow.new 4)

newEnum :: PrimMonad m => Schema.Variant -> Boxed.Vector Schema.Variant -> m (MTable (PrimState m))
newEnum variant0 variants = do
  tag <- newInt
  vtables <- traverse (new . variantSchema) (Boxed.cons variant0 variants)
  MTable
    <$> pure (Schema.Enum variant0 variants)
    <*> Ref.newRef 0
    <*> pure (Boxed.concatMap mtableColumns $ Boxed.cons tag vtables)

newStruct :: PrimMonad m => Boxed.Vector Schema.Field -> m (MTable (PrimState m))
newStruct fields =
  MTable
    <$> pure (Schema.Struct fields)
    <*> Ref.newRef 0
    <*> (Boxed.concatMap mtableColumns <$> traverse (new . fieldSchema) fields)

newArray :: PrimMonad m => MTable (PrimState m) -> m (MTable (PrimState m))
newArray item =
  MTable
    <$> pure (Schema.Array $ mtableSchema item)
    <*> Ref.newRef 0
    <*> (Boxed.singleton . flip MArrayColumn item <$> Grow.new 4)

new :: PrimMonad m => Schema -> m (MTable (PrimState m))
new = \case
  Schema.Bool ->
    newBool
  Schema.Byte ->
    newByte
  Schema.Int ->
    newInt
  Schema.Double ->
    newDouble
  Schema.Struct fields ->
    newStruct fields
  Schema.Enum variant0 variants -> do
    newEnum variant0 variants
  Schema.Array schema ->
    newArray =<< new schema

------------------------------------------------------------------------

unsafeFromByteString :: ByteString -> Storable.Vector Word8
unsafeFromByteString (PS fp off len) =
  Storable.unsafeFromForeignPtr fp off len

unsafeToByteString :: Storable.Vector Word8 -> ByteString
unsafeToByteString xs =
  let
    (fp, off, len) =
      Storable.unsafeToForeignPtr xs
  in
    PS fp off len
