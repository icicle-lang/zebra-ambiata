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

  , newMTable
  , insertMaybeValue
  , insertValue
  , insertDefault
  , withMTable
  , unsafeFreezeTable

  , thawTable

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
import qualified Data.Text.Encoding as T
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
import           Zebra.Data.Schema
import           Zebra.Data.Table (Table(..), Column(..))


data MTable s =
  MTable {
      mtableRowCount :: !(Ref MUnboxed.MVector s Int)
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
  | MutableStructFieldsMismatch !(Boxed.Vector Value) !(Boxed.Vector FieldSchema)
  | MutableEnumVariantMismatch !Int !Value !(Boxed.Vector VariantSchema)
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

unsafeFreezeTables :: PrimMonad m => MTableBox (PrimState m) -> m (Boxed.Vector Table)
unsafeFreezeTables (MTableBox tables) =
  traverse (unsafeFreezeTable . snd) tables

unsafeFreezeTable :: PrimMonad m => MTable (PrimState m) -> m Table
unsafeFreezeTable (MTable nref cols) = do
  Table
    <$> Ref.readRef nref
    <*> traverse unsafeFreezeColumn cols

unsafeFreezeColumn :: PrimMonad m => MColumn (PrimState m) -> m Column
unsafeFreezeColumn = \case
  MByteColumn g ->
    ByteColumn . unsafeToByteString <$> Grow.unsafeFreeze g
  MIntColumn g ->
    IntColumn <$> Grow.unsafeFreeze g
  MDoubleColumn g ->
    DoubleColumn <$> Grow.unsafeFreeze g
  MArrayColumn g mtable -> do
    ns <- Grow.unsafeFreeze g
    table <- unsafeFreezeTable mtable
    pure $ ArrayColumn ns table

thawTable :: PrimMonad m => Table -> m (MTable (PrimState m))
thawTable (Table n columns) =
  MTable
    <$> Ref.newRef n
    <*> Boxed.mapM thawColumn columns


thawColumn :: PrimMonad m => Column -> m (MColumn (PrimState m))
thawColumn = \case
  ByteColumn bs -> do
    MByteColumn <$> growOfVector (unsafeFromByteString bs)
  IntColumn vs ->
    MIntColumn <$> growOfVector vs
  DoubleColumn vs ->
    MDoubleColumn <$> growOfVector vs
  ArrayColumn vs rec -> do
    ns <- growOfVector vs
    table <- thawTable rec
    pure $ MArrayColumn ns table
 where
  growOfVector vv = do
    g <- Grow.new (Storable.length vv)
    Grow.append g vv
    return g

------------------------------------------------------------------------

appendTable :: PrimMonad m => MTable (PrimState m) -> Table -> EitherT MutableError m ()
appendTable (MTable nref mfs) (Table n fs) = do
  Ref.modifyRef nref (+ n)
  Boxed.zipWithM_ appendColumn mfs fs

appendColumn :: PrimMonad m => MColumn (PrimState m) -> Column -> EitherT MutableError m ()
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
  xs <- traverse newMTable schemas
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
  Either MutableError (Boxed.Vector Table)
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
    withMTable table $ do
      insertDefault schema
      MTable nref _ <- get
      Ref.modifyRef nref (+ 1)
  Just' value -> do
    withMTable table $ do
      insertValue schema value
      MTable nref _ <- get
      Ref.modifyRef nref (+ 1)

insertValue ::
  PrimMonad m =>
  Schema ->
  Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertValue schema value =
  case schema of
    BoolSchema ->
      case value of
        BoolValue False ->
          insertInt 0
        BoolValue True ->
          insertInt 1
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    Int64Schema ->
      case value of
        Int64Value x ->
          insertInt $ fromIntegral x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    DoubleSchema ->
      case value of
        DoubleValue x ->
          insertDouble x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    StringSchema ->
      case value of
        StringValue x ->
          insertString $ T.encodeUtf8 x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    DateSchema ->
      case value of
        DateValue x ->
          insertInt . fromIntegral $ fromDay x
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    ListSchema ischema ->
      case value of
        ListValue xs ->
          withArray $ \ns -> do
            let
              n =
                fromIntegral $ Boxed.length xs

              insertElem x = do
                table <- get
                insertValue ischema x
                MTable _ columns <- get
                if Boxed.null columns then do
                  put table
                else
                  lift . left . MutableLeftoverColumns . Boxed.toList $
                    fmap describeColumn columns

            Grow.add ns n
            traverse_ insertElem xs

            MTable nref _ <- get
            Ref.modifyRef nref (+ Boxed.length xs)

            -- TODO wtf?
            nref0 <- Ref.newRef 0
            put $ MTable nref0 Boxed.empty

        _ ->
          lift . left $ MutableSchemaMismatch value schema

    StructSchema fields ->
      case value of
        StructValue values ->
          insertStruct fields values
        _ ->
          lift . left $ MutableSchemaMismatch value schema

    EnumSchema variant0 variants ->
      case value of
        EnumValue tag x -> do
          insertInt $ fromIntegral tag
          (vs0, v1, vs2) <-
            lift . hoistMaybe (MutableEnumVariantMismatch tag x $ Boxed.cons variant0 variants) $
              focus tag variant0 variants

          traverse_ (insertDefault . variantSchema) vs0
          insertValue (variantSchema v1) x
          traverse_ (insertDefault . variantSchema) vs2

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
  Boxed.Vector FieldSchema ->
  Boxed.Vector Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertStruct fields values =
  if Boxed.length fields /= Boxed.length values then
    lift . left $ MutableStructFieldsMismatch values fields
  else
    Boxed.zipWithM_ insertValue (fmap fieldSchema fields) values

------------------------------------------------------------------------

takeNext ::
  PrimMonad m =>
  StateT (MTable (PrimState m)) (EitherT MutableError m) (MColumn (PrimState m))
takeNext = do
  MTable nref xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . MTable nref $ Boxed.drop 1 xs
      pure x
    Nothing ->
      lift $ left MutableNoMoreColumns

takeByte ::
  PrimMonad m =>
  StateT
    (MTable (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Word8)
takeByte =
  takeNext >>= \case
    MByteColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedByteColumn $ describeColumn x

takeInt ::
  PrimMonad m =>
  StateT
    (MTable (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Int64)
takeInt =
  takeNext >>= \case
    MIntColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedIntColumn $ describeColumn x

takeDouble ::
  PrimMonad m =>
  StateT
    (MTable (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Double)
takeDouble =
  takeNext >>= \case
    MDoubleColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedDoubleColumn $ describeColumn x

withMTable ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) a ->
  EitherT MutableError m a
withMTable table0 m = do
  (result, MTable _ xs) <- runStateT m table0
  if Boxed.null xs then
    pure result
  else
    left . MutableLeftoverColumns . Boxed.toList $ fmap describeColumn xs

withArray ::
  PrimMonad m =>
  (Grow MStorable.MVector (PrimState m) Int64 ->
    StateT (MTable (PrimState m)) (EitherT MutableError m) a) ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) a
withArray f =
  takeNext >>= \case
    MArrayColumn ns table0 ->
      lift . withMTable table0 $ do
        x <- f ns

        -- TODO this makes me sad
        n <- fromIntegral . Storable.sum <$> Grow.unsafeFreeze ns
        MTable nref _ <- get
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
  MArrayColumn _ (MTable _ xs) ->
    FoundMArrayColumn . Boxed.toList $ fmap describeColumn xs

------------------------------------------------------------------------

insertInt ::
  PrimMonad m =>
  Int64 ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertInt w = do
  xs <- takeInt
  Grow.add xs w

insertDouble ::
  PrimMonad m =>
  Double ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDouble d = do
  xs <- takeDouble
  Grow.add xs d

insertString ::
  PrimMonad m =>
  ByteString ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertString bs = do
  withArray $ \ns -> do
    Grow.add ns (fromIntegral $ B.length bs)
    xs <- takeByte
    Grow.append xs (unsafeFromByteString bs)

insertDefault ::
  PrimMonad m =>
  Schema ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDefault = \case
  BoolSchema ->
    insertInt 0
  Int64Schema ->
    insertInt 0
  DoubleSchema ->
    insertDouble 0
  StringSchema ->
    insertString B.empty
  DateSchema ->
    insertInt 0
  ListSchema _ ->
    withArray $ \ns -> do
      Grow.add ns 0

      -- TODO wtf?
      nref <- Ref.newRef 0
      put $ MTable nref Boxed.empty
  StructSchema fields ->
    traverse_ (insertDefault . fieldSchema) fields
  EnumSchema variant0 variants -> do
    insertInt 0
    traverse_ (insertDefault . variantSchema) (Boxed.cons variant0 variants)

------------------------------------------------------------------------

newMByteColumn :: PrimMonad m => m (MTable (PrimState m))
newMByteColumn =
  MTable
    <$> Ref.newRef 0
    <*> (Boxed.singleton . MByteColumn <$> Grow.new 4)

newMIntColumn :: PrimMonad m => m (MTable (PrimState m))
newMIntColumn =
  MTable
    <$> Ref.newRef 0
    <*> (Boxed.singleton . MIntColumn <$> Grow.new 4)

newMDoubleColumn :: PrimMonad m => m (MTable (PrimState m))
newMDoubleColumn =
  MTable
    <$> Ref.newRef 0
    <*> (Boxed.singleton . MDoubleColumn <$> Grow.new 4)

newMArrayColumn :: PrimMonad m => MTable (PrimState m) -> m (MTable (PrimState m))
newMArrayColumn vs =
  MTable
    <$> Ref.newRef 0
    <*> (Boxed.singleton . flip MArrayColumn vs <$> Grow.new 4)

newMTable :: PrimMonad m => Schema -> m (MTable (PrimState m))
newMTable = \case
  BoolSchema ->
    newMIntColumn
  Int64Schema ->
    newMIntColumn
  DoubleSchema ->
    newMDoubleColumn
  StringSchema ->
    newMArrayColumn =<< newMByteColumn
  DateSchema ->
    newMIntColumn
  ListSchema schema ->
    newMArrayColumn =<< newMTable schema
  StructSchema fields ->
    MTable
      <$> Ref.newRef 0
      <*> (Boxed.concatMap mtableColumns <$> traverse (newMTable . fieldSchema) fields)
  EnumSchema variant0 variants -> do
    tag <- newMIntColumn
    vtables <- traverse (newMTable . variantSchema) (Boxed.cons variant0 variants)
    MTable
      <$> Ref.newRef 0
      <*> pure (Boxed.concatMap mtableColumns $ Boxed.cons tag vtables)

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
