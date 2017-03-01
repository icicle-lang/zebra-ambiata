{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Table.Mutable (
    MTable(..)
  , MColumn(..)

  , MutableError(..)
  , renderMutableError

  , schema
  , rowCount
  , columns

  , new

  , insertRowOrDefault
  , insertRow
  , insertDefault

  , append

  , unsafeFreeze
  , thaw

  , fromFacts
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
import qualified Zebra.Data.Table as Table
import qualified Zebra.Data.Vector.Storable as Storable


data MTable s =
  MTable
    !Schema
    !(Ref MUnboxed.MVector s Int)
    !(Boxed.Vector (MColumn s))
  deriving (Generic, Typeable)

data MColumn s =
    MByteColumn !(Grow MStorable.MVector s Word8)
  | MIntColumn !(Grow MStorable.MVector s Int64)
  | MDoubleColumn !(Grow MStorable.MVector s Double)
  | MArrayColumn !(Grow MStorable.MVector s Int64) !(MTable s)
    deriving (Generic, Typeable)

newtype ColumnIndex =
  ColumnIndex {
      unColumnIndex :: Int
    } deriving (Eq, Ord, Show, Num)

schema :: MTable s -> Schema
schema (MTable x _ _) =
  x

rowCount :: PrimMonad m => MTable (PrimState m) -> m Int
rowCount (MTable _ x _) =
  Ref.readRef x

allocateRows :: PrimMonad m => MTable (PrimState m) -> Int -> m ()
allocateRows (MTable _ x _) n =
  Ref.modifyRef x (+ n)

columns :: MTable s -> Boxed.Vector (MColumn s)
columns (MTable _ _ x) =
  x

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
  MutableLeftoverColumns xs ->
    "Found more columns than expected in table: " <> renderFoundMColumns xs
  MutableSchemaMismatch value vschema ->
    "Schema did not match value:" <>
    "\n" <>
    "\n  schema =" <>
    "\n    " <> T.pack (show vschema) <>
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

unsafeFreeze :: PrimMonad m => MTable (PrimState m) -> m (Table Schema)
unsafeFreeze x = do
  Table
    <$> pure (schema x)
    <*> rowCount x
    <*> traverse unsafeFreezeColumn (columns x)

unsafeFreezeColumn :: PrimMonad m => MColumn (PrimState m) -> m (Column Schema)
unsafeFreezeColumn = \case
  MByteColumn g ->
    ByteColumn . Storable.unsafeToByteString <$> Grow.unsafeFreeze g
  MIntColumn g ->
    IntColumn <$> Grow.unsafeFreeze g
  MDoubleColumn g ->
    DoubleColumn <$> Grow.unsafeFreeze g
  MArrayColumn g mtable -> do
    ns <- Grow.unsafeFreeze g
    table <- unsafeFreeze mtable
    pure $ ArrayColumn ns table

thaw :: PrimMonad m => Table Schema -> m (MTable (PrimState m))
thaw x =
  MTable
    <$> pure (Table.annotation x)
    <*> Ref.newRef (Table.rowCount x)
    <*> Boxed.mapM thawColumn (Table.columns x)

thawColumn :: PrimMonad m => Column Schema -> m (MColumn (PrimState m))
thawColumn = \case
  ByteColumn bs -> do
    MByteColumn <$> growOfVector (Storable.unsafeFromByteString bs)
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

append :: PrimMonad m => MTable (PrimState m) -> Table a -> EitherT MutableError m ()
append mtable table = do
  allocateRows mtable $ Table.rowCount table
  Boxed.zipWithM_ appendColumn (columns mtable) (Table.columns table)

appendColumn :: PrimMonad m => MColumn (PrimState m) -> Column a -> EitherT MutableError m ()
appendColumn mf = \case
  ByteColumn bs
   | MByteColumn g <- mf
   -> Grow.append g (Storable.unsafeFromByteString bs)
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
          append mrec rec
   | otherwise
   -> left $ MutableExpectedArrayColumn $ describeColumn mf

------------------------------------------------------------------------

insertRowOrDefault ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Maybe' Value ->
  EitherT MutableError m ()
insertRowOrDefault table = \case
  Nothing' -> do
    consumeColumns table $ do
      insertDefault table (schema table)
      allocateRows table 1
  Just' value -> do
    consumeColumns table $ do
      insertRow table (schema table) value
      allocateRows table 1

insertRow ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Schema ->
  Value ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertRow table vschema value =
  case vschema of
    Schema.Bool ->
      case value of
        Bool x ->
          insertBool table x
        _ ->
          lift . left $ MutableSchemaMismatch value vschema

    Schema.Byte ->
      case value of
        Byte x ->
          insertByte table x
        _ ->
          lift . left $ MutableSchemaMismatch value vschema

    Schema.Int ->
      case value of
        Int x ->
          insertInt table x
        _ ->
          lift . left $ MutableSchemaMismatch value vschema

    Schema.Double ->
      case value of
        Double x ->
          insertDouble table x
        _ ->
          lift . left $ MutableSchemaMismatch value vschema

    Schema.Enum variant0 variants ->
      case value of
        Enum tag x -> do
          insertInt table $ fromIntegral tag
          (vs0, v1, vs2) <-
            lift . hoistMaybe (MutableEnumVariantMismatch tag x $ Boxed.cons variant0 variants) $
              Schema.focusVariant tag variant0 variants

          traverse_ (insertDefault table . variantSchema) vs0
          insertRow table (variantSchema v1) x
          traverse_ (insertDefault table . variantSchema) vs2

        _ ->
          lift . left $ MutableSchemaMismatch value vschema

    Schema.Struct fields ->
      case value of
        Struct values ->
          insertStruct table fields values
        _ ->
          lift . left $ MutableSchemaMismatch value vschema

    Schema.Array ischema ->
      case value of
        ByteArray xs ->
          case ischema of
            Schema.Byte ->
              insertByteArray table xs
            _ ->
              lift . left $ MutableSchemaMismatch value vschema

        Array xs ->
          popArrayColumn table $ \ns atable -> do
            traverse_ (consumeColumns atable . insertRow atable ischema) xs
            Grow.add ns . fromIntegral $ Boxed.length xs
            allocateRows atable $ Boxed.length xs

        _ ->
          lift . left $ MutableSchemaMismatch value vschema

insertStruct ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Boxed.Vector Field ->
  Boxed.Vector Value ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertStruct table fields values =
  if Boxed.length fields /= Boxed.length values then
    lift . left $ MutableStructFieldsMismatch values fields
  else
    Boxed.zipWithM_ (insertRow table) (fmap fieldSchema fields) values

------------------------------------------------------------------------

consumeColumns ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT ColumnIndex (EitherT MutableError m) a ->
  EitherT MutableError m a
consumeColumns table m = do
  (result, ColumnIndex index) <- runStateT m 0
  if index == Boxed.length (columns table) then
    pure result
  else
    left . MutableLeftoverColumns . Boxed.toList .
      fmap describeColumn . Boxed.drop index $ columns table

popColumn ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT ColumnIndex (EitherT MutableError m) (MColumn (PrimState m))
popColumn table = do
  index <- get
  case columns table Boxed.!? unColumnIndex index of
    Just x -> do
      put $ index + 1
      pure x
    Nothing ->
      lift $ left MutableNoMoreColumns

popByteColumn ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT ColumnIndex (EitherT MutableError m) (Grow MStorable.MVector (PrimState m) Word8)
popByteColumn table =
  popColumn table >>= \case
    MByteColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedByteColumn $ describeColumn x

popIntColumn ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT ColumnIndex (EitherT MutableError m) (Grow MStorable.MVector (PrimState m) Int64)
popIntColumn table =
  popColumn table >>= \case
    MIntColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedIntColumn $ describeColumn x

popDoubleColumn ::
  PrimMonad m =>
  MTable (PrimState m) ->
  StateT ColumnIndex (EitherT MutableError m) (Grow MStorable.MVector (PrimState m) Double)
popDoubleColumn table =
  popColumn table >>= \case
    MDoubleColumn xs ->
      pure xs
    x ->
      lift . left . MutableExpectedDoubleColumn $ describeColumn x

popArrayColumn ::
  PrimMonad m =>
  MTable (PrimState m) ->
  (Grow MStorable.MVector (PrimState m) Int64 -> MTable (PrimState m) -> EitherT MutableError m a) ->
  StateT ColumnIndex (EitherT MutableError m) a
popArrayColumn table update =
  popColumn table >>= \case
    MArrayColumn ns atable -> do
      lift $ update ns atable
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
  MTable (PrimState m) ->
  Bool ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertBool table x = do
  xs <- popIntColumn table
  Grow.add xs $ if x then 1 else 0

insertByte ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Word8 ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertByte table x = do
  xs <- popByteColumn table
  Grow.add xs x

insertInt ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Int64 ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertInt table x = do
  xs <- popIntColumn table
  Grow.add xs x

insertDouble ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Double ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertDouble table x = do
  xs <- popDoubleColumn table
  Grow.add xs x

insertByteArray ::
  PrimMonad m =>
  MTable (PrimState m) ->
  ByteString ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertByteArray table bs = do
  popArrayColumn table $ \ns atable ->
    consumeColumns atable $ do
      xs <- popByteColumn atable
      Grow.append xs $ Storable.unsafeFromByteString bs
      Grow.add ns . fromIntegral $ B.length bs
      allocateRows atable $ B.length bs

insertDefault ::
  PrimMonad m =>
  MTable (PrimState m) ->
  Schema ->
  StateT ColumnIndex (EitherT MutableError m) ()
insertDefault table = \case
  Schema.Bool ->
    insertBool table False
  Schema.Byte ->
    insertByte table 0
  Schema.Int ->
    insertInt table 0
  Schema.Double ->
    insertDouble table 0
  Schema.Struct fields ->
    traverse_ (insertDefault table . fieldSchema) fields
  Schema.Enum variant0 variants -> do
    insertInt table 0
    traverse_ (insertDefault table . variantSchema) (Boxed.cons variant0 variants)
  Schema.Array _schema ->
    popArrayColumn table $ \ns _atable -> do
      Grow.add ns 0

------------------------------------------------------------------------

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
    <*> pure (Boxed.concatMap columns $ Boxed.cons tag vtables)

newStruct :: PrimMonad m => Boxed.Vector Schema.Field -> m (MTable (PrimState m))
newStruct fields =
  MTable
    <$> pure (Schema.Struct fields)
    <*> Ref.newRef 0
    <*> (Boxed.concatMap columns <$> traverse (new . fieldSchema) fields)

newArray :: PrimMonad m => MTable (PrimState m) -> m (MTable (PrimState m))
newArray item =
  MTable
    <$> pure (Schema.Array $ schema item)
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
  Schema.Array ischema ->
    newArray =<< new ischema

------------------------------------------------------------------------

newtype MTableBox s =
  MTableBox (Boxed.Vector (MTable s))

mkMTableBox :: PrimMonad m => Boxed.Vector Schema -> m (MTableBox (PrimState m))
mkMTableBox schemas =
  MTableBox <$> traverse new schemas

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
    Just table ->
      insertRowOrDefault table mvalue

fromFacts ::
  Boxed.Vector Schema ->
  Boxed.Vector Fact ->
  Either MutableError (Boxed.Vector (Table Schema))
fromFacts schemas facts =
  runST $ runEitherT $ do
    box@(MTableBox tables) <- lift $ mkMTableBox schemas

    for_ facts $ \fact ->
      insertFact box (factAttributeId fact) (factValue fact)

    lift $ traverse unsafeFreeze tables
