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
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, left)
import           X.Data.Vector.Grow (Grow)
import qualified X.Data.Vector.Grow as Grow

import           Zebra.Data.Core
import           Zebra.Data.Fact
import           Zebra.Data.Schema
import           Zebra.Data.Table (Table(..), Column(..))


newtype MTable s =
  MTable {
      mtableColumns :: Boxed.Vector (MColumn s)
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
  | MutableSchemaMismatch !Schema !Value
  | MutableStructFieldsMismatch !(Boxed.Vector FieldSchema) !(Boxed.Vector (Maybe' Value))
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
  MutableSchemaMismatch schema value ->
    "Schema did not match value:" <>
    "\n" <>
    "\n  schema =" <>
    "\n    " <> T.pack (show schema) <>
    "\n" <>
    "\n  value =" <>
    "\n    " <> T.pack (show value)
  MutableStructFieldsMismatch fields values ->
    "Struct field schemas did not match their values:" <>
    "\n" <>
    "\n  field schemas =" <>
    "\n    " <> T.pack (show fields) <>
    "\n" <>
    "\n  fields values =" <>
    "\n    " <> T.pack (show values)
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
unsafeFreezeTable =
  fmap Table . traverse unsafeFreezeColumn . mtableColumns

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
thawTable (Table columns) =
  MTable <$> Boxed.mapM thawColumn columns


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
appendTable (MTable mfs) (Table fs) =
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
    withMTable table $ insertDefault schema
  Just' value -> do
    withMTable table $ insertValue schema value

insertValue ::
  PrimMonad m =>
  Schema ->
  Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertValue schema =
  case schema of
    BoolSchema -> \case
      BoolValue False ->
        insertInt 0
      BoolValue True ->
        insertInt 1
      value ->
        lift . left $ MutableSchemaMismatch schema value

    Int64Schema -> \case
      Int64Value x ->
        insertInt $ fromIntegral x
      value ->
        lift . left $ MutableSchemaMismatch schema value

    DoubleSchema -> \case
      DoubleValue x ->
        insertDouble x
      value ->
        lift . left $ MutableSchemaMismatch schema value

    StringSchema -> \case
      StringValue x ->
        insertString $ T.encodeUtf8 x
      value ->
        lift . left $ MutableSchemaMismatch schema value

    DateSchema -> \case
      DateValue x ->
        insertInt . fromIntegral $ fromDay x
      value ->
        lift . left $ MutableSchemaMismatch schema value

    StructSchema fields -> \case
      StructValue values ->
        insertStruct (fmap snd fields) values
      value ->
        lift . left $ MutableSchemaMismatch schema value

    ListSchema ischema -> \case
      ListValue xs ->
        withArray $ \ns -> do
          let
            n =
              fromIntegral $ Boxed.length xs

            insertElem x = do
              table <- get
              insertValue ischema x
              MTable columns <- get
              if Boxed.null columns then
                put table
              else
                lift . left . MutableLeftoverColumns . Boxed.toList $
                  fmap describeColumn columns

          Grow.add ns n
          traverse_ insertElem xs

          put $ MTable Boxed.empty

      value ->
        lift . left $ MutableSchemaMismatch schema value

insertStruct ::
  PrimMonad m =>
  Boxed.Vector FieldSchema ->
  Boxed.Vector (Maybe' Value) ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertStruct fields values =
  if Boxed.null fields then
    insertInt 0
  else if Boxed.length fields /= Boxed.length values then
    lift . left $ MutableStructFieldsMismatch fields values
  else
    Boxed.zipWithM_ insertField fields values

insertField ::
  PrimMonad m =>
  FieldSchema ->
  Maybe' Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertField fschema mvalue =
  case fschema of
    FieldSchema RequiredField schema ->
      case mvalue of
        Nothing' ->
          lift . left $ MutableRequiredFieldMissing schema
        Just' value ->
          insertValue schema value

    FieldSchema OptionalField schema ->
      case mvalue of
        Nothing' -> do
          insertInt 0
          insertDefault schema
        Just' value -> do
          insertInt 1
          insertValue schema value

------------------------------------------------------------------------

takeNext ::
  PrimMonad m =>
  StateT (MTable (PrimState m)) (EitherT MutableError m) (MColumn (PrimState m))
takeNext = do
  MTable xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . MTable $ Boxed.drop 1 xs
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
  (result, MTable xs) <- runStateT m table0
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
      lift . withMTable table0 $ f ns
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
  MArrayColumn _ (MTable xs) ->
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

insertDefaultColumn ::
  PrimMonad m =>
  FieldSchema ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDefaultColumn = \case
  FieldSchema RequiredField schema ->
    insertDefault schema
  FieldSchema OptionalField schema -> do
    insertInt 0
    insertDefault schema

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
  StructSchema fields ->
    if Boxed.null fields then
      insertInt 0
    else
      traverse_ (insertDefaultColumn . snd) fields
  ListSchema _ ->
    withArray $ \ns -> do
      Grow.add ns 0
      put $ MTable Boxed.empty

------------------------------------------------------------------------

newMByteColumn :: PrimMonad m => m (MTable (PrimState m))
newMByteColumn =
  MTable . Boxed.singleton . MByteColumn <$> Grow.new 4

newMIntColumn :: PrimMonad m => m (MTable (PrimState m))
newMIntColumn =
  MTable . Boxed.singleton . MIntColumn <$> Grow.new 4

newMDoubleColumn :: PrimMonad m => m (MTable (PrimState m))
newMDoubleColumn =
  MTable . Boxed.singleton . MDoubleColumn <$> Grow.new 4

newMArrayColumn :: PrimMonad m => MTable (PrimState m) -> m (MTable (PrimState m))
newMArrayColumn vs =
  MTable . Boxed.singleton . flip MArrayColumn vs <$> Grow.new 4

newMStructColumn :: PrimMonad m => FieldSchema -> m (MTable (PrimState m))
newMStructColumn = \case
  FieldSchema RequiredField schema ->
    newMTable schema
  FieldSchema OptionalField schema -> do
    b <- newMIntColumn
    x <- newMTable schema
    pure . MTable $
      mtableColumns b <>
      mtableColumns x

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
  StructSchema fields ->
    if Boxed.null fields then
      newMIntColumn
    else
      fmap (MTable . Boxed.concatMap mtableColumns) $
      traverse (newMStructColumn . snd) fields
  ListSchema schema ->
    newMArrayColumn =<< newMTable schema

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
