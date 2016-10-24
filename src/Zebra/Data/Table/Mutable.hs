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
import           Zebra.Data.Encoding
import           Zebra.Data.Fact
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
  | MutableEncodingMismatch !Encoding !Value
  | MutableStructFieldsMismatch !(Boxed.Vector FieldEncoding) !(Boxed.Vector (Maybe' Value))
  | MutableRequiredFieldMissing !Encoding
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
  MutableEncodingMismatch encoding value ->
    "Encoding did not match value:" <>
    "\n" <>
    "\n  encoding =" <>
    "\n    " <> T.pack (show encoding) <>
    "\n" <>
    "\n  value =" <>
    "\n    " <> T.pack (show value)
  MutableStructFieldsMismatch fields values ->
    "Struct field encodings did not match their values:" <>
    "\n" <>
    "\n  field encodings =" <>
    "\n    " <> T.pack (show fields) <>
    "\n" <>
    "\n  fields values =" <>
    "\n    " <> T.pack (show values)
  MutableRequiredFieldMissing encoding ->
    "Struct required field missing: " <> T.pack (show encoding)
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
  MTableBox (Boxed.Vector (Encoding, MTable s))

mkMTableBox :: PrimMonad m => Boxed.Vector Encoding -> m (MTableBox (PrimState m))
mkMTableBox encodings = do
  xs <- traverse newMTable encodings
  pure . MTableBox $ Boxed.zip encodings xs

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
    Just (encoding, table) ->
      insertMaybeValue encoding table mvalue

------------------------------------------------------------------------

tablesOfFacts ::
  Boxed.Vector Encoding ->
  Boxed.Vector Fact ->
  Either MutableError (Boxed.Vector Table)
tablesOfFacts encodings facts =
  runST $ runEitherT $ do
    box <- lift $ mkMTableBox encodings

    for_ facts $ \fact ->
      insertFact box (factAttributeId fact) (factValue fact)

    lift $ unsafeFreezeTables box

insertMaybeValue ::
  PrimMonad m =>
  Encoding ->
  MTable (PrimState m) ->
  Maybe' Value ->
  EitherT MutableError m ()
insertMaybeValue encoding table = \case
  Nothing' -> do
    withMTable table $ insertDefault encoding
  Just' value -> do
    withMTable table $ insertValue encoding value

insertValue ::
  PrimMonad m =>
  Encoding ->
  Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertValue encoding =
  case encoding of
    BoolEncoding -> \case
      BoolValue False ->
        insertInt 0
      BoolValue True ->
        insertInt 1
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    Int64Encoding -> \case
      Int64Value x ->
        insertInt $ fromIntegral x
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    DoubleEncoding -> \case
      DoubleValue x ->
        insertDouble x
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    StringEncoding -> \case
      StringValue x ->
        insertString $ T.encodeUtf8 x
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    DateEncoding -> \case
      DateValue x ->
        insertInt . fromIntegral $ fromDay x
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    StructEncoding fields -> \case
      StructValue values ->
        insertStruct (fmap snd fields) values
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    ListEncoding iencoding -> \case
      ListValue xs ->
        withArray $ \ns -> do
          let
            n =
              fromIntegral $ Boxed.length xs

            insertElem x = do
              table <- get
              insertValue iencoding x
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
        lift . left $ MutableEncodingMismatch encoding value

insertStruct ::
  PrimMonad m =>
  Boxed.Vector FieldEncoding ->
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
  FieldEncoding ->
  Maybe' Value ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertField fencoding mvalue =
  case fencoding of
    FieldEncoding RequiredField encoding ->
      case mvalue of
        Nothing' ->
          lift . left $ MutableRequiredFieldMissing encoding
        Just' value ->
          insertValue encoding value

    FieldEncoding OptionalField encoding ->
      case mvalue of
        Nothing' -> do
          insertInt 0
          insertDefault encoding
        Just' value -> do
          insertInt 1
          insertValue encoding value

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
  FieldEncoding ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDefaultColumn = \case
  FieldEncoding RequiredField encoding ->
    insertDefault encoding
  FieldEncoding OptionalField encoding -> do
    insertInt 0
    insertDefault encoding

insertDefault ::
  PrimMonad m =>
  Encoding ->
  StateT (MTable (PrimState m)) (EitherT MutableError m) ()
insertDefault = \case
  BoolEncoding ->
    insertInt 0
  Int64Encoding ->
    insertInt 0
  DoubleEncoding ->
    insertDouble 0
  StringEncoding ->
    insertString B.empty
  DateEncoding ->
    insertInt 0
  StructEncoding fields ->
    if Boxed.null fields then
      insertInt 0
    else
      traverse_ (insertDefaultColumn . snd) fields
  ListEncoding _ ->
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

newMStructColumn :: PrimMonad m => FieldEncoding -> m (MTable (PrimState m))
newMStructColumn = \case
  FieldEncoding RequiredField encoding ->
    newMTable encoding
  FieldEncoding OptionalField encoding -> do
    b <- newMIntColumn
    x <- newMTable encoding
    pure . MTable $
      mtableColumns b <>
      mtableColumns x

newMTable :: PrimMonad m => Encoding -> m (MTable (PrimState m))
newMTable = \case
  BoolEncoding ->
    newMIntColumn
  Int64Encoding ->
    newMIntColumn
  DoubleEncoding ->
    newMDoubleColumn
  StringEncoding ->
    newMArrayColumn =<< newMByteColumn
  DateEncoding ->
    newMIntColumn
  StructEncoding fields ->
    if Boxed.null fields then
      newMIntColumn
    else
      fmap (MTable . Boxed.concatMap mtableColumns) $
      traverse (newMStructColumn . snd) fields
  ListEncoding encoding ->
    newMArrayColumn =<< newMTable encoding

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
