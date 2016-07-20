{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Record (
    Record(..)
  , Field(..)

  , RecordError(..)
  , ValueError(..)

  , lengthOfRecord
  , schemaOfRecord
  , schemaOfField

  , recordsOfFacts
  , recordOfMaybeValue
  , recordOfValue
  , recordOfStruct
  , recordOfField

  , valuesOfRecord
  ) where

import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.ST (runST)
import           Control.Monad.State.Strict (MonadState(..))
import           Control.Monad.Trans (lift)
import           Control.Monad.Trans.State.Strict (State, runState)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Coerce (coerce)
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import qualified Data.Vector.Mutable as MBoxed
import           Data.Word (Word64)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, hoistEither, left)
import qualified X.Data.ByteString.Unsafe as B
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Storable as Storable
import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Encoding
import           Zebra.Data.Fact
import           Zebra.Data.Schema


newtype Record =
  Record {
      recordFields :: Boxed.Vector Field
    } deriving (Eq, Ord, Generic, Typeable)

data Field =
    ByteField !ByteString
  | WordField !(Storable.Vector Word64)
  | DoubleField !(Storable.Vector Double)
  | ListField !(Storable.Vector Word64) !Record
    deriving (Eq, Ord, Generic, Typeable)

data RecordError =
    RecordEncodingMismatch !Encoding !Value
  | RecordRequiredFieldMissing !Encoding
  | RecordCannotConcatEmpty
  | RecordAppendFieldsMismatch !Field !Field
  | RecordStructFieldsMismatch !(Boxed.Vector FieldEncoding) !(Boxed.Vector (Maybe' Value))
    deriving (Eq, Ord, Show, Generic, Typeable)

data ValueError =
    ValueExpectedByteField !Field
  | ValueExpectedWordField !Field
  | ValueExpectedDoubleField !Field
  | ValueExpectedListField !Field
  | ValueStringLengthMismatch !Int !Int
  | ValueListLengthMismatch !Int !Int
  | ValueNoMoreFields
  | ValueLeftoverFields !Schema
    deriving (Eq, Ord, Show, Generic, Typeable)

instance Show Record where
  showsPrec =
    gshowsPrec

instance Show Field where
  showsPrec =
    gshowsPrec

recordsOfFacts :: Boxed.Vector Encoding -> Boxed.Vector Fact -> Either RecordError (Boxed.Vector Record)
recordsOfFacts encodings facts =
  runST $ runEitherT $ do
    sm <- lift $ mkRecordBox encodings

    for_ facts $ \fact ->
      case encodings Boxed.!? (unAttributeId $ factAttributeId fact) of
        Nothing ->
          pure ()
        Just encoding -> do
          s <- hoistEither . recordOfMaybeValue encoding $ factValue fact
          insertRecord sm (factAttributeId fact) s

    lift $ freezeRecords sm

recordOfMaybeValue :: Encoding -> Maybe' Value -> Either RecordError Record
recordOfMaybeValue encoding = \case
  Nothing' ->
    pure $ defaultOfEncoding encoding
  Just' value ->
    recordOfValue encoding value

recordOfValue :: Encoding -> Value -> Either RecordError Record
recordOfValue encoding =
  case encoding of
    BoolEncoding -> \case
      BoolValue False ->
        pure $ singletonWord 0
      BoolValue True ->
        pure $ singletonWord 1
      value ->
        Left $ RecordEncodingMismatch encoding value

    Int64Encoding -> \case
      Int64Value x ->
        pure . singletonWord $ fromIntegral x
      value ->
        Left $ RecordEncodingMismatch encoding value

    DoubleEncoding -> \case
      DoubleValue x ->
        pure $ singletonDouble x
      value ->
        Left $ RecordEncodingMismatch encoding value

    StringEncoding -> \case
      StringValue x ->
        pure . singletonString $ T.encodeUtf8 x
      value ->
        Left $ RecordEncodingMismatch encoding value

    DateEncoding -> \case
      DateValue x ->
        pure . singletonWord . fromIntegral $ fromDay x
      value ->
        Left $ RecordEncodingMismatch encoding value

    StructEncoding fields -> \case
      StructValue values ->
        recordOfStruct (fmap snd fields) values
      value ->
        Left $ RecordEncodingMismatch encoding value

    ListEncoding iencoding -> \case
      ListValue xs -> do
        vs0 <- traverse (recordOfValue iencoding) xs
        vs1 <- concatRecords vs0
        pure . Record . Boxed.singleton $
          ListField (Storable.singleton . fromIntegral $ Boxed.length xs) vs1
      value ->
        Left $ RecordEncodingMismatch encoding value

recordOfStruct :: Boxed.Vector FieldEncoding -> Boxed.Vector (Maybe' Value) -> Either RecordError Record
recordOfStruct fields values =
  if Boxed.null fields then
    pure $ singletonWord 0
  else if Boxed.length fields /= Boxed.length values then
    Left $ RecordStructFieldsMismatch fields values
  else
    fmap (Record . Boxed.concatMap recordFields) $
    Boxed.zipWithM recordOfField fields values

recordOfField :: FieldEncoding -> Maybe' Value -> Either RecordError Record
recordOfField fencoding mvalue =
  case fencoding of
    FieldEncoding RequiredField encoding ->
      case mvalue of
        Nothing' ->
          Left $ RecordRequiredFieldMissing encoding

        Just' value ->
          recordOfValue encoding value

    FieldEncoding OptionalField encoding ->
      case mvalue of
        Nothing' ->
          pure . Record $
            recordFields (singletonWord 0) <>
            recordFields (defaultOfEncoding encoding)

        Just' value -> do
          struct <- recordOfValue encoding value
          pure . Record $
            recordFields (singletonWord 1) <>
            recordFields struct

------------------------------------------------------------------------

valuesOfRecord :: Encoding -> Record -> Either ValueError (Boxed.Vector Value)
valuesOfRecord encoding record0 =
  withRecord record0 $ takeValue encoding

withRecord :: Record -> EitherT ValueError (State Record) a -> Either ValueError a
withRecord record0 m =
  let
    (result, record) =
      runState (runEitherT m) record0
  in
    if Boxed.null $ recordFields record then
      result
    else
      Left . ValueLeftoverFields $ schemaOfRecord record

takeNext :: EitherT ValueError (State Record) Field
takeNext = do
  Record xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . Record $ Boxed.drop 1 xs
      pure x
    Nothing ->
      left ValueNoMoreFields

takeByte :: EitherT ValueError (State Record) ByteString
takeByte =
  takeNext >>= \case
    ByteField xs ->
      pure xs
    x ->
      left $ ValueExpectedByteField x

takeWord :: EitherT ValueError (State Record) (Storable.Vector Word64)
takeWord =
  takeNext >>= \case
    WordField xs ->
      pure xs
    x ->
      left $ ValueExpectedWordField x

takeDouble :: EitherT ValueError (State Record) (Storable.Vector Double)
takeDouble =
  takeNext >>= \case
    DoubleField xs ->
      pure xs
    x ->
      left $ ValueExpectedDoubleField x

takeList ::
  (Storable.Vector Word64 -> EitherT ValueError (State Record) a) ->
  EitherT ValueError (State Record) a
takeList f =
  takeNext >>= \case
    ListField ns record0 ->
      hoistEither $ withRecord record0 $ f ns
    x ->
      left $ ValueExpectedListField x

takeBool :: EitherT ValueError (State Record) (Boxed.Vector Bool)
takeBool =
  fmap (fmap (/= 0) . Boxed.convert) $ takeWord

takeValue :: Encoding -> EitherT ValueError (State Record) (Boxed.Vector Value)
takeValue = \case
  BoolEncoding ->
    fmap (fmap BoolValue) takeBool

  Int64Encoding ->
    fmap (fmap (Int64Value . fromIntegral) . Boxed.convert) takeWord

  DoubleEncoding ->
    fmap (fmap DoubleValue . Boxed.convert) takeDouble

  StringEncoding ->
    takeList $ \ns -> do
      bs <- takeByte
      fmap (fmap $ StringValue . T.decodeUtf8) . hoistEither $ restring ns bs

  DateEncoding ->
    fmap (fmap (DateValue . toDay . fromIntegral) . Boxed.convert) takeWord

  StructEncoding fields ->
    if Boxed.null fields then do
      xs <- takeWord
      pure $ Boxed.replicate (Storable.length xs) (StructValue Boxed.empty)
    else do
      xss <- traverse (takeField . snd) fields
      pure . fmap StructValue $ Boxed.transpose xss

  ListEncoding encoding ->
    takeList $ \ns -> do
      xs <- takeValue encoding
      fmap (fmap ListValue) . hoistEither $ relist ns xs

takeField :: FieldEncoding -> EitherT ValueError (State Record) (Boxed.Vector (Maybe' Value))
takeField fencoding =
  case fencoding of
    FieldEncoding RequiredField encoding ->
      fmap (fmap Just') $ takeValue encoding

    FieldEncoding OptionalField encoding -> do
      bs <- takeBool
      xs <- takeValue encoding
      pure $ Boxed.zipWith remaybe bs xs

remaybe :: Bool -> a -> Maybe' a
remaybe b x =
  if b then
    Just' x
  else
    Nothing'

restring :: Storable.Vector Word64 -> ByteString -> Either ValueError (Boxed.Vector ByteString)
restring ns bs =
  let
    !n =
      fromIntegral $ Storable.sum ns

    !m =
      B.length bs
  in
    if n /= m then
      Left $ ValueStringLengthMismatch n m
    else
      pure . B.unsafeSplits id bs $ Storable.map fromIntegral ns

relist :: Storable.Vector Word64 -> Boxed.Vector a -> Either ValueError (Boxed.Vector (Boxed.Vector a))
relist ns xs =
  let
    !n =
      fromIntegral $ Storable.sum ns

    !m =
      Boxed.length xs
  in
    if n /= m then
      Left $ ValueListLengthMismatch n m
    else
      pure . Generic.unsafeSplits id xs $ Storable.map fromIntegral ns

------------------------------------------------------------------------

lengthOfRecord :: Record -> Int
lengthOfRecord (Record fields) =
  case fields Boxed.!? 0 of
    Nothing ->
      0
    Just (ByteField xs) ->
      B.length xs
    Just (WordField xs) ->
      Storable.length xs
    Just (DoubleField xs) ->
      Storable.length xs
    Just (ListField xs _) ->
      Storable.length xs

schemaOfRecord :: Record -> Schema
schemaOfRecord =
  schemaOfFields . Boxed.toList . recordFields

schemaOfFields :: [Field] -> Schema
schemaOfFields =
  Schema . fmap schemaOfField

schemaOfField :: Field -> Format
schemaOfField = \case
  ByteField _ ->
    ByteFormat
  WordField _ ->
    WordFormat
  DoubleField _ ->
    DoubleFormat
  ListField _ record ->
    ListFormat $ schemaOfRecord record

concatRecords :: Boxed.Vector Record -> Either RecordError Record
concatRecords xss0 =
  let
    xss :: Boxed.Vector (Boxed.Vector Field)
    xss =
      coerce xss0

    yss =
      Boxed.transpose xss
  in
    fmap Record $
    traverse concatFields yss

appendRecords :: Record -> Record -> Either RecordError Record
appendRecords (Record xs) (Record ys) =
  Record <$> Boxed.zipWithM appendFields xs ys

concatFields :: Boxed.Vector Field -> Either RecordError Field
concatFields xs =
  if Boxed.null xs then
    Left RecordCannotConcatEmpty
  else
    Boxed.fold1M' appendFields xs

appendFields :: Field -> Field -> Either RecordError Field
appendFields x y =
  case (x, y) of
    (ByteField xs, ByteField ys) ->
      pure $ ByteField (xs <> ys)

    (WordField xs, WordField ys) ->
      pure $ WordField (xs <> ys)

    (DoubleField xs, DoubleField ys) ->
      pure $ DoubleField (xs <> ys)

    (ListField n xs, ListField m ys) ->
      ListField (n <> m) <$>
      concatRecords (Boxed.fromList [xs, ys])

    (_, _) ->
      Left $ RecordAppendFieldsMismatch x y

------------------------------------------------------------------------

newtype RecordBox s =
  RecordBox (MBoxed.MVector s Record)

mkRecordBox :: PrimMonad m => Boxed.Vector Encoding -> m (RecordBox (PrimState m))
mkRecordBox encodings = do
  mv <- Boxed.thaw $ fmap emptyOfEncoding encodings
  pure $ RecordBox mv

insertRecord :: PrimMonad m => RecordBox (PrimState m) -> AttributeId -> Record -> EitherT RecordError m ()
insertRecord (RecordBox mv) (AttributeId aid) new = do
  old <- MBoxed.read mv aid
  both <- hoistEither $ appendRecords old new
  MBoxed.write mv aid both

freezeRecords :: PrimMonad m => RecordBox (PrimState m) -> m (Boxed.Vector Record)
freezeRecords (RecordBox mv) =
  Boxed.freeze mv

------------------------------------------------------------------------

emptyByte :: Record
emptyByte =
  Record . Boxed.singleton $ ByteField B.empty

emptyWord :: Record
emptyWord =
  Record . Boxed.singleton $ WordField Storable.empty

emptyDouble :: Record
emptyDouble =
  Record . Boxed.singleton $ DoubleField Storable.empty

emptyList :: Record -> Record
emptyList vs =
  Record . Boxed.singleton $ ListField Storable.empty vs

emptyOfEncoding :: Encoding -> Record
emptyOfEncoding = \case
  BoolEncoding ->
    emptyWord
  Int64Encoding ->
    emptyWord
  DoubleEncoding ->
    emptyDouble
  StringEncoding ->
    emptyList emptyByte
  DateEncoding ->
    emptyWord
  StructEncoding fields ->
    if Boxed.null fields then
      emptyWord
    else
      Record $ Boxed.concatMap (recordFields . emptyOfFieldEncoding . snd) fields
  ListEncoding encoding ->
    emptyList $ emptyOfEncoding encoding

emptyOfFieldEncoding :: FieldEncoding -> Record
emptyOfFieldEncoding = \case
  FieldEncoding RequiredField encoding ->
    emptyOfEncoding encoding
  FieldEncoding OptionalField encoding ->
    Record $ recordFields emptyWord <> recordFields (emptyOfEncoding encoding)

singletonWord :: Word64 -> Record
singletonWord =
  Record . Boxed.singleton . WordField . Storable.singleton

singletonDouble :: Double -> Record
singletonDouble =
  Record . Boxed.singleton . DoubleField . Storable.singleton

singletonString :: ByteString -> Record
singletonString bs =
  Record .
  Boxed.singleton $
  ListField
    (Storable.singleton . fromIntegral $ B.length bs)
    (Record . Boxed.singleton $ ByteField bs)

singletonEmptyList :: Record -> Record
singletonEmptyList =
  Record . Boxed.singleton . ListField (Storable.singleton 1)

defaultOfEncoding :: Encoding -> Record
defaultOfEncoding = \case
  BoolEncoding ->
    singletonWord 0
  Int64Encoding ->
    singletonWord 0
  DoubleEncoding ->
    singletonDouble 0
  StringEncoding ->
    singletonString B.empty
  DateEncoding ->
    singletonWord 0
  StructEncoding fields ->
    if Boxed.null fields then
      singletonWord 0
    else
      Record $ Boxed.concatMap (recordFields . defaultOfFieldEncoding . snd) fields
  ListEncoding encoding ->
    singletonEmptyList $ defaultOfEncoding encoding

defaultOfFieldEncoding :: FieldEncoding -> Record
defaultOfFieldEncoding = \case
  FieldEncoding RequiredField encoding ->
    defaultOfEncoding encoding
  FieldEncoding OptionalField encoding ->
    Record $ recordFields (singletonWord 0) <> recordFields (defaultOfEncoding encoding)
