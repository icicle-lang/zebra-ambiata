{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Record (
    Record(..)
  , Field(..)

  , RecordError(..)

  , lengthOfRecord
  , schemaOfRecord
  , schemaOfField

  , recordsOfFacts
  , recordOfMaybeValue
  , recordOfValue
  , recordOfStruct
  , recordOfField
  ) where

import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.ST (runST)
import           Control.Monad.Trans (lift)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Coerce (coerce)
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import qualified Data.Vector.Mutable as MBoxed
import           Data.Word (Word64)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, hoistEither)
import qualified X.Data.Vector as Boxed
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
  Schema . fmap schemaOfField . Boxed.toList . recordFields

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
  mv <- Boxed.thaw $ fmap defaultOfEncoding encodings
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

emptyList :: Record -> Record
emptyList vs =
  Record . Boxed.singleton $ ListField (Storable.singleton 0) vs

emptyOfEncoding :: Encoding -> Record
emptyOfEncoding = \case
  BoolEncoding ->
    emptyWord
  Int64Encoding ->
    emptyWord
  DoubleEncoding ->
    emptyWord
  StringEncoding ->
    emptyList emptyByte
  DateEncoding ->
    emptyWord
  StructEncoding fields ->
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

defaultOfEncoding :: Encoding -> Record
defaultOfEncoding = \case
  BoolEncoding ->
    singletonWord 0
  Int64Encoding ->
    singletonWord 0
  DoubleEncoding ->
    singletonWord 0
  StringEncoding ->
    emptyList emptyByte
  DateEncoding ->
    singletonWord 0
  StructEncoding fields ->
    Record $ Boxed.concatMap (recordFields . defaultOfFieldEncoding . snd) fields
  ListEncoding encoding ->
    emptyList $ emptyOfEncoding encoding

defaultOfFieldEncoding :: FieldEncoding -> Record
defaultOfFieldEncoding = \case
  FieldEncoding RequiredField encoding ->
    defaultOfEncoding encoding
  FieldEncoding OptionalField encoding ->
    Record $ recordFields (singletonWord 0) <> recordFields (defaultOfEncoding encoding)
