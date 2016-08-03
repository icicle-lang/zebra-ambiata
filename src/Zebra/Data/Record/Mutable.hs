{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Record.Mutable (
    MRecord(..)
  , MField(..)

  , newMRecord
  , insertMaybeValue
  , insertValue
  , insertDefault
  , withMRecord
  , unsafeFreezeRecord
  ) where

import           Control.Monad.Primitive (PrimMonad(..))
import           Control.Monad.Trans (lift)
import           Control.Monad.Trans.State.Strict (StateT, runStateT, get, put)

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Storable.Mutable as MStorable
import           Data.Word (Word8, Word64)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, left)
import           X.Data.Vector.Grow (Grow)
import qualified X.Data.Vector.Grow as Grow

import           Zebra.Data.Record (Record(..), Field(..))
import           Zebra.Data.Encoding
import           Zebra.Data.Fact


newtype MRecord s =
  MRecord {
      mrecordFields :: Boxed.Vector (MField s)
    } deriving (Generic, Typeable)

data MField s =
    MByteField !(Grow MStorable.MVector s Word8)
  | MWordField !(Grow MStorable.MVector s Word64)
  | MDoubleField !(Grow MStorable.MVector s Double)
  | MListField !(Grow MStorable.MVector s Word64) !(MRecord s)
    deriving (Generic, Typeable)

data MutableError =
    MutableExpectedByteField !FoundMField
  | MutableExpectedWordField !FoundMField
  | MutableExpectedDoubleField !FoundMField
  | MutableExpectedListField !FoundMField
  | MutableNoMoreFields
  | MutableLeftoverFields ![FoundMField]
  | MutableEncodingMismatch !Encoding !Value
  | MutableStructFieldsMismatch !(Boxed.Vector FieldEncoding) !(Boxed.Vector (Maybe' Value))
  | MutableRequiredFieldMissing !Encoding
    deriving (Eq, Ord, Show)

data FoundMField =
    FoundMByteField
  | FoundMWordField
  | FoundMDoubleField
  | FoundMListField ![FoundMField]
    deriving (Eq, Ord, Show)

------------------------------------------------------------------------

unsafeFreezeRecord :: PrimMonad m => MRecord (PrimState m) -> m Record
unsafeFreezeRecord =
  fmap Record . traverse unsafeFreezeField . mrecordFields

unsafeFreezeField :: PrimMonad m => MField (PrimState m) -> m Field
unsafeFreezeField = \case
  MByteField g ->
    ByteField . unsafeToByteString <$> Grow.unsafeFreeze g
  MWordField g ->
    WordField <$> Grow.unsafeFreeze g
  MDoubleField g ->
    DoubleField <$> Grow.unsafeFreeze g
  MListField g mrecord -> do
    ns <- Grow.unsafeFreeze g
    record <- unsafeFreezeRecord mrecord
    pure $ ListField ns record

------------------------------------------------------------------------

insertMaybeValue ::
  PrimMonad m =>
  Encoding ->
  MRecord (PrimState m) ->
  Maybe' Value ->
  EitherT MutableError m ()
insertMaybeValue encoding record = \case
  Nothing' -> do
    withMRecord record $ insertDefault encoding
  Just' value -> do
    withMRecord record $ insertValue encoding value

insertValue ::
  PrimMonad m =>
  Encoding ->
  Value ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertValue encoding =
  case encoding of
    BoolEncoding -> \case
      BoolValue False ->
        insertWord 0
      BoolValue True ->
        insertWord 1
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    Int64Encoding -> \case
      Int64Value x ->
        insertWord $ fromIntegral x
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
        insertWord . fromIntegral $ fromDay x
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    StructEncoding fields -> \case
      StructValue values ->
        insertStruct (fmap snd fields) values
      value ->
        lift . left $ MutableEncodingMismatch encoding value

    ListEncoding iencoding -> \case
      ListValue xs ->
        withList $ \ns -> do
          let
            n =
              fromIntegral $ Boxed.length xs

            insertElem x = do
              record <- get
              insertValue iencoding x
              MRecord fields <- get
              if Boxed.null fields then
                put record
              else
                lift . left . MutableLeftoverFields . Boxed.toList $
                  fmap describeField fields

          Grow.add ns n
          traverse_ insertElem xs

          put $ MRecord Boxed.empty

      value ->
        lift . left $ MutableEncodingMismatch encoding value

insertStruct ::
  PrimMonad m =>
  Boxed.Vector FieldEncoding ->
  Boxed.Vector (Maybe' Value) ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertStruct fields values =
  if Boxed.null fields then
    insertWord 0
  else if Boxed.length fields /= Boxed.length values then
    lift . left $ MutableStructFieldsMismatch fields values
  else
    Boxed.zipWithM_ insertField fields values

insertField ::
  PrimMonad m =>
  FieldEncoding ->
  Maybe' Value ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
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
          insertWord 0
          insertDefault encoding
        Just' value -> do
          insertWord 1
          insertValue encoding value

------------------------------------------------------------------------

takeNext ::
  PrimMonad m =>
  StateT (MRecord (PrimState m)) (EitherT MutableError m) (MField (PrimState m))
takeNext = do
  MRecord xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . MRecord $ Boxed.drop 1 xs
      pure x
    Nothing ->
      lift $ left MutableNoMoreFields

takeByte ::
  PrimMonad m =>
  StateT
    (MRecord (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Word8)
takeByte =
  takeNext >>= \case
    MByteField xs ->
      pure xs
    x ->
      lift . left . MutableExpectedByteField $ describeField x

takeWord ::
  PrimMonad m =>
  StateT
    (MRecord (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Word64)
takeWord =
  takeNext >>= \case
    MWordField xs ->
      pure xs
    x ->
      lift . left . MutableExpectedWordField $ describeField x

takeDouble ::
  PrimMonad m =>
  StateT
    (MRecord (PrimState m))
    (EitherT MutableError m)
    (Grow MStorable.MVector (PrimState m) Double)
takeDouble =
  takeNext >>= \case
    MDoubleField xs ->
      pure xs
    x ->
      lift . left . MutableExpectedDoubleField $ describeField x

withMRecord ::
  PrimMonad m =>
  MRecord (PrimState m) ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) a ->
  EitherT MutableError m a
withMRecord record0 m = do
  (result, MRecord xs) <- runStateT m record0
  if Boxed.null xs then
    pure result
  else
    left . MutableLeftoverFields . Boxed.toList $ fmap describeField xs

withList ::
  PrimMonad m =>
  (Grow MStorable.MVector (PrimState m) Word64 ->
    StateT (MRecord (PrimState m)) (EitherT MutableError m) a) ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) a
withList f =
  takeNext >>= \case
    MListField ns record0 ->
      lift . withMRecord record0 $ f ns
    x ->
      lift . left . MutableExpectedListField $ describeField x

describeField :: MField s -> FoundMField
describeField = \case
  MByteField _ ->
    FoundMByteField
  MWordField _ ->
    FoundMWordField
  MDoubleField _ ->
    FoundMDoubleField
  MListField _ (MRecord xs) ->
    FoundMListField . Boxed.toList $ fmap describeField xs

------------------------------------------------------------------------

insertWord ::
  PrimMonad m =>
  Word64 ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertWord w = do
  xs <- takeWord
  Grow.add xs w

insertDouble ::
  PrimMonad m =>
  Double ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertDouble d = do
  xs <- takeDouble
  Grow.add xs d

insertString ::
  PrimMonad m =>
  ByteString ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertString bs = do
  withList $ \ns -> do
    Grow.add ns (fromIntegral $ B.length bs)
    xs <- takeByte
    Grow.append xs (unsafeFromByteString bs)

insertDefaultField ::
  PrimMonad m =>
  FieldEncoding ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertDefaultField = \case
  FieldEncoding RequiredField encoding ->
    insertDefault encoding
  FieldEncoding OptionalField encoding -> do
    insertWord 0
    insertDefault encoding

insertDefault ::
  PrimMonad m =>
  Encoding ->
  StateT (MRecord (PrimState m)) (EitherT MutableError m) ()
insertDefault = \case
  BoolEncoding ->
    insertWord 0
  Int64Encoding ->
    insertWord 0
  DoubleEncoding ->
    insertDouble 0
  StringEncoding ->
    insertString B.empty
  DateEncoding ->
    insertWord 0
  StructEncoding fields ->
    if Boxed.null fields then
      insertWord 0
    else
      traverse_ (insertDefaultField . snd) fields
  ListEncoding _ ->
    withList $ \ns -> do
      Grow.add ns 0
      put $ MRecord Boxed.empty

------------------------------------------------------------------------

newMByteField :: PrimMonad m => m (MRecord (PrimState m))
newMByteField =
  MRecord . Boxed.singleton . MByteField <$> Grow.new 4

newMWordField :: PrimMonad m => m (MRecord (PrimState m))
newMWordField =
  MRecord . Boxed.singleton . MWordField <$> Grow.new 4

newMDoubleField :: PrimMonad m => m (MRecord (PrimState m))
newMDoubleField =
  MRecord . Boxed.singleton . MDoubleField <$> Grow.new 4

newMListField :: PrimMonad m => MRecord (PrimState m) -> m (MRecord (PrimState m))
newMListField vs =
  MRecord . Boxed.singleton . flip MListField vs <$> Grow.new 4

newMStructField :: PrimMonad m => FieldEncoding -> m (MRecord (PrimState m))
newMStructField = \case
  FieldEncoding RequiredField encoding ->
    newMRecord encoding
  FieldEncoding OptionalField encoding -> do
    b <- newMWordField
    x <- newMRecord encoding
    pure . MRecord $
      mrecordFields b <>
      mrecordFields x

newMRecord :: PrimMonad m => Encoding -> m (MRecord (PrimState m))
newMRecord = \case
  BoolEncoding ->
    newMWordField
  Int64Encoding ->
    newMWordField
  DoubleEncoding ->
    newMDoubleField
  StringEncoding ->
    newMListField =<< newMByteField
  DateEncoding ->
    newMWordField
  StructEncoding fields ->
    if Boxed.null fields then
      newMWordField
    else
      fmap (MRecord . Boxed.concatMap mrecordFields) $
      traverse (newMStructField . snd) fields
  ListEncoding encoding ->
    newMListField =<< newMRecord encoding

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
