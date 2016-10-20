{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Data.Core (
    EntityId(..)
  , EntityHash(..)
  , AttributeId(..)
  , AttributeName(..)

  , Time(..)
  , Priority(..)
  , Tombstone(..)

  , hashEntityId
  , fromDay
  , toDay
  , int64OfTombstone
  , tombstoneOfInt64
  ) where

import           Anemone.Foreign.Hash (fasthash32)

import           Control.Lens ((^.), re)

import           Data.AffineSpace ((.-.), (.+^))
import           Data.ByteString (ByteString)
import           Data.Thyme.Calendar (Day, YearMonthDay(..), gregorian)
import           Data.Typeable (Typeable)
import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import           Data.Word (Word32)

import           Foreign.Ptr (castPtr)
import           Foreign.Storable (Storable(..))

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)


newtype EntityId =
  EntityId {
      unEntityId :: ByteString
    } deriving (Eq, Ord, Generic, Typeable)

newtype EntityHash =
  EntityHash {
      unEntityHash :: Word32
    } deriving (Eq, Ord, Generic, Typeable)

newtype AttributeId =
  AttributeId {
      unAttributeId :: Int
    } deriving (Eq, Ord, Generic, Typeable)

newtype AttributeName =
  AttributeName {
      unAttributeName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

newtype Time =
  Time {
      unTime :: Int64
    } deriving (Eq, Ord, Enum, Num, Real, Integral, Generic, Typeable, Storable)

newtype Priority =
  Priority {
      unPriority :: Int64
    } deriving (Eq, Ord, Enum, Num, Real, Integral, Generic, Typeable, Storable)

data Tombstone =
    NotTombstone
  | Tombstone
    deriving (Eq, Ord, Show, Generic, Typeable)

instance Storable Tombstone where
  sizeOf _ =
    sizeOf (0 :: Int64)

  alignment _ =
    alignment (0 :: Int64)

  peekElemOff p i =
    fmap tombstoneOfInt64 $
    peekElemOff (castPtr p) i

  pokeElemOff p i x =
    pokeElemOff (castPtr p) i $
    int64OfTombstone x

instance Show EntityId where
  showsPrec =
    gshowsPrec

instance Show EntityHash where
  showsPrec =
    gshowsPrec

instance Show AttributeId where
  showsPrec =
    gshowsPrec

instance Show AttributeName where
  showsPrec =
    gshowsPrec

instance Show Time where
  showsPrec =
    gshowsPrec

instance Show Priority where
  showsPrec =
    gshowsPrec

hashEntityId :: EntityId -> EntityHash
hashEntityId =
  EntityHash . fasthash32 . unEntityId
{-# INLINE hashEntityId #-}

fromDay :: Day -> Time
fromDay day =
  Time . fromIntegral $ (day .-. ivoryEpoch) * 86400
{-# INLINE fromDay #-}

toDay :: Time -> Day
toDay (Time time) =
  ivoryEpoch .+^ fromIntegral time `div` 86400
{-# INLINE toDay #-}

ivoryEpoch :: Day
ivoryEpoch =
  YearMonthDay 1600 3 1 ^. re gregorian
{-# INLINE ivoryEpoch #-}

int64OfTombstone :: Tombstone -> Int64
int64OfTombstone = \case
  NotTombstone ->
    0
  Tombstone ->
    1
{-# INLINE int64OfTombstone #-}

tombstoneOfInt64 :: Int64 -> Tombstone
tombstoneOfInt64 w =
  case w of
    0 ->
      NotTombstone
    _ ->
      Tombstone
{-# INLINE tombstoneOfInt64 #-}

derivingUnbox "EntityHash"
  [t| EntityHash -> Word32 |]
  [| unEntityHash |]
  [| EntityHash |]

derivingUnbox "AttributeId"
  [t| AttributeId -> Int |]
  [| unAttributeId |]
  [| AttributeId |]

derivingUnbox "Time"
  [t| Time -> Int64 |]
  [| unTime |]
  [| Time |]

derivingUnbox "Priority"
  [t| Priority -> Int64 |]
  [| unPriority |]
  [| Priority |]

derivingUnbox "Tombstone"
  [t| Tombstone -> Int64 |]
  [| int64OfTombstone |]
  [| tombstoneOfInt64 |]
