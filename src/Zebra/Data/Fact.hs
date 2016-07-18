{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Data.Fact (
    Fact(..)
  , Value(..)
  , EntityId(..)
  , EntityHash(..)
  , AttributeId(..)
  , AttributeName(..)
  , Time(..)
  , Priority(..)

  , hashEntityId
  , fromDay
  ) where

import           Control.Lens ((^.), re)

import           Data.AffineSpace ((.-.))
import           Data.ByteString (ByteString)
import           Data.Hashable (hash)
import           Data.Thyme.Calendar (Day, YearMonthDay(..), gregorian)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import           Data.Word (Word32)

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)


data Fact =
  Fact {
      factEntityHash :: !EntityHash
    , factEntityId :: !EntityId
    , factAttributeId :: !AttributeId
    , factTime :: !Time
    , factPriority :: !Priority
    , factValue :: !(Maybe' Value)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data Value =
    BoolValue !Bool
  | Int64Value !Int64
  | DoubleValue !Double
  | StringValue !Text
  | DateValue !Day
  | ListValue !(Boxed.Vector Value)
  | StructValue !(Boxed.Vector (Maybe' Value))
    deriving (Eq, Ord, Show, Generic, Typeable)

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
    } deriving (Eq, Ord, Enum, Num, Real, Integral, Generic, Typeable)

newtype Priority =
  Priority {
      unPriority :: Int
    } deriving (Eq, Ord, Generic, Typeable)

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
  EntityHash . fromIntegral . hash . unEntityId

fromDay :: Day -> Time
fromDay day =
  Time . fromIntegral $ (day .-. ivoryEpoch) * 86400

ivoryEpoch :: Day
ivoryEpoch =
  YearMonthDay 1600 3 1 ^. re gregorian

derivingUnbox "AttributeId"
  [t| AttributeId -> Int |]
  [| unAttributeId |]
  [| AttributeId |]

derivingUnbox "Time"
  [t| Time -> Int64 |]
  [| unTime |]
  [| Time |]

derivingUnbox "Priority"
  [t| Priority -> Int |]
  [| unPriority |]
  [| Priority |]
