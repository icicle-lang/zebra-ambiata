{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Fact (
    Fact(..)
  , Value(..)
  , EntityId(..)
  , EntityHash(..)
  , AttributeId(..)
  , AttributeName(..)
  , Time(..)
  , Priority(..)
  ) where

import           Data.ByteString (ByteString)
import           Data.Thyme.Calendar (Day)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
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
