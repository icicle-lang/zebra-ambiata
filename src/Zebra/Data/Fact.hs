{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Data.Fact (
    Fact(..)
  , Value(..)
  ) where

import           Data.Thyme.Calendar (Day)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P

import           Zebra.Data.Core


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
