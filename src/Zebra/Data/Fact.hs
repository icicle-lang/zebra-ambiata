{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Data.Fact (
    Fact(..)
  , Value(..)
  , renderFact
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import           Data.Thyme.Calendar (Day)
import           Data.Thyme.Format (formatTime)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P

import           System.Locale (defaultTimeLocale)

import           Text.Printf (printf)

import           Zebra.Data.Core


data Fact =
  Fact {
      factEntityHash :: !EntityHash
    , factEntityId :: !EntityId
    , factAttributeId :: !AttributeId
    , factTime :: !Time
    , factFactsetId :: !FactsetId
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

renderFact :: Fact -> ByteString
renderFact fact =
  Char8.intercalate "|" [
      renderEntityHash $ factEntityHash fact
    , unEntityId $ factEntityId fact
    , renderAttributeId $ factAttributeId fact
    , renderTime $ factTime fact
    , renderFactsetId $ factFactsetId fact
    , renderMaybeValue $ factValue fact
    ]

renderEntityHash :: EntityHash -> ByteString
renderEntityHash (EntityHash hash) =
  Char8.pack $ printf "0x%08X" hash

renderAttributeId :: AttributeId -> ByteString
renderAttributeId (AttributeId aid) =
  Char8.pack $ printf "attribute=%05d" aid

renderTime :: Time -> ByteString
renderTime =
  Char8.pack . formatTime defaultTimeLocale "%0Y-%m-%d %H:%M:%S" . toUTCTime

renderFactsetId :: FactsetId -> ByteString
renderFactsetId (FactsetId factsetid) =
  Char8.pack $ printf "factsetid=%04x" factsetid

renderMaybeValue :: Maybe' Value -> ByteString
renderMaybeValue =
  maybe' "NA" renderValue

renderValue :: Value -> ByteString
renderValue = \case
  BoolValue x ->
    Char8.pack $ show x
  Int64Value x ->
    Char8.pack $ show x
  DoubleValue x ->
    Char8.pack $ show x
  StringValue x ->
    Char8.pack $ show x
  DateValue x ->
    Char8.pack $ show x
  ListValue x ->
    if Boxed.null x then
      "[]"
    else
      "[ " <> (Char8.intercalate ", " . Boxed.toList $ fmap renderValue x) <> " ]"
  StructValue x ->
    if Boxed.null x then
      "{}"
    else
      "{ " <> (Char8.intercalate "; " . Boxed.toList $ fmap renderMaybeValue x) <> " }"
