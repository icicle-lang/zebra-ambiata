{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Fact (
    Fact(..)
  , Value(..)
  , renderFact
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import           Data.Thyme.Format (formatTime)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import           Data.Word (Word8)

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
    Bool !Bool
  | Byte !Word8
  | Int !Int64
  | Double !Double
  | Enum !Int !Value
  | Struct !(Boxed.Vector Value)
  | Array !(Boxed.Vector Value)
  | ByteArray !ByteString -- ^ Optimisation for Array [Byte, Byte, ..]
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
renderFactsetId (FactsetId factsetId) =
  Char8.pack $ printf "factset=%08x" factsetId

renderMaybeValue :: Maybe' Value -> ByteString
renderMaybeValue =
  maybe' "NA" renderValue

renderValue :: Value -> ByteString
renderValue = \case
  Bool x ->
    Char8.pack $ show x
  Byte x ->
    Char8.pack $ show x
  Int x ->
    Char8.pack $ show x
  Double x ->
    Char8.pack $ show x
  Struct x ->
    if Boxed.null x then
      "{}"
    else
      "{ " <> (Char8.intercalate ", " . Boxed.toList $ fmap renderValue x) <> " }"
  Enum tag x ->
    Char8.pack (show tag) <> ": " <> renderValue x
  Array x ->
    if Boxed.null x then
      "[]"
    else
      "[ " <> (Char8.intercalate ", " . Boxed.toList $ fmap renderValue x) <> " ]"
  ByteArray x ->
    Char8.pack $ show x
