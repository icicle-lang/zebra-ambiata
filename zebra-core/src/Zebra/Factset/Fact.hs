{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Factset.Fact (
    Fact(..)
  , toValueTable
  , render

  , FactConversionError(..)
  , renderFactConversionError

  , FactRenderError(..)
  , renderFactRenderError
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Text as Text
import           Data.Thyme.Format (formatTime)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P hiding (some)

import           System.Locale (defaultTimeLocale)

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Zebra.Factset.Data
import           Zebra.Serial.Json
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (TableSchema, ColumnSchema)
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped


data Fact =
  Fact {
      factEntityHash :: !EntityHash
    , factEntityId :: !EntityId
    , factAttributeId :: !AttributeId
    , factTime :: !Time
    , factFactsetId :: !FactsetId
    , factValue :: !(Maybe' Logical.Value)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data FactConversionError =
    FactStripedError !StripedError
  | FactConversionSchemaError !FactSchemaError
    deriving (Eq, Ord, Show, Generic, Typeable)

data FactRenderError =
    FactJsonEncodeError !JsonLogicalEncodeError
  | FactSchemaNotFoundForAttribute !AttributeId
  | FactRenderSchemaError !FactSchemaError
    deriving (Eq, Show, Generic, Typeable)

data FactSchemaError =
    FactExpectedArrayTable !TableSchema
    deriving (Eq, Ord, Show, Generic, Typeable)

renderFactConversionError :: FactConversionError -> Text
renderFactConversionError = \case
  FactStripedError err ->
    Striped.renderStripedError err
  FactConversionSchemaError err ->
    renderFactSchemaError err

renderFactRenderError :: FactRenderError -> Text
renderFactRenderError = \case
  FactJsonEncodeError err ->
    renderJsonLogicalEncodeError err
  FactSchemaNotFoundForAttribute (AttributeId aid) ->
    "Could not render fact, no schema found for attribute-id: " <> Text.pack (show aid)
  FactRenderSchemaError err ->
    renderFactSchemaError err

renderFactSchemaError :: FactSchemaError -> Text
renderFactSchemaError = \case
  FactExpectedArrayTable schema ->
    "Fact tables must be arrays, found: " <> Text.pack (ppShow schema)

toValueTable :: Boxed.Vector ColumnSchema -> Boxed.Vector Fact -> Either FactConversionError (Boxed.Vector Striped.Table)
toValueTable schemas facts =
  flip Boxed.imapM schemas $ \ix schema -> do
    let
      defaultValue =
        Logical.defaultValue schema

      matchId fact =
        AttributeId (fromIntegral ix) == factAttributeId fact

      values =
        Boxed.map (fromMaybe' defaultValue . factValue) $
        Boxed.filter matchId facts

    first FactStripedError . Striped.fromLogical (Schema.Array schema) $ Logical.Array values

render :: Boxed.Vector ColumnSchema -> Fact -> Either FactRenderError ByteString
render schemas fact = do
  let
    aid =
      factAttributeId fact

    ix =
      fromIntegral $ unAttributeId aid

  cschema <- maybeToRight (FactSchemaNotFoundForAttribute aid) (schemas Boxed.!? ix)
  rvalue <- renderMaybeValue cschema $ factValue fact

  pure $ Char8.intercalate "|" [
      renderEntityHash $ factEntityHash fact
    , unEntityId $ factEntityId fact
    , renderAttributeId $ factAttributeId fact
    , renderTime $ factTime fact
    , renderFactsetId $ factFactsetId fact
    , rvalue
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

renderMaybeValue :: ColumnSchema -> Maybe' Logical.Value -> Either FactRenderError ByteString
renderMaybeValue schema =
  maybe' (pure "NA") (first FactJsonEncodeError . encodeLogicalValue schema)
