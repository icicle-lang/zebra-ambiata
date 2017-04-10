{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Binary.Data (
    Header(..)
  , BinaryVersion(..)

  , headerOfAttributes
  , attributesOfHeader
  , schemaOfHeader

  , BinaryEncodeError(..)
  , renderBinaryEncodeError

  , BinaryDecodeError(..)
  , renderBinaryDecodeError
  ) where

import           Data.Map (Map)

import           P

import           Zebra.Factset.Data
import           Zebra.Factset.Table
import           Zebra.Table.Encoding (Utf8Error)
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Schema as Schema


data Header =
    HeaderV2 !(Map AttributeName Schema.Column)
  | HeaderV3 !Schema.Table
    deriving (Eq, Ord, Show)

data BinaryVersion =
--  BinaryV0 -- x Initial version.
--  BinaryV1 -- x Store factset-id instead of priority, this flips sort order.
    BinaryV2 -- ^ Schema is stored in header, instead of encoding.
  | BinaryV3 -- ^ Data is stored as tables instead of entity blocks.
    deriving (Eq, Ord, Show)

data BinaryEncodeError =
    BinaryEncodeUtf8 !Utf8Error
  | BinaryEncodeBlockTableError !BlockTableError
    deriving (Eq, Show)

data BinaryDecodeError =
    BinaryDecodeUtf8 !Utf8Error
    deriving (Eq, Show)

renderBinaryEncodeError :: BinaryEncodeError -> Text
renderBinaryEncodeError = \case
  BinaryEncodeUtf8 err ->
    "Failed encoding UTF-8 binary: " <>
    Encoding.renderUtf8Error err
  BinaryEncodeBlockTableError err ->
    renderBlockTableError err

renderBinaryDecodeError :: BinaryDecodeError -> Text
renderBinaryDecodeError = \case
  BinaryDecodeUtf8 err ->
    "Failed decoding UTF-8 binary: " <>
    Encoding.renderUtf8Error err

headerOfAttributes :: BinaryVersion -> Map AttributeName Schema.Column -> Header
headerOfAttributes version attributes =
  case version of
    BinaryV2 ->
      HeaderV2 attributes
    BinaryV3 ->
      HeaderV3 (tableSchemaOfAttributes attributes)

attributesOfHeader :: Header -> Either BlockTableError (Map AttributeName Schema.Column)
attributesOfHeader = \case
  HeaderV2 attributes ->
    pure attributes
  HeaderV3 table ->
    attributesOfTableSchema table

schemaOfHeader :: Header -> Schema.Table
schemaOfHeader = \case
  HeaderV2 attributes ->
    tableSchemaOfAttributes attributes
  HeaderV3 table ->
    table
