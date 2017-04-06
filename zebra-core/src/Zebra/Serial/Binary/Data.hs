{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Binary.Data (
    Header(..)
  , BinaryVersion(..)

  , headerOfAttributes
  , attributesOfHeader
  , schemaOfHeader
  ) where

import           Data.Map (Map)

import           P

import           Zebra.Factset.Data
import           Zebra.Factset.Table
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
