{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Text.Schema (
    TextVersion(..)
  , encodeSchema
  , decodeSchema

  , TextSchemaDecodeError(..)
  , renderTextSchemaDecodeError
  ) where

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.Text as Text

import           P

import           Zebra.Serial.Json.Schema (SchemaVersion(..), pTableSchemaV1, ppTableSchema)
import           Zebra.Serial.Json.Util
import           Zebra.Table.Data
import qualified Zebra.Table.Schema as Schema


data TextVersion =
    TextV0
    deriving (Eq, Show, Enum, Bounded)

data TextSchemaDecodeError =
    TextSchemaDecodeError !JsonDecodeError
    deriving (Eq, Show)

renderTextSchemaDecodeError :: TextSchemaDecodeError -> Text
renderTextSchemaDecodeError = \case
  TextSchemaDecodeError err ->
    renderJsonDecodeError err

encodeSchema :: TextVersion -> Schema.Table -> ByteString
encodeSchema version schema =
  encodeJsonIndented ["version", "key", "name"] (ppVersionedSchema version schema) <> "\n"

decodeSchema :: ByteString -> Either TextSchemaDecodeError Schema.Table
decodeSchema =
  first TextSchemaDecodeError . decodeJson pVersionedSchema

pVersionedSchema :: Aeson.Value -> Aeson.Parser Schema.Table
pVersionedSchema =
  Aeson.withObject "object containing versioned schema" $ \o -> do
    version <- withStructField "version" o pVersion
    case version of
      TextV0 ->
        withStructField "schema" o pTableSchemaV1

ppVersionedSchema :: TextVersion -> Schema.Table -> Aeson.Value
ppVersionedSchema version schema =
  ppStruct [
      Field "version" $
        ppVersion version
    , Field "schema" $
        ppTableSchema (schemaVersion version) schema
    ]

schemaVersion :: TextVersion -> SchemaVersion
schemaVersion = \case
  TextV0 ->
    SchemaV1

pVersion :: Aeson.Value -> Aeson.Parser TextVersion
pVersion =
  Aeson.withText "string containing version number" $ \case
    "v0" ->
      pure TextV0
    v ->
      fail $ "unknown/unsupported version: " <> Text.unpack v

ppVersion :: TextVersion -> Aeson.Value
ppVersion = \case
  TextV0 ->
    Aeson.String "v0"
