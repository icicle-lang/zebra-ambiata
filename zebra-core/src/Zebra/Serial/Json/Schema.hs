{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Json.Schema (
    SchemaVersion(..)
  , encodeSchema
  , decodeSchema

  , JsonSchemaDecodeError(..)
  , renderJsonSchemaDecodeError

  -- * Decode
  , pTableSchemaV0
  , pColumnSchemaV0

  -- * Encode
  , ppTableSchema
  , ppTableSchemaV0
  , ppColumnSchemaV0
  ) where

import           Data.Aeson ((.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)

import           P

import qualified X.Data.Vector as Boxed

import           Zebra.Serial.Json.Util
import           Zebra.Table.Data
import qualified Zebra.Table.Schema as Schema
import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons


data SchemaVersion =
    SchemaV0
    deriving (Eq, Show, Enum, Bounded)

data JsonSchemaDecodeError =
    JsonSchemaDecodeError !JsonDecodeError
    deriving (Eq, Show)

renderJsonSchemaDecodeError :: JsonSchemaDecodeError -> Text
renderJsonSchemaDecodeError = \case
  JsonSchemaDecodeError err ->
    renderJsonDecodeError err

encodeSchema :: SchemaVersion -> Schema.Table -> ByteString
encodeSchema version =
  encodeJson ["key", "name"] . ppTableSchema version

decodeSchema :: SchemaVersion -> ByteString -> Either JsonSchemaDecodeError Schema.Table
decodeSchema = \case
  SchemaV0 ->
    first JsonSchemaDecodeError . decodeJson pTableSchemaV0

ppTableSchema :: SchemaVersion -> Schema.Table -> Aeson.Value
ppTableSchema = \case
  SchemaV0 ->
    ppTableSchemaV0

------------------------------------------------------------------------
-- v0

pTableSchemaV0 :: Aeson.Value -> Aeson.Parser Schema.Table
pTableSchemaV0 =
  pEnum $ \case
    "binary" ->
      pure . const $ pure Schema.Binary
    "array" ->
      pure . Aeson.withObject "object containing array schema" $ \o ->
        Schema.Array
          <$> withStructField "element" o pColumnSchemaV0
    "map" ->
      pure . Aeson.withObject "object containing map schema" $ \o ->
        Schema.Map
          <$> withStructField "key" o pColumnSchemaV0
          <*> withStructField "value" o pColumnSchemaV0
    _ ->
      Nothing

ppTableSchemaV0 :: Schema.Table -> Aeson.Value
ppTableSchemaV0 = \case
  Schema.Binary ->
    ppEnum $ Variant "binary" ppUnit
  Schema.Array e ->
    ppEnum . Variant "array" $
      Aeson.object ["element" .= ppColumnSchemaV0 e]
  Schema.Map k v ->
    ppEnum . Variant "map" $
      Aeson.object ["key" .= ppColumnSchemaV0 k, "value" .= ppColumnSchemaV0 v]

pColumnSchemaV0 :: Aeson.Value -> Aeson.Parser Schema.Column
pColumnSchemaV0 =
  pEnum $ \case
    "unit" ->
      pure . const $ pure Schema.Unit
    "int" ->
      pure . const $ pure Schema.Int
    "double" ->
      pure . const $ pure Schema.Double
    "enum" ->
      pure . Aeson.withObject "object containing enum column schema" $ \o ->
        Schema.Enum <$> withStructField "variants" o pSchemaEnumVariantsV0
    "struct" ->
      pure . Aeson.withObject "object containing struct column schema" $ \o ->
        Schema.Struct <$> withStructField "fields" o pSchemaStructFieldsV0
    "nested" ->
      pure . Aeson.withObject "object containing nested column schema" $ \o ->
        Schema.Nested <$> withStructField "table" o pTableSchemaV0
    "reversed" ->
      pure . Aeson.withObject "object containing reversed column schema" $ \o ->
        Schema.Reversed <$> withStructField "column" o pColumnSchemaV0
    _ ->
      Nothing

ppColumnSchemaV0 :: Schema.Column -> Aeson.Value
ppColumnSchemaV0 = \case
  Schema.Unit ->
    ppEnum $ Variant "unit" ppUnit
  Schema.Int ->
    ppEnum $ Variant "int" ppUnit
  Schema.Double ->
    ppEnum $ Variant "double" ppUnit
  Schema.Enum vs ->
    ppEnum . Variant "enum" $
      Aeson.object ["variants" .= Aeson.Array (Cons.toVector $ fmap ppSchemaVariantV0 vs)]
  Schema.Struct fs ->
    ppEnum . Variant "struct" $
      Aeson.object ["fields" .= Aeson.Array (Cons.toVector $ fmap ppSchemaFieldV0 fs)]
  Schema.Nested s ->
    ppEnum . Variant "nested" $
      Aeson.object ["table" .= ppTableSchemaV0 s]
  Schema.Reversed s ->
    ppEnum . Variant "reversed" $
      Aeson.object ["column" .= ppColumnSchemaV0 s]

pSchemaEnumVariantsV0 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Variant Schema.Column))
pSchemaEnumVariantsV0 =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariantV0 xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just (v0, vs) ->
        pure $ Cons.from v0 vs

pSchemaVariantV0 :: Aeson.Value -> Aeson.Parser (Variant Schema.Column)
pSchemaVariantV0 =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withStructField "name" o (fmap VariantName . pText)
      <*> withStructField "column" o pColumnSchemaV0

pSchemaStructFieldsV0 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Field Schema.Column))
pSchemaStructFieldsV0 =
  Aeson.withArray "array of struct fields" $ \xs -> do
    fs0 <- kmapM pSchemaFieldV0 xs
    case Boxed.uncons fs0 of
      Nothing ->
        fail "structs must have at least one field"
      Just (f0, fs) ->
        pure $ Cons.from f0 fs

pSchemaFieldV0 :: Aeson.Value -> Aeson.Parser (Field Schema.Column)
pSchemaFieldV0 =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withStructField "name" o (fmap FieldName . pText)
      <*> withStructField "column" o pColumnSchemaV0

ppSchemaVariantV0 :: Variant Schema.Column -> Aeson.Value
ppSchemaVariantV0 (Variant (VariantName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]

ppSchemaFieldV0 :: Field Schema.Column -> Aeson.Value
ppSchemaFieldV0 (Field (FieldName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]
