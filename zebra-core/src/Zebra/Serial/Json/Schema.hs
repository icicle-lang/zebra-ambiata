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

import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons
import           Zebra.Serial.Json.Util
import           Zebra.Table.Schema


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

encodeSchema :: SchemaVersion -> TableSchema -> ByteString
encodeSchema version =
  encodeJson ["key", "name"] . ppTableSchema version

decodeSchema :: SchemaVersion -> ByteString -> Either JsonSchemaDecodeError TableSchema
decodeSchema = \case
  SchemaV0 ->
    first JsonSchemaDecodeError . decodeJson pTableSchemaV0

ppTableSchema :: SchemaVersion -> TableSchema -> Aeson.Value
ppTableSchema = \case
  SchemaV0 ->
    ppTableSchemaV0

------------------------------------------------------------------------
-- v0

pTableSchemaV0 :: Aeson.Value -> Aeson.Parser TableSchema
pTableSchemaV0 =
  pEnum $ \case
    "binary" ->
      pure . const $ pure Binary
    "array" ->
      pure . Aeson.withObject "object containing array schema" $ \o ->
        Array
          <$> withStructField "element" o pColumnSchemaV0
    "map" ->
      pure . Aeson.withObject "object containing map schema" $ \o ->
        Map
          <$> withStructField "key" o pColumnSchemaV0
          <*> withStructField "value" o pColumnSchemaV0
    _ ->
      Nothing

ppTableSchemaV0 :: TableSchema -> Aeson.Value
ppTableSchemaV0 = \case
  Binary ->
    ppEnum $ Variant "binary" ppUnit
  Array e ->
    ppEnum . Variant "array" $
      Aeson.object ["element" .= ppColumnSchemaV0 e]
  Map k v ->
    ppEnum . Variant "map" $
      Aeson.object ["key" .= ppColumnSchemaV0 k, "value" .= ppColumnSchemaV0 v]

pColumnSchemaV0 :: Aeson.Value -> Aeson.Parser ColumnSchema
pColumnSchemaV0 =
  pEnum $ \case
    "unit" ->
      pure . const $ pure Unit
    "int" ->
      pure . const $ pure Int
    "double" ->
      pure . const $ pure Double
    "enum" ->
      pure . Aeson.withObject "object containing enum column schema" $ \o ->
        Enum <$> withStructField "variants" o pSchemaEnumVariantsV0
    "struct" ->
      pure . Aeson.withObject "object containing struct column schema" $ \o ->
        Struct <$> withStructField "fields" o pSchemaStructFieldsV0
    "nested" ->
      pure . Aeson.withObject "object containing nested column schema" $ \o ->
        Nested <$> withStructField "table" o pTableSchemaV0
    "reversed" ->
      pure . Aeson.withObject "object containing reversed column schema" $ \o ->
        Reversed <$> withStructField "column" o pColumnSchemaV0
    _ ->
      Nothing

ppColumnSchemaV0 :: ColumnSchema -> Aeson.Value
ppColumnSchemaV0 = \case
  Unit ->
    ppEnum $ Variant "unit" ppUnit
  Int ->
    ppEnum $ Variant "int" ppUnit
  Double ->
    ppEnum $ Variant "double" ppUnit
  Enum vs ->
    ppEnum . Variant "enum" $
      Aeson.object ["variants" .= Aeson.Array (Cons.toVector $ fmap ppSchemaVariantV0 vs)]
  Struct fs ->
    ppEnum . Variant "struct" $
      Aeson.object ["fields" .= Aeson.Array (Cons.toVector $ fmap ppSchemaFieldV0 fs)]
  Nested s ->
    ppEnum . Variant "nested" $
      Aeson.object ["table" .= ppTableSchemaV0 s]
  Reversed s ->
    ppEnum . Variant "reversed" $
      Aeson.object ["column" .= ppColumnSchemaV0 s]

pSchemaEnumVariantsV0 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Variant ColumnSchema))
pSchemaEnumVariantsV0 =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariantV0 xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just (v0, vs) ->
        pure $ Cons.from v0 vs

pSchemaVariantV0 :: Aeson.Value -> Aeson.Parser (Variant ColumnSchema)
pSchemaVariantV0 =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withStructField "name" o (fmap VariantName . pText)
      <*> withStructField "column" o pColumnSchemaV0

pSchemaStructFieldsV0 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Field ColumnSchema))
pSchemaStructFieldsV0 =
  Aeson.withArray "array of struct fields" $ \xs -> do
    fs0 <- kmapM pSchemaFieldV0 xs
    case Boxed.uncons fs0 of
      Nothing ->
        fail "structs must have at least one field"
      Just (f0, fs) ->
        pure $ Cons.from f0 fs

pSchemaFieldV0 :: Aeson.Value -> Aeson.Parser (Field ColumnSchema)
pSchemaFieldV0 =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withStructField "name" o (fmap FieldName . pText)
      <*> withStructField "column" o pColumnSchemaV0

ppSchemaVariantV0 :: Variant ColumnSchema -> Aeson.Value
ppSchemaVariantV0 (Variant (VariantName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]

ppSchemaFieldV0 :: Field ColumnSchema -> Aeson.Value
ppSchemaFieldV0 (Field (FieldName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]
