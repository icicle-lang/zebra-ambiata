{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Json.Schema (
    encodeSchema
  , decodeSchema

  -- * Decode
  , pTableSchema
  , pColumnSchema

  -- * Encode
  , ppTableSchema
  , ppColumnSchema
  ) where

import           Data.Aeson ((.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)

import           P

import qualified X.Data.Vector as Boxed

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Json.Codec
import           Zebra.Schema


encodeSchema :: TableSchema -> ByteString
encodeSchema =
  encodeJsonIndented ["name", "column"] . ppTableSchema

decodeSchema :: ByteString -> Either JsonDecodeError TableSchema
decodeSchema =
  decodeJson pTableSchema

pTableSchema :: Aeson.Value -> Aeson.Parser TableSchema
pTableSchema =
  pEnum $ \case
    "binary" ->
      pure . const $ pure Binary
    "array" ->
      pure . Aeson.withObject "object containing array schema" $ \o ->
        Array
          <$> withKey "element" o pColumnSchema
    "map" ->
      pure . Aeson.withObject "object containing map schema" $ \o ->
        Map
          <$> withKey "key" o pColumnSchema
          <*> withKey "value" o pColumnSchema
    _ ->
      Nothing

pColumnSchema :: Aeson.Value -> Aeson.Parser ColumnSchema
pColumnSchema =
  pEnum $ \case
    "unit" ->
      pure . const $ pure Unit
    "int" ->
      pure . const $ pure Int
    "double" ->
      pure . const $ pure Double
    "enum" ->
      pure . Aeson.withObject "object containing enum column schema" $ \o ->
        Enum <$> withKey "variants" o pSchemaEnumVariants
    "struct" ->
      pure . Aeson.withObject "object containing struct column schema" $ \o ->
        Struct <$> withKey "fields" o pSchemaStructFields
    "nested" ->
      pure . Aeson.withObject "object containing nested column schema" $ \o ->
        Nested <$> withKey "table" o pTableSchema
    "reversed" ->
      pure . Aeson.withObject "object containing reversed column schema" $ \o ->
        Reversed <$> withKey "column" o pColumnSchema
    _ ->
      Nothing

pSchemaEnumVariants :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Variant ColumnSchema))
pSchemaEnumVariants =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariant xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just (v0, vs) ->
        pure $ Cons.from v0 vs

pSchemaVariant :: Aeson.Value -> Aeson.Parser (Variant ColumnSchema)
pSchemaVariant =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withKey "name" o (pure . VariantName)
      <*> withKey "column" o pColumnSchema

pSchemaStructFields :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Field ColumnSchema))
pSchemaStructFields =
  Aeson.withArray "array of struct fields" $ \xs -> do
    fs0 <- kmapM pSchemaField xs
    case Boxed.uncons fs0 of
      Nothing ->
        fail "structs must have at least one field"
      Just (f0, fs) ->
        pure $ Cons.from f0 fs

pSchemaField :: Aeson.Value -> Aeson.Parser (Field ColumnSchema)
pSchemaField =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withKey "name" o (pure . FieldName)
      <*> withKey "column" o pColumnSchema

ppTableSchema :: TableSchema -> Aeson.Value
ppTableSchema = \case
  Binary ->
    ppEnum $ Variant "binary" ppUnit
  Array e ->
    ppEnum . Variant "array" $
      Aeson.object ["element" .= ppColumnSchema e]
  Map k v ->
    ppEnum . Variant "map" $
      Aeson.object ["key" .= ppColumnSchema k, "value" .= ppColumnSchema v]

ppColumnSchema :: ColumnSchema -> Aeson.Value
ppColumnSchema = \case
  Unit ->
    ppEnum $ Variant "unit" ppUnit
  Int ->
    ppEnum $ Variant "int" ppUnit
  Double ->
    ppEnum $ Variant "double" ppUnit
  Enum vs ->
    ppEnum . Variant "enum" $
      Aeson.object ["variants" .= Aeson.Array (Cons.toVector $ fmap ppSchemaVariant vs)]
  Struct fs ->
    ppEnum . Variant "struct" $
      Aeson.object ["fields" .= Aeson.Array (Cons.toVector $ fmap ppSchemaField fs)]
  Nested s ->
    ppEnum . Variant "nested" $
      Aeson.object ["table" .= ppTableSchema s]
  Reversed s ->
    ppEnum . Variant "reversed" $
      Aeson.object ["column" .= ppColumnSchema s]

ppSchemaVariant :: Variant ColumnSchema -> Aeson.Value
ppSchemaVariant (Variant (VariantName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchema schema
    ]

ppSchemaField :: Field ColumnSchema -> Aeson.Value
ppSchemaField (Field (FieldName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchema schema
    ]
