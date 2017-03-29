{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Json.Schema (
    encodeSchema
  , encodeVersionedSchema
  , decodeSchema

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

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Json.Codec
import           Zebra.Schema


encodeSchema :: JsonVersion -> TableSchema -> ByteString
encodeSchema version =
  encodeJsonIndented ["key", "name"] . ppTableSchema version

encodeVersionedSchema :: JsonVersion -> TableSchema -> ByteString
encodeVersionedSchema version =
  encodeJsonIndented ["version", "key", "name"] . ppVersionedSchema version

decodeSchema :: ByteString -> Either JsonDecodeError TableSchema
decodeSchema =
  decodeJson pTableSchemaV0

pTableSchemaV0 :: Aeson.Value -> Aeson.Parser TableSchema
pTableSchemaV0 =
  pEnum $ \case
    "binary" ->
      pure . const $ pure Binary
    "array" ->
      pure . Aeson.withObject "object containing array schema" $ \o ->
        Array
          <$> withKey "element" o pColumnSchemaV0
    "map" ->
      pure . Aeson.withObject "object containing map schema" $ \o ->
        Map
          <$> withKey "key" o pColumnSchemaV0
          <*> withKey "value" o pColumnSchemaV0
    _ ->
      Nothing

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
        Enum <$> withKey "variants" o pSchemaEnumVariants
    "struct" ->
      pure . Aeson.withObject "object containing struct column schema" $ \o ->
        Struct <$> withKey "fields" o pSchemaStructFields
    "nested" ->
      pure . Aeson.withObject "object containing nested column schema" $ \o ->
        Nested <$> withKey "table" o pTableSchemaV0
    "reversed" ->
      pure . Aeson.withObject "object containing reversed column schema" $ \o ->
        Reversed <$> withKey "column" o pColumnSchemaV0
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
      <*> withKey "column" o pColumnSchemaV0

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
      <*> withKey "column" o pColumnSchemaV0

ppVersionedSchema :: JsonVersion -> TableSchema -> Aeson.Value
ppVersionedSchema version schema =
  ppStruct [
      Field "version" $
        ppVersion version
    , Field "schema" $
        ppTableSchema version schema
    ]

ppTableSchema :: JsonVersion -> TableSchema -> Aeson.Value
ppTableSchema = \case
  JsonV0 ->
    ppTableSchemaV0

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
      Aeson.object ["variants" .= Aeson.Array (Cons.toVector $ fmap ppSchemaVariant vs)]
  Struct fs ->
    ppEnum . Variant "struct" $
      Aeson.object ["fields" .= Aeson.Array (Cons.toVector $ fmap ppSchemaField fs)]
  Nested s ->
    ppEnum . Variant "nested" $
      Aeson.object ["table" .= ppTableSchemaV0 s]
  Reversed s ->
    ppEnum . Variant "reversed" $
      Aeson.object ["column" .= ppColumnSchemaV0 s]

ppSchemaVariant :: Variant ColumnSchema -> Aeson.Value
ppSchemaVariant (Variant (VariantName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]

ppSchemaField :: Field ColumnSchema -> Aeson.Value
ppSchemaField (Field (FieldName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]
