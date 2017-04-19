{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Json.Schema (
    SchemaVersion(..)
  , encodeSchema
  , decodeSchema
  , ppTableSchema

  , JsonSchemaDecodeError(..)
  , renderJsonSchemaDecodeError

  -- * V0
  , pTableSchemaV0
  , pColumnSchemaV0
  , ppTableSchemaV0
  , ppColumnSchemaV0

  -- * V1
  , pTableSchemaV1
  , pColumnSchemaV1
  , ppTableSchemaV1
  , ppColumnSchemaV1
  ) where

import           Data.Aeson ((.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)

import           P

import qualified X.Data.Vector as Boxed
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Serial.Json.Util
import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Schema as Schema


data SchemaVersion =
    SchemaV0
  | SchemaV1
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
{-# INLINABLE encodeSchema #-}

decodeSchema :: SchemaVersion -> ByteString -> Either JsonSchemaDecodeError Schema.Table
decodeSchema = \case
  SchemaV0 ->
    first JsonSchemaDecodeError . decodeJson pTableSchemaV0
  SchemaV1 ->
    first JsonSchemaDecodeError . decodeJson pTableSchemaV1
{-# INLINABLE decodeSchema #-}

ppTableSchema :: SchemaVersion -> Schema.Table -> Aeson.Value
ppTableSchema = \case
  SchemaV0 ->
    ppTableSchemaV0
  SchemaV1 ->
    ppTableSchemaV1
{-# INLINABLE ppTableSchema #-}

------------------------------------------------------------------------
-- v0

pTableSchemaV0 :: Aeson.Value -> Aeson.Parser Schema.Table
pTableSchemaV0 =
  pEnum $ \case
    "binary" ->
      pure . const . pure $ Schema.Binary Nothing
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
{-# INLINABLE pTableSchemaV0 #-}

ppTableSchemaV0 :: Schema.Table -> Aeson.Value
ppTableSchemaV0 = \case
  Schema.Binary _encoding ->
    ppEnum $ Variant "binary" ppUnit
  Schema.Array e ->
    ppEnum . Variant "array" $
      Aeson.object ["element" .= ppColumnSchemaV0 e]
  Schema.Map k v ->
    ppEnum . Variant "map" $
      Aeson.object ["key" .= ppColumnSchemaV0 k, "value" .= ppColumnSchemaV0 v]
{-# INLINABLE ppTableSchemaV0 #-}

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
{-# INLINABLE pColumnSchemaV0 #-}

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
{-# INLINABLE ppColumnSchemaV0 #-}

pSchemaEnumVariantsV0 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Variant Schema.Column))
pSchemaEnumVariantsV0 =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariantV0 xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just (v0, vs) ->
        pure $ Cons.from v0 vs
{-# INLINABLE pSchemaEnumVariantsV0 #-}

pSchemaVariantV0 :: Aeson.Value -> Aeson.Parser (Variant Schema.Column)
pSchemaVariantV0 =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withStructField "name" o (fmap VariantName . pText)
      <*> withStructField "column" o pColumnSchemaV0
{-# INLINABLE pSchemaVariantV0 #-}

pSchemaStructFieldsV0 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Field Schema.Column))
pSchemaStructFieldsV0 =
  Aeson.withArray "array of struct fields" $ \xs -> do
    fs0 <- kmapM pSchemaFieldV0 xs
    case Boxed.uncons fs0 of
      Nothing ->
        fail "structs must have at least one field"
      Just (f0, fs) ->
        pure $ Cons.from f0 fs
{-# INLINABLE pSchemaStructFieldsV0 #-}

pSchemaFieldV0 :: Aeson.Value -> Aeson.Parser (Field Schema.Column)
pSchemaFieldV0 =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withStructField "name" o (fmap FieldName . pText)
      <*> withStructField "column" o pColumnSchemaV0
{-# INLINABLE pSchemaFieldV0 #-}

ppSchemaVariantV0 :: Variant Schema.Column -> Aeson.Value
ppSchemaVariantV0 (Variant (VariantName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]
{-# INLINABLE ppSchemaVariantV0 #-}

ppSchemaFieldV0 :: Field Schema.Column -> Aeson.Value
ppSchemaFieldV0 (Field (FieldName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "column" $
        ppColumnSchemaV0 schema
    ]
{-# INLINABLE ppSchemaFieldV0 #-}

------------------------------------------------------------------------
-- v1

pTableSchemaV1 :: Aeson.Value -> Aeson.Parser Schema.Table
pTableSchemaV1 =
  pEnum pTableVariantV1
{-# INLINABLE pTableSchemaV1 #-}

pTableVariantV1 :: VariantName -> Maybe (Aeson.Value -> Aeson.Parser Schema.Table)
pTableVariantV1 = \case
  "binary" ->
    pure . Aeson.withObject "object containing binary schema" $ \o ->
      Schema.Binary
        <$> withOptionalField "encoding" o pBinaryEncodingV1
  "array" ->
    pure . Aeson.withObject "object containing array schema" $ \o ->
      Schema.Array
        <$> withStructField "element" o pColumnSchemaV1
  "map" ->
    pure . Aeson.withObject "object containing map schema" $ \o ->
      Schema.Map
        <$> withStructField "key" o pColumnSchemaV1
        <*> withStructField "value" o pColumnSchemaV1
  _ ->
    Nothing
{-# INLINABLE pTableVariantV1 #-}

ppTableSchemaV1 :: Schema.Table -> Aeson.Value
ppTableSchemaV1 = \case
  Schema.Binary mencoding ->
    ppEnum . Variant "binary" . Aeson.object $
      case mencoding of
        Nothing ->
          []
        Just encoding ->
          [ "encoding" .= ppBinaryEncodingV1 encoding ]
  Schema.Array e ->
    ppEnum . Variant "array" $
      Aeson.object ["element" .= ppColumnSchemaV1 e]
  Schema.Map k v ->
    ppEnum . Variant "map" $
      Aeson.object ["key" .= ppColumnSchemaV1 k, "value" .= ppColumnSchemaV1 v]
{-# INLINABLE ppTableSchemaV1 #-}

pBinaryEncodingV1 :: Aeson.Value -> Aeson.Parser Encoding.Binary
pBinaryEncodingV1 =
  pEnum $ \case
    "utf8" ->
      pure . const $ pure Encoding.Utf8
    _ ->
      Nothing
{-# INLINABLE pBinaryEncodingV1 #-}

ppBinaryEncodingV1 :: Encoding.Binary -> Aeson.Value
ppBinaryEncodingV1 = \case
  Encoding.Utf8 ->
    ppEnum $ Variant "utf8" ppUnit
{-# INLINABLE ppBinaryEncodingV1 #-}

pColumnSchemaV1 :: Aeson.Value -> Aeson.Parser Schema.Column
pColumnSchemaV1 =
  pEnum $ \case
    "unit" ->
      pure . const $ pure Schema.Unit
    "int" ->
      pure . const $ pure Schema.Int
    "double" ->
      pure . const $ pure Schema.Double
    "enum" ->
      pure . Aeson.withObject "object containing enum column schema" $ \o ->
        Schema.Enum <$> withStructField "variants" o pSchemaEnumVariantsV1
    "struct" ->
      pure . Aeson.withObject "object containing struct column schema" $ \o ->
        Schema.Struct <$> withStructField "fields" o pSchemaStructFieldsV1
    "reversed" ->
      pure $
        fmap Schema.Reversed . pColumnSchemaV1
    nested ->
      fmap2 Schema.Nested <$> pTableVariantV1 nested
{-# INLINABLE pColumnSchemaV1 #-}

ppColumnSchemaV1 :: Schema.Column -> Aeson.Value
ppColumnSchemaV1 = \case
  Schema.Unit ->
    ppEnum $ Variant "unit" ppUnit
  Schema.Int ->
    ppEnum $ Variant "int" ppUnit
  Schema.Double ->
    ppEnum $ Variant "double" ppUnit
  Schema.Enum vs ->
    ppEnum . Variant "enum" $
      Aeson.object ["variants" .= Aeson.Array (Cons.toVector $ fmap ppSchemaVariantV1 vs)]
  Schema.Struct fs ->
    ppEnum . Variant "struct" $
      Aeson.object ["fields" .= Aeson.Array (Cons.toVector $ fmap ppSchemaFieldV1 fs)]
  Schema.Nested s ->
    ppTableSchemaV1 s
  Schema.Reversed s ->
    ppEnum . Variant "reversed" $
      ppColumnSchemaV1 s
{-# INLINABLE ppColumnSchemaV1 #-}

pSchemaEnumVariantsV1 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Variant Schema.Column))
pSchemaEnumVariantsV1 =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariantV1 xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just (v0, vs) ->
        pure $ Cons.from v0 vs
{-# INLINABLE pSchemaEnumVariantsV1 #-}

pSchemaVariantV1 :: Aeson.Value -> Aeson.Parser (Variant Schema.Column)
pSchemaVariantV1 =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withStructField "name" o (fmap VariantName . pText)
      <*> withStructField "schema" o pColumnSchemaV1
{-# INLINABLE pSchemaVariantV1 #-}

pSchemaStructFieldsV1 :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Field Schema.Column))
pSchemaStructFieldsV1 =
  Aeson.withArray "array of struct fields" $ \xs -> do
    fs0 <- kmapM pSchemaFieldV1 xs
    case Boxed.uncons fs0 of
      Nothing ->
        fail "structs must have at least one field"
      Just (f0, fs) ->
        pure $ Cons.from f0 fs
{-# INLINABLE pSchemaStructFieldsV1 #-}

pSchemaFieldV1 :: Aeson.Value -> Aeson.Parser (Field Schema.Column)
pSchemaFieldV1 =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withStructField "name" o (fmap FieldName . pText)
      <*> withStructField "schema" o pColumnSchemaV1
{-# INLINABLE pSchemaFieldV1 #-}

ppSchemaVariantV1 :: Variant Schema.Column -> Aeson.Value
ppSchemaVariantV1 (Variant (VariantName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "schema" $
        ppColumnSchemaV1 schema
    ]
{-# INLINABLE ppSchemaVariantV1 #-}

ppSchemaFieldV1 :: Field Schema.Column -> Aeson.Value
ppSchemaFieldV1 (Field (FieldName name) schema) =
  ppStruct [
      Field "name" $
        Aeson.String name
    , Field "schema" $
        ppColumnSchemaV1 schema
    ]
{-# INLINABLE ppSchemaFieldV1 #-}
