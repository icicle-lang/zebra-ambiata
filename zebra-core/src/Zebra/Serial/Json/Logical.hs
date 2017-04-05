{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
module Zebra.Serial.Json.Logical (
    encodeLogical
  , encodeLogicalValue

  , decodeLogical
  , decodeLogicalValue

  , pTable
  , pPair
  , pValue

  , ppTable
  , ppPair
  , ppValue

  , JsonLogicalEncodeError(..)
  , renderJsonLogicalEncodeError

  , JsonLogicalDecodeError(..)
  , renderJsonLogicalDecodeError
  ) where

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.List as List
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           P

import           Zebra.Serial.Json.Util
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (Field(..), Variant(..), VariantName(..))
import           Zebra.Table.Schema (TableSchema, ColumnSchema)
import qualified Zebra.Table.Schema as Schema
import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons


data JsonLogicalEncodeError =
    JsonLogicalTableSchemaMismatch !TableSchema !Logical.Table
  | JsonLogicalValueSchemaMismatch !ColumnSchema !Logical.Value
    deriving (Eq, Show)

data JsonLogicalDecodeError =
    JsonLogicalDecodeError !JsonDecodeError
    deriving (Eq, Show)

renderJsonLogicalEncodeError :: JsonLogicalEncodeError -> Text
renderJsonLogicalEncodeError = \case
  JsonLogicalTableSchemaMismatch schema table ->
    "Error processing logical table, schema did not match:" <>
    Logical.renderField "table" table <>
    Logical.renderField "schema" schema

  JsonLogicalValueSchemaMismatch schema value ->
    "Error processing logical value, schema did not match:" <>
    Logical.renderField "value" value <>
    Logical.renderField "schema" schema

renderJsonLogicalDecodeError :: JsonLogicalDecodeError -> Text
renderJsonLogicalDecodeError = \case
  JsonLogicalDecodeError err ->
    renderJsonDecodeError err

encodeLogical :: TableSchema -> Logical.Table -> Either JsonLogicalEncodeError ByteString
encodeLogical schema value =
  encodeJson ["key"] <$> ppTable schema value

encodeLogicalValue :: ColumnSchema -> Logical.Value -> Either JsonLogicalEncodeError ByteString
encodeLogicalValue schema value =
  encodeJson ["key"] <$> ppValue schema value

decodeLogical :: TableSchema -> ByteString -> Either JsonLogicalDecodeError Logical.Table
decodeLogical schema =
  first JsonLogicalDecodeError . decodeJson (pTable schema)

decodeLogicalValue :: ColumnSchema -> ByteString -> Either JsonLogicalDecodeError Logical.Value
decodeLogicalValue schema =
  first JsonLogicalDecodeError . decodeJson (pValue schema)

pTable :: TableSchema -> Aeson.Value -> Aeson.Parser Logical.Table
pTable schema =
  case schema of
    Schema.Binary ->
      fmap Logical.Binary . pBinary

    Schema.Array element ->
      fmap Logical.Array .
      Aeson.withArray "array containing values" (kmapM $ pValue element)

    Schema.Map kschema vschema ->
      fmap (Logical.Map . Map.fromList . Boxed.toList) .
      Aeson.withArray "array containing key/value pairs" (kmapM $ pPair kschema vschema)

ppTable :: TableSchema -> Logical.Table -> Either JsonLogicalEncodeError Aeson.Value
ppTable schema table0 =
  case schema of
    Schema.Binary
      | Logical.Binary bs <- table0
      ->
        pure $ ppBinary bs

    Schema.Array element
      | Logical.Array xs <- table0
      ->
        Aeson.Array <$> traverse (ppValue element) xs

    Schema.Map kschema vschema
      | Logical.Map kvs <- table0
      -> do
        ks <- traverse (ppValue kschema) $ Map.keys kvs
        vs <- traverse (ppValue vschema) $ Map.elems kvs
        pure . Aeson.Array . Boxed.fromList $
          List.zipWith ppPair ks vs

    _ ->
      Left $ JsonLogicalTableSchemaMismatch schema table0

pPair :: ColumnSchema -> ColumnSchema -> Aeson.Value -> Aeson.Parser (Logical.Value, Logical.Value)
pPair kschema vschema =
  Aeson.withObject "object containing key/value pair" $ \o ->
    (,)
      <$> withStructField "key" o (pValue kschema)
      <*> withStructField "value" o (pValue vschema)

ppPair :: Aeson.Value -> Aeson.Value -> Aeson.Value
ppPair key value =
  ppStruct [
      Field "key" key
    , Field "value" value
    ]

pValue :: ColumnSchema -> Aeson.Value -> Aeson.Parser Logical.Value
pValue schema =
  case schema of
    Schema.Unit ->
      (Logical.Unit <$) . pUnit

    Schema.Int ->
      fmap Logical.Int . pInt

    Schema.Double ->
      fmap Logical.Double . pDouble

    Schema.Enum variants0 ->
      let
        variants =
          mkVariantMap variants0
      in
        pEnum $ flip Map.lookup variants

    Schema.Struct fields0 ->
      Aeson.withObject "object containing a struct" $ \o ->
        fmap Logical.Struct . for fields0 $ \(Field name fschema) ->
          withStructField name o (pValue fschema)

    Schema.Nested snested ->
      fmap Logical.Nested . pTable snested

    Schema.Reversed sreversed ->
      fmap Logical.Reversed . pValue sreversed

mkVariantMap :: Cons Boxed.Vector (Variant ColumnSchema) -> Map VariantName (Aeson.Value -> Aeson.Parser Logical.Value)
mkVariantMap =
  let
    fromVariant (tag, Variant name schema) =
      (name, fmap (Logical.Enum tag) . pValue schema)
  in
    Map.fromList . fmap fromVariant . List.zip [0..] . Cons.toList

ppValue :: ColumnSchema -> Logical.Value -> Either JsonLogicalEncodeError Aeson.Value
ppValue schema value0 =
  case schema of
    Schema.Unit
      | Logical.Unit <- value0
      ->
        pure ppUnit

    Schema.Int
      | Logical.Int x <- value0
      ->
        pure $ ppInt x

    Schema.Double
      | Logical.Double x <- value0
      ->
        pure $ ppDouble x

    Schema.Enum variants
      | Logical.Enum tag x <- value0
      , Just var <- Schema.lookupVariant tag variants
      ->
        ppEnum <$> traverse (flip ppValue x) var

    Schema.Struct fields
      | Logical.Struct values <- value0
      , Cons.length fields == Cons.length values
      -> do
        ppStruct . Cons.toList <$>
          Cons.zipWithM (\f v -> traverse (\s -> ppValue s v) f) fields values

    Schema.Nested snested
      | Logical.Nested vnested <- value0
      ->
        ppTable snested vnested

    Schema.Reversed sreversed
      | Logical.Reversed vreversed <- value0
      ->
        ppValue sreversed vreversed

    _ ->
      Left $ JsonLogicalValueSchemaMismatch schema value0
