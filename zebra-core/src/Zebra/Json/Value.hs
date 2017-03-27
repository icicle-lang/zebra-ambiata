{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
module Zebra.Json.Value (
    encodeCollection
  , encodeValue

  , ppCollection
  , ppPair
  , ppValue

  , JsonValueEncodeError(..)
  , renderJsonValueEncodeError
  ) where

import qualified Data.Aeson as Aeson
import           Data.ByteString (ByteString)
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed

import           P

import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Json.Codec
import           Zebra.Schema (TableSchema, ColumnSchema, Field(..))
import qualified Zebra.Schema as Schema
import           Zebra.Value (Collection(..), Value(..))
import qualified Zebra.Value as Value


data JsonValueEncodeError =
    JsonValueEncodeError !JsonEncodeError
  | JsonCollectionSchemaMismatch !TableSchema !Collection
  | JsonValueSchemaMismatch !ColumnSchema !Value
    deriving (Eq, Show)

renderJsonValueEncodeError :: JsonValueEncodeError -> Text
renderJsonValueEncodeError = \case
  JsonValueEncodeError err ->
    renderJsonEncodeError err

  JsonCollectionSchemaMismatch schema collection ->
    "Error processing collection, schema did not match:" <>
    Value.renderField "collection" collection <>
    Value.renderField "schema" schema

  JsonValueSchemaMismatch schema value ->
    "Error processing value, schema did not match:" <>
    Value.renderField "value" value <>
    Value.renderField "schema" schema

encodeCollection :: TableSchema -> Collection -> Either JsonValueEncodeError ByteString
encodeCollection schema collection =
  first JsonValueEncodeError . encodeJsonRows =<< ppCollection schema collection

encodeValue :: ColumnSchema -> Value -> Either JsonValueEncodeError ByteString
encodeValue schema value =
  encodeJson <$> ppValue schema value

ppCollection :: TableSchema -> Collection -> Either JsonValueEncodeError Aeson.Value
ppCollection schema collection0 =
  case schema of
    Schema.Binary
      | Binary bs <- collection0
      ->
        -- FIXME we need some metadata in the schema to say this is ok
        pure . Aeson.String $ Text.decodeUtf8 bs

    Schema.Array element
      | Array values <- collection0
      ->
        fmap Aeson.Array $
          traverse (ppValue element) values

    Schema.Map kschema vschema
      | Map kvs <- collection0
      -> do
        ks <- traverse (ppValue kschema) $ Map.keys kvs
        vs <- traverse (ppValue vschema) $ Map.elems kvs
        pure . Aeson.Array . Boxed.fromList $
          List.zipWith ppPair ks vs

    _ ->
      Left $ JsonCollectionSchemaMismatch schema collection0

ppPair :: Aeson.Value -> Aeson.Value -> Aeson.Value
ppPair key value =
  ppStruct [
      Field "key" key
    , Field "value" value
    ]

ppValue :: ColumnSchema -> Value -> Either JsonValueEncodeError Aeson.Value
ppValue schema value0 =
  case schema of
    Schema.Unit
      | Unit <- value0
      ->
        pure ppUnit

    Schema.Int
      | Int x <- value0
      ->
        pure $ ppInt x

    Schema.Double
      | Double x <- value0
      ->
        pure $ ppDouble x

    Schema.Enum variants
      | Enum tag x <- value0
      , Just variant <- Schema.lookupVariant tag variants
      ->
        ppEnum <$> traverse (flip ppValue x) variant

    Schema.Struct fields
      | Struct values <- value0
      , Cons.length fields == Cons.length values
      -> do
        ppStruct . Cons.toList <$>
          Cons.zipWithM (\f v -> traverse (\s -> ppValue s v) f) fields values

    Schema.Nested snested
      | Nested vnested <- value0
      ->
        ppCollection snested vnested

    Schema.Reversed sreversed
      | Reversed vreversed <- value0
      ->
        ppValue sreversed vreversed

    _ ->
      Left $ JsonValueSchemaMismatch schema value0
