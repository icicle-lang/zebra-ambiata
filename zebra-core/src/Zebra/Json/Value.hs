{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
module Zebra.Json.Value (
    encodeCollection
  , encodeValue

  , decodeCollection
  , decodeValue

  , pCollection
  , pPair
  , pValue

  , ppCollection
  , ppPair
  , ppValue

  , JsonValueEncodeError(..)
  , renderJsonValueEncodeError

  , JsonValueDecodeError(..)
  , renderJsonValueDecodeError
  ) where

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.List as List
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text
import qualified Data.Vector as Boxed

import           P

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Json.Codec
import           Zebra.Schema (Field(..), Variant(..), VariantName(..))
import           Zebra.Schema (TableSchema, ColumnSchema)
import qualified Zebra.Schema as Schema
import           Zebra.Value (Collection(..), Value(..))
import qualified Zebra.Value as Value


data JsonValueEncodeError =
    JsonCannotEncodeBinaryToRows !ByteString
  | JsonCollectionSchemaMismatch !TableSchema !Collection
  | JsonValueSchemaMismatch !ColumnSchema !Value
    deriving (Eq, Show)

data JsonValueDecodeError =
    JsonCannotDecodeRowsToBinary
  | JsonValueDecodeError !JsonDecodeError
    deriving (Eq, Show)

renderJsonValueEncodeError :: JsonValueEncodeError -> Text
renderJsonValueEncodeError = \case
  JsonCannotEncodeBinaryToRows bs ->
    "Cannot encode binary value as rows: " <>
    renderSnippet 30 (show bs)

  JsonCollectionSchemaMismatch schema collection ->
    "Error processing collection, schema did not match:" <>
    Value.renderField "collection" collection <>
    Value.renderField "schema" schema

  JsonValueSchemaMismatch schema value ->
    "Error processing value, schema did not match:" <>
    Value.renderField "value" value <>
    Value.renderField "schema" schema

renderJsonValueDecodeError :: JsonValueDecodeError -> Text
renderJsonValueDecodeError = \case
  JsonCannotDecodeRowsToBinary ->
    "Cannot decode binary value, it cannot be stored as json rows."

  JsonValueDecodeError err ->
    renderJsonDecodeError err

renderSnippet :: Int -> [Char] -> Text
renderSnippet n xs =
  let
    snippet =
      Text.pack (List.take (n + 1) xs)
  in
    if Text.length snippet == n + 1 then
      Text.take n snippet <> "..."
    else
      Text.take n snippet

encodeValue :: ColumnSchema -> Value -> Either JsonValueEncodeError ByteString
encodeValue schema value =
  encodeJson ["key"] <$> ppValue schema value

decodeValue :: ColumnSchema -> ByteString -> Either JsonDecodeError Value
decodeValue schema =
  decodeJson (pValue schema)

encodeJsonRows :: [Text] -> Boxed.Vector Aeson.Value -> ByteString
encodeJsonRows keyOrder =
  Char8.unlines . Boxed.toList . fmap (encodeJson keyOrder)

encodeCollection :: TableSchema -> Collection -> Either JsonValueEncodeError ByteString
encodeCollection schema collection0 =
  case schema of
    Schema.Binary
      | Binary bs <- collection0
      ->
        Left $ JsonCannotEncodeBinaryToRows bs

    Schema.Array element
      | Array xs <- collection0
      ->
        encodeJsonRows ["key"] <$> traverse (ppValue element) xs

    Schema.Map kschema vschema
      | Map kvs <- collection0
      -> do
        ks <- traverse (ppValue kschema) $ Map.keys kvs
        vs <- traverse (ppValue vschema) $ Map.elems kvs
        pure . encodeJsonRows ["key"] . Boxed.fromList $
          List.zipWith ppPair ks vs

    _ ->
      Left $ JsonCollectionSchemaMismatch schema collection0

decodeJsonRows :: (Aeson.Value -> Aeson.Parser a) -> ByteString -> Either JsonDecodeError (Boxed.Vector a)
decodeJsonRows p =
  traverse (decodeJson p) . Boxed.fromList . Char8.lines

decodeCollection :: TableSchema -> ByteString -> Either JsonValueDecodeError Collection
decodeCollection schema bs =
  case schema of
    Schema.Binary ->
      Left JsonCannotDecodeRowsToBinary

    Schema.Array element ->
      first JsonValueDecodeError $
        Array <$> decodeJsonRows (pValue element) bs

    Schema.Map kschema vschema ->
      first JsonValueDecodeError $
        Map . Map.fromList . Boxed.toList <$> decodeJsonRows (pPair kschema vschema) bs

pCollection :: TableSchema -> Aeson.Value -> Aeson.Parser Collection
pCollection schema =
  case schema of
    Schema.Binary ->
      fmap Binary . pBinary

    Schema.Array element ->
      fmap Array .
      Aeson.withArray "array containing values" (kmapM $ pValue element)

    Schema.Map kschema vschema ->
      fmap (Map . Map.fromList . Boxed.toList) .
      Aeson.withArray "array containing key/value pairs" (kmapM $ pPair kschema vschema)

ppCollection :: TableSchema -> Collection -> Either JsonValueEncodeError Aeson.Value
ppCollection schema collection0 =
  case schema of
    Schema.Binary
      | Binary bs <- collection0
      ->
        pure $ ppBinary bs

    Schema.Array element
      | Array xs <- collection0
      ->
        Aeson.Array <$> traverse (ppValue element) xs

    Schema.Map kschema vschema
      | Map kvs <- collection0
      -> do
        ks <- traverse (ppValue kschema) $ Map.keys kvs
        vs <- traverse (ppValue vschema) $ Map.elems kvs
        pure . Aeson.Array . Boxed.fromList $
          List.zipWith ppPair ks vs

    _ ->
      Left $ JsonCollectionSchemaMismatch schema collection0

pPair :: ColumnSchema -> ColumnSchema -> Aeson.Value -> Aeson.Parser (Value, Value)
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

pValue :: ColumnSchema -> Aeson.Value -> Aeson.Parser Value
pValue schema =
  case schema of
    Schema.Unit ->
      (Unit <$) . pUnit

    Schema.Int ->
      fmap Int . pInt

    Schema.Double ->
      fmap Double . pDouble

    Schema.Enum variants0 ->
      let
        variants =
          mkVariantMap variants0
      in
        pEnum $ flip Map.lookup variants

    Schema.Struct fields0 ->
      Aeson.withObject "object containing a struct" $ \o ->
        fmap Struct . for fields0 $ \(Field name fschema) ->
          withStructField name o (pValue fschema)

    Schema.Nested snested ->
      fmap Nested . pCollection snested

    Schema.Reversed sreversed ->
      fmap Reversed . pValue sreversed

mkVariantMap :: Cons Boxed.Vector (Variant ColumnSchema) -> Map VariantName (Aeson.Value -> Aeson.Parser Value)
mkVariantMap =
  let
    fromVariant (tag, Variant name schema) =
      (name, fmap (Enum tag) . pValue schema)
  in
    Map.fromList . fmap fromVariant . List.zip [0..] . Cons.toList

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
      , Just var <- Schema.lookupVariant tag variants
      ->
        ppEnum <$> traverse (flip ppValue x) var

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
