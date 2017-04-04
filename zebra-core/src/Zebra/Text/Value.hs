{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
module Zebra.Text.Value (
    encodeCollection
  , decodeCollection

  , TextValueEncodeError(..)
  , renderTextValueEncodeError

  , TextValueDecodeError(..)
  , renderTextValueDecodeError
  ) where

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           P

import           Zebra.Json.Codec
import           Zebra.Json.Value (JsonValueEncodeError, renderJsonValueEncodeError)
import           Zebra.Json.Value (ppValue, ppPair, pValue, pPair)
import           Zebra.Schema (TableSchema)
import qualified Zebra.Schema as Schema
import           Zebra.Value (Collection(..))
import qualified Zebra.Value as Value


data TextValueEncodeError =
    TextCollectionSchemaMismatch !TableSchema !Collection
  | TextValueEncodeError !JsonValueEncodeError
    deriving (Eq, Show)

data TextValueDecodeError =
    TextValueDecodeError !JsonDecodeError
    deriving (Eq, Show)

renderTextValueEncodeError :: TextValueEncodeError -> Text
renderTextValueEncodeError = \case
  TextCollectionSchemaMismatch schema collection ->
    "Error processing collection, schema did not match:" <>
    Value.renderField "collection" collection <>
    Value.renderField "schema" schema

  TextValueEncodeError err ->
    renderJsonValueEncodeError err

renderTextValueDecodeError :: TextValueDecodeError -> Text
renderTextValueDecodeError = \case
  TextValueDecodeError err ->
    renderJsonDecodeError err

encodeJsonRows :: [Text] -> Boxed.Vector Aeson.Value -> ByteString
encodeJsonRows keyOrder =
  Char8.unlines . Boxed.toList . fmap (encodeJson keyOrder)

encodeCollection :: TableSchema -> Collection -> Either TextValueEncodeError ByteString
encodeCollection schema collection0 =
  case schema of
    Schema.Binary
      | Binary bs <- collection0
      ->
        pure bs

    Schema.Array element
      | Array xs <- collection0
      ->
        first TextValueEncodeError $
          encodeJsonRows ["key"] <$> traverse (ppValue element) xs

    Schema.Map kschema vschema
      | Map kvs <- collection0
      ->
        first TextValueEncodeError $ do
          ks <- traverse (ppValue kschema) $ Map.keys kvs
          vs <- traverse (ppValue vschema) $ Map.elems kvs
          pure . encodeJsonRows ["key"] . Boxed.fromList $
            List.zipWith ppPair ks vs

    _ ->
      Left $ TextCollectionSchemaMismatch schema collection0

decodeJsonRows :: (Aeson.Value -> Aeson.Parser a) -> ByteString -> Either JsonDecodeError (Boxed.Vector a)
decodeJsonRows p =
  traverse (decodeJson p) . Boxed.fromList . Char8.lines

decodeCollection :: TableSchema -> ByteString -> Either TextValueDecodeError Collection
decodeCollection schema bs =
  case schema of
    Schema.Binary ->
      pure $ Binary bs

    Schema.Array element ->
      first TextValueDecodeError $
        Array <$> decodeJsonRows (pValue element) bs

    Schema.Map kschema vschema ->
      first TextValueDecodeError $
        Map . Map.fromList . Boxed.toList <$> decodeJsonRows (pPair kschema vschema) bs
