{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
module Zebra.Serial.Text.Logical (
    encodeLogical
  , decodeLogical

  , TextLogicalEncodeError(..)
  , renderTextValueEncodeError

  , TextLogicalDecodeError(..)
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

import           Zebra.Serial.Json.Logical (JsonLogicalEncodeError, renderJsonLogicalEncodeError)
import           Zebra.Serial.Json.Logical (ppValue, ppPair, pValue, pPair)
import           Zebra.Serial.Json.Util
import           Zebra.Table.Encoding (Utf8Error)
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema


data TextLogicalEncodeError =
    TextLogicalSchemaMismatch !Schema.Table !Logical.Table
  | TextLogicalEncodeError !JsonLogicalEncodeError
  | TextLogicalEncodeUtf8 !Utf8Error
    deriving (Eq, Show)

data TextLogicalDecodeError =
    TextLogicalDecodeError !JsonDecodeError
  | TextLogicalDecodeUtf8 !Utf8Error
    deriving (Eq, Show)

renderTextValueEncodeError :: TextLogicalEncodeError -> Text
renderTextValueEncodeError = \case
  TextLogicalSchemaMismatch schema table ->
    "Error processing table, schema did not match:" <>
    Logical.renderField "table" table <>
    Logical.renderField "schema" schema

  TextLogicalEncodeError err ->
    renderJsonLogicalEncodeError err

  TextLogicalEncodeUtf8 err ->
    "Error encoding UTF-8 binary: " <>
    Encoding.renderUtf8Error err

renderTextValueDecodeError :: TextLogicalDecodeError -> Text
renderTextValueDecodeError = \case
  TextLogicalDecodeError err ->
    renderJsonDecodeError err

  TextLogicalDecodeUtf8 err ->
    "Error decoding UTF-8 binary: " <>
    Encoding.renderUtf8Error err

encodeJsonRows :: [Text] -> Boxed.Vector Aeson.Value -> ByteString
encodeJsonRows keyOrder =
  Char8.unlines . Boxed.toList . fmap (encodeJson keyOrder)

encodeLogical :: Schema.Table -> Logical.Table -> Either TextLogicalEncodeError ByteString
encodeLogical schema table0 =
  case schema of
    Schema.Binary encoding
      | Logical.Binary bs <- table0
      -> do
        () <- first TextLogicalEncodeUtf8 $ Encoding.validateBinary encoding bs
        pure bs

    Schema.Array element
      | Logical.Array xs <- table0
      ->
        first TextLogicalEncodeError $
          encodeJsonRows ["key"] <$> traverse (ppValue element) xs

    Schema.Map kschema vschema
      | Logical.Map kvs <- table0
      ->
        first TextLogicalEncodeError $ do
          ks <- traverse (ppValue kschema) $ Map.keys kvs
          vs <- traverse (ppValue vschema) $ Map.elems kvs
          pure . encodeJsonRows ["key"] . Boxed.fromList $
            List.zipWith ppPair ks vs

    _ ->
      Left $ TextLogicalSchemaMismatch schema table0

decodeJsonRows :: (Aeson.Value -> Aeson.Parser a) -> ByteString -> Either JsonDecodeError (Boxed.Vector a)
decodeJsonRows p =
  traverse (decodeJson p) . Boxed.fromList . Char8.lines

decodeLogical :: Schema.Table -> ByteString -> Either TextLogicalDecodeError Logical.Table
decodeLogical schema bs =
  case schema of
    Schema.Binary encoding -> do
      () <- first TextLogicalDecodeUtf8 $ Encoding.validateBinary encoding bs
      pure $ Logical.Binary bs

    Schema.Array element ->
      first TextLogicalDecodeError $
        Logical.Array <$> decodeJsonRows (pValue element) bs

    Schema.Map kschema vschema ->
      first TextLogicalDecodeError $
        Logical.Map . Map.fromList . Boxed.toList <$> decodeJsonRows (pPair kschema vschema) bs
