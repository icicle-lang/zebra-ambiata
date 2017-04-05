{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Json.Striped (
    encodeStriped
  , decodeStriped

  , JsonStripedEncodeError(..)
  , renderJsonStripedEncodeError

  , JsonStripedDecodeError(..)
  , renderJsonStripedDecodeError
  ) where

import           Data.ByteString (ByteString)

import           P

import           Zebra.Serial.Json.Logical
import           Zebra.Table.Schema (TableSchema)
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped

data JsonStripedEncodeError =
    JsonStripedEncodeError !StripedError
  | JsonStripedLogicalEncodeError !JsonLogicalEncodeError
    deriving (Eq, Show)

data JsonStripedDecodeError =
    JsonStripedDecodeError !StripedError
  | JsonStripedLogicalDecodeError !JsonLogicalDecodeError
    deriving (Eq, Show)

renderJsonStripedEncodeError :: JsonStripedEncodeError -> Text
renderJsonStripedEncodeError = \case
  JsonStripedEncodeError err ->
    Striped.renderStripedError err
  JsonStripedLogicalEncodeError err ->
    renderJsonLogicalEncodeError err

renderJsonStripedDecodeError :: JsonStripedDecodeError -> Text
renderJsonStripedDecodeError = \case
  JsonStripedDecodeError err ->
    Striped.renderStripedError err
  JsonStripedLogicalDecodeError err ->
    renderJsonLogicalDecodeError err

encodeStriped :: Striped.Table -> Either JsonStripedEncodeError ByteString
encodeStriped striped = do
  logical <- first JsonStripedEncodeError $ Striped.toLogical striped
  first JsonStripedLogicalEncodeError $ encodeLogical (Striped.schema striped) logical

decodeStriped :: TableSchema -> ByteString -> Either JsonStripedDecodeError Striped.Table
decodeStriped schema bs = do
  logical <- first JsonStripedLogicalDecodeError $ decodeLogical schema bs
  first JsonStripedDecodeError $ Striped.fromLogical schema logical
