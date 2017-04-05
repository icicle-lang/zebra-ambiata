{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Text.Striped (
    encodeStriped
  , decodeStriped

  , TextStripedEncodeError(..)
  , renderTextStripedEncodeError

  , TextStripedDecodeError(..)
  , renderTextStripedDecodeError
  ) where

import           Data.ByteString (ByteString)

import           P

import           Zebra.Serial.Text.Logical
import           Zebra.Table.Schema (TableSchema)
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped

data TextStripedEncodeError =
    TextStripedEncodeError !StripedError
  | TextStripedLogicalEncodeError !TextLogicalEncodeError
    deriving (Eq, Show)

data TextStripedDecodeError =
    TextStripedDecodeError !StripedError
  | TextStripedLogicalDecodeError !TextLogicalDecodeError
    deriving (Eq, Show)

renderTextStripedEncodeError :: TextStripedEncodeError -> Text
renderTextStripedEncodeError = \case
  TextStripedEncodeError err ->
    Striped.renderStripedError err
  TextStripedLogicalEncodeError err ->
    renderTextValueEncodeError err

renderTextStripedDecodeError :: TextStripedDecodeError -> Text
renderTextStripedDecodeError = \case
  TextStripedDecodeError err ->
    Striped.renderStripedError err
  TextStripedLogicalDecodeError err ->
    renderTextValueDecodeError err

encodeStriped :: Striped.Table -> Either TextStripedEncodeError ByteString
encodeStriped striped = do
  logical <- first TextStripedEncodeError $ Striped.toLogical striped
  first TextStripedLogicalEncodeError $ encodeLogical (Striped.schema striped) logical

decodeStriped :: TableSchema -> ByteString -> Either TextStripedDecodeError Striped.Table
decodeStriped schema bs = do
  logical <- first TextStripedLogicalDecodeError $ decodeLogical schema bs
  first TextStripedDecodeError $ Striped.fromLogical schema logical
