{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Text.Table (
    encodeTable
  , decodeTable

  , TextTableEncodeError(..)
  , renderTextTableEncodeError

  , TextTableDecodeError(..)
  , renderTextTableDecodeError
  ) where

import           Data.ByteString (ByteString)

import           P

import           Zebra.Schema (TableSchema)
import           Zebra.Table (Table(..), TableError)
import qualified Zebra.Table as Table
import           Zebra.Text.Value

data TextTableEncodeError =
    TextTableEncodeError !TableError
  | TextTableValueEncodeError !TextValueEncodeError
    deriving (Eq, Show)

data TextTableDecodeError =
    TextTableDecodeError !TableError
  | TextTableValueDecodeError !TextValueDecodeError
    deriving (Eq, Show)

renderTextTableEncodeError :: TextTableEncodeError -> Text
renderTextTableEncodeError = \case
  TextTableEncodeError err ->
    Table.renderTableError err
  TextTableValueEncodeError err ->
    renderTextValueEncodeError err

renderTextTableDecodeError :: TextTableDecodeError -> Text
renderTextTableDecodeError = \case
  TextTableDecodeError err ->
    Table.renderTableError err
  TextTableValueDecodeError err ->
    renderTextValueDecodeError err

encodeTable :: Table -> Either TextTableEncodeError ByteString
encodeTable table = do
  collection <- first TextTableEncodeError $ Table.toCollection table
  first TextTableValueEncodeError $ encodeCollection (Table.schema table) collection

decodeTable :: TableSchema -> ByteString -> Either TextTableDecodeError Table
decodeTable schema bs = do
  collection <- first TextTableValueDecodeError $ decodeCollection schema bs
  first TextTableDecodeError $ Table.fromCollection schema collection
