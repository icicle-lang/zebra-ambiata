{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Json.Table (
    encodeTable
  , decodeTable

  , JsonTableEncodeError(..)
  , renderJsonTableEncodeError

  , JsonTableDecodeError(..)
  , renderJsonTableDecodeError
  ) where

import           Data.ByteString (ByteString)

import           P

import           Zebra.Json.Value
import           Zebra.Schema (TableSchema)
import           Zebra.Table (Table(..), TableError)
import qualified Zebra.Table as Table

data JsonTableEncodeError =
    JsonTableEncodeError !TableError
  | JsonTableValueEncodeError !JsonValueEncodeError
    deriving (Eq, Show)

data JsonTableDecodeError =
    JsonTableDecodeError !TableError
  | JsonTableValueDecodeError !JsonValueDecodeError
    deriving (Eq, Show)

renderJsonTableEncodeError :: JsonTableEncodeError -> Text
renderJsonTableEncodeError = \case
  JsonTableEncodeError err ->
    Table.renderTableError err
  JsonTableValueEncodeError err ->
    renderJsonValueEncodeError err

renderJsonTableDecodeError :: JsonTableDecodeError -> Text
renderJsonTableDecodeError = \case
  JsonTableDecodeError err ->
    Table.renderTableError err
  JsonTableValueDecodeError err ->
    renderJsonValueDecodeError err

encodeTable :: Table -> Either JsonTableEncodeError ByteString
encodeTable table = do
  collection <- first JsonTableEncodeError $ Table.toCollection table
  first JsonTableValueEncodeError $ encodeCollection (Table.schema table) collection

decodeTable :: TableSchema -> ByteString -> Either JsonTableDecodeError Table
decodeTable schema bs = do
  collection <- first JsonTableValueDecodeError $ decodeCollection schema bs
  first JsonTableDecodeError $ Table.fromCollection schema collection
