{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Json.Table (
    encodeTable

  , JsonTableEncodeError(..)
  , renderJsonTableEncodeError
  ) where

import           Data.ByteString (ByteString)

import           P

import           Zebra.Json.Value
import           Zebra.Table (Table(..), TableError)
import qualified Zebra.Table as Table

data JsonTableEncodeError =
    JsonTableError !TableError
  | JsonTableValueEncodeError !JsonValueEncodeError
    deriving (Eq, Show)

renderJsonTableEncodeError :: JsonTableEncodeError -> Text
renderJsonTableEncodeError = \case
  JsonTableError err ->
    Table.renderTableError err
  JsonTableValueEncodeError err ->
    renderJsonValueEncodeError err

encodeTable :: Table -> Either JsonTableEncodeError ByteString
encodeTable table = do
  collection <- first JsonTableError $ Table.toCollection table
  first JsonTableValueEncodeError $ encodeCollection (Table.schema table) collection
