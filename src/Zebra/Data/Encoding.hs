{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Encoding (
  -- * Data
    Encoding(..)
  , ColumnEncoding(..)

  -- * Text Conversion
  , renderEncoding
  , renderColumnEncoding
  , parseEncoding
  , parseColumnEncoding

  -- * Attoparsec Parsers
  , pEncoding
  , pColumnEncoding

  -- * Dictionary Translation
  , encodingOfDictionary
  , encodingOfSchema

  -- * Recover lossy schema from encoding
  , recoverSchemaOfEncoding
  ) where

import qualified Data.Attoparsec.Text as Atto
import qualified Data.List as List
import           Data.Map (Map)
import qualified Data.Text as T

import           GHC.Generics (Generic)

import           P

import           Text.Printf (printf)

import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Core
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Schema (TableSchema, ColumnSchema, Field(..), FieldName(..))
import qualified Zebra.Schema as Schema


newtype Encoding =
  Encoding {
      unEncoding :: [ColumnEncoding]
    } deriving (Eq, Ord, Monoid, Generic)

instance Show Encoding where
  showsPrec =
    gshowsPrec

data ColumnEncoding =
    ByteEncoding
  | IntEncoding
  | DoubleEncoding
  | ArrayEncoding !Encoding
    deriving (Eq, Ord, Show)

-- | Render an encoding as a string. The encoding string is run of characters
--   which describes the physical structure of flattened data as arrays:
--
-- @
--   b   - byte
--   i   - int
--   d   - double
--   [?] - array
-- @
--
renderEncoding :: Encoding -> Text
renderEncoding =
  foldMap renderColumnEncoding . unEncoding

renderColumnEncoding :: ColumnEncoding -> Text
renderColumnEncoding = \case
  ByteEncoding ->
    "b"
  IntEncoding ->
    "i"
  DoubleEncoding ->
    "d"
  ArrayEncoding s ->
    "[" <> renderEncoding s <> "]"

parseEncoding :: Text -> Maybe Encoding
parseEncoding =
  rightToMaybe . Atto.parseOnly (pEncoding <* Atto.endOfInput)

parseColumnEncoding :: Text -> Maybe ColumnEncoding
parseColumnEncoding =
  rightToMaybe . Atto.parseOnly (pColumnEncoding <* Atto.endOfInput)

pEncoding :: Atto.Parser Encoding
pEncoding =
  Encoding <$> many pColumnEncoding

pColumnEncoding :: Atto.Parser ColumnEncoding
pColumnEncoding =
  Atto.choice [
      ByteEncoding <$ Atto.char 'b'
    , IntEncoding <$ Atto.char 'i'
    , DoubleEncoding <$ Atto.char 'd'
    , ArrayEncoding <$> (Atto.char '[' *> pEncoding <* Atto.char ']')
    ]

encodingOfDictionary :: Map AttributeName TableSchema -> Map AttributeName Encoding
encodingOfDictionary =
  fmap encodingOfSchema

encodingOfSchema :: TableSchema -> Encoding
encodingOfSchema = \case
  Schema.Binary ->
    Encoding (pure . ArrayEncoding . Encoding $ pure ByteEncoding)
  Schema.Array schema ->
    Encoding (pure . ArrayEncoding $ encodingOfColumn schema)
  Schema.Map key value ->
    Encoding (pure . ArrayEncoding $ encodingOfColumn key <> encodingOfColumn value)

encodingOfColumn :: ColumnSchema -> Encoding
encodingOfColumn = \case
  Schema.Unit ->
    Encoding []
  Schema.Int ->
    Encoding $ pure IntEncoding
  Schema.Double ->
    Encoding $ pure DoubleEncoding
  Schema.Enum variants ->
    Encoding (pure IntEncoding) <>
    foldMap (encodingOfColumn . Schema.variant) variants
  Schema.Struct fields ->
    foldMap (encodingOfColumn . Schema.field) fields
  Schema.Nested schema ->
    encodingOfSchema schema
  Schema.Reversed schema ->
    encodingOfColumn schema

-- Best effort to recover an schema
recoverSchemaOfEncoding :: Encoding -> TableSchema
recoverSchemaOfEncoding (Encoding encodings) =
  Schema.Array $ 
  case recoverSchemaOfColumns encodings of
    [] ->
      Schema.Unit
    [x] ->
      x
    xs ->
      let
        digits =
          length (show $ length xs)

        format =
          "%0" <> show digits <> "d"

        mkField :: Int -> ColumnSchema -> Field ColumnSchema
        mkField i x =
          Field (FieldName . T.pack $ printf format i) x
      in
        Schema.Struct .
        Cons.unsafeFromList $
        List.zipWith mkField [0..] xs

recoverSchemaOfColumns :: [ColumnEncoding] -> [ColumnSchema]
recoverSchemaOfColumns = \case
  [] ->
    []
  IntEncoding : xs ->
    Schema.Int : recoverSchemaOfColumns xs
  DoubleEncoding : xs ->
    Schema.Double : recoverSchemaOfColumns xs
  ArrayEncoding (Encoding [ByteEncoding]) : xs -> do
    Schema.Nested Schema.Binary : recoverSchemaOfColumns xs
  ArrayEncoding encoding : xs -> do
    Schema.Nested (recoverSchemaOfEncoding encoding) : recoverSchemaOfColumns xs

  -- TODO this is no longer possible, maybe error?
  ByteEncoding : xs ->
    Schema.Int : recoverSchemaOfColumns xs
