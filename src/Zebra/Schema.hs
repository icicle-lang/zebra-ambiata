{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Schema (
  -- * Data
    Schema(..)
  , Format(..)

  -- * Text Conversion
  , renderSchema
  , parseSchema

  -- * Attoparsec Parsers
  , pSchema
  , pFormat

  -- * Dictionary Translation
  , schemaOfDictionary
  , schemaOfEncoding
  , schemaOfFieldEncoding
  ) where

import qualified Data.Attoparsec.Text as Atto
import           Data.Map (Map)

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)

import           Zebra.Encoding
import           Zebra.Fact


newtype Schema =
  Schema {
      unSchema :: [Format]
    } deriving (Eq, Ord, Monoid, Generic)

instance Show Schema where
  showsPrec =
    gshowsPrec

data Format =
    ByteFormat
  | WordFormat
  | DoubleFormat
  | ArrayFormat Schema
    deriving (Eq, Ord, Show)

-- | Render a schema as a string. The schema string is run of characters which
--   describes the layout/format of flattened data as arrays:
--
-- @
--   b   - byte
--   w   - word
--   d   - double
--   [?] - array
-- @
--
renderSchema :: Schema -> Text
renderSchema =
  let
    go = \case
      ByteFormat ->
        "b"
      WordFormat ->
        "w"
      DoubleFormat ->
        "d"
      ArrayFormat s ->
        "[" <> renderSchema s <> "]"
  in
    foldMap go . unSchema

parseSchema :: Text -> Maybe Schema
parseSchema =
  rightToMaybe . Atto.parseOnly (pSchema <* Atto.endOfInput)

pSchema :: Atto.Parser Schema
pSchema =
  Schema <$> many pFormat

pFormat :: Atto.Parser Format
pFormat =
  Atto.choice [
      ByteFormat <$ Atto.char 'b'
    , WordFormat <$ Atto.char 'w'
    , DoubleFormat <$ Atto.char 'd'
    , ArrayFormat <$> (Atto.char '[' *> pSchema <* Atto.char ']')
    ]

schemaOfDictionary :: Map AttributeName Encoding -> Map AttributeName Schema
schemaOfDictionary =
  fmap schemaOfEncoding

schemaOfEncoding :: Encoding -> Schema
schemaOfEncoding = \case
  BoolEncoding ->
    Schema (pure WordFormat)
  Int64Encoding ->
    Schema (pure WordFormat)
  DoubleEncoding ->
    Schema (pure DoubleFormat)
  StringEncoding ->
    Schema (pure ByteFormat)
  DateEncoding ->
    Schema (pure WordFormat)
  StructEncoding fields ->
    foldMap (schemaOfFieldEncoding . snd) fields
  ListEncoding encoding ->
    Schema (pure WordFormat) <> schemaOfEncoding encoding

schemaOfFieldEncoding :: FieldEncoding -> Schema
schemaOfFieldEncoding = \case
  FieldEncoding obligation encoding ->
    case obligation of
      FieldRequired ->
        schemaOfEncoding encoding
      FieldOptional ->
        Schema (pure WordFormat) <> schemaOfEncoding encoding
