{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Schema (
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
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Encoding
import           Zebra.Data.Fact


newtype Schema =
  Schema {
      unSchema :: [Format]
    } deriving (Eq, Ord, Monoid, Generic)

instance Show Schema where
  showsPrec =
    gshowsPrec

data Format =
    ByteFormat
  | IntFormat
  | DoubleFormat
  | ArrayFormat !Schema
    deriving (Eq, Ord, Show)

-- | Render a schema as a string. The schema string is run of characters which
--   describes the layout/format of flattened data as arrays:
--
-- @
--   b   - byte
--   i   - int
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
      IntFormat ->
        "i"
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
    , IntFormat <$ Atto.char 'i'
    , DoubleFormat <$ Atto.char 'd'
    , ArrayFormat <$> (Atto.char '[' *> pSchema <* Atto.char ']')
    ]

schemaOfDictionary :: Map AttributeName Encoding -> Map AttributeName Schema
schemaOfDictionary =
  fmap schemaOfEncoding

schemaOfEncoding :: Encoding -> Schema
schemaOfEncoding = \case
  BoolEncoding ->
    Schema (pure IntFormat)
  Int64Encoding ->
    Schema (pure IntFormat)
  DoubleEncoding ->
    Schema (pure DoubleFormat)
  StringEncoding ->
    Schema (pure . ArrayFormat $ Schema [ByteFormat])
  DateEncoding ->
    Schema (pure IntFormat)
  StructEncoding fields ->
    if Boxed.null fields then
      Schema (pure IntFormat)
    else
      foldMap (schemaOfFieldEncoding . snd) fields
  ListEncoding encoding ->
    Schema (pure . ArrayFormat $ schemaOfEncoding encoding)

schemaOfFieldEncoding :: FieldEncoding -> Schema
schemaOfFieldEncoding = \case
  FieldEncoding obligation encoding ->
    case obligation of
      RequiredField ->
        schemaOfEncoding encoding
      OptionalField ->
        Schema (pure IntFormat) <> schemaOfEncoding encoding
