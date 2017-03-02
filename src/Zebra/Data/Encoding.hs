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
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P

import           Text.Printf (printf)

import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Core
import           Zebra.Schema


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

encodingOfDictionary :: Map AttributeName Schema -> Map AttributeName Encoding
encodingOfDictionary =
  fmap encodingOfSchema

encodingOfSchema :: Schema -> Encoding
encodingOfSchema = \case
  Byte ->
    Encoding $ pure ByteEncoding
  Int ->
    Encoding $ pure IntEncoding
  Double ->
    Encoding $ pure DoubleEncoding
  Enum variant0 variants ->
    Encoding (pure IntEncoding) <>
    foldMap (encodingOfSchema . variantSchema) (Boxed.cons variant0 variants)
  Struct fields ->
    foldMap (encodingOfSchema . fieldSchema) fields
  Array schema ->
    Encoding (pure . ArrayEncoding $ encodingOfSchema schema)

-- Best effort to recover an schema
recoverSchemaOfEncoding :: Encoding -> Schema
recoverSchemaOfEncoding (Encoding encodings) =
  case recoverSchemaOfColumns encodings of
    [] ->
      Struct Boxed.empty
    [x] ->
      x
    xs ->
      let
        digits =
          length (show $ length xs)

        format =
          "%0" <> show digits <> "d"

        mkField :: Int -> Schema -> Field
        mkField i x =
          Field (FieldName . T.pack $ printf format i) x
      in
        Struct .
        Boxed.fromList $
        List.zipWith mkField [0..] xs

recoverSchemaOfColumns :: [ColumnEncoding] -> [Schema]
recoverSchemaOfColumns = \case
  [] ->
    []
  ByteEncoding : xs ->
    Byte : recoverSchemaOfColumns xs
  IntEncoding : xs ->
    Int : recoverSchemaOfColumns xs
  DoubleEncoding : xs ->
    Double : recoverSchemaOfColumns xs
  ArrayEncoding encoding : xs -> do
    Array (recoverSchemaOfEncoding encoding) : recoverSchemaOfColumns xs
