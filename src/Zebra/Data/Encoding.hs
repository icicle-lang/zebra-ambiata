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
  , encodingOfFieldSchema

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

import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Core
import           Zebra.Data.Schema


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
  BoolSchema ->
    Encoding (pure IntEncoding)
  Int64Schema ->
    Encoding (pure IntEncoding)
  DoubleSchema ->
    Encoding (pure DoubleEncoding)
  StringSchema ->
    Encoding (pure . ArrayEncoding $ Encoding [ByteEncoding])
  DateSchema ->
    Encoding (pure IntEncoding)
  StructSchema fields ->
    if Boxed.null fields then
      Encoding (pure IntEncoding)
    else
      foldMap (encodingOfFieldSchema . snd) fields
  ListSchema schema ->
    Encoding (pure . ArrayEncoding $ encodingOfSchema schema)

encodingOfFieldSchema :: FieldSchema -> Encoding
encodingOfFieldSchema = \case
  FieldSchema obligation schema ->
    case obligation of
      RequiredField ->
        encodingOfSchema schema
      OptionalField ->
        Encoding (pure IntEncoding) <> encodingOfSchema schema

-- Best effort to recover an schema
recoverSchemaOfEncoding :: Encoding -> Maybe Schema
recoverSchemaOfEncoding (Encoding encodings) =
  case recoverSchemaOfColumns encodings of
    Nothing ->
      Nothing
    Just [] ->
      Nothing
    Just [x] ->
      Just x
    Just xs ->
      let
        mkField :: Int -> Schema -> (FieldName, FieldSchema)
        mkField i x =
          (FieldName . T.pack $ show i, FieldSchema RequiredField x)
      in
        Just .
          StructSchema .
          Boxed.fromList $
          List.zipWith mkField [0..] xs

recoverSchemaOfColumns :: [ColumnEncoding] -> Maybe [Schema]
recoverSchemaOfColumns = \case
  [] ->
    Just []
  ByteEncoding : _ ->
    Nothing
  IntEncoding : xs ->
    (Int64Schema :) <$> recoverSchemaOfColumns xs
  DoubleEncoding : xs ->
    (DoubleSchema :) <$> recoverSchemaOfColumns xs
  ArrayEncoding (Encoding [ByteEncoding]) : xs ->
    (StringSchema :) <$> recoverSchemaOfColumns xs
  ArrayEncoding encoding : xs -> do
    schema <- recoverSchemaOfEncoding encoding
    (ListSchema schema :) <$> recoverSchemaOfColumns xs
