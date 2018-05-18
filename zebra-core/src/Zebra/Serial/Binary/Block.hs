{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Serial.Binary.Block (
    bBlockTable
  , getBlockTable

  -- * Internal
  , bBlockTableV3
  , getBlockTableV3
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder

import           P

import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.Table
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped

bBlockTable :: Header -> Striped.Table -> Either BinaryEncodeError Builder
bBlockTable header table =
  case header of
    HeaderV3 _ -> do
      bBlockTableV3 table
{-# INLINABLE bBlockTable #-}

getBlockTable :: Header -> Get Striped.Table
getBlockTable = \case
  HeaderV3 x ->
    getBlockTableV3 x
{-# INLINABLE getBlockTable #-}

bBlockTableV3 :: Striped.Table -> Either BinaryEncodeError Builder
bBlockTableV3 table0 = do
  table <- bTable BinaryV3 table0
  pure $
    Builder.word32LE (fromIntegral $ Striped.length table0) <>
    table
{-# INLINABLE bBlockTableV3 #-}

getBlockTableV3 :: Schema.Table -> Get Striped.Table
getBlockTableV3 schema = do
  n <- fromIntegral <$> Get.getWord32le
  getTable BinaryV3 n schema
{-# INLINABLE getBlockTableV3 #-}
