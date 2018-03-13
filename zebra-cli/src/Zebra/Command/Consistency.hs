{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Command.Consistency (
    Consistency(..)

  , zebraConsistency
  , renderConsistencyError
  ) where

import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import qualified Data.Text as Text
import qualified Data.Map as Map

import           P

import           System.IO (IO, FilePath)
import           System.IO.Error (IOError)

import qualified Viking.ByteStream as ByteStream
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, firstJoin, hoistEither)

import           Zebra.Serial.Binary (BinaryStripedDecodeError)
import qualified Zebra.Serial.Binary as Binary
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped


data Consistency =
  Consistency {
      consistencyInput :: !FilePath
    } deriving (Eq, Ord, Show)

data ConsistencyError =
    ConsistencyIOError !IOError
  | ConsistencyStripedDecodeError !BinaryStripedDecodeError
  | ConsistencyStripedError !StripedError
  | ConsistencyInterBlock Int Logical.Value Logical.Value
    deriving (Eq, Show)

data BlockRange =
  BlockRange {
      _blockMinKey :: Maybe (Logical.Value)
    , _blockMaxKey :: Maybe (Logical.Value)
    } deriving (Eq, Ord, Show)

renderConsistencyError :: ConsistencyError -> Text
renderConsistencyError = \case
  ConsistencyIOError err ->
    Text.pack (show err)
  ConsistencyStripedDecodeError err ->
    "Error decoding: " <> Binary.renderBinaryStripedDecodeError err
  ConsistencyStripedError err ->
    Striped.renderStripedError err
  ConsistencyInterBlock chunk keya keyb ->
    Text.unlines [
      "Consistency check failure:"
    , "Chunk " <> Text.pack (show chunk) <> " has max key:"
    , "  " <> Text.pack (show keya)
    , "while the following chunk starts with"
    , "  " <> Text.pack (show keyb)
    ]

summariseBlock :: Logical.Table -> BlockRange
summariseBlock = \case
  Logical.Binary _ -> BlockRange Nothing Nothing
  Logical.Array _ -> BlockRange Nothing Nothing
  Logical.Map m ->
    if Map.null m then
      BlockRange Nothing Nothing
    else
      let
        bmin = fst $ Map.findMin m
        bmax = fst $ Map.findMax m
      in
        BlockRange (Just bmin) (Just bmax)

zebraConsistency :: (MonadResource m, MonadCatch m) => Consistency -> EitherT ConsistencyError m ()
zebraConsistency x = do
  let
    tables =
      hoist (firstJoin ConsistencyStripedDecodeError) .
        Binary.decodeStriped .
      hoist (firstT ConsistencyIOError) $
        ByteStream.readFile (consistencyInput x)

    fromStriped =
      Stream.mapM (hoistEither . first ConsistencyStripedError . Striped.toLogical)

    blockRanges =
      Stream.map summariseBlock $ fromStriped tables

    loop mseen (BlockRange mbmin mbmax) =
      case (mseen, mbmin) of
        ((chunk, Just seen), Just bmin) ->
          case compare seen bmin of
            LT -> pure (chunk + 1, mbmax)
            _  -> hoistEither (Left $ ConsistencyInterBlock chunk seen bmin)
        ((chunk, _), _) ->
          pure (chunk + 1, mbmax)

  _ <- Stream.foldM_ loop (pure (0, Nothing)) pure blockRanges
  pure ()

{-# SPECIALIZE zebraConsistency :: Consistency -> EitherT ConsistencyError (ResourceT IO) () #-}
