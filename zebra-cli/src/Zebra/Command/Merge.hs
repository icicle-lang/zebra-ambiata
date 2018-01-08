{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Command.Merge (
    Merge(..)
  , MergeRowsPerBlock(..)
  , MergeMaximumRowSize(..)
  , MergeMode(..)
  , zebraMerge

  , MergeError(..)
  , renderMergeError
  ) where

import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import qualified Data.ByteString as ByteString
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath)
import           System.IO.Error (IOError)

import           Viking (Stream, Of)
import qualified Viking.ByteStream as ByteStream

import           X.Control.Monad.Trans.Either (EitherT, firstJoin, hoistEither)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Command.Util
import           Zebra.Merge.Table (UnionTableError)
import qualified Zebra.Merge.Table as Merge
import           Zebra.Serial.Binary (BinaryStripedEncodeError, BinaryStripedDecodeError)
import           Zebra.Serial.Binary (BinaryVersion(..))
import qualified Zebra.Serial.Binary as Binary
import           Zebra.Serial.Text (TextSchemaDecodeError)
import qualified Zebra.Serial.Text as Text
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped


data Merge =
  Merge {
      mergeInputs :: !(NonEmpty FilePath)
    , mergeSchema :: !(Maybe FilePath)
    , mergeOutput :: !(Maybe FilePath)
    , mergeVersion :: !BinaryVersion
    , mergeRowsPerChunk :: !MergeRowsPerBlock
    , mergeMaximumRowSize :: !(Maybe MergeMaximumRowSize)
    , mergeMode :: MergeMode
    } deriving (Eq, Ord, Show)

data MergeMode =
    MergeValue
  | MergeMeasure
  deriving (Eq, Ord, Show)

newtype MergeRowsPerBlock =
  MergeRowsPerBlock {
      unMergeRowsPerBlock :: Int
    } deriving (Eq, Ord, Show)

newtype MergeMaximumRowSize =
  MergeMaximumRowSize {
      unMergeMaximumRowSize :: Int64
    } deriving (Eq, Ord, Show)

data MergeError =
    MergeIOError !IOError
  | MergeBinaryStripedDecodeError !FilePath !BinaryStripedDecodeError
  | MergeBinaryStripedEncodeError !BinaryStripedEncodeError
  | MergeTextSchemaDecodeError !TextSchemaDecodeError
  | MergeStripedError !StripedError
  | MergeUnionTableError !UnionTableError
    deriving (Eq, Show)

renderMergeError :: MergeError -> Text
renderMergeError = \case
  MergeIOError err ->
    Text.pack (show err)
  MergeBinaryStripedDecodeError path err ->
    "Error decoding: " <> Text.pack path <> "\n" <>
    Binary.renderBinaryStripedDecodeError err
  MergeBinaryStripedEncodeError err ->
    Binary.renderBinaryStripedEncodeError err
  MergeTextSchemaDecodeError err ->
    Text.renderTextSchemaDecodeError err
  MergeStripedError err ->
    Striped.renderStripedError err
  MergeUnionTableError err ->
    Merge.renderUnionTableError err

readStriped :: (MonadResource m, MonadCatch m) => FilePath -> Stream (Of Striped.Table) (EitherT MergeError m) ()
readStriped path =
  hoist (firstJoin (MergeBinaryStripedDecodeError path)) .
    Binary.decodeStriped .
  hoist (firstT MergeIOError) $
    ByteStream.readFile path

readSchema :: MonadResource m => Maybe FilePath -> EitherT MergeError m (Maybe Schema.Table)
readSchema = \case
  Nothing ->
    pure Nothing
  Just path-> do
    schema <- liftIO $ ByteString.readFile path
    fmap Just . firstT MergeTextSchemaDecodeError . hoistEither $
      Text.decodeSchema schema

zebraMerge :: (MonadResource m, MonadCatch m) => Merge -> EitherT MergeError m ()
zebraMerge x = do
  mschema <- readSchema (mergeSchema x)

  let
    inputs =
      fmap readStriped . Cons.fromNonEmpty $ mergeInputs x

    msize =
      fmap (Merge.MaximumRowSize . unMergeMaximumRowSize) $ mergeMaximumRowSize x

    union =
      case mergeMode x of
        MergeValue ->
          maybe (Merge.unionStriped Merge.valueMonoid) (Merge.unionStripedWith Merge.valueMonoid) mschema
        MergeMeasure ->
          maybe (Merge.unionStriped Merge.measureMonoid) (Merge.unionStripedWith Merge.measureMonoid) mschema

  firstJoin MergeIOError .
      writeFileOrStdout (mergeOutput x) .
    hoist (firstJoin MergeBinaryStripedEncodeError) .
      Binary.encodeStripedWith (mergeVersion x) .
    hoist (firstJoin MergeStripedError) .
      Striped.rechunk (unMergeRowsPerBlock $ mergeRowsPerChunk x) .
    hoist (firstJoin MergeUnionTableError) $
      union msize inputs
{-# SPECIALIZE zebraMerge :: Merge -> EitherT MergeError (ResourceT IO) () #-}
