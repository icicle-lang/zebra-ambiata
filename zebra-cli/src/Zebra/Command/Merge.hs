{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Command.Merge (
    Merge(..)
  , zebraMerge

  , MergeError(..)
  , renderMergeError
  ) where

import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import           Data.List.NonEmpty (NonEmpty)
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath)
import           System.IO.Error (IOError)

import           X.Control.Monad.Trans.Either (EitherT, firstJoin)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Command.Util
import           Zebra.Merge.Table (UnionTableError)
import qualified Zebra.Merge.Table as Merge
import           Zebra.Serial.Binary (BinaryStripedEncodeError, BinaryStripedDecodeError)
import           Zebra.Serial.Binary (BinaryVersion(..))
import qualified Zebra.Serial.Binary as Binary
import qualified Zebra.Table.Striped as Striped
import qualified Zebra.X.ByteStream as ByteStream
import           Zebra.X.Stream (Stream, Of)


data Merge =
  Merge {
      mergeInputs :: !(NonEmpty FilePath)
    , mergeOutput :: !(Maybe FilePath)
    , mergeVersion :: !BinaryVersion
    } deriving (Eq, Ord, Show)

data MergeError =
    MergeIOError !IOError
  | MergeBinaryStripedDecodeError !BinaryStripedDecodeError
  | MergeBinaryStripedEncodeError !BinaryStripedEncodeError
  | MergeUnionTableError !UnionTableError
    deriving (Eq, Show)

renderMergeError :: MergeError -> Text
renderMergeError = \case
  MergeIOError err ->
    Text.pack (show err)
  MergeBinaryStripedDecodeError err ->
    Binary.renderBinaryStripedDecodeError err
  MergeBinaryStripedEncodeError err ->
    Binary.renderBinaryStripedEncodeError err
  MergeUnionTableError err ->
    Merge.renderUnionTableError err

readStriped :: (MonadResource m, MonadCatch m) => FilePath -> Stream (Of Striped.Table) (EitherT MergeError m) ()
readStriped path =
  hoist (firstJoin MergeBinaryStripedDecodeError) .
    Binary.decodeStriped .
  hoist (firstT MergeIOError) $
    ByteStream.readFile path

zebraMerge :: (MonadResource m, MonadCatch m) => Merge -> EitherT MergeError m ()
zebraMerge x =
  let
    inputs =
      fmap readStriped . Cons.fromNonEmpty $ mergeInputs x
  in
    firstJoin MergeIOError .
      writeFileOrStdout (mergeOutput x) .
    hoist (firstJoin MergeBinaryStripedEncodeError) .
      Binary.encodeStripedWith (mergeVersion x) .
    hoist (firstJoin MergeUnionTableError) $
      Merge.unionStriped inputs
{-# SPECIALIZE zebraMerge :: Merge -> EitherT MergeError (ResourceT IO) () #-}
