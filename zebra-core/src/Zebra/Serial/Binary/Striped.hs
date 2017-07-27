{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Binary.Striped (
    encodeStriped
  , encodeStripedWith
  , decodeStriped

  , BinaryStripedEncodeError(..)
  , renderBinaryStripedEncodeError

  , BinaryStripedDecodeError(..)
  , renderBinaryStripedDecodeError
  ) where

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)

import           P

import           Viking (ByteStream, Stream, Of(..))
import qualified Viking.ByteStream as ByteStream
import qualified Viking.Stream as Stream
import           Viking.Stream.Binary (BinaryError, renderBinaryError)
import qualified Viking.Stream.Binary as Stream

import           X.Control.Monad.Trans.Either (EitherT, left, hoistEither)

import           Zebra.Factset.Table
import           Zebra.Serial.Binary.Block
import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.Header
import qualified Zebra.Table.Striped as Striped


data BinaryStripedEncodeError =
    BinaryStripedEncodeEmpty
  | BinaryStripedEncodeBlockTableError !BlockTableError
  | BinaryStripedEncodeError !BinaryEncodeError
    deriving (Eq, Show)

data BinaryStripedDecodeError =
    BinaryStripedDecodeHeaderError !BinaryError
  | BinaryStripedDecodeBlockError !BinaryError
    deriving (Eq, Show)

renderBinaryStripedEncodeError :: BinaryStripedEncodeError -> Text
renderBinaryStripedEncodeError = \case
  BinaryStripedEncodeEmpty ->
    "Cannot encode a zebra file with no schema"
  BinaryStripedEncodeBlockTableError err ->
    renderBlockTableError err
  BinaryStripedEncodeError err ->
    renderBinaryEncodeError err

renderBinaryStripedDecodeError :: BinaryStripedDecodeError -> Text
renderBinaryStripedDecodeError = \case
  BinaryStripedDecodeHeaderError err ->
    "Error decoding header: " <> renderBinaryError err
  BinaryStripedDecodeBlockError err ->
    "Error decoding block: " <> renderBinaryError err

encodeStriped ::
     Monad m
  => Stream (Of Striped.Table) m r
  -> ByteStream (EitherT BinaryStripedEncodeError m) r
encodeStriped =
  encodeStripedWith BinaryV3
{-# INLINABLE encodeStriped #-}

encodeStripedWith ::
     Monad m
  => BinaryVersion
  -> Stream (Of Striped.Table) m r
  -> ByteStream (EitherT BinaryStripedEncodeError m) r
encodeStripedWith version input = do
  e <- lift . lift $ Stream.next input
  case e of
    Left _r ->
      lift $ left BinaryStripedEncodeEmpty

    Right (hd, tl) -> do
      header <- lift . hoistEither . first BinaryStripedEncodeBlockTableError $
        headerOfSchema version (Striped.schema hd)

      ByteStream.fromBuilders . Stream.cons (bHeader header) .
        Stream.mapM (hoistEither . first BinaryStripedEncodeError . bBlockTable header) $
        hoist lift (Stream.cons hd tl)
{-# INLINABLE encodeStripedWith #-}

decodeStriped ::
     Monad m
  => ByteStream m r
  -> Stream (Of Striped.Table) (EitherT BinaryStripedDecodeError m) r
decodeStriped bss0 = do
  (header, bss1) <- lift . firstT BinaryStripedDecodeHeaderError $
    Stream.runGet getHeader bss0

  hoist (firstT BinaryStripedDecodeBlockError) $
    Stream.runGetSome (getBlockTable header) bss1
{-# INLINABLE decodeStriped #-}
