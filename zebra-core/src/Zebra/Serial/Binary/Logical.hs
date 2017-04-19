{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
module Zebra.Serial.Binary.Logical (
    encodeLogical
  , encodeLogicalWith
  , decodeLogical

  , BinaryLogicalEncodeError(..)
  , renderBinaryLogicalEncodeError

  , BinaryLogicalDecodeError(..)
  , renderBinaryLogicalDecodeError
  ) where

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)

import           P

import           X.Control.Monad.Trans.Either (EitherT, left, hoistEither, firstJoin)

import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.Striped
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.ByteStream (ByteStream)
import           Zebra.X.Stream (Stream, Of(..))
import qualified Zebra.X.Stream as Stream


data BinaryLogicalEncodeError =
    BinaryLogicalEncodeStripedError !StripedError
  | BinaryLogicalStripedEncodeError !BinaryStripedEncodeError
    deriving (Eq, Show)

data BinaryLogicalDecodeError =
    BinaryLogicalDecodeEmpty
  | BinaryLogicalDecodeStripedError !StripedError
  | BinaryLogicalStripedDecodeError !BinaryStripedDecodeError
    deriving (Eq, Show)

renderBinaryLogicalEncodeError :: BinaryLogicalEncodeError -> Text
renderBinaryLogicalEncodeError = \case
  BinaryLogicalEncodeStripedError err ->
    Striped.renderStripedError err
  BinaryLogicalStripedEncodeError err ->
    renderBinaryStripedEncodeError err

renderBinaryLogicalDecodeError :: BinaryLogicalDecodeError -> Text
renderBinaryLogicalDecodeError = \case
  BinaryLogicalDecodeEmpty ->
    "Cannot resolve schema of an empty zebra file"
  BinaryLogicalDecodeStripedError err ->
    Striped.renderStripedError err
  BinaryLogicalStripedDecodeError err ->
    renderBinaryStripedDecodeError err

encodeLogical ::
     Monad m
  => Schema.Table
  -> Stream (Of Logical.Table) m ()
  -> ByteStream (EitherT BinaryLogicalEncodeError m) ()
encodeLogical =
  encodeLogicalWith BinaryV3
{-# INLINABLE encodeLogical #-}

encodeLogicalWith ::
     Monad m
  => BinaryVersion
  -> Schema.Table
  -> Stream (Of Logical.Table) m ()
  -> ByteStream (EitherT BinaryLogicalEncodeError m) ()
encodeLogicalWith version schema input =
  hoist (firstJoin BinaryLogicalStripedEncodeError) .
  encodeStripedWith version $
  Stream.mapM
    (hoistEither . first BinaryLogicalEncodeStripedError . Striped.fromLogical schema)
    (hoist lift input)
{-# INLINABLE encodeLogicalWith #-}

decodeLogical ::
     Monad m
  => ByteStream m r
  -> EitherT BinaryLogicalDecodeError m
       (Schema.Table, Stream (Of Logical.Table) (EitherT BinaryLogicalDecodeError m) r)
decodeLogical bss0 = do
  e <- firstT BinaryLogicalStripedDecodeError . Stream.next $ decodeStriped bss0
  case e of
    Left _r ->
      left $ BinaryLogicalDecodeEmpty

    Right (hd, tl) ->
      pure . (Striped.schema hd,) $
        Stream.mapM (hoistEither . first BinaryLogicalDecodeStripedError . Striped.toLogical) $
        hoist (firstT BinaryLogicalStripedDecodeError) (Stream.cons hd tl)
{-# INLINABLE decodeLogical #-}
