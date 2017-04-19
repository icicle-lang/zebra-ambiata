{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Text.Striped (
    encodeStriped
  , encodeStripedBlock

  , decodeStriped
  , decodeStripedBlock

  , TextStripedEncodeError(..)
  , renderTextStripedEncodeError

  , TextStripedDecodeError(..)
  , renderTextStripedDecodeError
  ) where

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)

import           Data.ByteString (ByteString)

import           P

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, firstJoin)

import           Zebra.Serial.Text.Logical
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.ByteStream (ByteStream)
import qualified Zebra.X.ByteStream as ByteStream
import           Zebra.X.Stream (Stream, Of(..))
import qualified Zebra.X.Stream as Stream


data TextStripedEncodeError =
    TextStripedEncodeError !StripedError
  | TextStripedLogicalEncodeError !TextLogicalEncodeError
    deriving (Eq, Show)

data TextStripedDecodeError =
    TextStripedDecodeError !StripedError
  | TextStripedLogicalDecodeError !TextLogicalDecodeError
    deriving (Eq, Show)

renderTextStripedEncodeError :: TextStripedEncodeError -> Text
renderTextStripedEncodeError = \case
  TextStripedEncodeError err ->
    Striped.renderStripedError err
  TextStripedLogicalEncodeError err ->
    renderTextLogicalEncodeError err

renderTextStripedDecodeError :: TextStripedDecodeError -> Text
renderTextStripedDecodeError = \case
  TextStripedDecodeError err ->
    Striped.renderStripedError err
  TextStripedLogicalDecodeError err ->
    renderTextLogicalDecodeError err

encodeStriped ::
     Monad m
  => Stream (Of Striped.Table) m ()
  -> ByteStream (EitherT TextStripedEncodeError m) ()
encodeStriped input = do
  m <- lift . lift $ Stream.uncons input
  case m of
    Nothing ->
      ByteStream.empty

    Just (hd, tl) ->
      hoist (firstJoin TextStripedLogicalEncodeError) .
      encodeLogical (Striped.schema hd) .
      Stream.mapM (hoistEither . first TextStripedEncodeError . Striped.toLogical) $
      hoist lift (Stream.cons hd tl)
{-# INLINABLE encodeStriped #-}

encodeStripedBlock :: Striped.Table -> Either TextStripedEncodeError ByteString
encodeStripedBlock striped = do
  logical <- first TextStripedEncodeError $ Striped.toLogical striped
  first TextStripedLogicalEncodeError $ encodeLogicalBlock (Striped.schema striped) logical
{-# INLINABLE encodeStripedBlock #-}

decodeStriped ::
     Monad m
  => Schema.Table
  -> ByteStream m ()
  -> Stream (Of Striped.Table) (EitherT TextStripedDecodeError m) ()
decodeStriped schema =
  Stream.mapM (hoistEither . first TextStripedDecodeError . Striped.fromLogical schema) .
  hoist (firstT TextStripedLogicalDecodeError) .
  decodeLogical schema
{-# INLINABLE decodeStriped #-}

decodeStripedBlock :: Schema.Table -> ByteString -> Either TextStripedDecodeError Striped.Table
decodeStripedBlock schema bs = do
  logical <- first TextStripedLogicalDecodeError $ decodeLogicalBlock schema bs
  first TextStripedDecodeError $ Striped.fromLogical schema logical
{-# INLINABLE decodeStripedBlock #-}
