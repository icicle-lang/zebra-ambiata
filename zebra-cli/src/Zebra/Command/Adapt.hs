{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Command.Adapt (
    Adapt(..)
  , zebraAdapt

  , AdaptError(..)
  , renderAdaptError
  ) where

import           Control.Monad.Catch (MonadMask(..))
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (hoist, squash, lift)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import qualified Data.ByteString as ByteString
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath)
import           System.IO.Error (IOError)

import           X.Control.Monad.Trans.Either (EitherT, firstJoin, hoistEither)

import           Zebra.Command.Util
import           Zebra.Serial.Binary (BinaryStripedEncodeError, BinaryStripedDecodeError)
import qualified Zebra.Serial.Binary as Binary
import           Zebra.Serial.Text (TextSchemaDecodeError)
import qualified Zebra.Serial.Text as Text
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped
import qualified Zebra.X.ByteStream as ByteStream
import           Zebra.X.Stream (Stream, Of(..))
import qualified Zebra.X.Stream as Stream


data Adapt =
  Adapt {
      adaptInput :: !FilePath
    , adaptSchema :: !FilePath
    , adaptOutput :: !(Maybe FilePath)
    } deriving (Eq, Ord, Show)

data AdaptError =
    AdaptIOError !IOError
  | AdaptTextSchemaDecodeError !TextSchemaDecodeError
  | AdaptBinaryStripedEncodeError !BinaryStripedEncodeError
  | AdaptBinaryStripedDecodeError !BinaryStripedDecodeError
  | AdaptStripedError !StripedError
    deriving (Eq, Show)

renderAdaptError :: AdaptError -> Text
renderAdaptError = \case
  AdaptIOError err ->
    Text.pack (show err)
  AdaptTextSchemaDecodeError err ->
    Text.renderTextSchemaDecodeError err
  AdaptBinaryStripedEncodeError err ->
    Binary.renderBinaryStripedEncodeError err
  AdaptBinaryStripedDecodeError err ->
    Binary.renderBinaryStripedDecodeError err
  AdaptStripedError err ->
    Striped.renderStripedError err

transmute :: Monad m => Schema.Table -> Stream (Of Striped.Table) m r -> Stream (Of Striped.Table) (EitherT StripedError m) r
transmute schema =
  Stream.mapM (hoistEither . Striped.transmute schema) .
  hoist lift
{-# INLINE transmute #-}

zebraAdapt :: forall m. (MonadResource m, MonadMask m) => Adapt -> EitherT AdaptError m ()
zebraAdapt x = do
  schema0 <- liftIO . ByteString.readFile $ adaptSchema x
  schema <- firstT AdaptTextSchemaDecodeError . hoistEither $ Text.decodeSchema schema0

  squash . firstJoin AdaptIOError .
      writeFileOrStdout (adaptOutput x) .
    hoist (firstJoin AdaptBinaryStripedEncodeError) .
      Binary.encodeStriped .
    hoist (firstJoin AdaptStripedError) .
      transmute schema .
    hoist (firstJoin AdaptBinaryStripedDecodeError) .
      Binary.decodeStriped .
    hoist (firstT AdaptIOError) $
      ByteStream.readFile (adaptInput x)
{-# SPECIALIZE zebraAdapt :: Adapt -> EitherT AdaptError (ResourceT IO) () #-}
