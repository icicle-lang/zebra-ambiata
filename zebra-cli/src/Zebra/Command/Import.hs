{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Command.Import (
    Import(..)
  , zebraImport

  , ImportError(..)
  , renderImportError
  ) where

import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (hoist, squash)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import qualified Data.ByteString as ByteString
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath)
import           System.IO.Error (IOError)

import qualified Viking.ByteStream as ByteStream

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, firstJoin)

import           Zebra.Command.Util
import           Zebra.Serial.Binary (BinaryStripedEncodeError)
import qualified Zebra.Serial.Binary as Binary
import           Zebra.Serial.Text (TextSchemaDecodeError, TextStripedDecodeError)
import qualified Zebra.Serial.Text as Text


data Import =
  Import {
      importInput :: !FilePath
    , importSchema :: !FilePath
    , importOutput :: !(Maybe FilePath)
    } deriving (Eq, Ord, Show)

data ImportError =
    ImportIOError !IOError
  | ImportTextSchemaDecodeError !TextSchemaDecodeError
  | ImportTextStripedDecodeError !TextStripedDecodeError
  | ImportBinaryStripedEncodeError !BinaryStripedEncodeError
    deriving (Eq, Show)

renderImportError :: ImportError -> Text
renderImportError = \case
  ImportIOError err ->
    Text.pack (show err)
  ImportTextSchemaDecodeError err ->
    Text.renderTextSchemaDecodeError err
  ImportTextStripedDecodeError err ->
    Text.renderTextStripedDecodeError err
  ImportBinaryStripedEncodeError err ->
    Binary.renderBinaryStripedEncodeError err

zebraImport :: (MonadResource m, MonadCatch m) => Import -> EitherT ImportError m ()
zebraImport x = do
  schema0 <- liftIO . ByteString.readFile $ importSchema x
  schema <- firstT ImportTextSchemaDecodeError . hoistEither $ Text.decodeSchema schema0

  squash . firstJoin ImportIOError .
      writeFileOrStdout (importOutput x) .
    hoist (firstJoin ImportBinaryStripedEncodeError) .
      Binary.encodeStriped .
    hoist (firstJoin ImportTextStripedDecodeError) .
      Text.decodeStriped schema .
    hoist (firstT ImportIOError) $
      ByteStream.readFile (importInput x)
{-# SPECIALIZE zebraImport :: Import -> EitherT ImportError (ResourceT IO) () #-}
