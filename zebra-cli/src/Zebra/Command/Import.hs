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
import qualified Data.Char as Char
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath, Handle)
import           System.IO (stdout, hFlush, hIsTerminalDevice, putStr, getLine)
import           System.IO.Error (IOError)

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, firstJoin)

import           Zebra.ByteStream (ByteStream)
import qualified Zebra.ByteStream as ByteStream
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

chunkSize :: Int
chunkSize =
  1024 * 1024

checkStdout :: MonadIO m => m (Maybe Handle)
checkStdout = do
  tty <- liftIO $ hIsTerminalDevice stdout
  if tty then do
    liftIO $ putStr "About to write a binary file to stdout. Are you sure? [y/N] "
    liftIO $ hFlush stdout
    line <- fmap Char.toLower <$> liftIO getLine
    if line == "y" then
      pure $ Just stdout
    else
      pure Nothing
  else
    pure $ Just stdout

writeFile ::
     MonadResource m
  => MonadCatch m
  => Maybe FilePath
  -> ByteStream m ()
  -> EitherT ImportError m ()
writeFile mpath bss = do
  case mpath of
    Nothing -> do
      mhandle <- checkStdout
      case mhandle of
        Nothing ->
          pure ()
        Just handle ->
          firstT ImportIOError $
            ByteStream.hPut handle bss

    Just path ->
      firstT ImportIOError $
        ByteStream.writeFile path bss

zebraImport :: (MonadResource m, MonadCatch m) => Import -> EitherT ImportError m ()
zebraImport x = do
  schema0 <- liftIO . ByteString.readFile $ importSchema x
  schema <- firstT ImportTextSchemaDecodeError . hoistEither $ Text.decodeSchema schema0

  squash . writeFile (importOutput x) .
    hoist (firstJoin ImportBinaryStripedEncodeError) .
      Binary.encodeStriped .
    hoist (firstJoin ImportTextStripedDecodeError) .
      Text.decodeStriped schema .
    hoist (firstT ImportIOError) $
      ByteStream.readFileN chunkSize (importInput x)
{-# SPECIALIZE zebraImport :: Import -> EitherT ImportError (ResourceT IO) () #-}
