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

import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Resource (MonadResource)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Char as Char

import           P

import           System.IO (FilePath, Handle, IOMode(..), stdout, hFlush, hIsTerminalDevice, putStr, getLine)

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, joinEitherT)
import           X.Data.Vector.Stream (Stream(..))
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Binary.Block
import           Zebra.Binary.Data
import           Zebra.Binary.File
import           Zebra.Binary.Header
import           Zebra.Data.Block
import           Zebra.Json.Schema
import           Zebra.Text


data Import =
  Import {
      importInput :: !FilePath
    , importSchema :: !FilePath
    , importOutput :: !(Maybe FilePath)
    } deriving (Eq, Ord, Show)

data ImportError =
    ImportFileError !FileError
  | ImportJsonSchemaDecodeError !JsonSchemaDecodeError
  | ImportTextTableDecodeError !TextTableDecodeError
  | ImportBlockTableError !BlockTableError
    deriving (Eq, Show)

renderImportError :: ImportError -> Text
renderImportError = \case
  ImportFileError err ->
    renderFileError err
  ImportJsonSchemaDecodeError err ->
    renderJsonSchemaDecodeError err
  ImportTextTableDecodeError err ->
    renderTextTableDecodeError err
  ImportBlockTableError err ->
    renderBlockTableError err

lineBoundary :: Monad m => Stream m ByteString -> Stream m ByteString
lineBoundary (Stream step sinit) =
  let
    loop = \case
      Nothing ->
        pure Stream.Done

      Just (s0, bs0) ->
        step s0 >>= \case
          Stream.Skip s ->
            pure . Stream.Skip $ Just (s, bs0)

          Stream.Done ->
            pure $ Stream.Yield bs0 Nothing

          Stream.Yield bs s ->
            case ByteString.elemIndexEnd 0x0A bs of
              Nothing ->
                pure . Stream.Skip $ Just (s, bs0 <> bs)

              Just ix ->
                case ByteString.splitAt (ix + 1) bs of
                  (bs1, bs2) ->
                    pure . Stream.Yield (bs0 <> bs1) $ Just (s, bs2)
  in
    Stream loop $ Just (sinit, ByteString.empty)

openHandle :: MonadResource m => Maybe FilePath -> EitherT ImportError m (Maybe (EitherT ImportError m (), Handle))
openHandle = \case
  Nothing -> do
    tty <- liftIO $ hIsTerminalDevice stdout
    if tty then do
      liftIO $ putStr "About to write a binary file to stdout. Are you sure? [y/N] "
      liftIO $ hFlush stdout
      line <- fmap Char.toLower <$> liftIO getLine
      if line == "y" then
        pure $ Just (pure (), stdout)
      else
        pure Nothing
    else
      pure $ Just (pure (), stdout)

  Just file ->
    fmap (Just . first (firstT ImportFileError)) . firstT ImportFileError $
      openFile file WriteMode

zebraImport :: MonadResource m => Import -> EitherT ImportError m ()
zebraImport x = do
  mhandle <- openHandle (importOutput x)
  case mhandle of
    Nothing ->
      pure ()
    Just (close, handle) -> do
      schema0 <- liftIO . ByteString.readFile $ importSchema x
      schema <- firstT ImportJsonSchemaDecodeError . hoistEither $ decodeVersionedSchema schema0

      bytes <- firstT ImportFileError . readBytes $ importInput x

      let header = HeaderV3 schema
      liftIO . Builder.hPutBuilder handle $ bHeader header

      joinEitherT id . firstT ImportFileError .
        hPutStream (maybe "<stdout>" id $ importOutput x) handle .
        fmap (Lazy.toStrict . Builder.toLazyByteString) .
        Stream.mapM (hoistEither . first ImportBlockTableError . bRootTable header) .
        Stream.mapM (hoistEither . first ImportTextTableDecodeError . decodeTable schema) .
        lineBoundary $
        Stream.trans (firstT ImportFileError) bytes

      close
