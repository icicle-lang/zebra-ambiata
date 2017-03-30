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

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath, IOMode(..), Handle)
import           System.IO (withBinaryFile)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither)
import           X.Data.Vector.Stream (Stream(..))
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Binary.Block
import           Zebra.Binary.File
import           Zebra.Binary.Header
import           Zebra.Data.Block
import           Zebra.Json


data Import =
  Import {
      importInput :: !FilePath
    , importSchema :: !FilePath
    , importOutput :: !FilePath
    } deriving (Eq, Ord, Show)

data ImportError =
    ImportFileError !FileError
  | ImportJsonDecodeError !JsonDecodeError
  | ImportJsonTableError !JsonTableDecodeError
  | ImportBlockTableError !BlockTableError
    deriving (Eq, Show)

renderImportError :: ImportError -> Text
renderImportError = \case
  ImportFileError err ->
    renderFileError err
  ImportJsonDecodeError err ->
    renderJsonDecodeError err
  err ->
    Text.pack (show err)

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

zebraImport :: Import -> EitherT ImportError IO ()
zebraImport x = do
  schema0 <- liftIO . ByteString.readFile $ importSchema x
  schema <- firstT ImportJsonDecodeError . hoistEither $ decodeVersionedSchema schema0

  -- FIXME MonadResource
  bytes <- firstT ImportFileError . readBytes $ importInput x

  withFile (importOutput x) $ \handle -> do
    let header = HeaderV3 schema
    liftIO . Builder.hPutBuilder handle $ bHeader header
    hPutStream handle .
      fmap (Lazy.toStrict . Builder.toLazyByteString) .
      Stream.mapM (hoistEither . first ImportBlockTableError . bRootTable header) .
      Stream.mapM (hoistEither . first ImportJsonTableError . decodeTable schema) .
      lineBoundary $
      Stream.trans (firstT ImportFileError) bytes

withFile :: FilePath -> (Handle -> EitherT x IO ()) -> EitherT x IO ()
withFile path io =
  EitherT $ withBinaryFile path WriteMode (runEitherT . io)
