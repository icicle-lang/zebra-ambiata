{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Command.Export (
    Export(..)
  , zebraExport

  , ExportError(..)
  , renderExportError
  ) where

import           P

import           System.IO (IO, FilePath, IOMode(..), Handle)
import           System.IO (stdout, withBinaryFile)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither)
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Binary.File
import           Zebra.Json.Table


data Export =
  Export {
      exportInput :: !FilePath
    , exportOutput :: !(Maybe FilePath)
    } deriving (Eq, Ord, Show)

data ExportError =
    ExportFileError !FileError
  | ExportJsonTableEncodeError !JsonTableEncodeError
    deriving (Eq, Show)

renderExportError :: ExportError -> Text
renderExportError = \case
  ExportFileError err ->
    renderFileError err
  ExportJsonTableEncodeError err ->
    renderJsonTableEncodeError err

zebraExport :: Export -> EitherT ExportError IO ()
zebraExport export = do
  (_, tables) <- firstT ExportFileError $ readTables (exportInput export)
  withFile (exportOutput export) $ \handle ->
    hPutStream handle .
      Stream.mapM (hoistEither . first ExportJsonTableEncodeError . encodeTable) $
      Stream.trans (firstT ExportFileError) tables

withFile :: Maybe FilePath -> (Handle -> EitherT x IO a) -> EitherT x IO a
withFile mpath io =
  case mpath of
    Nothing ->
      io stdout
    Just path ->
      EitherT $ withBinaryFile path WriteMode (runEitherT . io)
