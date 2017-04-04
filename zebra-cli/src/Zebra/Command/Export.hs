{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Command.Export (
    Export(..)
  , ExportOutput(..)
  , zebraExport

  , ExportError(..)
  , renderExportError
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Resource (MonadResource)

import qualified Data.ByteString as ByteString
import           Data.List.NonEmpty (NonEmpty)

import           P

import           System.IO (FilePath, IOMode(..), Handle, stdout)

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, joinEitherT)
import           X.Data.Vector.Stream (Stream)
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Binary.File
import           Zebra.Json.Codec
import           Zebra.Json.Schema
import           Zebra.Schema (TableSchema)
import           Zebra.Table (Table)
import           Zebra.Text


data Export =
  Export {
      exportInput :: !FilePath
    , exportOutputs :: !(NonEmpty ExportOutput)
    } deriving (Eq, Ord, Show)

data ExportOutput =
    ExportTextStdout
  | ExportText !FilePath
  | ExportSchemaStdout
  | ExportSchema !FilePath
    deriving (Eq, Ord, Show)

data ExportError =
    ExportFileError !FileError
  | ExportTextTableEncodeError !TextTableEncodeError
    deriving (Eq, Show)

renderExportError :: ExportError -> Text
renderExportError = \case
  ExportFileError err ->
    renderFileError err
  ExportTextTableEncodeError err ->
    renderTextTableEncodeError err

zebraExport :: MonadResource m => Export -> EitherT ExportError m ()
zebraExport export = do
  (schema, tables) <- firstT ExportFileError $ readTables (exportInput export)

  for_ (exportOutputs export) $ \case
    ExportTextStdout ->
      writeText "<stdout>" stdout tables

    ExportText path -> do
      (close, handle) <- firstT ExportFileError $ openFile path WriteMode
      writeText path handle tables
      firstT ExportFileError close

    ExportSchemaStdout ->
      writeSchema "<stdout>" stdout schema

    ExportSchema path -> do
      (close, handle) <- firstT ExportFileError $ openFile path WriteMode
      writeSchema path handle schema
      firstT ExportFileError close

writeText ::
  MonadResource m =>
  FilePath ->
  Handle ->
  Stream (EitherT FileError m) Table ->
  EitherT ExportError m ()
writeText path handle tables =
  joinEitherT id .
    firstT ExportFileError .
    hPutStream path handle .
    Stream.mapM (hoistEither . first ExportTextTableEncodeError . encodeTable) $
    Stream.trans (firstT ExportFileError) tables

writeSchema :: MonadIO m => FilePath -> Handle -> TableSchema -> EitherT ExportError m ()
writeSchema path handle schema =
  tryIO (ExportFileError . FileWriteError path) $
    ByteString.hPut handle (encodeVersionedSchema JsonV0 schema)
