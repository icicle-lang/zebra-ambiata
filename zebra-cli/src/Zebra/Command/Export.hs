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

import           Control.Monad.IO.Class (liftIO)

import qualified Data.ByteString as ByteString
import           Data.List.NonEmpty (NonEmpty)

import           P

import           System.IO (IO, FilePath, IOMode(..), Handle)
import           System.IO (stdout, withBinaryFile)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither)
import           X.Data.Vector.Stream (Stream)
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Binary.File
import           Zebra.Json
import           Zebra.Schema (TableSchema)
import           Zebra.Table (Table)


data Export =
  Export {
      exportInput :: !FilePath
    , exportOutputs :: !(NonEmpty ExportOutput)
    } deriving (Eq, Ord, Show)

data ExportOutput =
    ExportJsonStdout
  | ExportJson !FilePath
  | ExportSchemaStdout
  | ExportSchema !FilePath
    deriving (Eq, Ord, Show)

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
  (schema, tables) <- firstT ExportFileError $ readTables (exportInput export)

  for_ (exportOutputs export) $ \case
    ExportJsonStdout ->
      writeJson tables stdout

    ExportJson path ->
      withFile path $ writeJson tables

    ExportSchemaStdout ->
      writeSchema schema stdout

    ExportSchema path ->
      withFile path $ writeSchema schema

writeJson :: Stream (EitherT FileError IO) Table -> Handle -> EitherT ExportError IO ()
writeJson tables handle =
  hPutStream handle .
    Stream.mapM (hoistEither . first ExportJsonTableEncodeError . encodeTable) $
    Stream.trans (firstT ExportFileError) tables

writeSchema :: TableSchema -> Handle -> EitherT ExportError IO ()
writeSchema schema handle =
  liftIO $ ByteString.hPut handle (encodeVersionedSchema JsonV0 schema)

withFile :: FilePath -> (Handle -> EitherT x IO ()) -> EitherT x IO ()
withFile path io =
  EitherT $ withBinaryFile path WriteMode (runEitherT . io)
