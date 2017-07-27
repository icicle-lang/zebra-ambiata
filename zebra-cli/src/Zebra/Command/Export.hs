{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Command.Export (
    Export(..)
  , ExportOutput(..)
  , zebraExport

  , ExportError(..)
  , renderExportError
  ) where

import           Control.Monad.Catch (MonadCatch(..))
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import qualified Data.ByteString as ByteString
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath, stdout)
import           System.IO.Error (IOError)

import           Viking (ByteStream)
import qualified Viking.ByteStream as ByteStream

import           X.Control.Monad.Trans.Either (EitherT, tryEitherT, firstJoin)

import           Zebra.Serial.Binary (BinaryLogicalDecodeError)
import qualified Zebra.Serial.Binary as Binary
import           Zebra.Serial.Text (TextLogicalEncodeError)
import qualified Zebra.Serial.Text as Text
import qualified Zebra.Table.Schema as Schema


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
    ExportIOError !IOError
  | ExportBinaryLogicalDecodeError !BinaryLogicalDecodeError
  | ExportTextLogicalEncodeError !TextLogicalEncodeError
    deriving (Eq, Show)

renderExportError :: ExportError -> Text
renderExportError = \case
  ExportIOError err ->
    Text.pack (show err)
  ExportBinaryLogicalDecodeError err ->
    Binary.renderBinaryLogicalDecodeError err
  ExportTextLogicalEncodeError err ->
    Text.renderTextLogicalEncodeError err

takeSchemaOutputs :: [ExportOutput] -> [Maybe FilePath]
takeSchemaOutputs =
  mapMaybe $ \case
    ExportSchemaStdout ->
      Just Nothing
    ExportSchema x ->
      Just (Just x)
    _ ->
      Nothing

takeTextOutputs :: [ExportOutput] -> [Maybe FilePath]
takeTextOutputs =
  mapMaybe $ \case
    ExportTextStdout ->
      Just Nothing
    ExportText x ->
      Just (Just x)
    _ ->
      Nothing

writeSchema :: (MonadIO m, MonadCatch m) => Schema.Table -> Maybe FilePath -> EitherT ExportError m ()
writeSchema schema = \case
  Nothing ->
    tryEitherT ExportIOError . liftIO $
      ByteString.hPut stdout (Text.encodeSchema schema)

  Just path ->
    tryEitherT ExportIOError . liftIO $
      ByteString.writeFile path (Text.encodeSchema schema)

writeText :: (MonadCatch m, MonadResource m) => [Maybe FilePath] -> ByteStream (EitherT ExportError m) r -> ByteStream (EitherT ExportError m) r
writeText xs0 tables =
  case xs0 of
    [] ->
      tables

    Nothing : xs ->
      writeText xs . hoist (firstJoin ExportIOError) . ByteStream.injectEitherT $
        ByteStream.hPut stdout (ByteStream.copy tables)

    Just path : xs -> do
      writeText xs . hoist (firstJoin ExportIOError) . ByteStream.injectEitherT $
        ByteStream.writeFile path (ByteStream.copy tables)

zebraExport :: forall m. (MonadResource m, MonadCatch m) => Export -> EitherT ExportError m ()
zebraExport export = do
  (schema, tables0) <-
    firstJoin ExportBinaryLogicalDecodeError .
      Binary.decodeLogical .
    hoist (firstT ExportIOError) $
      ByteStream.readFile (exportInput export)

  let
    outputs =
      toList $ exportOutputs export

  traverse_ (writeSchema schema) $ takeSchemaOutputs outputs

  case takeTextOutputs outputs of
    [] ->
      pure ()

    xs ->
      let
        tables1 :: ByteStream (EitherT ExportError m) ()
        tables1 =
          hoist (firstJoin ExportTextLogicalEncodeError) .
            Text.encodeLogical schema .
          hoist (firstJoin ExportBinaryLogicalDecodeError) $
            tables0
      in
        ByteStream.effects $
          writeText xs tables1
{-# SPECIALIZE zebraExport :: Export -> EitherT ExportError (ResourceT IO) () #-}
