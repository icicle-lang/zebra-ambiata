{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
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

import           Control.Monad.Catch (MonadCatch(..))
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.Text as Text

import           P

import           System.IO (IO, FilePath, stdout)
import           System.IO.Error (IOError)

import           X.Control.Monad.Trans.Either (EitherT, tryEitherT, firstJoin)

import           Zebra.ByteStream (ByteStream)
import qualified Zebra.ByteStream as ByteStream
import           Zebra.Serial.Binary (BinaryLogicalDecodeError)
import qualified Zebra.Serial.Binary as Binary
import           Zebra.Serial.Text (TextLogicalEncodeError)
import qualified Zebra.Serial.Text as Text


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

zebraExport :: forall m. (MonadResource m, MonadCatch m) => Export -> EitherT ExportError m ()
zebraExport export = do
  (schema, tables0) <-
    firstJoin ExportBinaryLogicalDecodeError .
      Binary.decodeLogical .
    hoist (firstT ExportIOError) $
      ByteStream.readFile (exportInput export)

  let
    bschema :: ByteString
    bschema =
      Text.encodeSchema schema

    tables1 :: ByteStream (EitherT ExportError m) ()
    tables1 =
      hoist (firstJoin ExportTextLogicalEncodeError) .
        Text.encodeLogical schema .
      hoist (firstJoin ExportBinaryLogicalDecodeError) $
        tables0

    loop ::
         [ExportOutput]
      -> ByteStream (EitherT ExportError m) r
      -> ByteStream (EitherT ExportError m) r
    loop xs0 tables =
      case xs0 of
        [] ->
          tables

        ExportTextStdout : xs ->
          loop xs . hoist (firstJoin ExportIOError) . ByteStream.injectEitherT $
            ByteStream.hPut stdout (ByteStream.copy tables)

        ExportText path : xs -> do
          loop xs . hoist (firstJoin ExportIOError) . ByteStream.injectEitherT $
            ByteStream.writeFile path (ByteStream.copy tables)

        ExportSchemaStdout : xs -> do
          lift . tryEitherT ExportIOError . liftIO $
            ByteString.hPut stdout bschema
          loop xs tables

        ExportSchema path : xs -> do
          lift . tryEitherT ExportIOError . liftIO $
            ByteString.writeFile path bschema
          loop xs tables

  ByteStream.effects $
    loop (toList (exportOutputs export)) tables1
{-# SPECIALIZE zebraExport :: Export -> EitherT ExportError (ResourceT IO) () #-}
