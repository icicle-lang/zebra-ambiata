{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Command.Summary (
    Summary(..)
  , zebraSummary

  , SummaryError(..)
  , renderSummaryError

  -- * Display for other consumers
  , zebraDisplay
  ) where

import           Control.Monad.Catch (MonadMask(..))
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (MonadResource, ResourceT)

import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed

import           P

import qualified System.Console.Regions as Concurrent
import           System.IO (IO, FilePath)
import           System.IO.Error (IOError)

import           Viking (Of(..))
import qualified Viking.ByteStream as ByteStream
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, firstJoin)

import           Zebra.Serial.Binary (BinaryStripedDecodeError)
import qualified Zebra.Serial.Binary as Binary
import qualified Zebra.Serial.Json as Json
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


data Summary =
  Summary {
      summaryInput :: !FilePath
    } deriving (Eq, Ord, Show)

data SummaryError =
    SummaryIOError !IOError
  | SummaryBinaryStripedDecodeError !BinaryStripedDecodeError
    deriving (Eq, Show)

renderSummaryError :: SummaryError -> Text
renderSummaryError = \case
  SummaryIOError err ->
    Text.pack (show err)
  SummaryBinaryStripedDecodeError err ->
    Binary.renderBinaryStripedDecodeError err

data FileSummary =
  FileSummary {
      fileBlocks :: !Int
    , fileRows :: !Int
    , fileMaxRowsPerBlock :: !Int
    , fileKeySchema :: !(Maybe Schema.Column)
    , fileFirstKey :: !(Maybe Logical.Value)
    , fileLastKey :: !(Maybe Logical.Value)
    }

instance Monoid FileSummary where
  mempty =
    FileSummary 0 0 0 Nothing Nothing Nothing
  mappend (FileSummary b0 r0 m0 s0 f0 l0) (FileSummary b1 r1 m1 s1 f1 l1) =
    FileSummary (b0 + b1) (r0 + r1) (max m0 m1) (s0 <|> s1) (f0 <|> f1) (l1 <|> l0)

fromTable :: Striped.Table -> FileSummary
fromTable table =
  let
    !n =
      Striped.length table

    !summary =
      mempty {
          fileBlocks =
            1
        , fileRows =
            n
        , fileMaxRowsPerBlock =
            n
        }
  in
    case table of
      Striped.Binary _ _ _ ->
        summary

      Striped.Array _ _ ->
        summary

      Striped.Map _ k _ ->
        case Striped.toValues k of
          Left _ ->
            summary

          Right ks ->
            if Boxed.null ks then
              summary
            else
              summary {
                  fileKeySchema =
                    Just $ Striped.schemaColumn k

                , fileFirstKey =
                    Just $ Boxed.head ks

                , fileLastKey =
                    Just $ Boxed.last ks
                }

renderKey :: Maybe Schema.Column -> Maybe Logical.Value -> Maybe Text
renderKey mschema mkey = do
  schema <- mschema
  key <- mkey
  rightToMaybe . fmap Text.decodeUtf8 $
    Json.encodeLogicalValue schema key

renderFileSummary :: FileSummary -> Text
renderFileSummary (FileSummary nblocks nrows nmaxrows mschema mfirst mlast) =
  Text.intercalate "\n" [
      "block_count        = " <> Text.pack (show nblocks)
    , "row_count          = " <> Text.pack (show nrows)
    , "max_rows_per_block = " <> Text.pack (show nmaxrows)
    , "first_key          = " <> fromMaybe "<file is not a map>" (renderKey mschema mfirst)
    , "last_key           = " <> fromMaybe "<file is not a map>" (renderKey mschema mlast)
    ]

-- | Display a summary of a stream.
--
-- Typically, one would use this with this with 'store' if further
-- work is also required.
--
-- @
-- Stream.store (zebraDisplay region)
-- @
zebraDisplay :: forall m r. (MonadResource m) => Concurrent.ConsoleRegion -> Stream.Stream (Of Striped.Table) m r -> m r
zebraDisplay region tables = do
  let
    loop summary0 table = do
      let
        !summary =
          summary0 <> fromTable table

      liftIO $ Concurrent.setConsoleRegion region (renderFileSummary summary)
      pure summary

  summary :> r <-
    Stream.foldM loop (pure mempty) pure tables

  liftIO $ Concurrent.finishConsoleRegion region (renderFileSummary summary)
  return r

zebraSummary :: forall m. (MonadResource m, MonadMask m) => Summary -> EitherT SummaryError m ()
zebraSummary export =
  EitherT . Concurrent.displayConsoleRegions . runEitherT $ do
    region <- liftIO $ Concurrent.openConsoleRegion Concurrent.Linear

    let
      tables =
        hoist (firstJoin SummaryBinaryStripedDecodeError) .
          Binary.decodeStriped .
        hoist (firstT SummaryIOError) $
          ByteStream.readFile (summaryInput export)

    zebraDisplay region tables
{-# SPECIALIZE zebraSummary :: Summary -> EitherT SummaryError (ResourceT IO) () #-}
