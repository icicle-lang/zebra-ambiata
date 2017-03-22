{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Serial.File (
    DecodeError(..)
  , renderDecodeError
  , blocksOfBytes
  , fileOfBytes
  , fileOfBytesV3
  , runStreamOne
  , runStreamMany
  , streamOfFile
  , fileOfFilePath
  , fileOfFilePathV3
  ) where

import           Control.Monad.Trans.Class (lift)
import           Control.Monad.IO.Class (MonadIO(..))

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import qualified Data.ByteString as B
import qualified Data.Text as Text
import           Data.String (String)

import           P

import           System.IO (FilePath)
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, left)
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Data.Block
import           Zebra.Serial.Block
import           Zebra.Serial.Header
import           Zebra.Table (Table)
import           Zebra.Schema (TableSchema)


data DecodeError =
   DecodeError String
 | DecodeErrorV2
 | DecodeErrorBadParserFailImmediately String
 | DecodeErrorBadParserExpectsMoreAfterEnd
   deriving (Eq, Ord, Show)

renderDecodeError :: DecodeError -> Text
renderDecodeError = \case
  DecodeError s ->
    Text.pack s
  DecodeErrorV2 ->
    "Decode error: this operation is not supported on v2 zebra files."
  DecodeErrorBadParserFailImmediately s ->
    "Decode error: the parser failed immediately before consuming anything. " <>
    "This means there is a bug in the parser.\n" <>
    "The parser failed with: " <> Text.pack s
  DecodeErrorBadParserExpectsMoreAfterEnd ->
    "Decode error: the parser asked for more input after telling it the stream " <>
    "has ended. This means there is a bug in the parser."

fileOfBytes ::
  Monad m =>
  Stream.Stream m B.ByteString ->
  EitherT DecodeError m (Header, Stream.Stream (EitherT DecodeError m) Block)
fileOfBytes input =
  EitherT $ do
    (mheader, rest) <- runStreamOne getHeader input
    case mheader of
     Left err ->
       pure $ Left err
     Right header ->
      let
        blocks =
          runStreamMany (getBlock header) rest
      in
        pure $ Right (header, blocks)

fileOfBytesV3 ::
  Monad m =>
  Stream.Stream m B.ByteString ->
  EitherT DecodeError m (TableSchema, Stream.Stream (EitherT DecodeError m) Table)
fileOfBytesV3 input =
  EitherT $ do
    (mheader, rest) <- runStreamOne getHeader input
    case mheader of
     Left err ->
       pure $ Left err
     Right (HeaderV2 _) ->
       pure . Left $ DecodeErrorV2
     Right (HeaderV3 schema) ->
      let
        tables =
          runStreamMany (getTableV3 schema) rest
      in
        pure $ Right (schema, tables)

blocksOfBytes ::
  Monad m =>
  Header ->
  Stream.Stream m B.ByteString -> Stream.Stream (EitherT DecodeError m) Block
blocksOfBytes header inp =
  runStreamMany (getBlock header) inp

-- | Run a 'Get' binary decoder over a stream of strict bytestrings.
-- Return the value we got, as well as the rest of the stream
runStreamOne :: Monad m => Get a -> Stream.Stream m B.ByteString -> m (Either DecodeError a, Stream.Stream m B.ByteString)
runStreamOne get (Stream.Stream s'go s'init) = do
  (v,s') <- runOne Stream.SPEC s'init $ Get.runGetIncremental get
  return (v, Stream.Stream runRest s')
  where
    runOne !_ s (Get.Fail leftovers _ err)
     = return (Left $ DecodeError err, (leftovers, s))
    runOne !_ s (Get.Done leftovers _ a)
     = return (Right a, (leftovers, s))
    runOne !_ s (Get.Partial p)
     = s'go s >>= \case
        Stream.Yield str' s'
         -> runOne Stream.SPEC s' (p $ Just str')
        Stream.Skip s'
         -> runOne Stream.SPEC s' (Get.Partial p)
        Stream.Done
         -> runOne Stream.SPEC s (p Nothing)

    runRest (leftovers, s)
     | B.null leftovers
     = s'go s >>= \case
        Stream.Yield str' s'
         -> return $ Stream.Yield str' (leftovers, s')
        Stream.Skip  s'
         -> return $ Stream.Skip (leftovers, s')
        Stream.Done
         -> return $ Stream.Done
     | otherwise
     = return $ Stream.Yield leftovers ("", s)


-- | Keep running a 'Get' binary decoder over a stream of strict bytestrings.
runStreamMany :: Monad m => Get a -> Stream.Stream m B.ByteString -> Stream.Stream (EitherT DecodeError m) a
runStreamMany g (Stream.Stream s'go s'init) =
  Stream.Stream go (s'init, "", Nothing)
  where
    g'init = Get.runGetIncremental g

    go (s, str, decoder)
     | B.null str
     = lift (s'go s) >>= \case
        Stream.Yield str' s'
         -> return $ Stream.Skip (s', str', decoder)
        Stream.Skip  s'
         -> return $ Stream.Skip (s', "", decoder)
        Stream.Done
         -> case decoder of
             Nothing ->
              return $ Stream.Done
             Just partial ->
              case partial Nothing of
                Get.Fail _ _ err -> left $ DecodeError err
                Get.Partial _ -> left $ DecodeErrorBadParserExpectsMoreAfterEnd
                Get.Done _ _ a -> return $ Stream.Yield a (s, "", Nothing)
    go (s, str, Nothing)
     = case g'init of
        Get.Partial p -> return $ Stream.Skip (s, str, Just p)
        -- If the parser fails immediately, we want to throw away the input since otherwise we would have an infinite stream of errors
        Get.Fail _ _ err -> left $ DecodeErrorBadParserFailImmediately err
        -- If the parser returns immediately, we will have an infinite stream of values. Strange. I expect this shouldn't happen.
        Get.Done _ _ a -> return $ Stream.Yield a (s, str, Nothing)
    go (s, str, Just decoder)
     = case decoder (Just str) of
        Get.Fail _ _ err
         -> left $ DecodeError err
        Get.Partial decoder'
         -> return $ Stream.Skip (s, "", Just decoder')
        Get.Done str' _ ret
         -> return $ Stream.Yield ret (s, str', Nothing)


fileOfFilePath :: MonadIO m => FilePath -> EitherT DecodeError m (Header, Stream.Stream (EitherT DecodeError m) Block)
fileOfFilePath path = do
  bytes <- lift $ streamOfFile path
  fileOfBytes bytes

fileOfFilePathV3 :: MonadIO m => FilePath -> EitherT DecodeError m (TableSchema, Stream.Stream (EitherT DecodeError m) Table)
fileOfFilePathV3 path = do
  bytes <- lift $ streamOfFile path
  fileOfBytesV3 bytes

-- This should catch and wrap in EitherT. Too bad.
streamOfFile :: MonadIO m => FilePath -> m (Stream.Stream m B.ByteString)
streamOfFile fp = do
  handle <- liftIO $ IO.openBinaryFile fp IO.ReadMode
  return $ Stream.Stream getBytes (Just handle)
  where
    getBytes Nothing = return $ Stream.Done
    getBytes (Just handle) = do
      bytes <- liftIO $ B.hGet handle (1024*1024)
      case B.null bytes of
       True -> do
        liftIO $ IO.hClose handle
        return $ Stream.Skip Nothing
       False -> do
        return $ Stream.Yield bytes (Just handle)

