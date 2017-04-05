{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections #-}
module Zebra.Serial.Binary.File (
    FileError(..)
  , renderFileError

  , readBlocks
  , readTables
  , readBytes

  , hPutStream

  , decodeBlocks
  , decodeTables

  , decodeGetOne
  , decodeGetAll

  , openFile
  , tryIO
  , allocateT
  ) where

import           Control.Monad.Catch (catchIOError)
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Resource (MonadResource(..), ReleaseKey)
import qualified Control.Monad.Trans.Resource as Resource

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.String (String)
import qualified Data.Text as Text

import           P

import           System.IO (IO, IOMode(..), FilePath, Handle)
import qualified System.IO as IO
import           System.IO.Error (IOError)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, left)
import           X.Data.Vector.Stream (Stream(..), Step, SPEC(..))
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Factset.Block
import           Zebra.Serial.Binary.Block
import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.Header
import           Zebra.Table.Schema (TableSchema)
import qualified Zebra.Table.Striped as Striped


data FileError =
   FileOpenError !FilePath !IOError
 | FileReadError !FilePath !IOError
 | FileWriteError !FilePath !IOError
 | FileCloseError !FilePath !IOError
 | FileGetOneError !String
 | FileGetAllError !String
 | FileDecodeFailedImmediately !String
 | FileDecodeEndOfFile
   deriving (Eq, Show)

renderFileError :: FileError -> Text
renderFileError = \case
  FileOpenError path err ->
    "Error opening " <> Text.pack path <> ": " <> Text.pack (show err)
  FileReadError path err ->
    "Error reading " <> Text.pack path <> ": " <> Text.pack (show err)
  FileWriteError path err ->
    "Error writing " <> Text.pack path <> ": " <> Text.pack (show err)
  FileCloseError path err ->
    "Error closing " <> Text.pack path <> ": " <> Text.pack (show err)
  FileGetOneError s ->
    "Decode error reading header: " <> Text.pack s
  FileGetAllError s ->
    "Decode error reading blocks: " <> Text.pack s
  FileDecodeFailedImmediately s ->
    "Decode error: the parser failed immediately before consuming anything. " <>
    "This means there is a bug in the parser.\n" <>
    "The parser failed with: " <> Text.pack s
  FileDecodeEndOfFile ->
    "Decode error: the parser asked for more input after telling it the stream " <>
    "has ended. This means there is a bug in the parser."

readBlocks :: MonadResource m => FilePath -> EitherT FileError m (Header, Stream (EitherT FileError m) Block)
readBlocks path = do
  bytes <- readBytes path
  decodeBlocks bytes

readTables :: MonadResource m => FilePath -> EitherT FileError m (TableSchema, Stream (EitherT FileError m) Striped.Table)
readTables path = do
  bytes <- readBytes path
  decodeTables bytes

readBytes :: MonadResource m => FilePath -> EitherT FileError m (Stream (EitherT FileError m) ByteString)
readBytes path = do
  (close, handle) <- openFile path ReadMode
  pure . Stream (stepReadChunk path close) $ Just handle

stepReadChunk ::
  MonadIO m =>
  FilePath ->
  EitherT FileError m () ->
  Maybe Handle ->
  EitherT FileError m (Step (Maybe Handle) ByteString)
stepReadChunk path close = \case
  Nothing ->
    pure Stream.Done

  Just handle -> do
    bytes <- tryIO (FileReadError path) $ ByteString.hGet handle (1024 * 1024)

    if ByteString.null bytes then do
      close
      pure $ Stream.Skip Nothing
    else
      pure . Stream.Yield bytes $ Just handle

hPutStream :: MonadIO m => FilePath -> Handle -> Stream m ByteString -> EitherT FileError m ()
hPutStream path handle (Stream step sinit) =
  let
    loop !_ s0 = do
      lift (step s0) >>= \case
        Stream.Yield bs s -> do
          tryIO (FileWriteError path) $ ByteString.hPut handle bs
          loop SPEC s

        Stream.Skip s ->
          loop SPEC s

        Stream.Done ->
          pure ()
  in
    loop SPEC sinit

decodeBlocks ::
  Monad m =>
  Stream (EitherT FileError m) ByteString ->
  EitherT FileError m (Header, Stream (EitherT FileError m) Block)
decodeBlocks input = do
  (header, rest) <- decodeGetOne getHeader input
  pure (
      header
    , decodeGetAll (getBlock header) rest
    )

decodeTables ::
  Monad m =>
  Stream (EitherT FileError m) ByteString ->
  EitherT FileError m (TableSchema, Stream (EitherT FileError m) Striped.Table)
decodeTables input = do
  (header, rest) <- decodeGetOne getHeader input
  pure (
      schemaOfHeader header
    , decodeGetAll (getRootTable header) rest
    )

------------------------------------------------------------------------

-- | Run a 'Get' binary decoder over a stream of strict 'ByteString'.
--
--   Return the value we got, as well as the rest of the stream.
--
decodeGetOne ::
  Monad m =>
  Get a ->
  Stream (EitherT FileError m) ByteString ->
  EitherT FileError m (a, Stream (EitherT FileError m) ByteString)
decodeGetOne get (Stream step sinit) =
  let
    runOne !_ s0 = \case
      Get.Fail _bs _off err ->
        left $ FileGetOneError err

      Get.Done bs _off x ->
        pure (x, (bs, s0))

      Get.Partial k ->
       step s0 >>= \case
         Stream.Yield bs s ->
           runOne SPEC s $ k (Just bs)

         Stream.Skip s ->
           runOne SPEC s $ Get.Partial k

         Stream.Done ->
           runOne SPEC s0 $ k Nothing

    runRest = \case
      Left (bs, s) ->
        pure . Stream.Yield bs $ Right s

      Right s0 ->
        step s0 >>= \case
          Stream.Yield bs s ->
            pure . Stream.Yield bs $ Right s

          Stream.Skip s ->
            pure . Stream.Skip $ Right s

          Stream.Done ->
            pure Stream.Done
  in
    second (Stream runRest . Left) <$> runOne SPEC sinit (Get.runGetIncremental get)

data Action s a =
    Pull !(Maybe s) !(Maybe (Maybe ByteString -> Get.Decoder a))
  | Decode !(Maybe s) !(Get.Decoder a)

decode :: Get a -> Maybe s -> Maybe (Maybe ByteString -> Get.Decoder a) -> ByteString -> Action s a
decode get ms mk bs =
  if ByteString.null bs then
    Pull ms mk
  else
    case mk of
      Nothing ->
        Decode ms $ Get.runGetIncremental get `Get.pushChunk` bs
      Just k ->
        Decode ms $ k (Just bs)

finish :: Maybe (Maybe ByteString -> Get.Decoder a) -> Action s a
finish = \case
  Nothing ->
    Pull Nothing Nothing
  Just k ->
    Decode Nothing (k Nothing)

-- | Keep running a 'Get' binary decoder over a stream of strict 'ByteString'.
--
decodeGetAll :: Monad m => Get a -> Stream (EitherT FileError m) ByteString -> Stream (EitherT FileError m) a
decodeGetAll get (Stream pull sinit) =
  let
    skip =
      pure . Stream.Skip

    yield x =
      pure . Stream.Yield x

    loop = \case
      Pull Nothing Nothing ->
        pure Stream.Done

      Pull Nothing (Just _) ->
        left FileDecodeEndOfFile

      Pull (Just s0) mk ->
        pull s0 >>= \case
          Stream.Skip s ->
            skip $ Pull (Just s) mk

          Stream.Yield bs s ->
            skip $ decode get (Just s) mk bs

          Stream.Done ->
            skip $ finish mk

      Decode ms decoder ->
        case decoder of
          Get.Fail _bs _off err ->
            left $ FileGetAllError err

          Get.Partial k ->
            skip $ Pull ms (Just k)

          Get.Done bs _off x ->
            yield x $ decode get ms Nothing bs
  in
    Stream loop $ Pull (Just sinit) Nothing

------------------------------------------------------------------------

openFile ::
  MonadResource m =>
  FilePath ->
  IOMode ->
  EitherT FileError m (EitherT FileError m (), Handle)
openFile path mode = do
  (key, handle) <-
    allocateT (tryIO (FileOpenError path) $ IO.openBinaryFile path mode) IO.hClose

  pure (tryIO (FileCloseError path) $ Resource.release key, handle)

tryIO :: MonadIO m => (IOError -> x) -> IO a -> EitherT x m a
tryIO onErr io =
  EitherT . liftIO $ catchIOError (fmap Right io) (pure . Left . onErr)

allocateT :: MonadResource m => EitherT x IO a -> (a -> IO ()) -> EitherT x m (ReleaseKey, a)
allocateT acquire release =
  EitherT $ do
    (key, ex) <-
      Resource.allocate (runEitherT acquire) $ \case
        Left _ ->
          pure () -- error allocating, no need to release
        Right x ->
          release x
    pure $
      fmap (key,) ex
