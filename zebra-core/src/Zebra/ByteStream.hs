{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
module Zebra.ByteStream (
    ByteStream

  , defaultChunkSize

  , readFile
  , readFileN
  , writeFile

  , hGetContents
  , hGetContentsN
  , hPut

  , hoistEither
  , runEitherT
  , injectEitherT
  , tryEitherT

  , fromBuilders

  , embed

  , module Data.ByteString.Streaming
  , bracketByteString
  , consChunk
  ) where

import           Control.Monad.Catch (MonadCatch, Exception, try)
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Resource (MonadResource)

import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Streaming as ByteStream
import           Data.ByteString.Streaming hiding (writeFile, readFile, hGetContents, hGetContentsN, hPut)
import           Data.ByteString.Streaming.Internal (ByteString(..))
import           Data.ByteString.Streaming.Internal (bracketByteString, consChunk)

import           P hiding (concat)

import           System.IO (FilePath, Handle, IOMode(..))
import qualified System.IO as IO
import           System.IO.Error (IOError)

import           X.Control.Monad.Trans.Either (EitherT)
import qualified X.Control.Monad.Trans.Either as EitherT

import           Zebra.Stream (Stream, Of(..))
import qualified Zebra.Stream as Stream


-- | Rename the streaming 'ByteString' because otherwise it's too hard to work
--   with in conjunction with strict @ByteString@.
--
type ByteStream =
  ByteString

defaultChunkSize :: Int
defaultChunkSize =
  1024 * 1024
{-# INLINABLE defaultChunkSize #-}

readFile :: (MonadResource m, MonadCatch m) => FilePath -> ByteString (EitherT IOError m) ()
readFile =
  readFileN defaultChunkSize
{-# INLINABLE readFile #-}

readFileN :: (MonadResource m, MonadCatch m) => Int -> FilePath -> ByteString (EitherT IOError m) ()
readFileN chunkSize path =
  tryEitherT . bracketByteString (IO.openBinaryFile path ReadMode) IO.hClose $
    ByteStream.hGetContentsN chunkSize
{-# INLINABLE readFileN #-}

writeFile :: (MonadResource m, MonadCatch m) => FilePath -> ByteString m r -> EitherT IOError m r
writeFile path bss = do
  h <- EitherT.tryEitherT id . liftIO $ IO.openBinaryFile path WriteMode
  r <- hPut h bss
  EitherT.tryEitherT id . liftIO $ IO.hClose h
  pure r
{-# INLINABLE writeFile #-}

hGetContents :: (MonadIO m, MonadCatch m) => Handle -> ByteString (EitherT IOError m) ()
hGetContents =
  hGetContentsN defaultChunkSize
{-# INLINABLE hGetContents #-}

hGetContentsN :: (MonadIO m, MonadCatch m) => Int -> Handle -> ByteString (EitherT IOError m) ()
hGetContentsN chunkSize handle =
  tryEitherT $ ByteStream.hGetContentsN chunkSize handle
{-# INLINABLE hGetContentsN #-}

hPut :: (MonadIO m, MonadCatch m) => Handle -> ByteString m r -> EitherT IOError m r
hPut handle bss =
  EitherT.tryEitherT id $ ByteStream.hPut handle bss
{-# INLINABLE hPut #-}

hoistEither :: Monad m => ByteString m (Either x r) -> ByteString (EitherT x m) r
hoistEither =
  bind (lift . EitherT.hoistEither) . hoist lift
{-# INLINABLE hoistEither #-}

runEitherT :: Monad m => ByteString (EitherT x m) r -> ByteString m (Either x r)
runEitherT =
  EitherT.runEitherT . distribute
{-# INLINABLE runEitherT #-}

injectEitherT :: Monad m => EitherT x (ByteString m) r -> ByteString (EitherT x m) r
injectEitherT =
  hoistEither . EitherT.runEitherT
{-# INLINABLE injectEitherT #-}

tryEitherT :: (MonadCatch m, Exception x) => ByteString m a -> ByteString (EitherT x m) a
tryEitherT =
  hoistEither . try
{-# INLINABLE tryEitherT #-}

fromBuilders :: Monad m => Stream (Of Builder) m r -> ByteStream m r
fromBuilders =
  concat . Stream.maps (Stream.unfoldOf $ fromLazy . Builder.toLazyByteString)
{-# INLINABLE fromBuilders #-}

embed :: Monad n => (forall a. m a -> ByteString n a) -> ByteString m b -> ByteString n b
embed phi =
  let
    loop = \case
      Empty r ->
        Empty r
      Chunk bs rest ->
        Chunk bs (loop rest)
      Go m ->
        loop =<< phi m
  in
    loop
{-# INLINABLE embed #-}
