{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Command.Util (
    getBinaryStdout
  , writeFileOrStdout
  ) where

import           Control.Monad.Catch (MonadCatch)
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Resource (MonadResource)

import qualified Data.Char as Char

import           P

import           System.IO (FilePath, Handle, getLine, putStr)
import           System.IO (stdout, hIsTerminalDevice, hFlush)
import           System.IO.Error (IOError)

import           Viking (ByteStream)
import qualified Viking.ByteStream as ByteStream

import           X.Control.Monad.Trans.Either (EitherT)


getBinaryStdout :: MonadIO m => m (Maybe Handle)
getBinaryStdout =
  liftIO $ do
    tty <- hIsTerminalDevice stdout
    if tty then do
      putStr "About to write a binary file to stdout. Are you sure? [y/N] "
      hFlush stdout
      line <- fmap Char.toLower <$> getLine
      if line == "y" then
        pure $ Just stdout
      else
        pure Nothing
    else
      pure $ Just stdout

writeFileOrStdout ::
     MonadResource m
  => MonadCatch m
  => Maybe FilePath
  -> ByteStream m ()
  -> EitherT IOError m ()
writeFileOrStdout mpath bss = do
  case mpath of
    Nothing -> do
      mhandle <- getBinaryStdout
      case mhandle of
        Nothing ->
          pure ()
        Just handle ->
          ByteStream.hPut handle bss

    Just path ->
      ByteStream.writeFile path bss
{-# INLINABLE writeFileOrStdout #-}
