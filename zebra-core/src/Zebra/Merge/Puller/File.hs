{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Merge.Puller.File
  ( blockChainPuller
  , pullerOfStream
  , PullerError (..)
  , PullId (..)
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Resource (MonadResource(..))

import qualified Data.IORef as IORef

import           P

import           System.IO (FilePath)

import           X.Control.Monad.Trans.Either (EitherT, left)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Factset.Block
import           Zebra.Foreign.Util
import           Zebra.Serial.Binary.File


data PullerError =
   PullerForeign ForeignError
 | PullerDifferentHeaders FilePath FilePath
 | PullerDecode FileError
 deriving Show

newtype PullId =
   PullId { unPullId :: Int }
   deriving Show


blockChainPuller :: MonadResource m
  => Boxed.Vector FilePath
  -> EitherT PullerError m (PullId -> EitherT FileError m (Maybe Block), Boxed.Vector PullId)
blockChainPuller files
 | Just (file0, _) <- Boxed.uncons files
 = do
    (header0,_) <- firstT PullerDecode $ readBlocks file0
    pullers <- mapM (makePuller file0 header0) files
    let puller pid = pullers Boxed.! unPullId pid
    let pullids = Boxed.map (PullId . fst) $ Boxed.indexed files
    return (puller, pullids)

 -- No input files, don't even bother
 | otherwise
 = return (\_id -> return Nothing, Boxed.empty)

 where
  makePuller file0 header0 fileN = do
    blocks <- readBlocksCheckHeader file0 header0 fileN
    lift $ pullerOfStream blocks

  readBlocksCheckHeader file0 header0 fileN = do
    (h,bs) <- firstT PullerDecode $ readBlocks fileN
    -- It is tempting to put the header in entirety
    -- but I think it will be too big to print with Show so worry about it later
    when (h /= header0) $
      left $ PullerDifferentHeaders file0 fileN
    return bs



pullerOfStream :: MonadIO m => Stream.Stream (EitherT e m) b -> m (EitherT e m (Maybe b))
pullerOfStream (Stream.Stream loop state0) = do
  stateRef <- liftIO $ IORef.newIORef state0
  return $ go stateRef
 where
  go stateRef = do
    state <- liftIO $ IORef.readIORef stateRef
    step  <- loop state
    case step of
      Stream.Yield v state' -> do
        liftIO $ IORef.writeIORef stateRef state'
        return (Just v)
      Stream.Skip state' -> do
        liftIO $ IORef.writeIORef stateRef state'
        go stateRef
      Stream.Done -> do
        return Nothing

