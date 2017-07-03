{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.X.Stream (
    unfoldOf
  , whenEmpty

  , hoistEither
  , runEitherT
  , injectEitherT
  , tryEitherT

  , fork
  , pullMVar
  , pushMVar

  , module Streaming -- Contains the 'Stream' and 'Of' data type
  , module Streaming.Prelude
  ) where

import           Control.Concurrent.Lifted (MVar)
import qualified Control.Concurrent.Lifted as Lifted
import           Control.Monad.Catch (MonadCatch, Exception, try)
import           Control.Monad.Trans.Control (MonadBaseControl)

import           P

import           Streaming
import           Streaming.Prelude

import           System.IO (IO)

import           X.Control.Monad.Trans.Either (EitherT)
import qualified X.Control.Monad.Trans.Either as EitherT


unfoldOf :: Functor f => (t -> f ()) -> Of t b -> f b
unfoldOf f (x :> r) =
  f x $> r
{-# INLINABLE unfoldOf #-}

whenEmpty :: Monad m => a -> Stream (Of a) m r -> Stream (Of a) m r
whenEmpty def input = do
  e <- lift $ next input
  case e of
    Left r -> do
      yield def
      pure r

    Right (hd, tl) ->
      cons hd tl
{-# INLINABLE whenEmpty #-}

hoistEither :: Monad m => Stream (Of a) m (Either x r) -> Stream (Of a) (EitherT x m) r
hoistEither =
  bind (lift . EitherT.hoistEither) . hoist lift
{-# INLINABLE hoistEither #-}

runEitherT :: Monad m => Stream (Of a) (EitherT x m) r -> Stream (Of a) m (Either x r)
runEitherT =
  EitherT.runEitherT . distribute
{-# INLINABLE runEitherT #-}

injectEitherT :: Monad m => EitherT x (Stream (Of a) m) r -> Stream (Of a) (EitherT x m) r
injectEitherT =
  hoistEither . EitherT.runEitherT
{-# INLINABLE injectEitherT #-}

tryEitherT :: (MonadCatch m, Exception x) => Stream (Of a) m a -> Stream (Of a) (EitherT x m) a
tryEitherT =
  hoistEither . try
{-# INLINABLE tryEitherT #-}

pushMVar :: MonadBaseControl IO m => MVar (Either r a) -> Stream (Of a) m r -> m ()
pushMVar mvar input = do
  e <- next input
  case e of
    Left r ->
      Lifted.putMVar mvar $ Left r
    Right (hd, tl) -> do
      Lifted.putMVar mvar (Right hd)
      pushMVar mvar tl
{-# INLINABLE pushMVar #-}

pullMVar :: MonadBaseControl IO m => MVar (Either r a) -> Stream (Of a) m r
pullMVar mvar = do
  e <- Lifted.takeMVar mvar
  case e of
    Left r ->
      return r
    Right x -> do
      yield x
      pullMVar mvar
{-# INLINABLE pullMVar #-}

fork :: MonadBaseControl IO m => Stream (Of a) m () -> Stream (Of a) m ()
fork input = do
  mvar <- Lifted.newEmptyMVar
  _ <- lift . Lifted.fork $ pushMVar mvar input
  pullMVar mvar
{-# INLINABLE fork #-}
