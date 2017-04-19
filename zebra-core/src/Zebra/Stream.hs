{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Stream (
    unfoldOf
  , whenEmpty

  , hoistEither
  , runEitherT
  , injectEitherT
  , tryEitherT

  , module Streaming -- Contains the 'Stream' and 'Of' data type
  , module Streaming.Prelude
  ) where

import           Control.Monad.Catch (MonadCatch, Exception, try)

import           P

import           Streaming
import           Streaming.Prelude

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
