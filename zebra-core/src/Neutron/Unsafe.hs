{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE UnboxedTuples #-}
module Neutron.Unsafe (
    Unsafe
  , unsafePerformT
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Primitive (RealWorld)
import           Control.Monad.Trans.Class (MonadTrans(..))

import           GHC.Types (IO(..))
import           GHC.Prim (State#, realWorld#, noDuplicate#)

import           P


newtype Unsafe m a =
  Unsafe {
      unUnsafe :: State# RealWorld -> m (World a)
    }

data World a =
  World (State# RealWorld) a

instance Monad m => Monad (Unsafe m) where
  return x =
    Unsafe (\world ->
      return $! World world x)
  {-# INLINE return #-}

  (>>=) (Unsafe m) k =
    Unsafe (\world0 -> do
      World world x <- m world0
      unUnsafe (k x) world)
  {-# INLINE (>>=) #-}

instance MonadTrans Unsafe where
  lift m =
    Unsafe (\world -> do
      !x <- m
      pure $! World world x)
  {-# INLINE lift #-}

instance Monad m => MonadIO (Unsafe m) where
  liftIO (IO io) =
    Unsafe (\world0 ->
      case io world0 of
        (# world, x #) ->
          pure $! World world x)
  {-# INLINE liftIO #-}

instance Monad m => Functor (Unsafe m) where
  fmap =
    liftM
  {-# INLINE fmap #-}

instance Monad m => Applicative (Unsafe m) where
  pure =
    return
  {-# INLINE pure #-}

  (<*>) =
    ap
  {-# INLINE (<*>) #-}

unsafePerformT :: Monad m => Unsafe m a -> m a
unsafePerformT (Unsafe m) = do
  case realWorld# of
    world0 ->
      case noDuplicate# world0 of
        world -> do
          World _ x <- m world
          pure x
{-# INLINE unsafePerformT #-}
