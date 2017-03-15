{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Merge.Puller.List (
    mergeLists
  ) where

import           Control.Monad.IO.Class (MonadIO(..))

import qualified Data.IORef as Ref

import           P

import           System.IO (IO)

import           X.Control.Monad.Trans.Either (EitherT, joinEitherT)
import qualified X.Data.Vector as Boxed

import           Zebra.Data.Block
import           Zebra.Data.Entity
import           Zebra.Foreign.Entity
import           Zebra.Merge.BlockC


mergeLists :: Int64 -> [[Block]] -> EitherT MergeError IO [Entity]
mergeLists gcEvery blocks0 = do
  blockRef <- liftIO $ Ref.newIORef blocks0
  entityRef <- liftIO $ Ref.newIORef []
  let pull ix = do
      blocks <- liftIO $ Ref.readIORef blockRef
      case headIx blocks ix of
        Nothing -> return Nothing
        Just (b,blocks') -> do
          liftIO $ Ref.writeIORef blockRef blocks'
          return (Just b)
  let push e = do
      e' <- entityOfForeign e
      entities <- liftIO $ Ref.readIORef entityRef
      liftIO $ Ref.writeIORef entityRef (e' : entities)
  let opts = MergeOptions pull push gcEvery

  let ixes = Boxed.enumFromN (0 :: Int) (length blocks0)

  joinEitherT MergeForeign $ mergeBlocks opts ixes
  reverse <$> liftIO (Ref.readIORef entityRef)

 where

  headIx ((b:as):bs) 0 = Just (b, as:bs)
  headIx _           0 = Nothing
  headIx (as:bs) n
   | Just (b,bs') <- headIx bs (n-1)
   = Just (b, as:bs')
  headIx _ _
   = Nothing
