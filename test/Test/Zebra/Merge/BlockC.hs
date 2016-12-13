{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Test.Zebra.Merge.BlockC where

import           P

import           System.IO (IO)
import qualified Data.IORef as Ref

import           Zebra.Merge.BlockC

import qualified X.Data.Vector as Boxed

import Zebra.Data hiding (BlockEntity(..))

import           Control.Monad.IO.Class (MonadIO(..))
import           X.Control.Monad.Trans.Either (EitherT, joinEitherT)


import           Zebra.Data.Entity
import           Zebra.Foreign.Entity

mergeLists :: [[Block]] -> EitherT MergeError IO [Entity]
mergeLists blocks0 = do
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
  let opts = MergeOptions pull push 2

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


