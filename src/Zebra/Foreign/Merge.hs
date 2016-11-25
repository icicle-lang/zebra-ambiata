{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Merge (
    mergeEntity
  ) where

import           Anemone.Foreign.Mempool (Mempool)
import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.IO.Class (MonadIO(..))

import           P

import           X.Control.Monad.Trans.Either (EitherT, hoistEither)

import           Zebra.Foreign.Bindings
import           Zebra.Foreign.Entity
import           Zebra.Foreign.Util

mergeEntity :: MonadIO m => Mempool -> CEntity -> CEntity -> EitherT ForeignError m CEntity
mergeEntity pool (CEntity c_entity1) (CEntity c_entity2) = do
  merge_into <- liftIO $ Mempool.alloc pool
  c_error    <- liftIO $ c'zebra_merge_entity pool c_entity1 c_entity2 merge_into
  hoistEither $ fromCError c_error
  return $ CEntity merge_into

