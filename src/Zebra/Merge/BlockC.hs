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
module Zebra.Merge.BlockC
  ( mergeFiles
  , MergeOptions(..)
  ) where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Mempool (Mempool)

import Zebra.Data hiding (BlockEntity(..))
import Zebra.Data.Entity
import Zebra.Foreign.Block
import Zebra.Foreign.Entity
import Zebra.Foreign.Merge
import Zebra.Foreign.Util

import qualified X.Data.Vector as Boxed

import P

import           Control.Monad.IO.Class (MonadIO(..))
import           X.Control.Monad.Trans.Either (EitherT)
import           Control.Monad.Trans.Class (lift)

import qualified Data.Map as Map

data MergeOptions c m =
  MergeOptions
  { optionPullBlock  :: c -> m (Maybe Block)
  , optionPushEntity :: CEntity -> m ()
  , optionGCEvery    :: !Int
  }

data MergeState c =
  MergeState
  { stateEntityCount   :: !Int
  , stateEntityRefills :: Map.Map EntityId [c]
  , stateMempool       :: Mempool
  , stateMergeMany     :: CMergeMany
  }

-- | Merge a whole bunch of files together.
-- All files must have the same header.
mergeFiles :: MonadIO m
  => MergeOptions c m
  -> Boxed.Vector c
  -> EitherT ForeignError m ()

mergeFiles options files = do
  pool <- liftIO Mempool.create
  merger <- mergeManyInit pool
  let state0 = MergeState 0 Map.empty pool merger
  -- TODO bracket/catch and clean up last memory pool on error
  state <- foldM fill state0 files
  state' <- go state
  liftIO $ Mempool.free $ stateMempool state'
  return ()
 where
  go state0 = do
    state <- gc state0
    pop <- mergeManyPop (stateMergeMany state)
    case pop of
      Nothing ->
        return state
      Just centity -> do
        lift $ optionPushEntity options centity
        eid <- centityId centity
        state' <- refill state eid
        return state' { stateEntityCount = stateEntityCount state' + 1 }

  refill state eid =
    case Map.lookup eid $ stateEntityRefills state of
      Nothing -> return state
      Just refills -> do
        let state' = state { stateEntityRefills = Map.delete eid $ stateEntityRefills state }
        foldM fill state' refills

  fill state fileId = do
    pblock <- lift $ optionPullBlock options fileId
    case pblock of
      Nothing ->
        return state
      Just block -> do
        cblock <- lift $ foreignOfBlock (stateMempool state) block
        entities <- foreignEntitiesOfBlock (stateMempool state) cblock
        mergeManyPush (stateMempool state) (stateMergeMany state) (Boxed.convert entities)

        -- TODO do this better
        case Boxed.uncons $ Boxed.reverse entities of
          Nothing ->
            return state
          Just (centity,_) -> do
            eid <- centityId centity
            let refills  = Map.insertWith (<>) eid [fileId]
                         $ stateEntityRefills state
            let state'   = state { stateEntityRefills = refills }
            return state'


  gc state
   | stateEntityCount state + 1 `mod` optionGCEvery options == 0 = do
    pool' <- liftIO Mempool.create
    merger' <- mergeManyClone pool' $ stateMergeMany state
    liftIO $ Mempool.free $ stateMempool state
    return state { stateMempool = pool', stateMergeMany = merger' }

   | otherwise
   = return state

  centityId centity = do
    -- TODO don't convert CEntity to Entity
    entityId <$> entityOfForeign centity

