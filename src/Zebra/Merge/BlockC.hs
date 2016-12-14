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
  ( mergeBlocks
  , MergeOptions(..)
  , MergeError(..)
  ) where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Mempool (Mempool)

import Zebra.Data hiding (BlockEntity(..))
import Zebra.Foreign.Block
import Zebra.Foreign.Entity
import Zebra.Foreign.Merge
import Zebra.Foreign.Util

import qualified X.Data.Vector as Boxed

import P

import           Control.Monad.IO.Class (MonadIO(..))
import           X.Control.Monad.Trans.Either (EitherT, left)
import           Control.Monad.Trans.Class (lift)

import qualified Data.Map.Strict as Map

data MergeError =
    MergeInputEntitiesOutOfOrder (EntityHash,EntityId) (EntityHash,EntityId)
  | MergeForeign ForeignError
  deriving (Eq, Ord, Show)


data MergeOptions c m =
  MergeOptions
  { optionPullBlock  :: c -> m (Maybe Block)
  , optionPushEntity :: CEntity -> m ()
  , optionGCEvery    :: !Int
  }

data MergeState c =
  MergeState
  { stateEntityCount   :: !Int
  , stateEntityRefills :: !(Map.Map EntityId [c])
  , stateMempool       :: !Mempool
  , stateMergeMany     :: !CMergeMany
  -- Last seen entity id, to check
  , stateLastEntityId  :: !(Maybe (EntityHash, EntityId))
  }

-- | Merge a whole bunch of files together.
-- All files must have the same header.
mergeBlocks :: MonadIO m
  => MergeOptions c m
  -> Boxed.Vector c
  -> EitherT MergeError m ()

mergeBlocks options files = do
  pool <- liftIO Mempool.create
  merger <- foreign $ mergeManyInit pool
  let state0 = MergeState 0 Map.empty pool merger Nothing
  -- TODO bracket/catch and clean up last memory pool on error
  -- need to convert state into an IORef for this
  state <- foldM fill state0 files
  state' <- go state
  liftIO $ Mempool.free $ stateMempool state'
  return ()
 where

  go state0 = do
    state <- gc state0
    pop <- foreign $ mergeManyPop (stateMergeMany state)
    case pop of
      Nothing ->
        return state
      Just centity -> do
        lift $ optionPushEntity options centity
        eid <- centityId centity
        ehash <- centityHash centity

        case stateLastEntityId state of
         Nothing -> return ()
         Just last
          -> when ((ehash,eid) <= last) $
              left $ MergeInputEntitiesOutOfOrder last (ehash,eid)

        state' <- refill state eid
        go state' { stateEntityCount = stateEntityCount state' + 1
                  , stateLastEntityId = Just (ehash,eid) }

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
        entities <- foreign $ foreignEntitiesOfBlock (stateMempool state) cblock
        foreign $ mergeManyPush (stateMempool state) (stateMergeMany state) (Boxed.convert entities)

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
    merger' <- foreign $ mergeManyClone pool' $ stateMergeMany state
    liftIO $ Mempool.free $ stateMempool state
    return state { stateMempool = pool', stateMergeMany = merger' }

   | otherwise
   = return state

  centityId = foreign . peekEntityId . unCEntity
  centityHash = foreign . peekEntityHash . unCEntity

  foreign = firstT MergeForeign

