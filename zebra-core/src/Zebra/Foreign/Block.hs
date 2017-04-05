{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Block (
    CBlock(..)
  , blockOfForeign
  , foreignOfBlock
  , foreignEntitiesOfBlock
  , appendEntityToBlock

  , peekBlock
  , pokeBlock
  , peekBlockRowCount
  , peekBlockEntity
  , pokeBlockEntity
  ) where

import           Anemone.Foreign.Mempool (Mempool, alloc, calloc)

import           Control.Monad.IO.Class (MonadIO(..))

import qualified Data.ByteString as B
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Foreign.Ptr (Ptr, plusPtr, nullPtr)
import           Foreign.Storable (Storable(..))

import           P

import qualified Prelude as Savage

import           X.Control.Monad.Trans.Either (EitherT)

import           Zebra.Factset.Block
import           Zebra.Factset.Data
import           Zebra.Foreign.Bindings
import           Zebra.Foreign.Entity
import           Zebra.Foreign.Table
import           Zebra.Foreign.Util


newtype CBlock =
  CBlock {
      unCBlock :: Ptr C'zebra_block
    }

blockOfForeign :: MonadIO m => CBlock -> EitherT ForeignError m Block
blockOfForeign (CBlock c_block) =
  peekBlock c_block

foreignOfBlock :: MonadIO m => Mempool -> Block -> m CBlock
foreignOfBlock pool block = do
  c_block <- liftIO $ alloc pool
  pokeBlock pool c_block block
  pure $ CBlock c_block

foreignEntitiesOfBlock :: MonadIO m => Mempool -> CBlock -> EitherT ForeignError m (Boxed.Vector CEntity)
foreignEntitiesOfBlock pool (CBlock c_block) =
  allocStack $ \p_entity_count ->
  allocStack $ \pp_entities -> do
    liftCError $ unsafe'c'zebra_entities_of_block pool c_block p_entity_count pp_entities

    entity_count <- liftIO $ peek p_entity_count
    p_entities <- liftIO $ peek pp_entities

    pure . Boxed.generate (fromIntegral entity_count) $ \ix ->
      let
        offset =
          ix * sizeOf (Savage.undefined :: C'zebra_entity)
      in
        CEntity (p_entities `plusPtr` offset)

appendEntityToBlock :: MonadIO m => Mempool -> CEntity -> Maybe CBlock -> EitherT ForeignError m CBlock
appendEntityToBlock pool (CEntity c_entity) c_block =
  allocStack $ \pp_block -> do
    pokeIO pp_block $ maybe nullPtr unCBlock c_block
    liftCError $ unsafe'c'zebra_append_block_entity pool c_entity pp_block
    CBlock <$> peekIO pp_block

peekBlock :: MonadIO m => Ptr C'zebra_block -> EitherT ForeignError m Block
peekBlock c_block = do
  n_entities <- fmap fromIntegral . peekIO $ p'zebra_block'entity_count c_block
  c_entities <- peekIO $ p'zebra_block'entities c_block
  entities <- peekMany c_entities n_entities $ peekBlockEntity

  n_rows <- fmap fromIntegral . peekIO $ p'zebra_block'row_count c_block
  times <- peekVector n_rows $ p'zebra_block'times c_block
  factset_ids <- peekVector n_rows $ p'zebra_block'factset_ids c_block
  tombstones <- peekVector n_rows $ p'zebra_block'tombstones c_block

  let
    indices =
      Unboxed.zipWith3 BlockIndex
        (Storable.convert $ timesOfForeign times)
        (Storable.convert $ factsetIdsOfForeign factset_ids)
        (Storable.convert $ tombstonesOfForeign tombstones)

  n_tables <- peekIO $ p'zebra_block'table_count c_block
  c_tables <- peekIO $ p'zebra_block'tables c_block
  tables <- peekMany c_tables n_tables peekTable

  pure $ Block entities indices tables

pokeBlock :: MonadIO m => Mempool -> Ptr C'zebra_block -> Block -> m ()
pokeBlock pool c_block (Block entities indices tables) = do
  let
    n_entities =
      Boxed.length entities

  c_entities <- liftIO . calloc pool $ fromIntegral n_entities

  pokeIO (p'zebra_block'entity_count c_block) $ fromIntegral n_entities
  pokeIO (p'zebra_block'entities c_block) c_entities
  pokeMany c_entities entities $ pokeBlockEntity pool

  let
    n_rows =
      Unboxed.length indices

    times =
      foreignOfTimes .
      Storable.convert $
      Unboxed.map indexTime indices

    factset_ids =
      foreignOfFactsetIds .
      Storable.convert $
      Unboxed.map indexFactsetId indices

    tombstones =
      foreignOfTombstones .
      Storable.convert $
      Unboxed.map indexTombstone indices

  pokeIO (p'zebra_block'row_count c_block) $ fromIntegral n_rows
  pokeVector pool (p'zebra_block'times c_block) times
  pokeVector pool (p'zebra_block'factset_ids c_block) factset_ids
  pokeVector pool (p'zebra_block'tombstones c_block) tombstones

  let
    n_tables =
      Boxed.length tables

  c_tables <- liftIO . calloc pool $ fromIntegral n_tables

  pokeIO (p'zebra_block'tables c_block) c_tables
  pokeIO (p'zebra_block'table_count c_block) $ fromIntegral n_tables
  pokeMany c_tables tables $ pokeTable pool

peekBlockRowCount :: MonadIO m => Ptr C'zebra_block -> m Int
peekBlockRowCount c_block =
  fmap fromIntegral . peekIO $ p'zebra_block'row_count c_block


peekBlockEntity :: MonadIO m => Ptr C'zebra_block_entity -> m BlockEntity
peekBlockEntity c_entity = do
  hash <- fmap EntityHash . peekIO $ p'zebra_block_entity'hash c_entity
  eid_len <- fmap fromIntegral . peekIO $ p'zebra_block_entity'id_length c_entity
  eid <- fmap EntityId . peekByteString eid_len $ p'zebra_block_entity'id_bytes c_entity

  n_attrs <- fmap fromIntegral . peekIO $ p'zebra_block_entity'attribute_count c_entity
  attr_ids <- fmap attributeIdsOfForeign . peekVector n_attrs $ p'zebra_block_entity'attribute_ids c_entity
  attr_counts <- peekVector n_attrs $ p'zebra_block_entity'attribute_row_counts c_entity

  let
    attrs =
      Unboxed.zipWith BlockAttribute
        (Storable.convert attr_ids)
        (Storable.convert attr_counts)

  pure $ BlockEntity hash eid attrs

pokeBlockEntity :: MonadIO m => Mempool -> Ptr C'zebra_block_entity -> BlockEntity -> m ()
pokeBlockEntity pool c_entity (BlockEntity hash eid attrs) = do
  let
    eid_len =
      B.length $ unEntityId eid

  pokeIO (p'zebra_block_entity'hash c_entity) $ unEntityHash hash
  pokeIO (p'zebra_block_entity'id_length c_entity) $ fromIntegral eid_len
  pokeByteString pool (p'zebra_block_entity'id_bytes c_entity) $ unEntityId eid

  let
    n_attrs =
      fromIntegral $
      Unboxed.length attrs

    attr_ids =
      foreignOfAttributeIds .
      Storable.convert $
      Unboxed.map attributeId attrs

    attr_counts =
      Storable.convert $
      Unboxed.map attributeRows attrs

  pokeIO (p'zebra_block_entity'attribute_count c_entity) n_attrs
  pokeVector pool (p'zebra_block_entity'attribute_ids c_entity) attr_ids
  pokeVector pool (p'zebra_block_entity'attribute_row_counts c_entity) attr_counts
