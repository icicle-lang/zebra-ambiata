{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Entity (
    CEntity(..)
  , entityOfForeign
  , foreignOfEntity

  , peekEntity
  , pokeEntity
  , peekEntityHash
  , peekEntityId
  , peekAttribute
  , pokeAttribute
  ) where

import           Anemone.Foreign.Mempool (Mempool, alloc, calloc)

import           Control.Monad.IO.Class (MonadIO(..))

import qualified Data.ByteString as B
import qualified Data.Vector as Boxed

import           Foreign.Ptr (Ptr)
import           Foreign.Storable (Storable(..))

import           P

import           X.Control.Monad.Trans.Either (EitherT)

import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Data.Table
import           Zebra.Foreign.Bindings
import           Zebra.Foreign.Table
import           Zebra.Foreign.Util


newtype CEntity =
  CEntity {
      unCEntity :: Ptr C'zebra_entity
    }
  deriving Storable

entityOfForeign :: MonadIO m => CEntity -> EitherT ForeignError m Entity
entityOfForeign (CEntity c_entity) =
  peekEntity c_entity

foreignOfEntity :: MonadIO m => Mempool -> Entity -> m CEntity
foreignOfEntity pool entity = do
  c_entity <- liftIO $ alloc pool
  pokeEntity pool c_entity entity
  pure $ CEntity c_entity

peekEntity :: MonadIO m => Ptr C'zebra_entity -> EitherT ForeignError m Entity
peekEntity c_entity = do
  hash <- peekEntityHash c_entity
  eid <- peekEntityId c_entity

  n_attrs <- fromIntegral <$> peekIO (p'zebra_entity'attribute_count c_entity)
  c_attributes <- peekIO (p'zebra_entity'attributes c_entity)

  fmap (Entity hash eid) $ peekMany c_attributes n_attrs peekAttribute

pokeEntity :: MonadIO m => Mempool -> Ptr C'zebra_entity -> Entity -> m ()
pokeEntity pool c_entity (Entity hash eid attributes) = do
  let
    eid_len =
      B.length $ unEntityId eid

  pokeIO (p'zebra_entity'hash c_entity) $ unEntityHash hash
  pokeIO (p'zebra_entity'id_length c_entity) $ fromIntegral eid_len
  pokeByteString pool (p'zebra_entity'id_bytes c_entity) $ unEntityId eid

  let
    n_attrs =
      Boxed.length attributes

  c_attributes <- liftIO . calloc pool $ fromIntegral n_attrs

  pokeIO (p'zebra_entity'attribute_count c_entity) $ fromIntegral n_attrs
  pokeIO (p'zebra_entity'attributes c_entity) c_attributes
  pokeMany c_attributes attributes $ pokeAttribute pool


peekEntityHash :: MonadIO m => Ptr C'zebra_entity -> EitherT ForeignError m EntityHash
peekEntityHash c_entity = do
  EntityHash <$> peekIO (p'zebra_entity'hash c_entity)

peekEntityId :: MonadIO m => Ptr C'zebra_entity -> EitherT ForeignError m EntityId
peekEntityId c_entity = do
  eid_len <- fromIntegral <$> peekIO (p'zebra_entity'id_length c_entity)
  EntityId <$> peekByteString eid_len (p'zebra_entity'id_bytes c_entity)




peekAttribute :: MonadIO m => Ptr C'zebra_attribute -> EitherT ForeignError m Attribute
peekAttribute c_attribute = do
  table <- peekTable (p'zebra_attribute'table c_attribute)

  let
    n_rows =
      rowsOfTable table

  times <- fmap timesOfForeign . peekVector n_rows $ p'zebra_attribute'times c_attribute
  factsetids <- fmap factsetIdsOfForeign . peekVector n_rows $ p'zebra_attribute'factsetids c_attribute
  tombstones <- fmap tombstonesOfForeign . peekVector n_rows $ p'zebra_attribute'tombstones c_attribute

  pure $ Attribute times factsetids tombstones table

pokeAttribute :: MonadIO m => Mempool -> Ptr C'zebra_attribute -> Attribute -> m ()
pokeAttribute pool c_attribute (Attribute times factsetids tombstones table) = do
  pokeVector pool (p'zebra_attribute'times c_attribute) $ foreignOfTimes times
  pokeVector pool (p'zebra_attribute'factsetids c_attribute) $ foreignOfFactsetIds factsetids
  pokeVector pool (p'zebra_attribute'tombstones c_attribute) $ foreignOfTombstones tombstones
  pokeTable pool (p'zebra_attribute'table c_attribute) table
