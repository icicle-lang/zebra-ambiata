{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Foreign.Entity (
    ForeignEntity(..)
  , foreignOfEntity
  ) where

import           Anemone.Foreign.Mempool

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import           Data.Word (Word8)

import           Foreign.ForeignPtr
import           Foreign.Marshal.Utils
import           Foreign.Ptr
import           Foreign.Storable

import           P

import qualified Prelude as Savage

import           System.IO

import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Data.Table

import           Zebra.Foreign.Bindings

newtype ForeignEntity =
  ForeignEntity {
      unForeignEntity :: Ptr C'zebra_entity
    }

foreignOfEntity :: Mempool -> Entity -> IO ForeignEntity
foreignOfEntity pool entity = do
  c_entity <- alloc pool
  pokeEntity pool c_entity entity
  pure $ ForeignEntity c_entity

pokeEntity :: Mempool -> Ptr C'zebra_entity -> Entity -> IO ()
pokeEntity pool c_entity (Entity hash eid attributes) = do
  let
    n_attrs =
      Boxed.length attributes

    eid_len =
      B.length $ unEntityId eid

  c_attributes <- calloc pool $ fromIntegral n_attrs

  poke (p'zebra_entity'hash c_entity) $ unEntityHash hash
  poke (p'zebra_entity'id_length c_entity) $ fromIntegral eid_len
  pokeByteString pool (p'zebra_entity'id_bytes c_entity) $ unEntityId eid
  poke (p'zebra_entity'attribute_count c_entity) $ fromIntegral n_attrs
  poke (p'zebra_entity'attributes c_entity) c_attributes

  flip Boxed.imapM_ attributes $ \i attribute ->
    let
      ptr =
        c_attributes `plusPtr`
        (i * sizeOf (Savage.undefined :: C'zebra_attribute))
    in
      pokeAttribute pool ptr attribute

pokeAttribute :: Mempool -> Ptr C'zebra_attribute -> Attribute -> IO ()
pokeAttribute pool c_attribute (Attribute times priorities tombstones table) = do
  -- NOTE: These casts depend on the Storable instance for
  -- NOTE: time/priority/tombstone being a single peek/poke
  -- NOTE: of an Int64, which they should be.
  pokeVector pool (castPtr $ p'zebra_attribute'times c_attribute) times
  pokeVector pool (castPtr $ p'zebra_attribute'priorities c_attribute) priorities
  pokeVector pool (castPtr $ p'zebra_attribute'tombstones c_attribute) tombstones
  pokeTable pool (p'zebra_attribute'table c_attribute) table

pokeTable :: Mempool -> Ptr C'zebra_table -> Table -> IO ()
pokeTable pool c_table table@(Table columns) = do
  let
    n_rows =
      rowsOfTable table

    n_cols =
      Boxed.length columns

  c_columns <- calloc pool $ fromIntegral n_cols

  poke (p'zebra_table'row_count c_table) $ fromIntegral n_rows
  poke (p'zebra_table'row_capacity c_table) $ fromIntegral n_rows
  poke (p'zebra_table'column_count c_table) $ fromIntegral n_cols
  poke (p'zebra_table'columns c_table) c_columns

  flip Boxed.imapM_ columns $ \i column ->
    let
      ptr =
        c_columns `plusPtr`
        (i * sizeOf (Savage.undefined :: C'zebra_column))
    in
      pokeColumn pool ptr column

pokeColumn :: Mempool -> Ptr C'zebra_column -> Column -> IO ()
pokeColumn pool c_column = \case
  ByteColumn bs -> do
    poke (p'zebra_column'type c_column) C'ZEBRA_BYTE
    pokeByteString pool (p'zebra_data'b $ p'zebra_column'data c_column) bs

  IntColumn xs -> do
    poke (p'zebra_column'type c_column) C'ZEBRA_INT
    pokeVector pool (p'zebra_data'i $ p'zebra_column'data c_column) xs

  DoubleColumn xs -> do
    poke (p'zebra_column'type c_column) C'ZEBRA_DOUBLE
    pokeVector pool (p'zebra_data'd $ p'zebra_column'data c_column) xs

  ArrayColumn ns table -> do
    poke (p'zebra_column'type c_column) C'ZEBRA_ARRAY
    pokeVector pool (p'zebra_data'a'n $ p'zebra_column'data c_column) ns
    pokeTable pool (p'zebra_data'a'table $ p'zebra_column'data c_column) table

pokeByteString :: Mempool -> Ptr (Ptr Word8) -> ByteString -> IO ()
pokeByteString pool dst (PS fp off len) =
  poke dst =<< allocCopy pool fp off len

pokeVector :: Storable a => Mempool -> Ptr (Ptr a) -> Storable.Vector a -> IO ()
pokeVector pool dst xs =
  let
    (fp, off, len) =
      Storable.unsafeToForeignPtr xs
  in
    poke dst =<< allocCopy pool fp off len

allocCopy :: Mempool -> ForeignPtr a -> Int -> Int -> IO (Ptr a)
allocCopy pool fp off len =
  withForeignPtr fp $ \src -> do
    dst <- allocBytes pool (fromIntegral len)
    copyBytes dst (src `plusPtr` off) len
    pure dst
