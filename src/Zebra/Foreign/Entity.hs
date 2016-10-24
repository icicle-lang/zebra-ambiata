{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Entity (
    CEntity(..)
  , entityOfForeign
  , foreignOfEntity
  ) where

import           Anemone.Foreign.Mempool (Mempool, alloc, allocBytes, calloc)

import           Control.Monad.IO.Class (MonadIO(..))

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import           Data.Word (Word8)

import           Foreign.C.Types (CUInt)
import           Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrBytes, withForeignPtr)
import           Foreign.Marshal.Utils (copyBytes)
import           Foreign.Ptr (Ptr, plusPtr, castPtr)
import           Foreign.Storable (Storable(..))

import           P

import qualified Prelude as Savage

import           X.Control.Monad.Trans.Either (EitherT, left)

import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Data.Table
import           Zebra.Foreign.Bindings


data ForeignError =
    UnknownColumnType !CUInt
    deriving (Eq, Ord, Show)

newtype CEntity =
  CEntity {
      unCEntity :: Ptr C'zebra_entity
    }

entityOfForeign :: MonadIO m => CEntity -> EitherT ForeignError m Entity
entityOfForeign (CEntity c_entity) =
  peekEntity c_entity

foreignOfEntity :: MonadIO m => Mempool -> Entity -> m CEntity
foreignOfEntity pool entity = do
  c_entity <- liftIO $ alloc pool
  pokeEntity pool c_entity entity
  pure $ CEntity c_entity

pokeEntity :: MonadIO m => Mempool -> Ptr C'zebra_entity -> Entity -> m ()
pokeEntity pool c_entity (Entity hash eid attributes) = do
  let
    n_attrs =
      Boxed.length attributes

    eid_len =
      B.length $ unEntityId eid

  c_attributes <- liftIO . calloc pool $ fromIntegral n_attrs

  pokeIO (p'zebra_entity'hash c_entity) $ unEntityHash hash
  pokeIO (p'zebra_entity'id_length c_entity) $ fromIntegral eid_len
  pokeByteString pool (p'zebra_entity'id_bytes c_entity) $ unEntityId eid
  pokeIO (p'zebra_entity'attribute_count c_entity) $ fromIntegral n_attrs
  pokeIO (p'zebra_entity'attributes c_entity) c_attributes

  flip Boxed.imapM_ attributes $ \i attribute ->
    let
      ptr =
        c_attributes `plusPtr`
        (i * sizeOf (Savage.undefined :: C'zebra_attribute))
    in
      pokeAttribute pool ptr attribute

peekEntity :: MonadIO m => Ptr C'zebra_entity -> EitherT ForeignError m Entity
peekEntity c_entity = do
  hash <- EntityHash <$> peekIO (p'zebra_entity'hash c_entity)
  eid_len <- fromIntegral <$> peekIO (p'zebra_entity'id_length c_entity)
  eid <- EntityId <$> peekByteString eid_len (p'zebra_entity'id_bytes c_entity)

  n_attrs <- fromIntegral <$> peekIO (p'zebra_entity'attribute_count c_entity)
  c_attributes <- peekIO (p'zebra_entity'attributes c_entity)

  fmap (Entity hash eid) . Boxed.generateM n_attrs $ \i ->
    let
      ptr =
        c_attributes `plusPtr`
        (i * sizeOf (Savage.undefined :: C'zebra_attribute))
    in
      peekAttribute ptr

pokeAttribute :: MonadIO m => Mempool -> Ptr C'zebra_attribute -> Attribute -> m ()
pokeAttribute pool c_attribute (Attribute times priorities tombstones table) = do
  -- NOTE: These casts depend on the Storable instance for
  -- NOTE: time/priority/tombstone being a single peek/poke
  -- NOTE: of an Int64, which they should be.
  pokeVector pool (castPtr $ p'zebra_attribute'times c_attribute) times
  pokeVector pool (castPtr $ p'zebra_attribute'priorities c_attribute) priorities
  pokeVector pool (castPtr $ p'zebra_attribute'tombstones c_attribute) tombstones
  pokeTable pool (p'zebra_attribute'table c_attribute) table

peekAttribute :: MonadIO m => Ptr C'zebra_attribute -> EitherT ForeignError m Attribute
peekAttribute c_attribute = do
  table <- peekTable (p'zebra_attribute'table c_attribute)

  let
    n_rows =
      rowsOfTable table

  times <- peekVector n_rows (castPtr $ p'zebra_attribute'times c_attribute)
  priorities <- peekVector n_rows (castPtr $ p'zebra_attribute'priorities c_attribute)
  tombstones <- peekVector n_rows (castPtr $ p'zebra_attribute'tombstones c_attribute)

  pure $ Attribute times priorities tombstones table

peekTable :: MonadIO m => Ptr C'zebra_table -> EitherT ForeignError m Table
peekTable c_table = do
  n_rows <- fromIntegral <$> peekIO (p'zebra_table'row_count c_table)
  n_cols <- fromIntegral <$> peekIO (p'zebra_table'column_count c_table)
  c_columns <- peekIO (p'zebra_table'columns c_table)

  fmap Table . Boxed.generateM n_cols $ \i ->
    let
      ptr =
        c_columns `plusPtr`
        (i * sizeOf (Savage.undefined :: C'zebra_column))
    in
      peekColumn n_rows ptr

pokeTable :: MonadIO m => Mempool -> Ptr C'zebra_table -> Table -> m ()
pokeTable pool c_table table@(Table columns) = do
  let
    n_rows =
      rowsOfTable table

    n_cols =
      Boxed.length columns

  c_columns <- liftIO . calloc pool $ fromIntegral n_cols

  pokeIO (p'zebra_table'row_count c_table) $ fromIntegral n_rows
  pokeIO (p'zebra_table'row_capacity c_table) $ fromIntegral n_rows
  pokeIO (p'zebra_table'column_count c_table) $ fromIntegral n_cols
  pokeIO (p'zebra_table'columns c_table) c_columns

  flip Boxed.imapM_ columns $ \i column ->
    let
      ptr =
        c_columns `plusPtr`
        (i * sizeOf (Savage.undefined :: C'zebra_column))
    in
      pokeColumn pool ptr column

peekColumn :: MonadIO m => Int -> Ptr C'zebra_column -> EitherT ForeignError m Column
peekColumn n_rows c_column = do
  typ <- peekIO (p'zebra_column'type c_column)
  case typ of
    C'ZEBRA_BYTE ->
      ByteColumn
        <$> peekByteString n_rows (p'zebra_data'b $ p'zebra_column'data c_column)

    C'ZEBRA_INT ->
      IntColumn
        <$> peekVector n_rows (p'zebra_data'i $ p'zebra_column'data c_column)

    C'ZEBRA_DOUBLE ->
      DoubleColumn
        <$> peekVector n_rows (p'zebra_data'd $ p'zebra_column'data c_column)

    C'ZEBRA_ARRAY ->
      ArrayColumn
        <$> peekVector n_rows (p'zebra_data'a'n $ p'zebra_column'data c_column)
        <*> peekTable (p'zebra_data'a'table $ p'zebra_column'data c_column)

    _ ->
      left $ UnknownColumnType typ

pokeColumn :: MonadIO m => Mempool -> Ptr C'zebra_column -> Column -> m ()
pokeColumn pool c_column = \case
  ByteColumn bs -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_BYTE
    pokeByteString pool (p'zebra_data'b $ p'zebra_column'data c_column) bs

  IntColumn xs -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_INT
    pokeVector pool (p'zebra_data'i $ p'zebra_column'data c_column) xs

  DoubleColumn xs -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_DOUBLE
    pokeVector pool (p'zebra_data'd $ p'zebra_column'data c_column) xs

  ArrayColumn ns table -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_ARRAY
    pokeVector pool (p'zebra_data'a'n $ p'zebra_column'data c_column) ns
    pokeTable pool (p'zebra_data'a'table $ p'zebra_column'data c_column) table

peekByteString :: MonadIO m => Int -> Ptr (Ptr Word8) -> m ByteString
peekByteString len psrc =
  liftIO . B.create len $ \dst -> do
    src <- peek psrc
    copyBytes dst src len

pokeByteString :: MonadIO m => Mempool -> Ptr (Ptr Word8) -> ByteString -> m ()
pokeByteString pool dst (PS fp off len) =
  pokeIO dst =<< allocCopy pool fp off len

peekVector :: forall m a. (MonadIO m, Storable a) => Int -> Ptr (Ptr a) -> m (Storable.Vector a)
peekVector vlen psrc = do
  let
    len =
      vlen * sizeOf (Savage.undefined :: a)

  src <- peekIO psrc
  fpdst <- liftIO $ mallocForeignPtrBytes len

  liftIO . withForeignPtr fpdst $ \dst ->
    copyBytes dst src len

  pure $
    Storable.unsafeFromForeignPtr0 fpdst vlen

pokeVector :: forall m a. (MonadIO m, Storable a) => Mempool -> Ptr (Ptr a) -> Storable.Vector a -> m ()
pokeVector pool dst xs =
  let
    (fp, voff, vlen) =
      Storable.unsafeToForeignPtr xs

    off =
      voff * sizeOf (Savage.undefined :: a)

    len =
      vlen * sizeOf (Savage.undefined :: a)
  in
    pokeIO dst =<< allocCopy pool fp off len

allocCopy :: MonadIO m => Mempool -> ForeignPtr a -> Int -> Int -> m (Ptr a)
allocCopy pool fp off len =
  liftIO . withForeignPtr fp $ \src -> do
    dst <- allocBytes pool (fromIntegral len)
    copyBytes dst (src `plusPtr` off) len
    pure dst

peekIO :: (MonadIO m, Storable a) => Ptr a -> m a
peekIO ptr =
  liftIO $ peek ptr

pokeIO :: (MonadIO m, Storable a) => Ptr a -> a -> m ()
pokeIO ptr x =
  liftIO $ poke ptr x
