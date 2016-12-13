{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Foreign.Util (
    ForeignError(..)
  , fromCError
  , liftCError

  , allocCopy
  , allocStack
  , peekIO
  , pokeIO
  , peekByteString
  , pokeByteString
  , peekVector
  , pokeVector
  , peekMany
  , pokeMany
  ) where

import           Anemone.Foreign.Data (CError)
import           Anemone.Foreign.Mempool (Mempool, allocBytes)

import           Control.Monad.IO.Class (MonadIO(..))

import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.Vector.Generic as Generic
import qualified Data.Vector.Storable as Storable
import           Data.Word (Word8)

import           Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrBytes, withForeignPtr)
import           Foreign.Marshal.Alloc (alloca)
import           Foreign.Marshal.Utils (copyBytes)
import           Foreign.Ptr (Ptr, plusPtr)
import           Foreign.Storable (Storable(..))

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither)

import           Zebra.Foreign.Bindings


data ForeignError =
    ForeignInvalidAttributeCount !Int !Int
  | ForeignTableNotEnoughCapacity
  | ForeignInvalidColumnType
  | ForeignAttributeNotFound
  | ForeignNotEnoughBytes
  | ForeignNotEnoughRows
  | ForeignMergeDifferentColumnTypes
  | ForeignMergeDifferentEntities
  | ForeignAppendDifferentAttributeCount
  | ForeignUnknownError !CError
    deriving (Eq, Ord, Show)

fromCError :: CError -> Either ForeignError ()
fromCError = \case
  C'ZEBRA_SUCCESS ->
    Right ()
  C'ZEBRA_INVALID_COLUMN_TYPE ->
    Left ForeignInvalidColumnType
  C'ZEBRA_ATTRIBUTE_NOT_FOUND ->
    Left ForeignAttributeNotFound
  C'ZEBRA_NOT_ENOUGH_BYTES ->
    Left ForeignNotEnoughBytes
  C'ZEBRA_NOT_ENOUGH_ROWS ->
    Left ForeignNotEnoughRows
  C'ZEBRA_MERGE_DIFFERENT_COLUMN_TYPES ->
    Left ForeignMergeDifferentColumnTypes
  C'ZEBRA_MERGE_DIFFERENT_ENTITIES ->
    Left ForeignMergeDifferentEntities
  C'ZEBRA_APPEND_DIFFERENT_ATTRIBUTE_COUNT ->
    Left ForeignAppendDifferentAttributeCount
  err ->
    Left $ ForeignUnknownError err

liftCError :: MonadIO m => IO CError -> EitherT ForeignError m ()
liftCError f = do
  c_error <- liftIO f
  hoistEither $ fromCError c_error

allocCopy :: MonadIO m => Mempool -> ForeignPtr a -> Int -> Int -> m (Ptr a)
allocCopy pool fp off len =
  liftIO . withForeignPtr fp $ \src -> do
    dst <- allocBytes pool (fromIntegral len)
    copyBytes dst (src `plusPtr` off) len
    pure dst

allocStack :: MonadIO m => Storable a => (Ptr a -> EitherT e IO b) -> EitherT e m b
allocStack f =
  EitherT . liftIO . alloca $ \a -> runEitherT $ f a

peekIO :: (MonadIO m, Storable a) => Ptr a -> m a
peekIO ptr =
  liftIO $ peek ptr

pokeIO :: (MonadIO m, Storable a) => Ptr a -> a -> m ()
pokeIO ptr x =
  liftIO $ poke ptr x

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

peekMany :: forall m v a b. (MonadIO m, Storable a, Generic.Vector v b) => Ptr a -> Int64 -> (Ptr a -> m b) -> m (v b)
peekMany ptr0 n peekOne =
  Generic.generateM (fromIntegral n) $ \i ->
    let
      ptr =
        ptr0 `plusPtr` (i * sizeOf (Savage.undefined :: a))
    in
      peekOne ptr

pokeMany :: forall m v a b. (MonadIO m, Storable a, Generic.Vector v b) => Ptr a -> v b -> (Ptr a -> b -> m ()) -> m ()
pokeMany ptr0 xs pokeOne =
  flip Generic.imapM_ xs $ \i x ->
    let
      ptr =
        ptr0 `plusPtr`
        (i * sizeOf (Savage.undefined :: a))
    in
      pokeOne ptr x
