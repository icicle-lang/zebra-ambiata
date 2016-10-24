{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Util (
    ForeignError(..)

  , allocCopy
  , peekIO
  , pokeIO
  , peekByteString
  , pokeByteString
  , peekVector
  , pokeVector
  , peekMany
  , pokeMany
  ) where

import           Anemone.Foreign.Mempool (Mempool, allocBytes)

import           Control.Monad.IO.Class (MonadIO(..))

import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.Vector.Generic as Generic
import qualified Data.Vector.Storable as Storable
import           Data.Word (Word8)

import           Foreign.C.Types (CUInt)
import           Foreign.ForeignPtr (ForeignPtr, mallocForeignPtrBytes, withForeignPtr)
import           Foreign.Marshal.Utils (copyBytes)
import           Foreign.Ptr (Ptr, plusPtr)
import           Foreign.Storable (Storable(..))

import           P

import qualified Prelude as Savage


data ForeignError =
    ForeignUnknownColumnType !CUInt
  | ForeignInvalidAttributeCount !Int !Int
    deriving (Eq, Ord, Show)

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
