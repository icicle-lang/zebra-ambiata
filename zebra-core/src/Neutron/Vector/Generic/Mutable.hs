{-# LANGUAGE NoImplicitPrelude #-}
module Neutron.Vector.Generic.Mutable (
    MVector

  , unsafeNew
  , unsafeWrite
  , unsafeSlice
  , unsafeCopy
  , unsafeReplicate
  ) where

import           Control.Monad.Primitive (RealWorld)

import           Data.Vector.Generic.Mutable.Base (MVector(..))

import           P

import           System.IO (IO)


unsafeNew :: MVector v a => Int -> IO (v RealWorld a)
unsafeNew n =
  basicUnsafeNew n
{-# INLINE unsafeNew #-}

unsafeWrite :: MVector v a => v RealWorld a -> Int -> a -> IO ()
unsafeWrite xs i x =
  basicUnsafeWrite xs i x
{-# INLINE unsafeWrite #-}

unsafeSlice :: MVector v a => Int -> Int -> v RealWorld a -> v RealWorld a
unsafeSlice off len xs =
  basicUnsafeSlice off len xs
{-# INLINE unsafeSlice #-}

unsafeCopy :: MVector v a => v RealWorld a -> v RealWorld a -> IO ()
unsafeCopy dst src =
  basicUnsafeCopy dst src
{-# INLINE unsafeCopy #-}

unsafeReplicate :: MVector v a => Int -> a -> IO (v RealWorld a)
unsafeReplicate len x =
  basicUnsafeReplicate len x
{-# INLINE unsafeReplicate #-}
