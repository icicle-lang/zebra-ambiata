{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.X.Vector.Segment (
    Segment(..)
  , SegmentError(..)
  , renderSegmentError

  , length
  , slice
  , unsafeSlice
  , reify
  , unsafeReify
  ) where

import           Control.Monad.IO.Class (MonadIO(..))

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Text as Text
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import           Data.Vector.Unboxed (Unbox)
import qualified Data.Vector.Unboxed as Unboxed

import           Foreign.Storable (Storable)

import qualified Neutron.Vector.Generic as Generic
import qualified Neutron.Vector.Generic.Mutable as MGeneric

import           P hiding (length, concat, empty)

import           System.IO.Unsafe (unsafePerformIO)

import qualified X.Data.ByteString.Unsafe as ByteString


-- | Class of things that can contain segmented data.
class Segment a where
  segmentLength :: a -> Int
  segmentUnsafeSlice :: Int -> Int -> a -> a

instance Segment (Boxed.Vector a) where
  segmentLength =
    Generic.length
  {-# INLINE segmentLength #-}

  segmentUnsafeSlice =
    Generic.unsafeSlice
  {-# INLINE segmentUnsafeSlice #-}

instance Unbox a => Segment (Unboxed.Vector a) where
  segmentLength =
    Generic.length
  {-# INLINE segmentLength #-}

  segmentUnsafeSlice =
    Generic.unsafeSlice
  {-# INLINE segmentUnsafeSlice #-}

instance Storable a => Segment (Storable.Vector a) where
  segmentLength =
    Generic.length
  {-# INLINE segmentLength #-}

  segmentUnsafeSlice =
    Generic.unsafeSlice
  {-# INLINE segmentUnsafeSlice #-}

instance Segment ByteString where
  segmentLength =
    ByteString.length
  {-# INLINE segmentLength #-}

  segmentUnsafeSlice =
    ByteString.unsafeSlice
  {-# INLINE segmentUnsafeSlice #-}

data SegmentError =
  SegmentLengthMismatch !Int !Int
  deriving (Eq, Show)

renderSegmentError :: SegmentError -> Text
renderSegmentError = \case
  SegmentLengthMismatch n_sum n_xs ->
    "Sum of segment lengths <" <>
    Text.pack (show n_sum) <>
    "> did not match total length <" <>
    Text.pack (show n_xs) <>
    "> of nested segments"

-- | Yield the length of a segment.
length :: Segment a => a -> Int
length =
  segmentLength
{-# INLINE length #-}

slice :: Segment a => Int -> Int -> a -> Maybe a
slice off len xs =
  if off < 0 || off + len > length xs then
    Nothing
  else
    Just (unsafeSlice off len xs)
{-# INLINE slice #-}

-- | Yield a slice of the segment, the segment must contain at least
--   @off + len@ elements, but this is not checked.
unsafeSlice :: Segment a => Int -> Int -> a -> a
unsafeSlice =
  segmentUnsafeSlice
{-# INLINE unsafeSlice #-}

-- | Reify nested segments in to a vector of segments.
reify :: (Segment a, Generic.Vector v Int64) => v Int64 -> a -> Either SegmentError (Boxed.Vector a)
reify ns xs =
  let
    !n_sum =
      fromIntegral (Generic.sum ns)

    !n_xs =
      length xs
  in
    if n_sum /= n_xs then
      Left $ SegmentLengthMismatch n_sum n_xs
    else
      pure $ unsafeReify ns xs
{-# INLINE reify #-}

unsafeReify :: forall a v. (Segment a, Generic.Vector v Int64) => v Int64 -> a -> Boxed.Vector a
unsafeReify ns xs0 =
  unsafePerformIO $ do
    let
      !ns_len =
        Generic.length ns

    !dst <-
      liftIO $ MGeneric.unsafeNew ns_len

    let
      loop !i !off =
        if i >= ns_len then
          liftIO $ Generic.unsafeFreeze dst

        else do
          let
            !len =
              fromIntegral (Generic.unsafeIndex i ns)

            !xs =
              unsafeSlice off len xs0

          liftIO $ MGeneric.unsafeWrite dst i xs
          loop (i + 1) (off + len)

    loop 0 0
{-# INLINE unsafeReify #-}
