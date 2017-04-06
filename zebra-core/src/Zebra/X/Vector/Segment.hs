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

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Text as Text
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import           Data.Vector.Unboxed (Unbox)
import qualified Data.Vector.Unboxed as Unboxed

import           Foreign.Storable (Storable)

import           P hiding (length, concat, empty)

import qualified X.Data.ByteString.Unsafe as ByteString
import qualified X.Data.Vector.Generic as Generic


-- | Class of things that can contain segmented data.
class Segment a where
  segmentLength :: a -> Int
  segmentUnsafeSlice :: Int -> Int -> a -> a

instance Segment (Boxed.Vector a) where
  segmentLength =
    Boxed.length
  {-# INLINE segmentLength #-}

  segmentUnsafeSlice =
    Generic.unsafeSlice
  {-# INLINE segmentUnsafeSlice #-}

instance Unbox a => Segment (Unboxed.Vector a) where
  segmentLength =
    Unboxed.length
  {-# INLINE segmentLength #-}

  segmentUnsafeSlice =
    Generic.unsafeSlice
  {-# INLINE segmentUnsafeSlice #-}

instance Storable a => Segment (Storable.Vector a) where
  segmentLength =
    Storable.length
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

data IdxOff =
  IdxOff !Int !Int

unsafeReify :: (Segment a, Generic.Vector v Int64) => v Int64 -> a -> Boxed.Vector a
unsafeReify ns xs =
  let
    loop (IdxOff idx off) =
      let
        !len =
          fromIntegral (Generic.unsafeIndex ns idx)
      in
        Just (unsafeSlice off len xs, IdxOff (idx + 1) (off + len))
  in
    Generic.unfoldrN (Generic.length ns) loop (IdxOff 0 0)
{-# INLINE unsafeReify #-}
