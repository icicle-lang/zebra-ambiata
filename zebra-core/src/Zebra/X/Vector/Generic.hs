{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.X.Vector.Generic (
    group
  , segmentedGroup
  , segmentedGroupOn

  , module Generic
  ) where

import           X.Data.Vector.Stream (Stream(..), Step(..))
import qualified X.Data.Vector.Stream as Stream
import           X.Data.Vector.Generic as Generic

import           P hiding (concatMap, length, sum)

-- | Run-length encode a segmented vector.
segmentedGroup ::
     Eq a
  => Vector v a
  => Vector v Int
  => Vector v (Int, a)
  => Vector v (Int, (Int, a))
  => Vector v (Int, Int)
  => v Int
  -> v a
  -> (v Int, v (Int, a))
segmentedGroup = segmentedGroupOn id
{-# INLINE segmentedGroup #-}

-- | Run-length encode a segmented vector.
segmentedGroupOn ::
     Eq b
  => Vector v a
  => Vector v Int
  => Vector v (Int, a)
  => Vector v (Int, (Int, a))
  => Vector v (Int, Int)
  => (a -> b)
  -> v Int
  -> v a
  -> (v Int, v (Int, a))
segmentedGroupOn f ns xs =
  let
    indices =
      concatMap (uncurry $ flip replicate) (indexed ns)

    rotate (n, (s, x)) =
      (s, (n, x))
  in
    first (rezero ns . map fst . group) .
    unzip .
    map rotate $
    groupOn (second f) (zip indices xs)
{-# INLINE segmentedGroupOn #-}

-- | Run-length encode a vector.
group :: (Eq a, Vector v a, Vector v (Int, a)) => v a -> v (Int, a)
group =
  groupOn id
{-# INLINE group #-}

-- | Run-length encode a vector.
groupOn :: (Eq b, Vector v a, Vector v (Int, a)) => (a -> b) -> v a -> v (Int, a)
groupOn f =
  Stream.vectorOfStream . groupStreamOn f . Stream.streamOfVector
{-# INLINE groupOn #-}

data Run a =
    None
  | Run !Int !a

groupStreamOn :: (Monad m, Eq b) => (a -> b) -> Stream m a -> Stream m (Int, a)
groupStreamOn f (Stream step sinit) =
  let
    loop = \case
      Nothing ->
        pure Done

      Just (s0, run) ->
        step s0 >>= \case
          Done ->
            case run of
              None ->
                pure $ Done
              Run n y ->
                pure $ Yield (n, y) Nothing

          Skip s ->
            pure . Skip $
              Just (s, run)

          Yield x s ->
            case run of
              None ->
                pure . Skip $
                  Just (s, Run 1 x)
              Run n y ->
                if f x == f y then
                  pure . Skip $
                    Just (s, Run (n + 1) x)
                else
                  pure . Yield (n, y) $
                    Just (s, Run 1 x)
    {-# INLINE [0] loop #-}
  in
    Stream loop (Just (sinit, None))
{-# INLINE [1] groupStreamOn #-}

-- | Re-insert zeros that have been lost from a segement descriptor:
--
--   rezero [1,2,0,3] [4,5,6] == [4,5,0,6]
--
rezero :: Vector v Int => v Int -> v Int -> v Int
rezero xs ys =
  Stream.vectorOfStream $ rezeroStream
    (Stream.streamOfVector xs)
    (Stream.streamOfVector ys)
{-# INLINE rezero #-}

rezeroStream :: Monad m => Stream m Int -> Stream m Int -> Stream m Int
rezeroStream (Stream o_step o_sinit) (Stream n_step n_sinit) =
  let
    loop (o_s0, n_s0) =
      o_step o_s0 >>= \case
        Done ->
          pure Done

        Skip o_s ->
          pure $ Skip (o_s, n_s0)

        Yield 0 o_s ->
          pure $ Yield 0 (o_s, n_s0)

        Yield _ o_s ->
          n_step n_s0 >>= \case
            Done ->
              pure Done

            Skip n_s ->
              pure $ Skip (o_s, n_s)

            Yield x n_s ->
              pure $ Yield x (o_s, n_s)
    {-# INLINE [0] loop #-}
  in
    Stream loop (o_sinit, n_sinit)
{-# INLINE [1] rezeroStream #-}
