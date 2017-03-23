{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Data.Vector.Generic (
    group
  , segmentedGroup

  , module Generic
  ) where

import           X.Data.Vector.Stream (Stream(..), Step(..))
import qualified X.Data.Vector.Stream as Stream
import           X.Data.Vector.Generic as Generic

import           P hiding (concatMap, length, sum)

-- | Run-length encode a segmented vector.
segmentedGroup ::
  Eq a =>
  Vector v a =>
  Vector v Int =>
  Vector v (Int, a) =>
  Vector v (Int, (Int, a)) =>
  Vector v (Int, Int) =>
  v Int ->
  v a ->
  (v Int, v (Int, a))
segmentedGroup ns xs =
  let
    indices =
      concatMap (uncurry $ flip replicate) (indexed ns)

    rotate (n, (s, x)) =
      (s, (n, x))
  in
    first (rezero ns . map fst . group) .
    unzip .
    map rotate $
    group (zip indices xs)
{-# INLINE segmentedGroup #-}

-- | Run-length encode a vector.
group :: (Eq a, Vector v a, Vector v (Int, a)) => v a -> v (Int, a)
group =
  Stream.vectorOfStream . groupStream . Stream.streamOfVector
{-# INLINE group #-}

data Run a =
    None
  | Run !Int !a

groupStream :: (Monad m, Eq a) => Stream m a -> Stream m (Int, a)
groupStream (Stream step sinit) =
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
                if x == y then
                  pure . Skip $
                    Just (s, Run (n + 1) x)
                else
                  pure . Yield (n, y) $
                    Just (s, Run 1 x)
    {-# INLINE [0] loop #-}
  in
    Stream loop (Just (sinit, None))
{-# INLINE [1] groupStream #-}

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
