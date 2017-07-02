{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Neutron.Vector.Generic (
    Vector
  , Mutable

  , length
  , null
  , index
  , empty
  , singleton
  , replicate
  , cons
  , uncons
  , head
  , take
  , drop
  , splitAt
  , concat

  , unsafeIndex
  , unsafeHead
  , unsafeLast
  , unsafeSlice
  , unsafeInit
  , unsafeTail
  , unsafeTake
  , unsafeDrop
  , unsafeFreeze

  , imapMaybeM
  , mapMaybeM
  , imapM
  , imapM_
  , mapM
  , mapM_
  , imapMaybe
  , mapMaybe
  , imap
  , map

  , iforMaybeM
  , forMaybeM
  , iforM
  , iforM_
  , forM
  , forM_
  , iforMaybe
  , forMaybe
  , for

  , ifilterM
  , filterM
  , filter

  , izipWithM
  , zipWithM
  , izipWith
  , zipWith
  , zip

  , unzip
  , unzipWith
  , unzip3
  , unzip3With

  , ifoldM
  , ifoldM_
  , foldM
  , foldM_
  , ifold
  , fold

  , ifold1M
  , ifold1M_
  , fold1M
  , fold1M_
  , ifold1
  , fold1

  , ifoldrM
  , foldrM
  , ifoldr
  , foldr

  , sum
  , all
  , any
  , minimum
  , maximum

  , toList
  , fromList
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Primitive (RealWorld)
import           Control.Monad.Trans.Class (MonadTrans(..))

import           Data.Functor.Identity (Identity(..))
import qualified Data.List as List
import           Data.Vector.Generic.Base (Vector(..), Mutable)

import qualified Neutron.Vector.Generic.Mutable as MGeneric
import           Neutron.Unsafe

import           P hiding (
    all
  , any
  , empty
  , length
  , null
  , mapM
  , mapM_
  , mapMaybe
  , for
  , forM
  , forM_
  , filter
  , filterM
  , zipWithM
  , fold
  , foldr
  , foldM
  , foldM_
  , foldrM
  , sum
  , drop
  , head
  , splitAt
  , toList
  , concat
  )

import           System.IO (IO)
import           System.IO.Unsafe (unsafePerformIO)

------------------------------------------------------------------------
-- Basic

length :: Vector v a => v a -> Int
length xs = {-# SCC length #-}
  basicLength xs
{-# INLINE length #-}

null :: Vector v a => v a -> Bool
null xs = {-# SCC null #-}
  length xs == 0
{-# INLINE null #-}

index :: Vector v a => Int -> v a -> Maybe a
index i xs = {-# SCC index #-}
  if i >= 0 && i < length xs then
    Just $! unsafeIndex i xs
  else
    Nothing
{-# INLINE index #-}

empty :: Vector v a => v a
empty = {-# SCC empty #-}
  unsafePerformIO $ do
    !dst <- MGeneric.unsafeNew 0
    unsafeFreeze dst
{-# NOINLINE empty #-}

singleton :: Vector v a => a -> v a
singleton x = {-# SCC singleton #-}
  unsafePerformIO $ do
    !dst <- MGeneric.unsafeNew 1
    MGeneric.unsafeWrite dst 0 x
    unsafeFreeze dst
{-# INLINE singleton #-}

replicate :: Vector v a => Int -> a -> v a
replicate n0 x = {-# SCC replicate #-}
  unsafePerformIO $ do
    let
      !n =
        max 0 n0

    !dst <- MGeneric.unsafeReplicate n x
    unsafeFreeze dst
{-# INLINE replicate #-}

cons :: Vector v a => a -> v a -> v a
cons x xs = {-# SCC cons #-}
  unsafePerformIO $ do
    let
      !n =
        length xs

    !dst <- MGeneric.unsafeNew (n + 1)
    !src <- unsafeThaw xs

    MGeneric.unsafeCopy (MGeneric.unsafeSlice 1 n dst) src
    MGeneric.unsafeWrite dst 0 x
    unsafeFreeze dst
{-# INLINE cons #-}

uncons :: Vector v a => v a -> Maybe (a, v a)
uncons xs0 = {-# SCC uncons #-}
  if null xs0 then
    Nothing
  else
    let
      !x =
        unsafeHead xs0

      !xs =
        unsafeTail xs0
    in
      Just $! (x, xs)
{-# INLINE uncons #-}

head :: Vector v a => v a -> Maybe a
head xs = {-# SCC head #-}
  index 0 xs
{-# INLINE head #-}

take :: Vector v a => Int -> v a -> v a
take n0 xs = {-# SCC take #-}
  let
    !n =
      max n0 0 `min` length xs
  in
    unsafeTake n xs
{-# INLINE take #-}

drop :: Vector v a => Int -> v a -> v a
drop n0 xs = {-# SCC drop #-}
  let
    !n =
      max n0 0 `min` length xs
  in
    unsafeDrop n xs
{-# INLINE drop #-}

splitAt :: Vector v a => Int -> v a -> (v a, v a)
splitAt n0 xs = {-# SCC splitAt #-}
  let
    !n =
      max n0 0 `min` length xs
  in
    (unsafeTake n xs, unsafeDrop n xs)
{-# INLINE splitAt #-}

concat :: (Vector va a, Vector vv (va a)) => vv (va a) -> va a
concat xss = {-# SCC concat #-}
  unsafePerformIO $ do
    let
      sumLength !acc !xs =
        acc + length xs

      !n =
        fold sumLength 0 xss

    !dst <- MGeneric.unsafeNew n

    let
      !n_xss =
        length xss

      outer !i_dst0 !i =
        if i >= n_xss then
          unsafeFreeze dst

        else do
          let
            !xs =
              unsafeIndex i xss

            !n_xs =
              length xs

            inner !i_dst !j =
              if j >= n_xs then
                return i_dst

              else do
                let
                  !x =
                    unsafeIndex j xs

                MGeneric.unsafeWrite dst i_dst x
                inner (i_dst + 1) (j + 1)

          !i_dst1 <- inner i_dst0 0
          outer i_dst1 (i + 1)

    outer 0 0
{-# INLINE concat #-}

------------------------------------------------------------------------
-- Unsafe

unsafeIndex :: Vector v a => Int -> v a -> a
unsafeIndex i xs = {-# SCC unsafeIndex #-}
  runIdentity $! basicUnsafeIndexM xs i
{-# INLINE unsafeIndex #-}

unsafeHead :: Vector v a => v a -> a
unsafeHead xs = {-# SCC unsafeHead #-}
  unsafeIndex 0 xs
{-# INLINE unsafeHead #-}

unsafeLast :: Vector v a => v a -> a
unsafeLast xs = {-# SCC unsafeLast #-}
  unsafeIndex (length xs - 1) xs
{-# INLINE unsafeLast #-}

unsafeSlice :: Vector v a => Int -> Int -> v a -> v a
unsafeSlice off len xs = {-# SCC unsafeSlice #-}
  basicUnsafeSlice off len xs
{-# INLINE unsafeSlice #-}

unsafeInit :: Vector v a => v a -> v a
unsafeInit xs = {-# SCC unsafeInit #-}
  unsafeSlice 0 (length xs - 1) xs
{-# INLINE unsafeInit #-}

unsafeTail :: Vector v a => v a -> v a
unsafeTail xs = {-# SCC unsafeTail #-}
  unsafeSlice 1 (length xs - 1) xs
{-# INLINE unsafeTail #-}

unsafeTake :: Vector v a => Int -> v a -> v a
unsafeTake n xs = {-# SCC unsafeTake #-}
  unsafeSlice 0 n xs
{-# INLINE unsafeTake #-}

unsafeDrop :: Vector v a => Int -> v a -> v a
unsafeDrop n xs = {-# SCC unsafeDrop #-}
  unsafeSlice n (length xs - n) xs
{-# INLINE unsafeDrop #-}

unsafeFreeze :: Vector v a => Mutable v RealWorld a -> IO (v a)
unsafeFreeze xs = {-# SCC unsafeFreeze #-}
  basicUnsafeFreeze xs
{-# INLINE unsafeFreeze #-}

unsafeThaw :: Vector v a => v a -> IO (Mutable v RealWorld a)
unsafeThaw xs = {-# SCC unsafeThaw #-}
  basicUnsafeThaw xs
{-# INLINE unsafeThaw #-}

------------------------------------------------------------------------
-- Map

imapMaybeM :: (Monad m, Vector v a, Vector u b) => (Int -> a -> m (Maybe b)) -> v a -> m (u b)
imapMaybeM f xs = {-# SCC imapMaybeM #-}
  unsafePerformT $ do
    let
      !n =
        length xs

    !dst <-
      liftIO $ MGeneric.unsafeNew n

    let
      loop !i !j =
        if i >= n then
          liftIO $ unsafeFreeze (MGeneric.unsafeSlice 0 j dst)

        else do
          let
            !x =
              unsafeIndex i xs

          !my <-
            lift $ f i x

          case my of
            Nothing ->
              loop (i + 1) j

            Just y -> do
              liftIO $ MGeneric.unsafeWrite dst j y
              loop (i + 1) (j + 1)

    loop 0 0
{-# INLINE imapMaybeM #-}

mapMaybeM :: (Monad m, Vector v a, Vector u b) => (a -> m (Maybe b)) -> v a -> m (u b)
mapMaybeM f xs = {-# SCC mapMaybeM #-}
  imapMaybeM (\_ x -> f x) xs
{-# INLINE mapMaybeM #-}

imapM :: (Monad m, Vector v a, Vector u b) => (Int -> a -> m b) -> v a -> m (u b)
imapM f xs = {-# SCC imapM #-}
  unsafePerformT $ do
    let
      !n =
        length xs

    !dst <-
      liftIO $ MGeneric.unsafeNew n

    let
      loop !i =
        if i >= n then
          liftIO $ unsafeFreeze dst

        else do
          let
            !x =
              unsafeIndex i xs

          !y <-
            lift $ f i x

          liftIO $ MGeneric.unsafeWrite dst i y
          loop (i + 1)

    loop 0
{-# INLINE imapM #-}

mapM :: (Monad m, Vector v a, Vector u b) => (a -> m b) -> v a -> m (u b)
mapM f xs = {-# SCC mapM #-}
  imapM (\_ x -> f x) xs
{-# INLINE mapM #-}

imapMaybe :: (Vector v a, Vector u b) => (Int -> a -> Maybe b) -> v a -> u b
imapMaybe f xs = {-# SCC imapMaybe #-}
  runIdentity $! imapMaybeM (\i x -> return (f i x)) xs
{-# INLINE imapMaybe #-}

mapMaybe :: (Vector v a, Vector u b) => (a -> Maybe b) -> v a -> u b
mapMaybe f xs = {-# SCC mapMaybe #-}
  imapMaybe (\_ x -> f x) xs
{-# INLINE mapMaybe #-}

imap :: (Vector v a, Vector u b) => (Int -> a -> b) -> v a -> u b
imap f xs = {-# SCC imap #-}
  runIdentity $! imapM (\i x -> return (f i x)) xs
{-# INLINE imap #-}

map :: (Vector v a, Vector u b) => (a -> b) -> v a -> u b
map f xs = {-# SCC map #-}
  imap (\_ x -> f x) xs
{-# INLINE map #-}

------------------------------------------------------------------------
-- Map_

imapM_ :: (Monad m, Vector v a) =>(Int -> a -> m ()) -> v a -> m ()
imapM_ f xs = {-# SCC imapM_ #-}
  unsafePerformT $ do
    let
      !n =
        length xs

    let
      loop !i =
        if i >= n then
          return ()

        else do
          let
            !x =
              unsafeIndex i xs

          !_ <-
            lift $ f i x

          loop (i + 1)

    loop 0
{-# INLINE imapM_ #-}

mapM_ :: (Monad m, Vector v a) => (a -> m ()) -> v a -> m ()
mapM_ f xs = {-# SCC mapM_ #-}
  imapM_ (\_ x -> f x) xs
{-# INLINE mapM_ #-}

------------------------------------------------------------------------
-- For

iforMaybeM :: (Monad m, Vector v a, Vector u b) => v a -> (Int -> a -> m (Maybe b)) -> m (u b)
iforMaybeM xs f = {-# SCC iforMaybeM #-}
  imapMaybeM f xs
{-# INLINE iforMaybeM #-}

forMaybeM :: (Monad m, Vector v a, Vector u b) => v a -> (a -> m (Maybe b)) -> m (u b)
forMaybeM xs f = {-# SCC forMaybeM #-}
  mapMaybeM f xs
{-# INLINE forMaybeM #-}

iforM :: (Monad m, Vector v a, Vector u b) => v a -> (Int -> a -> m b) -> m (u b)
iforM xs f = {-# SCC iforM #-}
  imapM f xs
{-# INLINE iforM #-}

forM :: (Monad m, Vector v a, Vector u b) => v a -> (a -> m b) -> m (u b)
forM xs f = {-# SCC forM #-}
  mapM f xs
{-# INLINE forM #-}

iforMaybe :: (Vector v a, Vector u b) => v a -> (Int -> a -> Maybe b) -> u b
iforMaybe xs f = {-# SCC iforMaybe #-}
  imapMaybe f xs
{-# INLINE iforMaybe #-}

forMaybe :: (Vector v a, Vector u b) => v a -> (a -> Maybe b) -> u b
forMaybe xs f = {-# SCC forMaybe #-}
  mapMaybe f xs
{-# INLINE forMaybe #-}

for :: (Vector v a, Vector u b) => v a -> (a -> b) -> u b
for xs f = {-# SCC for #-}
  map f xs
{-# INLINE for #-}

------------------------------------------------------------------------
-- For_

iforM_ :: (Monad m, Vector v a) => v a -> (Int -> a -> m ()) -> m ()
iforM_ xs f = {-# SCC iforM_ #-}
  imapM_ f xs
{-# INLINE iforM_ #-}

forM_ :: (Monad m, Vector v a) => v a -> (a -> m ()) -> m ()
forM_ xs f = {-# SCC forM_ #-}
  mapM_ f xs
{-# INLINE forM_ #-}

------------------------------------------------------------------------
-- Filter

ifilterM :: (Monad m, Vector v a, Vector u a) => (Int -> a -> m Bool) -> v a -> m (u a)
ifilterM f xs = {-# SCC ifilterM #-}
  iforMaybeM xs $ \i x ->
    with (f i x) $ \b ->
      if b then
        Just x
      else
        Nothing
{-# INLINE ifilterM #-}

filterM :: (Monad m, Vector v a, Vector u a) => (a -> m Bool) -> v a -> m (u a)
filterM f xs = {-# SCC filterM #-}
  ifilterM (\_ x -> f x) xs
{-# INLINE filterM #-}

filter :: (Vector v a, Vector u a) => (a -> Bool) -> v a -> u a
filter f xs = {-# SCC filter #-}
  runIdentity $! filterM (return . f) xs
{-# INLINE filter #-}

------------------------------------------------------------------------
-- Zip

izipWithM ::
     (Monad m, Vector va a, Vector vb b, Vector vc c)
  => (Int -> a -> b -> m c)
  -> va a
  -> vb b
  -> m (vc c)
izipWithM f xs ys =
  unsafePerformT $ do
    let
      !n =
        min (length xs) (length ys)

    !dst <-
      liftIO $ MGeneric.unsafeNew n

    let
      loop !i =
        if i >= n then
          liftIO $ unsafeFreeze dst

        else do
          let
            !x =
              unsafeIndex i xs

            !y =
              unsafeIndex i ys

          !z <-
            lift $ f i x y

          liftIO $ MGeneric.unsafeWrite dst i z
          loop (i + 1)

    loop 0
{-# INLINE izipWithM #-}

zipWithM :: (Monad m, Vector va a, Vector vb b, Vector vc c) => (a -> b -> m c) -> va a -> vb b -> m (vc c)
zipWithM f xs ys = {-# SCC zipWithM #-}
  izipWithM (\_ x y -> f x y) xs ys
{-# INLINE zipWithM #-}

izipWith :: (Vector va a, Vector vb b, Vector vc c) => (Int -> a -> b -> c) -> va a -> vb b -> vc c
izipWith f xs ys = {-# SCC izipWith #-}
  runIdentity $! izipWithM (\i x y -> return (f i x y)) xs ys
{-# INLINE izipWith #-}

zipWith :: (Vector va a, Vector vb b, Vector vc c) => (a -> b -> c) -> va a -> vb b -> vc c
zipWith f xs ys = {-# SCC zipWith #-}
  runIdentity $! zipWithM (\x y -> return (f x y)) xs ys
{-# INLINE zipWith #-}

zip :: (Vector va a, Vector vb b, Vector vab (a, b)) => va a -> vb b -> vab (a, b)
zip xs ys = {-# SCC zip #-}
  zipWith (\x y -> (x, y)) xs ys
{-# INLINE zip #-}

------------------------------------------------------------------------

unzipWith :: (Vector vx x, Vector va a, Vector vb b) => (x -> (a, b)) -> vx x -> (va a, vb b)
unzipWith f xys = {-# SCC unzipWith #-}
  unsafePerformIO $ do
    let
      !n =
        length xys

    !dstX <-
      liftIO $ MGeneric.unsafeNew n

    !dstY <-
      liftIO $ MGeneric.unsafeNew n

    let
      loop !i =
        if i >= n then
          (,)
            <$> liftIO (unsafeFreeze dstX)
            <*> liftIO (unsafeFreeze dstY)

        else do
          let
            (!x, !y) =
              f $! unsafeIndex i xys

          liftIO $ MGeneric.unsafeWrite dstX i x
          liftIO $ MGeneric.unsafeWrite dstY i y
          loop (i + 1)

    loop 0
{-# INLINE unzipWith #-}

unzip :: (Vector vx (a, b), Vector va a, Vector vb b) => vx (a, b) -> (va a, vb b)
unzip xys = {-# SCC unzip #-}
  unzipWith id xys
{-# INLINE unzip #-}

unzip3With :: (Vector vx x, Vector va a, Vector vb b, Vector vc c) => (x -> (a, b, c)) -> vx x -> (va a, vb b, vc c)
unzip3With f xyzs = {-# SCC unzip3With #-}
  unsafePerformIO $ do
    let
      !n =
        length xyzs

    !dstX <-
      liftIO $ MGeneric.unsafeNew n

    !dstY <-
      liftIO $ MGeneric.unsafeNew n

    !dstZ <-
      liftIO $ MGeneric.unsafeNew n

    let
      loop !i =
        if i >= n then
          (,,)
            <$> liftIO (unsafeFreeze dstX)
            <*> liftIO (unsafeFreeze dstY)
            <*> liftIO (unsafeFreeze dstZ)

        else do
          let
            (!x, !y, !z) =
              f $! unsafeIndex i xyzs

          liftIO $ MGeneric.unsafeWrite dstX i x
          liftIO $ MGeneric.unsafeWrite dstY i y
          liftIO $ MGeneric.unsafeWrite dstZ i z
          loop (i + 1)

    loop 0
{-# INLINE unzip3With #-}

unzip3 :: (Vector vx (a, b, c), Vector va a, Vector vb b, Vector vc c) => vx (a, b, c) -> (va a, vb b, vc c)
unzip3 xyzs = {-# SCC unzip3 #-}
  unzip3With id xyzs
{-# INLINE unzip3 #-}

------------------------------------------------------------------------
-- Fold

ifoldM :: (Monad m, Vector v a) => (b -> Int -> a -> m b) -> b -> v a -> m b
ifoldM f z xs = {-# SCC ifoldM #-}
  unsafePerformT $! do
    let
      !n =
        length xs

      loop !s0 !i =
        if i >= n then
          return s0

        else do
          let
            !x =
              unsafeIndex i xs

          !s <-
            lift $ f s0 i x

          loop s (i + 1)

    loop z 0
{-# INLINE ifoldM #-}

ifoldM_ :: (Monad m, Vector v a) => (b -> Int -> a -> m b) -> b -> v a -> m ()
ifoldM_ f z xs = {-# SCC ifoldM_ #-}
  void $! ifoldM f z xs
{-# INLINE ifoldM_ #-}

foldM :: (Monad m, Vector v a) => (b -> a -> m b) -> b -> v a -> m b
foldM f z xs = {-# SCC foldM #-}
  ifoldM (\s _ x -> f s x) z xs
{-# INLINE foldM #-}

foldM_ :: (Monad m, Vector v a) => (b -> a -> m b) -> b -> v a -> m ()
foldM_ f z xs = {-# SCC foldM_ #-}
  void $! foldM f z xs
{-# INLINE foldM_ #-}

ifold :: Vector v a => (b -> Int -> a -> b) -> b -> v a -> b
ifold f z xs = {-# SCC ifold #-}
  runIdentity $! ifoldM (\s i x -> return (f s i x)) z xs
{-# INLINE ifold #-}

fold :: Vector v a => (b -> a -> b) -> b -> v a -> b
fold f z xs = {-# SCC fold #-}
  runIdentity $! foldM (\s x -> return (f s x)) z xs
{-# INLINE fold #-}

------------------------------------------------------------------------
-- Fold1

ifold1M :: (Monad m, Vector v a) => (a -> Int -> a -> m a) -> v a -> Maybe (m a)
ifold1M f xs0 = {-# SCC ifold1M #-}
  case uncons xs0 of
    Nothing ->
      Nothing
    Just (x, xs) ->
      Just (ifoldM f x xs)
{-# INLINE ifold1M #-}

ifold1M_ :: (Monad m, Vector v a) => (a -> Int -> a -> m a) -> v a -> Maybe (m ())
ifold1M_ f xs0 = {-# SCC ifold1M_ #-}
  case uncons xs0 of
    Nothing ->
      Nothing
    Just (x, xs) ->
      Just (void $! ifoldM f x xs)
{-# INLINE ifold1M_ #-}

fold1M :: (Monad m, Vector v a) => (a -> a -> m a) -> v a -> Maybe (m a)
fold1M f xs = {-# SCC fold1M #-}
  ifold1M (\s _ x -> f s x) xs
{-# INLINE fold1M #-}

fold1M_ :: (Monad m, Vector v a) => (a -> a -> m a) -> v a -> Maybe (m ())
fold1M_ f xs = {-# SCC fold1M_ #-}
  ifold1M_ (\s _ x -> f s x) xs
{-# INLINE fold1M_ #-}

ifold1 :: Vector v a => (a -> Int -> a -> a) -> v a -> Maybe a
ifold1 f xs = {-# SCC ifold1 #-}
  runIdentity <$!> ifold1M (\s i x -> return (f s i x)) xs
{-# INLINE ifold1 #-}

fold1 :: Vector v a => (a -> a -> a) -> v a -> Maybe a
fold1 f xs = {-# SCC fold1 #-}
  runIdentity <$!> fold1M (\s x -> return (f s x)) xs
{-# INLINE fold1 #-}

------------------------------------------------------------------------
-- Foldr

ifoldrM :: (Monad m, Vector v a) => (Int -> a -> b -> m b) -> b -> v a -> m b
ifoldrM f z xs = {-# SCC ifoldrM #-}
  unsafePerformT $! do
    let
      !n =
        length xs

      loop !s0 !i =
        if i < 0 then
          return s0

        else do
          let
            !x =
              unsafeIndex i xs

          !s <-
            lift $ f i x s0

          loop s (i - 1)

    loop z (n - 1)
{-# INLINE ifoldrM #-}

foldrM :: (Monad m, Vector v a) => (a -> b -> m b) -> b -> v a -> m b
foldrM f z xs = {-# SCC foldrM #-}
  ifoldrM (\_ x s -> f x s) z xs
{-# INLINE foldrM #-}

ifoldr :: Vector v a => (Int -> a -> b -> b) -> b -> v a -> b
ifoldr f z xs = {-# SCC ifoldr #-}
  runIdentity $! ifoldrM (\i x s -> return (f i x s)) z xs
{-# INLINE ifoldr #-}

foldr :: Vector v a => (a -> b -> b) -> b -> v a -> b
foldr f z xs = {-# SCC foldr #-}
  runIdentity $! foldrM (\x s -> return (f x s)) z xs
{-# INLINE foldr #-}

------------------------------------------------------------------------
-- Aggregates

sum :: (Vector v a, Num a) => v a -> a
sum xs = {-# SCC sum #-}
  fold (+) 0 xs
{-# INLINE sum #-}

all :: Vector v a => (a -> Bool) -> v a -> Bool
all f xs = {-# SCC all #-}
  let
    loop !b !x =
      b && f x
  in
    fold loop True xs
{-# INLINE all #-}

any :: Vector v a => (a -> Bool) -> v a -> Bool
any f xs = {-# SCC any #-}
  let
    loop !b !x =
      b || f x
  in
    fold loop False xs
{-# INLINE any #-}

minimum :: (Vector v a, Ord a) => v a -> Maybe a
minimum xs = {-# SCC minimum #-}
  fold1 min xs
{-# INLINE minimum #-}

maximum :: (Vector v a, Ord a) => v a -> Maybe a
maximum xs = {-# SCC maximum #-}
  fold1 max xs
{-# INLINE maximum #-}

------------------------------------------------------------------------
-- Conversion

toList :: Vector v a => v a -> [a]
toList xs = {-# SCC toList #-}
  foldr (:) [] xs
{-# INLINE toList #-}

fromList :: Vector v a => [a] -> v a
fromList xs0 = {-# SCC fromList #-}
  unsafePerformIO $ do
    let
      -- FIXME probably better to only walk the list once
      !n =
        List.length xs0

    !dst <-
      MGeneric.unsafeNew n

    let
      loop !i = \case
        [] ->
          unsafeFreeze dst

        x : xs -> do
          liftIO $ MGeneric.unsafeWrite dst i x
          loop (i + 1) xs

    loop 0 xs0
{-# INLINE fromList #-}
