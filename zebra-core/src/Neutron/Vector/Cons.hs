-- |
-- Non-empty vectors, polymorphic in the type of vector.
--
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
#if MIN_VERSION_base(4,9,0)
{-# OPTIONS_GHC -fno-warn-redundant-constraints #-} -- FIXME no unsafeCoerce
#endif
module Neutron.Vector.Cons (
    Cons
  , head
  , tail
  , uncons

  , length
  , index
  , take
  , drop

  , map
  , mapMaybe
  , mapM
  , imap
  , imapM
  , ifor
  , iforM

  , zipWith
  , zipWithM
  , unzip
  , unzipWith
  , unzip3

  , fold
  , fold1
  , fold1M
  , all
  , minimum
  , maximum

  , transpose
  , transposeCV
  , transposeVC

  , toVector
  , toVectorVector
  , toConsVectorVector
  , toList
  , toNonEmpty

  , from
  , from1
  , from2
  , from3
  , fromVector
  , fromList
  , fromNonEmpty
  , unsafeFromVector
  , unsafeFromList
  ) where

import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty

#if MIN_VERSION_base(4,9,0)
import           GHC.Stack (HasCallStack)
#endif

import qualified Neutron.Vector.Generic as Generic

import           P hiding (head, drop, length, zipWithM, toList, mapM, concatMap, fold, all, mapMaybe)

import qualified Prelude as Savage

import           Unsafe.Coerce (unsafeCoerce)

import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as XCons
import qualified X.Data.Vector.Generic as XGeneric


length :: Generic.Vector v a => Cons v a -> Int
length xs =
  Generic.length (toVector xs)
{-# INLINE length #-}

index :: Generic.Vector v a => Int -> Cons v a -> Maybe a
index i xs =
  Generic.index i (toVector xs)
{-# INLINE index #-}

head :: Generic.Vector v a => Cons v a -> a
head xs =
  Generic.unsafeHead (toVector xs)
{-# INLINE head #-}

tail :: Generic.Vector v a => Cons v a -> v a
tail xs =
  Generic.unsafeTail (toVector xs)
{-# INLINE tail #-}

uncons :: Generic.Vector v a => Cons v a -> (a, v a)
uncons xs =
  (head xs, tail xs)
{-# INLINE uncons #-}

take :: Generic.Vector v a => Int -> Cons v a -> v a
take n xs =
  Generic.take n (toVector xs)
{-# INLINE take #-}

drop :: Generic.Vector v a => Int -> Cons v a -> v a
drop n xs =
  Generic.drop n (toVector xs)
{-# INLINE drop #-}

map :: (Generic.Vector v a, Generic.Vector v b) => (a -> b) -> Cons v a -> Cons v b
map f xs =
  coerceVector $ Generic.map f (toVector xs)
{-# INLINE map #-}

mapM :: (Monad m, Generic.Vector v a, Generic.Vector v b) => (a -> m b) -> Cons v a -> m (Cons v b)
mapM f xs =
  coerceVector <$> Generic.mapM f (toVector xs)
{-# INLINE mapM #-}

mapMaybe :: (Generic.Vector v a, Generic.Vector v b) => (a -> Maybe b) -> Cons v a -> v b
mapMaybe f xs =
  Generic.mapMaybe f (toVector xs)
{-# INLINE mapMaybe #-}

imap ::
     Generic.Vector v a
  => Generic.Vector v b
  => (Int -> a -> b)
  -> Cons v a
  -> Cons v b
imap f xs =
  coerceVector $ Generic.imap f (toVector xs)
{-# INLINE imap #-}

imapM ::
     Monad m
  => Generic.Vector v a
  => Generic.Vector v b
  => (Int -> a -> m b)
  -> Cons v a
  -> m (Cons v b)
imapM f xs =
  coerceVector <$> Generic.imapM f (toVector xs)
{-# INLINE imapM #-}

ifor ::
     Generic.Vector v a
  => Generic.Vector v b
  => Cons v a
  -> (Int -> a -> b)
  -> Cons v b
ifor =
  flip imap
{-# INLINE ifor #-}

iforM ::
     Monad m
  => Generic.Vector v a
  => Generic.Vector v b
  => Cons v a
  -> (Int -> a -> m b)
  -> m (Cons v b)
iforM =
  flip imapM
{-# INLINE iforM #-}

zipWith ::
     Generic.Vector v a
  => Generic.Vector v b
  => Generic.Vector v c
  => (a -> b -> c)
  -> Cons v a
  -> Cons v b
  -> Cons v c
zipWith f xs ys =
  coerceVector $
    Generic.zipWith f (toVector xs) (toVector ys)
{-# INLINE zipWith #-}

zipWithM ::
     Monad m
  => Generic.Vector v a
  => Generic.Vector v b
  => Generic.Vector v c
  => (a -> b -> m c)
  -> Cons v a
  -> Cons v b
  -> m (Cons v c)
zipWithM f xs ys =
  fmap coerceVector $
    Generic.zipWithM f (toVector xs) (toVector ys)
{-# INLINE zipWithM #-}

unzip ::
     Generic.Vector v (a, b)
  => Generic.Vector v a
  => Generic.Vector v b
  => Cons v (a, b)
  -> (Cons v a, Cons v b)
unzip xys =
  bimap coerceVector coerceVector $
    Generic.unzip (toVector xys)
{-# INLINE unzip #-}

unzipWith ::
     Generic.Vector v x
  => Generic.Vector v a
  => Generic.Vector v b
  => (x -> (a, b))
  -> Cons v x
  -> (Cons v a, Cons v b)
unzipWith f xys =
  bimap coerceVector coerceVector $
    Generic.unzipWith f (toVector xys)
{-# INLINE unzipWith #-}

unzip3 ::
     Generic.Vector v (a, b, c)
  => Generic.Vector v a
  => Generic.Vector v b
  => Generic.Vector v c
  => Cons v (a, b, c)
  -> (Cons v a, Cons v b, Cons v c)
unzip3 xyzs =
  case Generic.unzip3 (toVector xyzs) of
    (x, y, z) ->
      (coerceVector x, coerceVector y, coerceVector z)
{-# INLINE unzip3 #-}

fold :: Generic.Vector v a => (a -> a -> a) -> a -> Cons v a -> a
fold f x xs =
  Generic.fold f x (toVector xs)
{-# INLINE fold #-}

fold1 :: Generic.Vector v a => (a -> a -> a) -> Cons v a -> a
fold1 f xs =
  case Generic.fold1 f (toVector xs) of
    Nothing ->
      Savage.error "Neutron.Vector.Cons.fold1: empty vector"
    Just x ->
      x
{-# INLINE fold1 #-}

fold1M :: (Monad m, Generic.Vector v a) => (a -> a -> m a) -> Cons v a -> m a
fold1M f xs =
  case Generic.fold1M f (toVector xs) of
    Nothing ->
      Savage.error "Neutron.Vector.Cons.fold1M: empty vector"
    Just m ->
      m
{-# INLINE fold1M #-}

all :: Generic.Vector v a => (a -> Bool) -> Cons v a -> Bool
all f xs =
 Generic.all f (toVector xs)
{-# INLINE all #-}

minimum :: (Ord a, Generic.Vector v a) => Cons v a -> a
minimum xs =
  case Generic.minimum (toVector xs) of
    Nothing ->
      Savage.error "Neutron.Vector.Cons.minimum: empty vector"
    Just m ->
      m
{-# INLINE minimum #-}

maximum :: (Ord a, Generic.Vector v a) => Cons v a -> a
maximum xs =
  case Generic.maximum (toVector xs) of
    Nothing ->
      Savage.error "Neutron.Vector.Cons.maximum: empty vector"
    Just m ->
      m
{-# INLINE maximum #-}

transpose ::
     forall vv va a.
     Generic.Vector va a
  => Generic.Vector vv (va a)
  => Generic.Vector vv (Cons va a)
  => Cons vv (Cons va a)
  -> Cons vv (Cons va a)
transpose =
  coerceVector .
  coerceVectorVector .
  (XGeneric.transpose :: vv (va a) -> vv (va a)) .
  toVectorVector .
  toVector
{-# INLINE transpose #-}

transposeCV ::
     forall vv va a.
     Generic.Vector va a
  => Generic.Vector vv (va a)
  => Generic.Vector vv (Cons va a)
  => Cons vv (va a)
  -> vv (Cons va a)
transposeCV =
  coerceVectorVector .
  (XGeneric.transpose :: vv (va a) -> vv (va a)) .
  toVector
{-# INLINE transposeCV #-}

transposeVC ::
     forall vv va a.
     Generic.Vector va a
  => Generic.Vector vv (va a)
  => Generic.Vector vv (Cons va a)
  => vv (Cons va a)
  -> Cons vv (va a)
transposeVC =
  coerceVector .
  (XGeneric.transpose :: vv (va a) -> vv (va a)) .
  toVectorVector
{-# INLINE transposeVC #-}

toVector :: Cons v a -> v a
toVector xs =
  XCons.toVector xs
{-# INLINE toVector #-}

toVectorVector :: (Generic.Vector vv (v a), Generic.Vector vv (Cons v a)) => vv (Cons v a) -> vv (v a)
toVectorVector xs =
  unsafeCoerce xs
{-# INLINE toVectorVector #-}

toConsVectorVector :: (Generic.Vector vv (v a), Generic.Vector vv (Cons v a)) => Cons vv (Cons v a) -> Cons vv (v a)
toConsVectorVector xs =
  unsafeCoerce xs
{-# INLINE toConsVectorVector #-}

toList :: Generic.Vector v a => Cons v a -> [a]
toList xs =
  Generic.toList (toVector xs)
{-# INLINE toList #-}

toNonEmpty :: Generic.Vector v a => Cons v a -> NonEmpty a
toNonEmpty xs =
  NonEmpty.fromList . Generic.toList $ toVector xs
{-# INLINE toNonEmpty #-}

coerceVector :: Generic.Vector v a => v a -> Cons v a
coerceVector xs =
  unsafeCoerce xs
{-# INLINE coerceVector #-}

coerceVectorVector :: (Generic.Vector vv (v a), Generic.Vector vv (Cons v a)) => vv (v a) -> vv (Cons v a)
coerceVectorVector xs =
  unsafeCoerce xs
{-# INLINE coerceVectorVector #-}

from :: Generic.Vector v a => a -> v a -> Cons v a
from x xs =
  coerceVector $ Generic.cons x xs
{-# INLINE from #-}

from1 :: Generic.Vector v a => a -> Cons v a
from1 x0 =
  coerceVector $ Generic.singleton x0
{-# INLINE from1 #-}

from2 :: Generic.Vector v a => a -> a -> Cons v a
from2 x0 x1 =
  coerceVector $ Generic.fromList [x0, x1]
{-# INLINE from2 #-}

from3 :: Generic.Vector v a => a -> a -> a -> Cons v a
from3 x0 x1 x2 =
  coerceVector $ Generic.fromList [x0, x1, x2]
{-# INLINE from3 #-}

fromVector :: Generic.Vector v a => v a -> Maybe (Cons v a)
fromVector xs =
  if Generic.null xs then
    Nothing
  else
    Just (coerceVector xs)
{-# INLINE fromVector #-}

fromList :: Generic.Vector v a => [a] -> Maybe (Cons v a)
fromList = \case
  [] ->
    Nothing
  xs ->
    Just (coerceVector (Generic.fromList xs))
{-# INLINE fromList #-}

fromNonEmpty :: Generic.Vector v a => NonEmpty a -> Cons v a
fromNonEmpty =
  coerceVector . Generic.fromList . NonEmpty.toList
{-# INLINE fromNonEmpty #-}

#if MIN_VERSION_base(4,9,0)
unsafeFromVector :: (HasCallStack, Generic.Vector v a) => v a -> Cons v a
#else
unsafeFromVector :: Generic.Vector v a => v a -> Cons v a
#endif
unsafeFromVector =
  let
    msg =
      "X.Data.Vector.Cons.unsafeFromVector: empty vector"
  in
    fromMaybe (Savage.error msg) . fromVector
{-# INLINE unsafeFromVector #-}

#if MIN_VERSION_base(4,9,0)
unsafeFromList :: (HasCallStack, Generic.Vector v a) => [a] -> Cons v a
#else
unsafeFromList :: Generic.Vector v a => [a] -> Cons v a
#endif
unsafeFromList =
  let
    msg =
      "X.Data.Vector.Cons.unsafeFromList: empty list"
  in
    fromMaybe (Savage.error msg) . fromList
{-# INLINE unsafeFromList #-}
