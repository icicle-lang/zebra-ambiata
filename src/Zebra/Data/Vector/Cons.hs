{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Data.Vector.Cons (
    Cons
  , head
  , tail
  , uncons

  , length
  , index
  , focus

  , map
  , mapMaybe
  , mapM
  , imap
  , imapM
  , ifor
  , iforM

  , concatMap

  , zipWith
  , zipWithM
  , unzip
  , unzip3

  , foldl
  , foldl1
  , fold1M'
  , all
  , minimum
  , maximum

  , transpose
  , transposeCV
  , transposeVC

  , toVector
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
import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

#if MIN_VERSION_base(4,9,0)
import           GHC.Stack (HasCallStack)
#endif

import           P hiding (head, drop, length, zipWithM, toList, mapM, concatMap, foldl, all, mapMaybe)

import qualified Prelude as Savage

import qualified X.Data.Vector.Generic as Generic


newtype Cons v a =
  Cons (v a)
  deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

instance (Show a, Generic.Vector v a) => Show (Cons v a) where
  showsPrec _ =
    showList . toList

length :: Generic.Vector v a => Cons v a -> Int
length (Cons xs) =
  Generic.length xs
{-# INLINE length #-}

index :: Generic.Vector v a => Int -> Cons v a -> Maybe a
index i (Cons xs) =
  xs Generic.!? i
{-# INLINE index #-}

head :: Generic.Vector v a => Cons v a -> a
head (Cons xs) =
  Generic.unsafeHead xs
{-# INLINE head #-}

tail :: Generic.Vector v a => Cons v a -> v a
tail (Cons xs) =
  Generic.unsafeTail xs
{-# INLINE tail #-}

uncons :: Generic.Vector v a => Cons v a -> (a, v a)
uncons xs =
  (head xs, tail xs)
{-# INLINE uncons #-}

take :: Generic.Vector v a => Int -> Cons v a -> v a
take n (Cons xs) =
  Generic.take n xs
{-# INLINE take #-}

drop :: Generic.Vector v a => Int -> Cons v a -> v a
drop n (Cons xs) =
  Generic.drop n xs
{-# INLINE drop #-}

focus :: Generic.Vector v a => Int -> Cons v a -> Maybe (v a, a, v a)
focus i xs =
  case index i xs of
    Nothing ->
      Nothing
    Just x ->
      Just (take i xs, x, drop i xs)
{-# INLINE focus #-}

map :: (Generic.Vector v a, Generic.Vector v b) => (a -> b) -> Cons v a -> Cons v b
map f (Cons xs) =
  Cons $ Generic.map f xs
{-# INLINE map #-}

mapM :: (Monad m, Generic.Vector v a, Generic.Vector v b) => (a -> m b) -> Cons v a -> m (Cons v b)
mapM f (Cons xs) =
  Cons <$> Generic.mapM f xs
{-# INLINE mapM #-}

mapMaybe :: (Generic.Vector v a, Generic.Vector v b) => (a -> Maybe b) -> Cons v a -> v b
mapMaybe f (Cons xs) =
  Generic.mapMaybe f xs
{-# INLINE mapMaybe #-}

imap ::
  Generic.Vector v a =>
  Generic.Vector v b =>
  (Int -> a -> b) ->
  Cons v a ->
  Cons v b
imap f (Cons xs) =
  Cons $ Generic.imap (\i -> f i) xs
{-# INLINE imap #-}

imapM ::
  Monad m =>
  Generic.Vector v a =>
  Generic.Vector v b =>
  (Int -> a -> m b) ->
  Cons v a ->
  m (Cons v b)
imapM f (Cons xs) =
  Cons <$> Generic.imapM (\i -> f i) xs
{-# INLINE imapM #-}

ifor ::
  Generic.Vector v a =>
  Generic.Vector v b =>
  Cons v a ->
  (Int -> a -> b) ->
  Cons v b
ifor =
  flip imap
{-# INLINE ifor #-}

iforM ::
  Monad m =>
  Generic.Vector v a =>
  Generic.Vector v b =>
  Cons v a ->
  (Int -> a -> m b) ->
  m (Cons v b)
iforM =
  flip imapM
{-# INLINE iforM #-}

concatMap :: (Generic.Vector v a, Generic.Vector v b) => (a -> Cons v b) -> Cons v a -> Cons v b
concatMap f (Cons xs) =
  Cons $ Generic.concatMap (toVector . f) xs
{-# INLINE concatMap #-}

zipWith ::
  Generic.Vector v a =>
  Generic.Vector v b =>
  Generic.Vector v c =>
  (a -> b -> c) ->
  Cons v a ->
  Cons v b ->
  Cons v c
zipWith f (Cons xs) (Cons ys) =
  Cons $
    Generic.zipWith f xs ys
{-# INLINE zipWith #-}

zipWithM ::
  Monad m =>
  Generic.Vector v a =>
  Generic.Vector v b =>
  Generic.Vector v c =>
  (a -> b -> m c) ->
  Cons v a ->
  Cons v b ->
  m (Cons v c)
zipWithM f (Cons xs) (Cons ys) =
  fmap Cons $
    Generic.zipWithM f xs ys
{-# INLINE zipWithM #-}

unzip ::
  Generic.Vector v (a, b) =>
  Generic.Vector v a =>
  Generic.Vector v b =>
  Cons v (a, b) ->
  (Cons v a, Cons v b)
unzip (Cons xys) =
  bimap Cons Cons $
    Generic.unzip xys
{-# INLINE unzip #-}

unzip3 ::
  Generic.Vector v (a, b, c) =>
  Generic.Vector v a =>
  Generic.Vector v b =>
  Generic.Vector v c =>
  Cons v (a, b, c) ->
  (Cons v a, Cons v b, Cons v c)
unzip3 (Cons xyzs) =
  case Generic.unzip3 xyzs of
    (x, y, z) ->
      (Cons x, Cons y, Cons z)
{-# INLINE unzip3 #-}

foldl :: Generic.Vector v a => (a -> a -> a) -> a -> Cons v a -> a
foldl f x (Cons xs) =
  Generic.foldl f x xs
{-# INLINE foldl #-}

foldl1 :: Generic.Vector v a => (a -> a -> a) -> Cons v a -> a
foldl1 f (Cons xs) =
  Generic.foldl1 f xs
{-# INLINE foldl1 #-}

fold1M' :: (Monad m, Generic.Vector v a) => (a -> a -> m a) -> Cons v a -> m a
fold1M' f (Cons xs) =
  Generic.fold1M' f xs
{-# INLINE fold1M' #-}

all :: Generic.Vector v a => (a -> Bool) -> Cons v a -> Bool
all f (Cons xs) =
 Generic.all f xs
{-# INLINE all #-}

minimum :: (Ord a, Generic.Vector v a) => Cons v a -> a
minimum (Cons xs) =
  Generic.minimum xs
{-# INLINE minimum #-}

maximum :: (Ord a, Generic.Vector v a) => Cons v a -> a
maximum (Cons xs) =
  Generic.maximum xs
{-# INLINE maximum #-}

transpose ::
  Generic.Vector va a =>
  Generic.Vector vv (va a) =>
  Generic.Vector vv (Cons va a) =>
  Cons vv (Cons va a) ->
  Cons vv (Cons va a)
transpose =
  map unsafeFromVector .
  unsafeFromVector .
  Generic.transpose .
  toVector .
  map toVector
{-# INLINE transpose #-}

transposeCV ::
  Generic.Vector va a =>
  Generic.Vector vv (va a) =>
  Generic.Vector vv (Cons va a) =>
  Cons vv (va a) ->
  vv (Cons va a)
transposeCV =
  Generic.map unsafeFromVector .
  Generic.transpose .
  toVector
{-# INLINE transposeCV #-}

transposeVC ::
  Generic.Vector va a =>
  Generic.Vector vv (va a) =>
  Generic.Vector vv (Cons va a) =>
  vv (Cons va a) ->
  Cons vv (va a)
transposeVC =
  unsafeFromVector .
  Generic.transpose .
  Generic.map toVector
{-# INLINE transposeVC #-}

toVector :: Cons v a -> v a
toVector (Cons xs) =
  xs
{-# INLINE toVector #-}

toList :: Generic.Vector v a => Cons v a -> [a]
toList (Cons xs) =
  Generic.toList xs
{-# INLINE toList #-}

toNonEmpty :: Generic.Vector v a => Cons v a -> NonEmpty a
toNonEmpty (Cons xs) =
  NonEmpty.fromList $ Generic.toList xs
{-# INLINE toNonEmpty #-}

from :: Generic.Vector v a => a -> v a -> Cons v a
from x xs =
  Cons $ Generic.cons x xs
{-# INLINE from #-}

from1 :: Generic.Vector v a => a -> Cons v a
from1 x0 =
  Cons $ Generic.singleton x0
{-# INLINE from1 #-}

from2 :: Generic.Vector v a => a -> a -> Cons v a
from2 x0 x1 =
  Cons $ Generic.fromList [x0, x1]
{-# INLINE from2 #-}

from3 :: Generic.Vector v a => a -> a -> a -> Cons v a
from3 x0 x1 x2 =
  Cons $ Generic.fromList [x0, x1, x2]
{-# INLINE from3 #-}

fromVector :: Generic.Vector v a => v a -> Maybe (Cons v a)
fromVector xs =
  if Generic.null xs then
    Nothing
  else
    Just (Cons xs)
{-# INLINE fromVector #-}

fromList :: Generic.Vector v a => [a] -> Maybe (Cons v a)
fromList = \case
  [] ->
    Nothing
  xs ->
    Just (Cons (Generic.fromList xs))
{-# INLINE fromList #-}

fromNonEmpty :: Generic.Vector v a => NonEmpty a -> Cons v a
fromNonEmpty =
  Cons . Generic.fromList . NonEmpty.toList
{-# INLINE fromNonEmpty #-}

#if MIN_VERSION_base(4,9,0)
unsafeFromVector :: (HasCallStack, Generic.Vector v a) => v a -> Cons v a
#else
unsafeFromVector :: Generic.Vector v a => v a -> Cons v a
#endif
unsafeFromVector =
  let
    msg =
      "Zebra.Data.Vector.Cons.unsafeFromVector: empty vector"
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
      "Zebra.Data.Vector.Cons.unsafeFromList: empty list"
  in
    fromMaybe (Savage.error msg) . fromList
{-# INLINE unsafeFromList #-}
