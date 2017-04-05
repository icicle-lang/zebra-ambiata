{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Merge.Block
  ( mergeFiles
  ) where

import Zebra.Factset.Block
import Zebra.Merge.Base
import Zebra.Merge.Entity

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import P

-- | Merge a whole bunch of files together.
-- All files must have the same header.
--
-- For fusion to work properly, the type parameter 'c' should not be a stream,
-- so that the f_map argument is the stream producer which can be inlined.
-- We need to be very careful that streams are not stored inside vectors,
-- because then they would be stored as a thunk and good codegen / fusion is impossible.
mergeFiles :: Monad m
  => (c -> Stream.Stream m Block)
  -> Boxed.Vector c
  -> Stream.Stream m (Either MergeError EntityMerged)
mergeFiles f_map fs
 = Stream.map entityMergedOfEntityValues
 $ treeFold mergeEntityValues Stream.empty getEntities
 $ Boxed.indexed fs
 where
  getEntities (ix,blocks)
   = Stream.concatMap (entityValuesOfBlock $ BlockDataId ix) (f_map blocks)

