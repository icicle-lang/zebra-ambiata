{-# LANGUAGE NoImplicitPrelude #-}
module Neutron.Vector.Boxed (
    Vector

  , concat
  ) where

import           Data.Vector (Vector)

import qualified Neutron.Vector.Generic as Generic


concat :: Vector (Vector a) -> Vector a
concat =
  Generic.concat
{-# INLINE concat #-}
