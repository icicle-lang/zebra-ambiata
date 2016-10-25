{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Merge (
    mergeAttribute
  ) where

import           Anemone.Foreign.Mempool (Mempool)
import           Anemone.Foreign.Data (CError(..))

import           Control.Monad.IO.Class (MonadIO(..))

import           Foreign.Ptr (Ptr)
import qualified Foreign.Marshal as Marshal
import qualified Foreign.Storable as Storable

import           P

import           X.Control.Monad.Trans.Either (EitherT, left)

import           Zebra.Foreign.Bindings
-- import           Zebra.Foreign.Util

mergeAttribute :: MonadIO m => Mempool -> Ptr C'zebra_attribute -> Ptr C'zebra_attribute -> EitherT CError m (Ptr C'zebra_attribute)
mergeAttribute pool c_attr1 c_attr2 = do
  (c_error, c_attr') <- go
  if c_error == 0 then return c_attr' else left c_error
 where
  go = liftIO $ Marshal.alloca $ \out -> do
    c_error <- c'merge_attribute pool c_attr1 c_attr2 out
    c_attr' <- Storable.peek out
    return (c_error, c_attr')
  

