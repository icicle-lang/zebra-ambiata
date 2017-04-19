{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Stream.Get (
    runGet
  , runGetAll

  , GetError(..)
  , renderGetError
  ) where

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import qualified Data.ByteString as Strict
import qualified Data.Text as Text

import           GHC.Types (SPEC(..))

import           P

import           X.Control.Monad.Trans.Either (EitherT, left)

import           Zebra.ByteStream (ByteStream)
import qualified Zebra.ByteStream as ByteStream
import           Zebra.Stream (Stream, Of)
import qualified Zebra.Stream as Stream


data GetError =
    GetError !Text
    deriving (Eq, Show)

renderGetError :: GetError -> Text
renderGetError = \case
  GetError msg ->
    msg

-- | Run a 'Get' binary decoder over a 'ByteStream'.
--
--   Return decoded value, as well as the rest of the stream.
--
runGet :: Monad m => Get a -> ByteStream m r -> EitherT GetError m (a, ByteStream m r)
runGet get input =
  let
    loop !_ bss0 = \case
      Get.Fail _bs _off err ->
        left . GetError $ Text.pack err

      Get.Done bs _off x ->
        pure (x, ByteStream.consChunk bs bss0)

      Get.Partial k -> do
        e <- lift $ ByteStream.nextChunk bss0
        case e of
          Left r ->
            loop SPEC (pure r) (k Nothing)

          Right (bs, bss) ->
            loop SPEC bss (k (Just bs))
  in
    loop SPEC input (Get.runGetIncremental get)

-- | Keep running a 'Get' binary decoder over a stream of strict 'ByteString'.
--
runGetAll :: Monad m => Get a -> ByteStream m r -> Stream (Of a) (EitherT GetError m) r
runGetAll get input =
  let
    nextGet !_ bss0 = do
      e <- lift $ ByteStream.nextChunk bss0
      case e of
        Left r ->
          pure r
        Right (bs, bss) ->
          loop SPEC bss (Get.runGetIncremental get `Get.pushChunk` bs)

    loop !_ bss0 = \case
      Get.Fail _bs _off err ->
        lift . left . GetError $ Text.pack err

      Get.Done bs _off x -> do
        Stream.yield x
        if Strict.null bs then
          nextGet SPEC bss0
        else
          loop SPEC bss0 (Get.runGetIncremental get `Get.pushChunk` bs)

      Get.Partial k -> do
        e <- lift $ ByteStream.nextChunk bss0
        case e of
          Left r ->
            loop SPEC (pure r) (k Nothing)

          Right (bs, bss) ->
            loop SPEC bss (k (Just bs))
  in
    loop SPEC (hoist lift input) (Get.runGetIncremental get)
