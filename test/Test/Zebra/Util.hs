{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Util (
    liftE
  , trippingIO
  , trippingByIO
  , trippingSerial
  , runGetEither
  , runGetEitherConsumeAll
  ) where

import           Control.Monad.Trans.Class (lift)

import           Data.Binary.Get (Get, ByteOffset)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Build
import qualified Data.ByteString.Lazy as Lazy
import           Data.String (String)
import           Data.Void (Void)

import           Disorder.Jack (Property, tripping, property, counterexample)
import           Disorder.Jack.Property.Diff (renderDiffs)

import           Text.Show.Pretty (ppShow)
import qualified Text.Show.Pretty as Pretty

import           P

import           System.IO (IO)

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


data TrippingError x y =
    EncodeError x
  | DecodeError y
    deriving (Eq, Ord, Show)

liftE :: IO a -> EitherT Void IO a
liftE =
  lift

trippingIO ::
  Eq a =>
  Eq x =>
  Eq y =>
  Show a =>
  Show x =>
  Show y =>
  (a -> EitherT x IO b) ->
  (b -> EitherT y IO a) ->
  a ->
  IO Property
trippingIO =
  trippingByIO id

trippingByIO ::
  Eq c =>
  Eq x =>
  Eq y =>
  Show c =>
  Show x =>
  Show y =>
  (a -> c) ->
  (a -> EitherT x IO b) ->
  (b -> EitherT y IO a) ->
  a ->
  IO Property
trippingByIO select to from a = do
  roundtrip <-
    runEitherT $ do
      b <- firstT EncodeError $ to a
      firstT DecodeError $ from b

  let
    original =
      pure a

    comparison =
      "=== Original ===" <>
      "\n" <> ppShow (fmap select original) <>
      "\n" <>
      "\n=== Roundtrip ===" <>
      "\n" <> ppShow (fmap select roundtrip)

    diff = do
      o <- Pretty.reify (fmap select original)
      r <- Pretty.reify (fmap select roundtrip)
      pure $
        "=== - Original / + Roundtrip ===" <>
        "\n" <> renderDiffs o r

  pure .
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample (fromMaybe comparison diff) $
      property (fmap select roundtrip == fmap select original)

trippingSerial :: (Eq a, Show a) => (a -> Builder) -> Get a -> a -> Property
trippingSerial build get =
  tripping (Build.toLazyByteString . build) (runGetEither get)

runGetEither :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEither g =
  let
    third (_, _, x) = x
  in
    second third . Get.runGetOrFail g

runGetEitherConsumeAll :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEitherConsumeAll g bs =
  case Get.runGetOrFail g bs of
    Left err -> Left err
    Right (leftovers,off,v)
     | Lazy.null leftovers
     -> Right v
     | otherwise
     -> Left (leftovers, off, "Not all input consumed")
