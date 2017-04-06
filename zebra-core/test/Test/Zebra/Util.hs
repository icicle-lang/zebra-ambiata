{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Test.Zebra.Util (
    liftE
  , trippingIO
  , trippingByIO
  , trippingSerial
  , trippingSerialE
  , runGetEither
  , runGetEitherConsumeAll
  ) where

import           Control.Monad.Trans.Class (lift)

import           Data.Binary.Get (Get, ByteOffset)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import           Data.String (String)
import           Data.Void (Void)

import           Disorder.Jack (Property, property, counterexample)
import           Disorder.Jack.Property.Diff (renderDiffs)

import           Text.Show.Pretty (ppShow)
import qualified Text.Show.Pretty as Pretty

import           P

import           System.IO (IO)

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


data TrippingError x y =
    EncodeError x
  | DecodeError y
    deriving (Eq, Show)

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
  pure $ diff (pure $ select a) (fmap select roundtrip)


trippingSerial :: forall a. (Eq a, Show a) => (a -> Builder) -> Get a -> a -> Property
trippingSerial build0 get a =
  let
    build :: a -> Either () Builder
    build =
      pure . build0
  in
    trippingSerialE build get a

trippingSerialE :: (Eq a, Show a, Show x) => (a -> Either x Builder) -> Get a -> a -> Property
trippingSerialE build get a =
  let
    roundtrip = do
      b <- bimap ppShow Builder.toLazyByteString $ build a
      first ppShow $ runGetEither get b
  in
    diff (pure a) roundtrip

diff :: (Eq x, Eq a, Show x, Show a) => Either x a -> Either x a -> Property
diff original roundtrip =
  let
    comparison =
      "=== Original ===" <>
      "\n" <> ppShow original <>
      "\n" <>
      "\n=== Roundtrip ===" <>
      "\n" <> ppShow roundtrip

    pdiff = do
      o <- Pretty.reify original
      r <- Pretty.reify roundtrip
      pure $
        "=== - Original / + Roundtrip ===" <>
        "\n" <> renderDiffs o r
  in
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample (fromMaybe comparison pdiff) $
      property (roundtrip == original)

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
