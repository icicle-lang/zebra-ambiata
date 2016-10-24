{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Util (
    liftE
  , trippingIO
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

import           P

import           System.IO (IO)

import           Text.Show.Pretty (ppShow)

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
trippingIO to from a = do
  roundtrip <-
    runEitherT $ do
      b <- firstT EncodeError $ to a
      firstT DecodeError $ from b

  let
    original =
      pure a

  pure .
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample "=== Original ===" .
    counterexample (ppShow original) .
    counterexample "" .
    counterexample "=== Roundtrip ===" .
    counterexample (ppShow roundtrip) $
      property (roundtrip == original)

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
