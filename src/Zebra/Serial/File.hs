{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.File (
    DecodeError(..)
  , renderDecodeError
  , getBlocks
  , getFile
  , runStreamOne
  , runStreamMany
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Get
import qualified Data.ByteString as B
import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Text as Text

import           Data.String (String)

import           P

import           Zebra.Data
import           Zebra.Serial.Header
import           Zebra.Serial.Block

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import           X.Control.Monad.Trans.Either
import           Control.Monad.Trans.Class

data DecodeError
 = DecodeError String
 | DecodeErrorBadParserFailImmediately String
 | DecodeErrorBadParserExpectsMoreAfterEnd
 deriving (Show)

renderDecodeError :: DecodeError -> Text
renderDecodeError = \case
  DecodeError s ->
    Text.pack s
  DecodeErrorBadParserFailImmediately s ->
    "Decode error: the parser failed immediately before consuming anything. This means there is a bug in the parser.\n" <>
    "The parser failed with: " <> Text.pack s
  DecodeErrorBadParserExpectsMoreAfterEnd ->
    "Decode error: the parser asked for more input after telling it the stream has ended. This means there is a bug in the parser."

getFile :: Monad m => Stream.Stream m B.ByteString -> EitherT DecodeError m (Map AttributeName Schema, Stream.Stream (EitherT DecodeError m) Block)
getFile input = EitherT $ do
  (header, rest) <- runStreamOne getHeader input
  case header of
   Left err -> return $ Left err
   Right header' ->
    let schema = Boxed.fromList $ Map.elems header'
        blocks = runStreamMany (getBlock schema) rest
    in  return $ Right (header', blocks)

getBlocks :: Monad m => Boxed.Vector Schema -> Stream.Stream m B.ByteString -> Stream.Stream (EitherT DecodeError m) Block
getBlocks schemas inp = runStreamMany (getBlock schemas) inp


-- | Run a 'Get' binary decoder over a stream of strict bytestrings.
-- Return the value we got, as well as the rest of the stream
runStreamOne :: Monad m => Get a -> Stream.Stream m B.ByteString -> m (Either DecodeError a, Stream.Stream m B.ByteString)
runStreamOne get (Stream.Stream s'go s'init) = do
  (v,s') <- runOne Stream.SPEC s'init $ Get.runGetIncremental get
  return (v, Stream.Stream runRest s')
  where
    runOne !_ s (Get.Fail leftovers _ err)
     = return (Left $ DecodeError err, (leftovers, s))
    runOne !_ s (Get.Done leftovers _ a)
     = return (Right a, (leftovers, s))
    runOne !_ s (Get.Partial p)
     = s'go s >>= \case
        Stream.Yield str' s'
         -> runOne Stream.SPEC s' (p $ Just str')
        Stream.Skip s'
         -> runOne Stream.SPEC s' (Get.Partial p)
        Stream.Done
         -> runOne Stream.SPEC s (p Nothing)

    runRest (leftovers, s)
     | B.null leftovers
     = s'go s >>= \case
        Stream.Yield str' s'
         -> return $ Stream.Yield str' (leftovers, s')
        Stream.Skip  s'
         -> return $ Stream.Skip (leftovers, s')
        Stream.Done
         -> return $ Stream.Done
     | otherwise
     = return $ Stream.Yield leftovers ("", s)


-- | Keep running a 'Get' binary decoder over a stream of strict bytestrings.
runStreamMany :: Monad m => Get a -> Stream.Stream m B.ByteString -> Stream.Stream (EitherT DecodeError m) a
runStreamMany g (Stream.Stream s'go s'init) =
  Stream.Stream go (s'init, "", Nothing)
  where
    g'init = Get.runGetIncremental g

    go (s, str, decoder)
     | B.null str
     = lift (s'go s) >>= \case
        Stream.Yield str' s'
         -> return $ Stream.Skip (s', str', decoder)
        Stream.Skip  s'
         -> return $ Stream.Skip (s', "", decoder)
        Stream.Done
         -> case decoder of
             Nothing ->
              return $ Stream.Done
             Just partial ->
              case partial Nothing of
                Get.Fail _ _ err -> left $ DecodeError err
                Get.Partial _ -> left $ DecodeErrorBadParserExpectsMoreAfterEnd
                Get.Done _ _ a -> return $ Stream.Yield a (s, "", Nothing)
    go (s, str, Nothing)
     = case g'init of
        Get.Partial p -> return $ Stream.Skip (s, str, Just p)
        -- If the parser fails immediately, we want to throw away the input since otherwise we would have an infinite stream of errors
        Get.Fail _ _ err -> left $ DecodeErrorBadParserFailImmediately err
        -- If the parser returns immediately, we will have an infinite stream of values. Strange. I expect this shouldn't happen.
        Get.Done _ _ a -> return $ Stream.Yield a (s, str, Nothing)
    go (s, str, Just decoder)
     = case decoder (Just str) of
        Get.Fail _ _ err
         -> left $ DecodeError err
        Get.Partial decoder'
         -> return $ Stream.Skip (s, "", Just decoder')
        Get.Done str' _ ret
         -> return $ Stream.Yield ret (s, str', Nothing)

