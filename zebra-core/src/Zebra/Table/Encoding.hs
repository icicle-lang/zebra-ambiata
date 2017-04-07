{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-} -- EncodeError
module Zebra.Table.Encoding (
    Binary(..)

  , validateBinary
  , validateUtf8
  , decodeUtf8

  , Utf8Error(..)
  , renderUtf8Error
  ) where

import           Data.ByteString (ByteString)
import           Data.String (String)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import           Data.Text.Encoding.Error (UnicodeException(..))
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import           Text.Printf (printf)


data Binary =
    Utf8
    deriving (Eq, Ord, Show, Generic)

data Utf8Error =
    Utf8Error !String !(Maybe Word8)
    deriving (Eq, Ord, Show, Generic)

renderUtf8Error :: Utf8Error -> Text
renderUtf8Error = \case
  Utf8Error msg Nothing ->
    "Not valid UTF-8: " <> Text.pack msg
  Utf8Error msg (Just byte) ->
    Text.pack $
      printf "Not valid UTF-8, cannot decode byte 0x%02x: %s" byte msg

validateBinary :: Maybe Binary -> ByteString -> Either Utf8Error ()
validateBinary = \case
  Nothing ->
    const $ pure ()
  Just Utf8 ->
    validateUtf8

-- FIXME replace with something that doesn't allocate
validateUtf8 :: ByteString -> Either Utf8Error ()
validateUtf8 bs =
  () <$ decodeUtf8 bs

decodeUtf8 :: ByteString -> Either Utf8Error Text
decodeUtf8 bs =
  case Text.decodeUtf8' bs of
    Left (DecodeError msg byte) ->
      Left (Utf8Error msg byte)

    Left (EncodeError msg _) ->
      Left (Utf8Error msg Nothing) -- good.

    Right txt ->
      pure txt
