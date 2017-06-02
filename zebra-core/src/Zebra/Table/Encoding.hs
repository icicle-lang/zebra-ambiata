{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-} -- EncodeError
module Zebra.Table.Encoding (
    Binary(..)
  , Int(..)

  , Utf8Error(..)
  , renderUtf8Error

  , validateBinary
  , validateUtf8
  , decodeUtf8

  , validateInt
  , decodeDate
  , encodeDate
  , decodeTimeSeconds
  , encodeTimeSeconds
  , decodeTimeMilliseconds
  , encodeTimeMilliseconds
  , decodeTimeMicroseconds
  , encodeTimeMicroseconds
  ) where

import           Data.ByteString (ByteString)
import           Data.String (String)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import           Data.Text.Encoding.Error (UnicodeException(..))
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P hiding (Int)

import           Text.Printf (printf)

import           Zebra.Time (Time, Date, TimeError)
import qualified Zebra.Time as Time


data Binary =
    Binary
  | Utf8
    deriving (Eq, Ord, Show, Generic)

--
-- Potentially we want to support epochs other than 1600-03-01, and possibly
-- even time zones, but we keep it super simple for now.
--

data Int =
    Int
  | Date -- ^ days since 1600-03-01
  | TimeSeconds -- ^ seconds since 1600-03-01
  | TimeMilliseconds -- ^ milliseconds since 1600-03-01
  | TimeMicroseconds -- ^ microseconds since 1600-03-01
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

validateBinary :: Binary -> ByteString -> Either Utf8Error ()
validateBinary = \case
  Binary ->
    const $ pure ()
  Utf8 ->
    validateUtf8
{-# INLINABLE validateBinary #-}

-- FIXME replace with something that doesn't allocate
validateUtf8 :: ByteString -> Either Utf8Error ()
validateUtf8 bs =
  () <$ decodeUtf8 bs
{-# INLINABLE validateUtf8 #-}

decodeUtf8 :: ByteString -> Either Utf8Error Text
decodeUtf8 bs =
  case Text.decodeUtf8' bs of
    Left (DecodeError msg byte) ->
      Left (Utf8Error msg byte)

    Left (EncodeError msg _) ->
      Left (Utf8Error msg Nothing) -- good.

    Right txt ->
      pure txt
{-# INLINABLE decodeUtf8 #-}

validateInt :: Int -> Int64 -> Either TimeError ()
validateInt = \case
  Int ->
    const $ pure ()
  Date ->
    fmap (const ()) . decodeDate
  TimeSeconds ->
    fmap (const ()) . decodeTimeSeconds
  TimeMilliseconds ->
    fmap (const ()) . decodeTimeMilliseconds
  TimeMicroseconds ->
    fmap (const ()) . decodeTimeMicroseconds
{-# INLINABLE validateInt #-}

decodeDate :: Int64 -> Either TimeError Date
decodeDate =
  Time.fromDays . Time.Days
{-# INLINE decodeDate #-}

encodeDate :: Date -> Int64
encodeDate =
  Time.unDays . Time.toDays
{-# INLINE encodeDate #-}

decodeTimeSeconds :: Int64 -> Either TimeError Time
decodeTimeSeconds =
  Time.fromSeconds . Time.Seconds
{-# INLINE decodeTimeSeconds #-}

encodeTimeSeconds :: Time -> Int64
encodeTimeSeconds =
  Time.unSeconds . Time.toSeconds
{-# INLINE encodeTimeSeconds #-}

decodeTimeMilliseconds :: Int64 -> Either TimeError Time
decodeTimeMilliseconds =
  Time.fromMilliseconds . Time.Milliseconds
{-# INLINE decodeTimeMilliseconds #-}

encodeTimeMilliseconds :: Time -> Int64
encodeTimeMilliseconds =
  Time.unMilliseconds . Time.toMilliseconds
{-# INLINE encodeTimeMilliseconds #-}

decodeTimeMicroseconds :: Int64 -> Either TimeError Time
decodeTimeMicroseconds =
  Time.fromMicroseconds . Time.Microseconds
{-# INLINE decodeTimeMicroseconds #-}

encodeTimeMicroseconds :: Time -> Int64
encodeTimeMicroseconds =
  Time.unMicroseconds . Time.toMicroseconds
{-# INLINE encodeTimeMicroseconds #-}
