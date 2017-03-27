{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Json.Codec (
    encodeJson
  , encodeJsonIndented
  , encodeJsonRows
  , decodeJson

  , JsonEncodeError(..)
  , renderJsonEncodeError

  , JsonDecodeError(..)
  , renderJsonDecodeError

  -- * Parsing
  , withKey
  , kmapM
  , pEnum

  -- * Pretty Printing
  , ppInt
  , ppDouble
  , ppEnum
  , ppStruct
  , ppStructField
  , ppUnit
  ) where

import           Data.Aeson ((.=), (.:))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson
import           Data.Aeson.Internal ((<?>))
import qualified Data.Aeson.Internal as Aeson
import qualified Data.Aeson.Parser as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.HashMap.Strict as HashMap
import qualified Data.List as List
import qualified Data.Text as Text
import qualified Data.Vector as Boxed

import           P

import           Zebra.Schema (Field(..), FieldName(..))
import           Zebra.Schema (Variant(..), VariantName(..))


data JsonEncodeError =
    JsonCannotConvertNonArrayToRows !Aeson.Value
    deriving (Eq, Show)

data JsonDecodeError =
    JsonDecodeError !Aeson.JSONPath !Text
    deriving (Eq, Show)

renderJsonEncodeError :: JsonEncodeError -> Text
renderJsonEncodeError = \case
  JsonCannotConvertNonArrayToRows value ->
    "Cannot encode non-array JSON value as rows: " <>
    renderSnippet 30 (show value)

renderJsonDecodeError :: JsonDecodeError -> Text
renderJsonDecodeError = \case
  JsonDecodeError path msg ->
    Text.pack . Aeson.formatError path $ Text.unpack msg

renderSnippet :: Int -> [Char] -> Text
renderSnippet n xs =
  let
    snippet =
      Text.pack (List.take (n + 1) xs)
  in
    if Text.length snippet == n + 1 then
      Text.take n snippet <> "..."
    else
      Text.take n snippet

encodeJson :: Aeson.Value -> ByteString
encodeJson =
  Lazy.toStrict . Aeson.encodePretty' standardConfig

encodeJsonIndented :: [Text] -> Aeson.Value -> ByteString
encodeJsonIndented keyOrder =
  Lazy.toStrict . Aeson.encodePretty' (indentConfig keyOrder)

encodeJsonRows :: Aeson.Value -> Either JsonEncodeError ByteString
encodeJsonRows = \case
  Aeson.Array xs ->
    pure . Char8.unlines . Boxed.toList $ fmap encodeJson xs
  value ->
    Left $ JsonCannotConvertNonArrayToRows value

decodeJson :: (Aeson.Value -> Aeson.Parser a) -> ByteString -> Either JsonDecodeError a
decodeJson p =
  first (uncurry JsonDecodeError . second Text.pack) .
    Aeson.eitherDecodeStrictWith Aeson.value' (Aeson.iparse p)

standardConfig :: Aeson.Config
standardConfig =
  Aeson.Config {
      Aeson.confIndent =
        Aeson.Spaces 0
    , Aeson.confCompare =
        Aeson.keyOrder []
    , Aeson.confNumFormat =
        Aeson.Generic
    }

indentConfig :: [Text] -> Aeson.Config
indentConfig keyOrder =
  Aeson.Config {
      Aeson.confIndent =
        Aeson.Spaces 2
    , Aeson.confCompare =
        Aeson.keyOrder keyOrder
    , Aeson.confNumFormat =
        Aeson.Generic
    }

withKey :: Aeson.FromJSON a => Text -> Aeson.Object ->  (a -> Aeson.Parser b) -> Aeson.Parser b
withKey key o p = do
  x <- o .: key
  p x <?> Aeson.Key key

kmapM :: (Aeson.Value -> Aeson.Parser a) -> Boxed.Vector Aeson.Value -> Aeson.Parser (Boxed.Vector a)
kmapM f =
  Boxed.imapM $ \i x ->
    f x <?> Aeson.Index i

pEnum :: (VariantName -> Maybe (Aeson.Value -> Aeson.Parser a)) -> Aeson.Value -> Aeson.Parser a
pEnum mkParser =
  Aeson.withObject "object containing an enum (i.e. a single member)" $ \o ->
    case HashMap.toList o of
      [(name, value)] -> do
        case mkParser (VariantName name) of
          Nothing ->
            fail ("unknown enum variant: " <> Text.unpack name) <?> Aeson.Key name
          Just parser ->
            parser value <?> Aeson.Key name
      [] ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with no members"
      kvs ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with more than one member:" <>
          "\n  " <> List.intercalate ", " (fmap (Text.unpack . fst) kvs)

ppInt :: Int64 -> Aeson.Value
ppInt =
  Aeson.Number . fromIntegral

ppDouble :: Double -> Aeson.Value
ppDouble =
  --
  -- This maps NaN/Inf -> 'null', and uses 'fromFloatDigits' to convert
  -- Double -> Scientific.
  --
  -- Don't use 'realToFrac' here, it converts the Double to a Scientific
  -- with far more decimal places than a 64-bit floating point number can
  -- possibly represent.
  --
  Aeson.toJSON

ppEnum :: Variant Aeson.Value -> Aeson.Value
ppEnum (Variant (VariantName name) value) =
  Aeson.object [
      name .= value
    ]

ppStruct :: [Field Aeson.Value] -> Aeson.Value
ppStruct =
  Aeson.object . fmap ppStructField

ppStructField :: Field Aeson.Value -> Aeson.Pair
ppStructField (Field (FieldName name) value) =
  name .= value

ppUnit :: Aeson.Value
ppUnit =
  Aeson.Object HashMap.empty
