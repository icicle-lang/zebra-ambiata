{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Json.Util (
    encodeJson
  , encodeJsonIndented
  , decodeJson

  , JsonDecodeError(..)
  , renderJsonDecodeError

  -- * Parsing
  , pText
  , pBinary
  , pUnit
  , pInt
  , pDouble
  , pEnum
  , withStructField
  , kmapM

  -- * Pretty Printing
  , ppText
  , ppBinary
  , ppUnit
  , ppInt
  , ppDouble
  , ppEnum
  , ppStruct
  , ppStructField
  ) where

import           Data.Aeson ((.=), (.:))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson
import           Data.Aeson.Internal ((<?>))
import qualified Data.Aeson.Internal as Aeson
import qualified Data.Aeson.Parser as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.HashMap.Strict as HashMap
import qualified Data.List as List
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed

import           P

import           Zebra.Table.Data


data JsonDecodeError =
    JsonDecodeError !Aeson.JSONPath !Text
    deriving (Eq, Show)

renderJsonDecodeError :: JsonDecodeError -> Text
renderJsonDecodeError = \case
  JsonDecodeError path msg ->
    Text.pack . Aeson.formatError path $ Text.unpack msg

encodeJson :: [Text] -> Aeson.Value -> ByteString
encodeJson keyOrder =
  Lazy.toStrict . Aeson.encodePretty' (standardConfig keyOrder)

encodeJsonIndented :: [Text] -> Aeson.Value -> ByteString
encodeJsonIndented keyOrder =
  Lazy.toStrict . Aeson.encodePretty' (indentConfig keyOrder)

decodeJson :: (Aeson.Value -> Aeson.Parser a) -> ByteString -> Either JsonDecodeError a
decodeJson p =
  first (uncurry JsonDecodeError . second Text.pack) .
    Aeson.eitherDecodeStrictWith Aeson.value' (Aeson.iparse p)

standardConfig :: [Text] -> Aeson.Config
standardConfig keyOrder =
  Aeson.Config {
      Aeson.confIndent =
        Aeson.Spaces 0
    , Aeson.confCompare =
        Aeson.keyOrder keyOrder
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

pText :: Aeson.Value -> Aeson.Parser Text
pText =
  Aeson.parseJSON

ppText :: Text -> Aeson.Value
ppText =
  Aeson.toJSON

pBinary :: Aeson.Value -> Aeson.Parser ByteString
pBinary = \case
  Aeson.String x ->
    pure $ Text.encodeUtf8 x

  Aeson.Object o ->
    withStructField "base64" o . Aeson.withText "base64 encoded binary data" $ \txt ->
      case Base64.decode $ Text.encodeUtf8 txt of
        Left err ->
          fail $ "could not decode base64 encoded binary data: " <> err
        Right bs ->
          pure bs

  v ->
    Aeson.typeMismatch "utf8 text or base64 encoded binary" v

-- | Attempt to store a 'ByteString' directly as a JSON string if it happens to
--   be valid UTF-8, otherwise Base64 encode it.
--
ppBinary :: ByteString -> Aeson.Value
ppBinary bs =
  case Text.decodeUtf8' bs of
    Left _ ->
      Aeson.object [
          "base64" .= Text.decodeUtf8 (Base64.encode bs)
        ]
    Right x ->
     ppText x

pUnit :: Aeson.Value -> Aeson.Parser ()
pUnit =
  Aeson.withObject "object containing unit (i.e. {})" $ \o ->
    if HashMap.null o then
      pure ()
    else
      fail $
        "expected an object containing unit (i.e. {})," <>
        "\nbut found an object with one or more members"

ppUnit :: Aeson.Value
ppUnit =
  Aeson.Object HashMap.empty

pInt :: Aeson.Value -> Aeson.Parser Int64
pInt =
  Aeson.parseJSON

ppInt :: Int64 -> Aeson.Value
ppInt =
  Aeson.toJSON

pDouble :: Aeson.Value -> Aeson.Parser Double
pDouble =
  Aeson.parseJSON

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

ppEnum :: Variant Aeson.Value -> Aeson.Value
ppEnum (Variant (VariantName name) value) =
  Aeson.object [
      name .= value
    ]

withStructField :: FieldName -> Aeson.Object -> (Aeson.Value -> Aeson.Parser a) -> Aeson.Parser a
withStructField name o p = do
  x <- o .: unFieldName name
  p x <?> Aeson.Key (unFieldName name)

ppStruct :: [Field Aeson.Value] -> Aeson.Value
ppStruct =
  Aeson.object . fmap ppStructField

ppStructField :: Field Aeson.Value -> Aeson.Pair
ppStructField (Field (FieldName name) value) =
  name .= value

kmapM :: (Aeson.Value -> Aeson.Parser a) -> Boxed.Vector Aeson.Value -> Aeson.Parser (Boxed.Vector a)
kmapM f =
  Boxed.imapM $ \i x ->
    f x <?> Aeson.Index i
