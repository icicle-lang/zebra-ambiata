{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Fact (
    Fact(..)
  , Value(..)
  , renderFact

  , FactRenderError(..)
  , renderFactRenderError
  ) where

import           Data.Aeson ((.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Thyme.Format (formatTime)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import           System.Locale (defaultTimeLocale)

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Core
import           Zebra.Data.Schema (Schema)
import qualified Zebra.Data.Schema as Schema


data Fact =
  Fact {
      factEntityHash :: !EntityHash
    , factEntityId :: !EntityId
    , factAttributeId :: !AttributeId
    , factTime :: !Time
    , factFactsetId :: !FactsetId
    , factValue :: !(Maybe' Value)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data Value =
    Byte !Word8
  | Int !Int64
  | Double !Double
  | Enum !Int !Value
  | Struct !(Boxed.Vector Value)
  | Array !(Boxed.Vector Value)
  | ByteArray !ByteString -- ^ Optimisation for Array [Byte, Byte, ..]
    deriving (Eq, Ord, Show, Generic, Typeable)

data FactRenderError =
    FactValueSchemaMismatch !Value !Schema
  | FactSchemaNotFoundForAttribute !AttributeId
    deriving (Eq, Ord, Show, Generic, Typeable)

renderFactRenderError :: FactRenderError -> Text
renderFactRenderError = \case
  FactValueSchemaMismatch value schema ->
    "Could not render fact, value did not match schema:" <>
    "\n" <>
    "\n  value =" <>
    ppPrefix "\n    " value <>
    "\n" <>
    "\n  schema =" <>
    ppPrefix "\n    " schema
  FactSchemaNotFoundForAttribute (AttributeId aid) ->
    "Could not render fact, no schema found for attribute-id: " <> T.pack (show aid)

ppPrefix :: Show a => Text -> a -> Text
ppPrefix prefix =
  T.concat . fmap (prefix <>) . T.lines . T.pack . ppShow

renderFact :: Boxed.Vector Schema -> Fact -> Either FactRenderError ByteString
renderFact schemas fact = do
  let
    aid =
      factAttributeId fact
    ix =
      fromIntegral $ unAttributeId aid

  schema <- maybeToRight (FactSchemaNotFoundForAttribute aid) (schemas Boxed.!? ix)
  rvalue <- renderMaybeValue schema $ factValue fact

  pure $ Char8.intercalate "|" [
      renderEntityHash $ factEntityHash fact
    , unEntityId $ factEntityId fact
    , renderAttributeId $ factAttributeId fact
    , renderTime $ factTime fact
    , renderFactsetId $ factFactsetId fact
    , rvalue
    ]

renderEntityHash :: EntityHash -> ByteString
renderEntityHash (EntityHash hash) =
  Char8.pack $ printf "0x%08X" hash

renderAttributeId :: AttributeId -> ByteString
renderAttributeId (AttributeId aid) =
  Char8.pack $ printf "attribute=%05d" aid

renderTime :: Time -> ByteString
renderTime =
  Char8.pack . formatTime defaultTimeLocale "%0Y-%m-%d %H:%M:%S" . toUTCTime

renderFactsetId :: FactsetId -> ByteString
renderFactsetId (FactsetId factsetId) =
  Char8.pack $ printf "factset=%08x" factsetId

renderMaybeValue :: Schema -> Maybe' Value -> Either FactRenderError ByteString
renderMaybeValue schema =
  maybe' (pure "NA") (renderValue schema)

renderValue :: Schema -> Value -> Either FactRenderError ByteString
renderValue schema value =
  fmap (Lazy.toStrict . Aeson.encodePretty' aesonConfig) $ toAeson schema value

-- We use aeson-pretty so that the struct field names are sorted alphabetically
-- rather than by hash order.
aesonConfig :: Aeson.Config
aesonConfig =
  Aeson.defConfig {
      Aeson.confIndent =
        Aeson.Spaces 0
    , Aeson.confCompare =
        compare
    }

toAeson :: Schema -> Value -> Either FactRenderError Aeson.Value
toAeson schema value0 =
  case schema of
    Schema.Byte
      | Byte x <- value0
      ->
        pure . Aeson.Number $ fromIntegral x

    Schema.Int
      | Int x <- value0
      ->
        pure . Aeson.Number $ fromIntegral x

    Schema.Double
      | Double x <- value0
      ->
        --
        -- This maps NaN/Inf -> 'null', and uses 'fromFloatDigits' to convert
        -- Double -> Scientific.
        --
        -- Don't use 'realToFrac' here, it converts the Double to a Scientific
        -- with far more decimal places than a 64-bit floating point number can
        -- possibly represent.
        --
        pure $ Aeson.toJSON x

    Schema.Enum variant0 variants
      | Enum tag value <- value0
      , Just variant <- Schema.lookupVariant tag variant0 variants
      -> do
        payload <- toAeson (Schema.variantSchema variant) value
        pure $ Aeson.object [
            Schema.unVariantName (Schema.variantName variant) .= payload
          ]

    Schema.Struct fields
      | Struct values <- value0
      , Boxed.length fields == Boxed.length values
      -> do
        let
          fnames =
            fmap (Schema.unFieldName . Schema.fieldName) fields

        fvalues <- Boxed.zipWithM toAeson (fmap Schema.fieldSchema fields) values

        pure . Aeson.object . Boxed.toList $ Boxed.zipWith (.=) fnames fvalues

    Schema.Array Schema.Byte
      | ByteArray bs <- value0
      ->
        -- FIXME we need some metadata in the schema to say this is ok
        pure . Aeson.String $ T.decodeUtf8 bs

    Schema.Array element
      | Array values <- value0
      ->
        fmap Aeson.Array $
          traverse (toAeson element) values

    _ ->
      Left $ FactValueSchemaMismatch value0 schema
