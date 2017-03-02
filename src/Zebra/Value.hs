{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Value (
    Value(..)
  , render

  , ValueRenderError(..)
  , renderValueRenderError

  , unit
  , false
  , true
  , none
  , some
  ) where

import           Data.Aeson ((.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P hiding (some)

import           Text.Show.Pretty (ppShow)

import           Zebra.Schema (Schema)
import qualified Zebra.Schema as Schema


data Value =
    Byte !Word8
  | Int !Int64
  | Double !Double
  | Enum !Int !Value
  | Struct !(Boxed.Vector Value)
  | Array !(Boxed.Vector Value)
  | ByteArray !ByteString -- ^ Optimisation for Array [Byte, Byte, ..]
    deriving (Eq, Ord, Show, Generic, Typeable)

data ValueRenderError =
    ValueSchemaMismatch !Value !Schema
    deriving (Eq, Ord, Show, Generic, Typeable)

renderValueRenderError :: ValueRenderError -> Text
renderValueRenderError = \case
  ValueSchemaMismatch value schema ->
    "Could not render value, schema did not match:" <>
    "\n" <>
    "\n  value =" <>
    ppPrefix "\n    " value <>
    "\n" <>
    "\n  schema =" <>
    ppPrefix "\n    " schema

ppPrefix :: Show a => Text -> a -> Text
ppPrefix prefix =
  T.concat . fmap (prefix <>) . T.lines . T.pack . ppShow

------------------------------------------------------------------------

unit :: Value
unit =
  Struct Boxed.empty

false :: Value
false =
  Enum 0 unit

true :: Value
true =
  Enum 1 unit

none :: Value
none =
  Enum 0 unit

some :: Value -> Value
some =
  Enum 1

------------------------------------------------------------------------

render :: Schema -> Value -> Either ValueRenderError ByteString
render schema value =
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

toAeson :: Schema -> Value -> Either ValueRenderError Aeson.Value
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
      Left $ ValueSchemaMismatch value0 schema
