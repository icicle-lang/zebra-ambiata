{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Schema (
    Schema(..)
  , Field(..)
  , FieldName(..)
  , Variant(..)
  , VariantName(..)
  , lookupVariant
  , focusVariant

  , unit
  , bool
  , false
  , true
  , option
  , none
  , some

  , SchemaDecodeError(..)
  , renderSchemaDecodeError

  , encode
  , decode
  ) where

import           Data.Aeson ((.=), (.:))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson
import           Data.Aeson.Internal ((<?>))
import qualified Data.Aeson.Internal as Aeson
import qualified Data.Aeson.Parser as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.HashMap.Lazy as HashMap
import qualified Data.List as List
import qualified Data.Text as T
import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           P hiding (bool, some)

import qualified X.Data.Vector as Boxed
import           X.Text.Show (gshowsPrec)


newtype FieldName =
  FieldName {
      unFieldName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show FieldName where
  showsPrec =
    gshowsPrec

data Field =
  Field {
      fieldName :: !FieldName
    , fieldSchema :: !Schema
    } deriving (Eq, Ord, Generic, Typeable)

instance Show Field where
  showsPrec =
    gshowsPrec

newtype VariantName =
  VariantName {
      unVariantName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show VariantName where
  showsPrec =
    gshowsPrec

data Variant =
  Variant {
      variantName :: !VariantName
    , variantSchema :: !Schema
    } deriving (Eq, Ord, Generic, Typeable)

instance Show Variant where
  showsPrec =
    gshowsPrec

data Schema =
    Byte
  | Int
  | Double
  | Enum !Variant !(Boxed.Vector Variant)
  | Struct !(Boxed.Vector Field)
  | Array !Schema
    deriving (Eq, Ord, Show, Generic, Typeable)

data SchemaDecodeError =
    SchemaDecodeError !Aeson.JSONPath !Text
    deriving (Eq, Show)

renderSchemaDecodeError :: SchemaDecodeError -> Text
renderSchemaDecodeError = \case
  SchemaDecodeError path msg ->
    T.pack . Aeson.formatError path $ T.unpack msg

lookupVariant :: Int -> Variant -> Boxed.Vector Variant -> Maybe Variant
lookupVariant ix variant0 variants =
  case ix of
    0 ->
      Just variant0
    _ ->
      variants Boxed.!? (ix - 1)

focusVariant :: Int -> Variant -> Boxed.Vector Variant -> Maybe (Boxed.Vector Variant, Variant, Boxed.Vector Variant)
focusVariant i x0 xs =
  case i of
    0 ->
      Just (Boxed.empty, x0, xs)
    _ -> do
      let !j = i - 1
      x <- xs Boxed.!? j
      pure (Boxed.cons x0 $ Boxed.take j xs, x, Boxed.drop (j + 1) xs)

------------------------------------------------------------------------

unit :: Schema
unit =
  Struct Boxed.empty

false :: Variant
false =
  Variant (VariantName "false") unit

true :: Variant
true =
  Variant (VariantName "true") unit

bool :: Schema
bool =
  Enum false $ Boxed.singleton true

none :: Variant
none =
  Variant (VariantName "none") unit

some :: Schema -> Variant
some =
  Variant (VariantName "some")

option :: Schema -> Schema
option =
  Enum none . Boxed.singleton . some

------------------------------------------------------------------------

encode :: Schema -> ByteString
encode =
  Lazy.toStrict . Aeson.encodePretty' aesonConfig . ppSchema

decode :: ByteString -> Either SchemaDecodeError Schema
decode =
  first (uncurry SchemaDecodeError . second T.pack) .
    Aeson.eitherDecodeStrictWith Aeson.value' (Aeson.iparse pSchema)

aesonConfig :: Aeson.Config
aesonConfig =
  Aeson.Config {
      Aeson.confIndent =
        Aeson.Spaces 2
    , Aeson.confCompare =
        Aeson.keyOrder ["name", "schema"]
    , Aeson.confNumFormat =
        Aeson.Generic
    }

pSchema :: Aeson.Value -> Aeson.Parser Schema
pSchema =
  pEnum $ \tag ->
    case tag of
      "byte" ->
        const $ pure Byte
      "int" ->
        const $ pure Int
      "double" ->
        const $ pure Double
      "enum" ->
        Aeson.withObject "object containing enum schema" $ \o ->
          uncurry Enum <$> withKey "variants" o pSchemaEnumVariants
      "struct" ->
        Aeson.withObject "object contain struct schema" $ \o ->
          Struct <$> withKey "fields" o pSchemaStructFields
      "array" ->
        Aeson.withObject "object containing array schema" $ \o ->
          Array <$> withKey "element" o pSchema
      _ ->
        const . fail $ "unknown schema type: " <> T.unpack tag

pSchemaEnumVariants :: Aeson.Value -> Aeson.Parser (Variant, Boxed.Vector Variant)
pSchemaEnumVariants =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariant xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just vs ->
        pure vs

pSchemaVariant :: Aeson.Value -> Aeson.Parser Variant
pSchemaVariant =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withKey "name" o (pure . VariantName)
      <*> withKey "schema" o pSchema

pSchemaStructFields :: Aeson.Value -> Aeson.Parser (Boxed.Vector Field)
pSchemaStructFields =
  Aeson.withArray "array of struct fields" $ \xs -> do
    traverse pSchemaField xs

pSchemaField :: Aeson.Value -> Aeson.Parser Field
pSchemaField =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withKey "name" o (pure . FieldName)
      <*> withKey "schema" o pSchema

pEnum :: (Text -> Aeson.Value -> Aeson.Parser a) -> Aeson.Value -> Aeson.Parser a
pEnum f =
  Aeson.withObject "object containing an enum (i.e. a single member)" $ \o ->
    case HashMap.toList o of
      [(tag, value)] ->
        f tag value
      [] ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with no members"
      kvs ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with more than one member:" <>
          "\n  " <> List.intercalate ", " (fmap (T.unpack . fst) kvs)

withKey :: Aeson.FromJSON a => Text -> Aeson.Object ->  (a -> Aeson.Parser b) -> Aeson.Parser b
withKey key o p = do
  x <- o .: key
  p x <?> Aeson.Key key

kmapM :: (Aeson.Value -> Aeson.Parser a) -> Boxed.Vector Aeson.Value -> Aeson.Parser (Boxed.Vector a)
kmapM f =
  Boxed.imapM $ \i x ->
    f x <?> Aeson.Index i

ppSchema :: Schema -> Aeson.Value
ppSchema = \case
  Byte ->
    ppEnum "byte" ppUnit
  Int ->
    ppEnum "int" ppUnit
  Double ->
    ppEnum "double" ppUnit
  Enum v0 vs ->
    ppEnum "enum" $
      Aeson.object ["variants" .= Aeson.Array (fmap ppSchemaVariant $ Boxed.cons v0 vs)]
  Struct fs ->
    ppEnum "struct" $
      Aeson.object ["fields" .= Aeson.Array (fmap ppSchemaField fs)]
  Array e ->
    ppEnum "array" $
      Aeson.object ["element" .= ppSchema e]

ppEnum :: Text -> Aeson.Value -> Aeson.Value
ppEnum tag value =
  Aeson.object [
      tag .= value
    ]

ppUnit :: Aeson.Value
ppUnit =
  Aeson.Object HashMap.empty

ppSchemaVariant :: Variant -> Aeson.Value
ppSchemaVariant (Variant (VariantName name) schema) =
  Aeson.object [
      "name" .=
        name
    , "schema" .=
        ppSchema schema
    ]

ppSchemaField :: Field -> Aeson.Value
ppSchemaField (Field (FieldName name) schema) =
  Aeson.object [
      "name" .=
        name
    , "schema" .=
        ppSchema schema
    ]
