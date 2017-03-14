{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Schema (
    TableSchema(..)
  , ColumnSchema(..)
  , Field(..)
  , FieldName(..)
  , Variant(..)
  , VariantName(..)
  , Tag(..)

  , hasVariant
  , forVariant
  , lookupVariant

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

  , foreignOfTags
  , tagsOfForeign
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
import           Data.String (IsString(..))
import qualified Data.Text as Text
import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           Foreign.Storable (Storable)

import           P hiding (bool, some)

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable
import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons


newtype FieldName =
  FieldName {
      unFieldName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show FieldName where
  showsPrec p =
    showsPrec p . unFieldName

instance IsString FieldName where
  fromString =
    FieldName . Text.pack

data Field a =
  Field {
      fieldName :: !FieldName
    , field :: !a
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

instance Show a => Show (Field a) where
  showsPrec =
    gshowsPrec

newtype VariantName =
  VariantName {
      unVariantName :: Text
    } deriving (Eq, Ord, Generic, Typeable)

instance Show VariantName where
  showsPrec p =
    showsPrec p . unVariantName

instance IsString VariantName where
  fromString =
    VariantName . Text.pack

data Variant a =
  Variant {
      variantName :: !VariantName
    , variant :: !a
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

instance Show a => Show (Variant a) where
  showsPrec =
    gshowsPrec

newtype Tag =
  Tag {
      unTag :: Int64
    } deriving (Eq, Ord, Generic, Typeable, Storable, Num, Enum, Real, Integral)

instance Show Tag where
  showsPrec =
    gshowsPrec

data TableSchema =
    Binary
  | Array !ColumnSchema
  | Map !ColumnSchema !ColumnSchema
    deriving (Eq, Ord, Show, Generic, Typeable)

data ColumnSchema =
    Unit
  | Int
  | Double
  | Enum !(Cons Boxed.Vector (Variant ColumnSchema))
  | Struct !(Cons Boxed.Vector (Field ColumnSchema))
  | Nested !TableSchema
  | Reversed !ColumnSchema
    deriving (Eq, Ord, Show, Generic, Typeable)

data SchemaDecodeError =
    SchemaDecodeError !Aeson.JSONPath !Text
    deriving (Eq, Show)

renderSchemaDecodeError :: SchemaDecodeError -> Text
renderSchemaDecodeError = \case
  SchemaDecodeError path msg ->
    Text.pack . Aeson.formatError path $ Text.unpack msg

------------------------------------------------------------------------

hasVariant :: Tag -> Cons Boxed.Vector (Variant a) -> Bool
hasVariant tag xs =
  fromIntegral tag < Cons.length xs
{-# INLINE hasVariant #-}

forVariant ::
  Monad m =>
  Cons Boxed.Vector (Variant a) ->
  (Tag -> VariantName -> a -> m b) ->
  m (Cons Boxed.Vector (Variant b))
forVariant xs f =
  Cons.iforM xs $ \i (Variant name x) ->
    Variant name <$> f (fromIntegral i) name x

lookupVariant :: Tag -> Cons Boxed.Vector (Variant a) -> Maybe (Variant a)
lookupVariant tag xs =
  Cons.index (fromIntegral tag) xs
{-# INLINE lookupVariant #-}

------------------------------------------------------------------------

false :: Variant ColumnSchema
false =
  Variant (VariantName "false") Unit

true :: Variant ColumnSchema
true =
  Variant (VariantName "true") Unit

bool :: ColumnSchema
bool =
  Enum $ Cons.from2 false true

none :: Variant ColumnSchema
none =
  Variant (VariantName "none") Unit

some :: ColumnSchema -> Variant ColumnSchema
some =
  Variant (VariantName "some")

option :: ColumnSchema -> ColumnSchema
option =
  Enum . Cons.from2 none . some

------------------------------------------------------------------------

encode :: TableSchema -> ByteString
encode =
  Lazy.toStrict . Aeson.encodePretty' aesonConfig . ppTableSchema

decode :: ByteString -> Either SchemaDecodeError TableSchema
decode =
  first (uncurry SchemaDecodeError . second Text.pack) .
    Aeson.eitherDecodeStrictWith Aeson.value' (Aeson.iparse pTableSchema)

aesonConfig :: Aeson.Config
aesonConfig =
  Aeson.Config {
      Aeson.confIndent =
        Aeson.Spaces 2
    , Aeson.confCompare =
        Aeson.keyOrder ["name", "column"]
    , Aeson.confNumFormat =
        Aeson.Generic
    }

pTableSchema :: Aeson.Value -> Aeson.Parser TableSchema
pTableSchema =
  pEnum $ \tag ->
    case tag of
      "binary" ->
        const $ pure Binary
      "array" ->
        Aeson.withObject "object containing array schema" $ \o ->
          Array
            <$> withKey "element" o pColumnSchema
      "map" ->
        Aeson.withObject "object containing map schema" $ \o ->
          Map
            <$> withKey "key" o pColumnSchema
            <*> withKey "value" o pColumnSchema
      _ ->
        const . fail $ "unknown table schema type: " <> Text.unpack tag

pColumnSchema :: Aeson.Value -> Aeson.Parser ColumnSchema
pColumnSchema =
  pEnum $ \tag ->
    case tag of
      "unit" ->
        const $ pure Unit
      "int" ->
        const $ pure Int
      "double" ->
        const $ pure Double
      "enum" ->
        Aeson.withObject "object containing enum column schema" $ \o ->
          Enum <$> withKey "variants" o pSchemaEnumVariants
      "struct" ->
        Aeson.withObject "object containing struct column schema" $ \o ->
          Struct <$> withKey "fields" o pSchemaStructFields
      "nested" ->
        Aeson.withObject "object containing nested column schema" $ \o ->
          Nested <$> withKey "table" o pTableSchema
      "reversed" ->
        Aeson.withObject "object containing reversed column schema" $ \o ->
          Reversed <$> withKey "column" o pColumnSchema
      _ ->
        const . fail $ "unknown column schema type: " <> Text.unpack tag

pSchemaEnumVariants :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Variant ColumnSchema))
pSchemaEnumVariants =
  Aeson.withArray "non-empty array of enum variants" $ \xs -> do
    vs0 <- kmapM pSchemaVariant xs
    case Boxed.uncons vs0 of
      Nothing ->
        fail "enums must have at least one variant"
      Just (v0, vs) ->
        pure $ Cons.from v0 vs

pSchemaVariant :: Aeson.Value -> Aeson.Parser (Variant ColumnSchema)
pSchemaVariant =
  Aeson.withObject "object containing an enum variant" $ \o ->
    Variant
      <$> withKey "name" o (pure . VariantName)
      <*> withKey "column" o pColumnSchema

pSchemaStructFields :: Aeson.Value -> Aeson.Parser (Cons Boxed.Vector (Field ColumnSchema))
pSchemaStructFields =
  Aeson.withArray "array of struct fields" $ \xs -> do
    fs0 <- kmapM pSchemaField xs
    case Boxed.uncons fs0 of
      Nothing ->
        fail "structs must have at least one field"
      Just (f0, fs) ->
        pure $ Cons.from f0 fs

pSchemaField :: Aeson.Value -> Aeson.Parser (Field ColumnSchema)
pSchemaField =
  Aeson.withObject "object containing a struct field" $ \o ->
    Field
      <$> withKey "name" o (pure . FieldName)
      <*> withKey "column" o pColumnSchema

pEnum :: (Text -> Aeson.Value -> Aeson.Parser a) -> Aeson.Value -> Aeson.Parser a
pEnum f =
  Aeson.withObject "object containing an enum (i.e. a single member)" $ \o ->
    case HashMap.toList o of
      [(tag, value)] ->
        f tag value <?> Aeson.Key tag
      [] ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with no members"
      kvs ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with more than one member:" <>
          "\n  " <> List.intercalate ", " (fmap (Text.unpack . fst) kvs)

withKey :: Aeson.FromJSON a => Text -> Aeson.Object ->  (a -> Aeson.Parser b) -> Aeson.Parser b
withKey key o p = do
  x <- o .: key
  p x <?> Aeson.Key key

kmapM :: (Aeson.Value -> Aeson.Parser a) -> Boxed.Vector Aeson.Value -> Aeson.Parser (Boxed.Vector a)
kmapM f =
  Boxed.imapM $ \i x ->
    f x <?> Aeson.Index i

ppTableSchema :: TableSchema -> Aeson.Value
ppTableSchema = \case
  Binary ->
    ppEnum "binary" ppUnit
  Array e ->
    ppEnum "array" $
      Aeson.object ["element" .= ppColumnSchema e]
  Map k v ->
    ppEnum "map" $
      Aeson.object ["key" .= ppColumnSchema k, "value" .= ppColumnSchema v]

ppColumnSchema :: ColumnSchema -> Aeson.Value
ppColumnSchema = \case
  Unit ->
    ppEnum "unit" ppUnit
  Int ->
    ppEnum "int" ppUnit
  Double ->
    ppEnum "double" ppUnit
  Enum vs ->
    ppEnum "enum" $
      Aeson.object ["variants" .= Aeson.Array (Cons.toVector $ fmap ppSchemaVariant vs)]
  Struct fs ->
    ppEnum "struct" $
      Aeson.object ["fields" .= Aeson.Array (Cons.toVector $ fmap ppSchemaField fs)]
  Nested s ->
    ppEnum "nested" $
      Aeson.object ["table" .= ppTableSchema s]
  Reversed s ->
    ppEnum "reversed" $
      Aeson.object ["column" .= ppColumnSchema s]

ppEnum :: Text -> Aeson.Value -> Aeson.Value
ppEnum tag value =
  Aeson.object [
      tag .= value
    ]

ppUnit :: Aeson.Value
ppUnit =
  Aeson.Object HashMap.empty

ppSchemaVariant :: Variant ColumnSchema -> Aeson.Value
ppSchemaVariant (Variant (VariantName name) schema) =
  Aeson.object [
      "name" .=
        name
    , "column" .=
        ppColumnSchema schema
    ]

ppSchemaField :: Field ColumnSchema -> Aeson.Value
ppSchemaField (Field (FieldName name) schema) =
  Aeson.object [
      "name" .=
        name
    , "column" .=
        ppColumnSchema schema
    ]

------------------------------------------------------------------------

foreignOfTags :: Storable.Vector Tag -> Storable.Vector Int64
foreignOfTags =
  Storable.unsafeCast
{-# INLINE foreignOfTags #-}

tagsOfForeign :: Storable.Vector Int64 -> Storable.Vector Tag
tagsOfForeign =
  Storable.unsafeCast
{-# INLINE tagsOfForeign #-}
