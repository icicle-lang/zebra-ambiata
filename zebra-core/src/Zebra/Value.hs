{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Value (
    Collection(..)
  , Value(..)

  , ValueUnionError(..)
  , renderValueUnionError

  , ValueSchemaError(..)
  , renderValueSchemaError

  , length
  , union
  , unionMap
  , unionMaps
  , unionValue

  , UnionStep(..)
  , unionStep

  , defaultCollection
  , defaultValue

  , takeBinary
  , takeArray
  , takeMap
  , takeInt
  , takeDouble
  , takeEnum
  , takeStruct
  , takeNested
  , takeReversed

  , false
  , true
  , none
  , some

  -- * Internal
  , renderField
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P hiding (some, length)

import           Text.Show.Pretty (ppShow)

import           Zebra.Data.Vector.Cons (Cons)
import qualified Zebra.Data.Vector.Cons as Cons
import           Zebra.Schema (TableSchema, ColumnSchema, Tag)
import qualified Zebra.Schema as Schema


data Collection =
    Binary !ByteString
  | Array !(Boxed.Vector Value)
  | Map !(Map Value Value)
    deriving (Eq, Ord, Show, Generic, Typeable)

data Value =
    Unit
  | Int !Int64
  | Double !Double
  | Enum !Tag !Value
  | Struct !(Cons Boxed.Vector Value)
  | Nested !Collection
  | Reversed !Value
    deriving (Eq, Ord, Show, Generic, Typeable)

data ValueUnionError =
    ValueCannotUnionMismatchedCollections !Collection !Collection
  | ValueCannotUnionMismatchedValues !Value !Value
  | ValueCannotUnionInt !Int64 !Int64
  | ValueCannotUnionDouble !Double !Double
  | ValueCannotUnionEnum !(Tag, Value) !(Tag, Value)
    deriving (Eq, Ord, Show, Generic, Typeable)

data ValueSchemaError =
    ValueExpectedBinary !Collection
  | ValueExpectedArray !Collection
  | ValueExpectedMap !Collection
  | ValueExpectedInt !Value
  | ValueExpectedDouble !Value
  | ValueExpectedEnum !Value
  | ValueExpectedStruct !Value
  | ValueExpectedNested !Value
  | ValueExpectedReversed !Value
    deriving (Eq, Ord, Show, Generic, Typeable)

renderValueUnionError :: ValueUnionError -> Text
renderValueUnionError = \case
  ValueCannotUnionMismatchedCollections x y ->
    "Cannot take union of mismatched collections:" <>
    renderField "first" x <>
    renderField "second" y

  ValueCannotUnionMismatchedValues x y ->
    "Cannot take union of mismatched values:" <>
    renderField "first" x <>
    renderField "second" y

  ValueCannotUnionInt x y ->
    "Cannot take the union of two integers: " <>
    renderField "first" x <>
    renderField "second" y

  ValueCannotUnionDouble x y ->
    "Cannot take the union of two doubles: " <>
    renderField "first" x <>
    renderField "second" y

  ValueCannotUnionEnum x y ->
    "Cannot take the union of two enums: " <>
    renderField "first" x <>
    renderField "second" y

renderValueSchemaError :: ValueSchemaError -> Text
renderValueSchemaError = \case
  ValueExpectedBinary x ->
    "Expected binary, but was: " <> ppTableSchema x
  ValueExpectedArray x ->
    "Expected array, but was: " <> ppTableSchema x
  ValueExpectedMap x ->
    "Expected map, but was: " <> ppTableSchema x
  ValueExpectedInt x ->
    "Expected int, but was: " <> ppColumnSchema x
  ValueExpectedDouble x ->
    "Expected double, but was: " <> ppColumnSchema x
  ValueExpectedEnum x ->
    "Expected enum, but was: " <> ppColumnSchema x
  ValueExpectedStruct x ->
    "Expected struct, but was: " <> ppColumnSchema x
  ValueExpectedNested x ->
    "Expected nested, but was: " <> ppColumnSchema x
  ValueExpectedReversed x ->
    "Expected reversed, but was: " <> ppColumnSchema x

renderField :: Show a => Text -> a -> Text
renderField name x =
  "\n" <>
  "\n  " <> name <> " =" <>
  ppPrefix "\n    " x

ppPrefix :: Show a => Text -> a -> Text
ppPrefix prefix =
  Text.concat . fmap (prefix <>) . Text.lines . Text.pack . ppShow

ppTableSchema :: Collection -> Text
ppTableSchema = \case
  Binary _ ->
    "binary"
  Array _ ->
    "array"
  Map _ ->
    "map"

ppColumnSchema :: Value -> Text
ppColumnSchema = \case
  Unit ->
    "unit"
  Int _ ->
    "int"
  Double _ ->
    "double"
  Enum _ _ ->
    "enum"
  Struct _ ->
    "struct"
  Nested _ ->
    "nested"
  Reversed _ ->
    "reversed"

------------------------------------------------------------------------

length :: Collection -> Int
length = \case
  Binary bs ->
    ByteString.length bs
  Array xs ->
    Boxed.length xs
  Map kvs ->
    Map.size kvs

------------------------------------------------------------------------

union :: Collection -> Collection -> Either ValueUnionError Collection
union x0 x1 =
  case (x0, x1) of
    (Binary bs0, Binary bs1) ->
      pure $ Binary (bs0 <> bs1)

    (Array xs0, Array xs1) ->
      pure $ Array (xs0 <> xs1)

    (Map kvs0, Map kvs1) ->
      Map <$> unionMap kvs0 kvs1

    _ ->
      Left $ ValueCannotUnionMismatchedCollections x0 x1

unionMap :: Map Value Value -> Map Value Value -> Either ValueUnionError (Map Value Value)
unionMap xs0 xs1 =
  sequenceA $
    Map.mergeWithKey (\_ x y -> Just (unionValue x y)) (fmap pure) (fmap pure) xs0 xs1

unionMaps :: Boxed.Vector (Map Value Value) -> Either ValueUnionError (Map Value Value)
unionMaps kvss =
  case Boxed.length kvss of
    0 ->
      pure $ Map.empty

    1 ->
      pure $ kvss Boxed.! 0

    2 ->
      unionMap
        (kvss Boxed.! 0)
        (kvss Boxed.! 1)

    n -> do
      let
        (kvss0, kvss1) =
          Boxed.splitAt (n `div` 2) kvss

      kvs0 <- unionMaps kvss0
      kvs1 <- unionMaps kvss1

      unionMap kvs0 kvs1

unionValue :: Value -> Value -> Either ValueUnionError Value
unionValue x0 x1 =
  case (x0, x1) of
    (Unit, Unit) ->
      pure Unit

    (Int v0, Int v1) ->
      Left $ ValueCannotUnionInt v0 v1

    (Double v0, Double v1) ->
      Left $ ValueCannotUnionDouble v0 v1

    (Enum tag0 v0, Enum tag1 v1) ->
      Left $ ValueCannotUnionEnum (tag0, v0) (tag1, v1)

    (Struct fs0, Struct fs1) ->
      Struct <$> Cons.zipWithM unionValue fs0 fs1

    (Nested xs0, Nested xs1) ->
      Nested <$> union xs0 xs1

    (Reversed v0, Reversed v1) ->
      Reversed <$> unionValue v0 v1

    _ ->
      Left $ ValueCannotUnionMismatchedValues x0 x1

------------------------------------------------------------------------

data UnionStep =
  UnionStep {
      unionComplete :: !(Map Value Value)
    , unionRemaining :: !(Cons Boxed.Vector (Map Value Value))
    } deriving (Eq, Ord, Show, Generic, Typeable)

maximumKey :: Map Value Value -> Maybe Value
maximumKey kvs =
  if Map.null kvs then
    Nothing
  else
    pure . fst $ Map.findMax kvs

unionStep :: Cons Boxed.Vector (Map Value Value) -> Either ValueUnionError UnionStep
unionStep kvss =
  let
    maximums =
      Cons.mapMaybe maximumKey kvss
  in
    if Boxed.null maximums then
      pure $ UnionStep Map.empty kvss
    else do
      let
        key =
          Boxed.minimum maximums

        -- brexit --
        (leaves0, nonvoters, remains) =
          Cons.unzip3 $ fmap (Map.splitLookup key) kvss

        insert = \case
          Nothing ->
            id
          Just x ->
            Map.insert key x

        leaves =
          Cons.zipWith insert nonvoters leaves0

      leave <- unionMaps $ Cons.toVector leaves

      pure $ UnionStep leave remains

------------------------------------------------------------------------

defaultCollection :: TableSchema -> Collection
defaultCollection = \case
  Schema.Binary ->
    Binary ByteString.empty
  Schema.Array _ ->
    Array Boxed.empty
  Schema.Map _ _ ->
    Map Map.empty

defaultValue :: ColumnSchema -> Value
defaultValue = \case
  Schema.Unit ->
    Unit
  Schema.Int ->
    Int 0
  Schema.Double ->
    Double 0
  Schema.Enum vs ->
    Enum 0 . defaultValue . Schema.variant $ Cons.head vs
  Schema.Struct fs ->
    Struct $ fmap (defaultValue . Schema.field) fs
  Schema.Nested s ->
    Nested $ defaultCollection s
  Schema.Reversed s ->
    Reversed $ defaultValue s

------------------------------------------------------------------------

takeBinary :: Collection -> Either ValueSchemaError ByteString
takeBinary = \case
  Binary x ->
    Right x
  x ->
    Left $ ValueExpectedBinary x
{-# INLINE takeBinary #-}

takeArray :: Collection -> Either ValueSchemaError (Boxed.Vector Value)
takeArray = \case
  Array x ->
    Right x
  x ->
    Left $ ValueExpectedArray x
{-# INLINE takeArray #-}

takeMap :: Collection -> Either ValueSchemaError (Map Value Value)
takeMap = \case
  Map x ->
    Right x
  x ->
    Left $ ValueExpectedMap x
{-# INLINE takeMap #-}

takeInt :: Value -> Either ValueSchemaError Int64
takeInt = \case
  Int x ->
    Right x
  x ->
    Left $ ValueExpectedInt x
{-# INLINE takeInt #-}

takeDouble :: Value -> Either ValueSchemaError Double
takeDouble = \case
  Double x ->
    Right x
  x ->
    Left $ ValueExpectedDouble x
{-# INLINE takeDouble #-}

takeEnum :: Value -> Either ValueSchemaError (Tag, Value)
takeEnum = \case
  Enum tag x ->
    Right (tag, x)
  x ->
    Left $ ValueExpectedEnum x
{-# INLINE takeEnum #-}

takeStruct :: Value -> Either ValueSchemaError (Cons Boxed.Vector Value)
takeStruct = \case
  Struct x ->
    Right x
  x ->
    Left $ ValueExpectedStruct x
{-# INLINE takeStruct #-}

takeNested :: Value -> Either ValueSchemaError Collection
takeNested = \case
  Nested x ->
    Right x
  x ->
    Left $ ValueExpectedNested x
{-# INLINE takeNested #-}

takeReversed :: Value -> Either ValueSchemaError Value
takeReversed = \case
  Reversed x ->
    Right x
  x ->
    Left $ ValueExpectedReversed x
{-# INLINE takeReversed #-}

------------------------------------------------------------------------

false :: Value
false =
  Enum 0 Unit

true :: Value
true =
  Enum 1 Unit

none :: Value
none =
  Enum 0 Unit

some :: Value -> Value
some =
  Enum 1
