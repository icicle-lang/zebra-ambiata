{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table.Logical (
    Table(..)
  , Value(..)

  , LogicalMergeError(..)
  , renderLogicalMergeError

  , LogicalSchemaError(..)
  , renderLogicalSchemaError

  , length
  , empty

  , defaultTable
  , defaultValue

  , merge
  , mergeValue
  , mergeMap
  , mergeMaps

  , UnionStep(..)
  , unionStep

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
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)
import           GHC.Prim ((>#), tagToEnum#, dataToTag#)

import           P hiding (empty, some, length)

import           Text.Show.Pretty (ppShow)

import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Data
import qualified Zebra.Table.Schema as Schema


data Table =
    Binary !ByteString
  | Array !(Boxed.Vector Value)
  | Map !(Map Value Value)
    deriving (Eq, Ord, Show, Generic)

data Value =
    Unit
  | Int !Int64
  | Double !Double
  | Enum !Tag !Value
  | Struct !(Cons Boxed.Vector Value)
  | Nested !Table
  | Reversed !Value
    deriving (Eq, Show, Generic) -- Ord defined at bottom of file.

data LogicalMergeError =
    LogicalCannotMergeMismatchedCollections !Table !Table
  | LogicalCannotMergeMismatchedValues !Value !Value
  | LogicalCannotMergeInt !Int64 !Int64
  | LogicalCannotMergeDouble !Double !Double
  | LogicalCannotMergeEnum !(Tag, Value) !(Tag, Value)
    deriving (Eq, Show)

data LogicalSchemaError =
    LogicalExpectedBinary !Table
  | LogicalExpectedArray !Table
  | LogicalExpectedMap !Table
  | LogicalExpectedInt !Value
  | LogicalExpectedDouble !Value
  | LogicalExpectedEnum !Value
  | LogicalExpectedStruct !Value
  | LogicalExpectedNested !Value
  | LogicalExpectedReversed !Value
    deriving (Eq, Show)

renderLogicalMergeError :: LogicalMergeError -> Text
renderLogicalMergeError = \case
  LogicalCannotMergeMismatchedCollections x y ->
    "Cannot merge mismatched collections:" <>
    renderField "first" x <>
    renderField "second" y

  LogicalCannotMergeMismatchedValues x y ->
    "Cannot merge mismatched values:" <>
    renderField "first" x <>
    renderField "second" y

  LogicalCannotMergeInt x y ->
    "Cannot merge two integers: " <>
    renderField "first" x <>
    renderField "second" y

  LogicalCannotMergeDouble x y ->
    "Cannot merge two doubles: " <>
    renderField "first" x <>
    renderField "second" y

  LogicalCannotMergeEnum x y ->
    "Cannot merge two enums: " <>
    renderField "first" x <>
    renderField "second" y

renderLogicalSchemaError :: LogicalSchemaError -> Text
renderLogicalSchemaError = \case
  LogicalExpectedBinary x ->
    "Expected binary, but was: " <> ppTableSchema x
  LogicalExpectedArray x ->
    "Expected array, but was: " <> ppTableSchema x
  LogicalExpectedMap x ->
    "Expected map, but was: " <> ppTableSchema x
  LogicalExpectedInt x ->
    "Expected int, but was: " <> ppColumnSchema x
  LogicalExpectedDouble x ->
    "Expected double, but was: " <> ppColumnSchema x
  LogicalExpectedEnum x ->
    "Expected enum, but was: " <> ppColumnSchema x
  LogicalExpectedStruct x ->
    "Expected struct, but was: " <> ppColumnSchema x
  LogicalExpectedNested x ->
    "Expected nested, but was: " <> ppColumnSchema x
  LogicalExpectedReversed x ->
    "Expected reversed, but was: " <> ppColumnSchema x

renderField :: Show a => Text -> a -> Text
renderField name x =
  "\n" <>
  "\n  " <> name <> " =" <>
  ppPrefix "\n    " x

ppPrefix :: Show a => Text -> a -> Text
ppPrefix prefix =
  Text.concat . fmap (prefix <>) . Text.lines . Text.pack . ppShow

ppTableSchema :: Table -> Text
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

length :: Table -> Int
length = \case
  Binary bs ->
    ByteString.length bs
  Array xs ->
    Boxed.length xs
  Map kvs ->
    Map.size kvs
{-# INLINABLE length #-}

------------------------------------------------------------------------

merge :: Table -> Table -> Either LogicalMergeError Table
merge x0 x1 =
  case (x0, x1) of
    (Binary bs0, Binary bs1) ->
      pure $ Binary (bs0 <> bs1)

    (Array xs0, Array xs1) ->
      pure $ Array (xs0 <> xs1)

    (Map kvs0, Map kvs1) ->
      Map <$> mergeMap kvs0 kvs1

    _ ->
      Left $ LogicalCannotMergeMismatchedCollections x0 x1
{-# INLINABLE merge #-}

mergeMap :: Map Value Value -> Map Value Value -> Either LogicalMergeError (Map Value Value)
mergeMap xs0 xs1 =
  sequenceA $
    Map.mergeWithKey (\_ x y -> Just (mergeValue x y)) (fmap pure) (fmap pure) xs0 xs1
{-# INLINABLE mergeMap #-}

mergeMaps :: Boxed.Vector (Map Value Value) -> Either LogicalMergeError (Map Value Value)
mergeMaps kvss =
  case Boxed.length kvss of
    0 ->
      pure $ Map.empty

    1 ->
      pure $ kvss Boxed.! 0

    2 ->
      mergeMap
        (kvss Boxed.! 0)
        (kvss Boxed.! 1)

    n -> do
      let
        (kvss0, kvss1) =
          Boxed.splitAt (n `div` 2) kvss

      kvs0 <- mergeMaps kvss0
      kvs1 <- mergeMaps kvss1

      mergeMap kvs0 kvs1
{-# INLINABLE mergeMaps #-}

mergeValue :: Value -> Value -> Either LogicalMergeError Value
mergeValue x0 x1 =
  case (x0, x1) of
    (Unit, Unit) ->
      pure Unit

    (Int v0, Int v1) ->
      Left $ LogicalCannotMergeInt v0 v1

    (Double v0, Double v1) ->
      Left $ LogicalCannotMergeDouble v0 v1

    (Enum tag0 v0, Enum tag1 v1) ->
      Left $ LogicalCannotMergeEnum (tag0, v0) (tag1, v1)

    (Struct fs0, Struct fs1) ->
      Struct <$> Cons.zipWithM mergeValue fs0 fs1

    (Nested xs0, Nested xs1) ->
      Nested <$> merge xs0 xs1

    (Reversed v0, Reversed v1) ->
      Reversed <$> mergeValue v0 v1

    _ ->
      Left $ LogicalCannotMergeMismatchedValues x0 x1
{-# INLINABLE mergeValue #-}

------------------------------------------------------------------------

data UnionStep =
  UnionStep {
      unionComplete :: !(Map Value Value)
    , unionRemaining :: !(Cons Boxed.Vector (Map Value Value))
    } deriving (Eq, Ord, Show)

maximumKey :: Map Value Value -> Maybe Value
maximumKey kvs =
  if Map.null kvs then
    Nothing
  else
    pure . fst $ Map.findMax kvs
{-# INLINABLE maximumKey #-}

unionStep :: Cons Boxed.Vector (Map Value Value) -> Either LogicalMergeError UnionStep
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

      leave <- mergeMaps $ Cons.toVector leaves

      pure $ UnionStep leave remains
{-# INLINABLE unionStep #-}

------------------------------------------------------------------------

empty :: Schema.Table -> Table
empty = \case
  Schema.Binary _ ->
    Binary ByteString.empty
  Schema.Array _ ->
    Array Boxed.empty
  Schema.Map _ _ ->
    Map Map.empty
{-# INLINABLE empty #-}

defaultTable :: Schema.Table -> Table
defaultTable =
  empty
{-# INLINABLE defaultTable #-}

defaultValue :: Schema.Column -> Value
defaultValue = \case
  Schema.Unit ->
    Unit
  Schema.Int ->
    Int 0
  Schema.Double ->
    Double 0
  Schema.Enum vs ->
    Enum 0 . defaultValue . variantData $ Cons.head vs
  Schema.Struct fs ->
    Struct $ fmap (defaultValue . fieldData) fs
  Schema.Nested s ->
    Nested $ defaultTable s
  Schema.Reversed s ->
    Reversed $ defaultValue s
{-# INLINABLE defaultValue #-}

------------------------------------------------------------------------

takeBinary :: Table -> Either LogicalSchemaError ByteString
takeBinary = \case
  Binary x ->
    Right x
  x ->
    Left $ LogicalExpectedBinary x
{-# INLINE takeBinary #-}

takeArray :: Table -> Either LogicalSchemaError (Boxed.Vector Value)
takeArray = \case
  Array x ->
    Right x
  x ->
    Left $ LogicalExpectedArray x
{-# INLINE takeArray #-}

takeMap :: Table -> Either LogicalSchemaError (Map Value Value)
takeMap = \case
  Map x ->
    Right x
  x ->
    Left $ LogicalExpectedMap x
{-# INLINE takeMap #-}

takeInt :: Value -> Either LogicalSchemaError Int64
takeInt = \case
  Int x ->
    Right x
  x ->
    Left $ LogicalExpectedInt x
{-# INLINE takeInt #-}

takeDouble :: Value -> Either LogicalSchemaError Double
takeDouble = \case
  Double x ->
    Right x
  x ->
    Left $ LogicalExpectedDouble x
{-# INLINE takeDouble #-}

takeEnum :: Value -> Either LogicalSchemaError (Tag, Value)
takeEnum = \case
  Enum tag x ->
    Right (tag, x)
  x ->
    Left $ LogicalExpectedEnum x
{-# INLINE takeEnum #-}

takeStruct :: Value -> Either LogicalSchemaError (Cons Boxed.Vector Value)
takeStruct = \case
  Struct x ->
    Right x
  x ->
    Left $ LogicalExpectedStruct x
{-# INLINE takeStruct #-}

takeNested :: Value -> Either LogicalSchemaError Table
takeNested = \case
  Nested x ->
    Right x
  x ->
    Left $ LogicalExpectedNested x
{-# INLINE takeNested #-}

takeReversed :: Value -> Either LogicalSchemaError Value
takeReversed = \case
  Reversed x ->
    Right x
  x ->
    Left $ LogicalExpectedReversed x
{-# INLINE takeReversed #-}

------------------------------------------------------------------------

false :: Value
false =
  Enum 0 Unit
{-# INLINE false #-}

true :: Value
true =
  Enum 1 Unit
{-# INLINE true #-}

none :: Value
none =
  Enum 0 Unit
{-# INLINE none #-}

some :: Value -> Value
some =
  Enum 1
{-# INLINE some #-}

------------------------------------------------------------------------
-- Ord Value

instance Ord Value where
  compare = \case
    Unit -> \case
      Unit ->
        EQ
      y ->
        compareTag Unit y

    Int x -> \case
      Int y ->
        compare x y
      y ->
        compareTag (Int x) y

    Double x -> \case
      Double y ->
        compare x y
      y ->
        compareTag (Double x) y

    Enum xtag xvar -> \case
      Enum ytag yvar ->
        compare (xtag, xvar) (ytag, yvar)
      y ->
        compareTag (Enum xtag xvar) y

    Struct xs -> \case
      Struct ys ->
        compare xs ys
      y ->
        compareTag (Struct xs) y

    Nested x -> \case
      Nested y ->
        compare x y
      y ->
        compareTag (Nested x) y

    Reversed x -> \case
      Reversed y ->
        compare y x -- Deliberately flipped x/y, that's what 'Reversed' does.
      y ->
        compareTag (Reversed x) y
  {-# INLINABLE compare #-}

compareTag :: a -> a -> Ordering
compareTag x y =
  if tagToEnum# (dataToTag# x ># dataToTag# y) then
    GT
  else
    LT
{-# INLINE compareTag #-}
