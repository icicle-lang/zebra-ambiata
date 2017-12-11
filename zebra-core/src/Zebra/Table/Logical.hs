{-# LANGUAGE BangPatterns #-}
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

  -- * Summary
  , length

  -- * Destruction
  , takeBinary
  , takeArray
  , takeMap
  , takeInt
  , takeDouble
  , takeEnum
  , takeStruct
  , takeNested
  , takeReversed

  -- * Construction
  , false
  , true
  , none
  , some
  , left
  , right
  , pair

  , empty
  , defaultTable
  , defaultValue

  -- * Merging
  , merge
  , mergeValue
  , mergeMap
  , mergeMaps

  , UnionStep(..)
  , unionStep

  -- * Internal
  , renderField
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text

import           GHC.Generics (Generic)
import           GHC.Prim ((>#), tagToEnum#, dataToTag#)

import qualified Neutron.Vector.Boxed as Boxed
import           Neutron.Vector.Cons (Cons)
import qualified Neutron.Vector.Cons as Cons
import qualified Neutron.Vector.Generic as Generic

import           P hiding (empty, some, length)

import           Text.Show.Pretty (ppShow)

import           Zebra.Table.Data
import qualified Zebra.Table.Schema as Schema


data Table =
    Binary !ByteString
  | Array !(Boxed.Vector Value)
  | Map !(Map Value Value)
    deriving (Eq, Ord, Show, Generic)

instance NFData Table

data Value =
    Unit
  | Int !Int64
  | Double !Double
  | Enum !Tag !Value
  | Struct !(Cons Boxed.Vector Value)
  | Nested !Table
  | Reversed !Value
    deriving (Eq, Show, Generic) -- Ord defined at bottom of file.

instance NFData Value

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
renderLogicalMergeError = {-# SCC renderLogicalMergeError #-} \case
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
renderLogicalSchemaError = {-# SCC renderLogicalSchemaError #-} \case
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
renderField name x = {-# SCC renderField #-}
  "\n" <>
  "\n  " <> name <> " =" <>
  ppPrefix "\n    " x

ppPrefix :: Show a => Text -> a -> Text
ppPrefix prefix = {-# SCC ppPrefix #-}
  Text.concat . fmap (prefix <>) . Text.lines . Text.pack . ppShow

ppTableSchema :: Table -> Text
ppTableSchema = {-# SCC ppTableSchema #-} \case
  Binary _ ->
    "binary"
  Array _ ->
    "array"
  Map _ ->
    "map"

ppColumnSchema :: Value -> Text
ppColumnSchema = {-# SCC ppColumnSchema #-} \case
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
length = {-# SCC length #-} \case
  Binary bs ->
    ByteString.length bs
  Array xs ->
    Generic.length xs
  Map kvs ->
    Map.size kvs
{-# INLINABLE length #-}

------------------------------------------------------------------------

merge :: Table -> Table -> Either LogicalMergeError Table
merge !x0 !x1 = {-# SCC merge #-}
  case (x0, x1) of
    (Binary bs0, Binary bs1) ->
      return $ Binary (bs0 <> bs1)

    (Array xs0, Array xs1) ->
      return $ Array (xs0 <> xs1)

    (Map kvs0, Map kvs1) ->
      Map <$> mergeMap kvs0 kvs1

    _ ->
      Left $ LogicalCannotMergeMismatchedCollections x0 x1
{-# INLINABLE merge #-}

mergeMap :: Map Value Value -> Map Value Value -> Either LogicalMergeError (Map Value Value)
mergeMap !xs0 !xs1 = {-# SCC mergeMap #-}
  if Map.null xs0 then
    Right xs1
  else if Map.null xs1 then
    Right xs0
  else
    sequenceA $
      Map.mergeWithKey (\_ x y -> Just (mergeValue x y)) (Map.map Right) (Map.map Right) xs0 xs1
{-# INLINABLE mergeMap #-}

mergeMaps :: Boxed.Vector (Map Value Value) -> Either LogicalMergeError (Map Value Value)
mergeMaps kvss = {-# SCC mergeMaps #-}
  case Generic.length kvss of
    0 ->
      return $! Map.empty

    1 ->
      return $! Generic.unsafeIndex 0 kvss

    2 ->
      mergeMap
        (Generic.unsafeIndex 0 kvss)
        (Generic.unsafeIndex 1 kvss)

    n -> do
      let
        (kvss0, kvss1) =
          Generic.splitAt (n `div` 2) kvss

        ekvs0 =
          mergeMaps kvss0

        ekvs1 =
          mergeMaps kvss1

      !kvs0 <- ekvs0
      !kvs1 <- ekvs1

      mergeMap kvs0 kvs1
{-# INLINABLE mergeMaps #-}

mergeValue :: Value -> Value -> Either LogicalMergeError Value
mergeValue x0 x1 = {-# SCC mergeValue #-}
  case (x0, x1) of
    (Unit, Unit) ->
      return Unit

    (Int v0, Int v1) ->
      Left $! LogicalCannotMergeInt v0 v1

    (Double v0, Double v1) ->
      Left $! LogicalCannotMergeDouble v0 v1

    (Enum tag0 v0, Enum tag1 v1) ->
      Left $! LogicalCannotMergeEnum (tag0, v0) (tag1, v1)

    (Struct fs0, Struct fs1) ->
      Struct <$!> Cons.zipWithM mergeValue fs0 fs1

    (Nested xs0, Nested xs1) ->
      Nested <$!> merge xs0 xs1

    (Reversed v0, Reversed v1) ->
      Reversed <$!> mergeValue v0 v1

    _ ->
      Left $! LogicalCannotMergeMismatchedValues x0 x1
{-# INLINABLE mergeValue #-}

------------------------------------------------------------------------

data UnionStep =
  UnionStep {
      unionComplete :: !(Map Value Value)
    , unionRemaining :: !(Cons Boxed.Vector (Map Value Value))
    } deriving (Eq, Ord, Show)

maximumKey :: Map Value Value -> Maybe Value
maximumKey kvs = {-# SCC maximumKey #-}
  if Map.null kvs then
    Nothing
  else
    return . fst $ Map.findMax kvs
{-# INLINABLE maximumKey #-}

unionStep :: Cons Boxed.Vector (Map Value Value) -> Either LogicalMergeError UnionStep
unionStep kvss = {-# SCC unionStep #-}
  let
    !maximums =
      Cons.mapMaybe maximumKey kvss
  in
    case Generic.minimum maximums of
      Nothing ->
        return $ UnionStep Map.empty kvss
      Just key -> do
        let
          (!ready, !remains) =
            flip Cons.unzipWith kvss $ \kvs ->
              let
                (!kvs0, !m, !kvs1) =
                  Map.splitLookup key kvs
              in
                case m of
                  Nothing ->
                    (kvs0, kvs1)
                  Just v ->
                    (Map.insert key v kvs0, kvs1)

        !merged <- mergeMaps $ Cons.toVector ready

        return $ UnionStep merged remains
{-# INLINABLE unionStep #-}

------------------------------------------------------------------------

empty :: Schema.Table -> Table
empty = {-# SCC empty #-} \case
  Schema.Binary _ _ ->
    Binary ByteString.empty
  Schema.Array _ _ ->
    Array Generic.empty
  Schema.Map _ _ _ ->
    Map Map.empty
{-# INLINABLE empty #-}

defaultTable :: Schema.Table -> Table
defaultTable = {-# SCC defaultTable #-}
  empty
{-# INLINABLE defaultTable #-}

defaultValue :: Schema.Column -> Value
defaultValue = {-# SCC defaultValue #-} \case
  Schema.Unit ->
    Unit
  Schema.Int _ _ ->
    Int 0
  Schema.Double _ ->
    Double 0
  Schema.Enum _ vs ->
    Enum 0 . defaultValue . variantData $ Cons.head vs
  Schema.Struct _ fs ->
    Struct $ Cons.map (defaultValue . fieldData) fs
  Schema.Nested s ->
    Nested $ defaultTable s
  Schema.Reversed s ->
    Reversed $ defaultValue s
{-# INLINABLE defaultValue #-}

------------------------------------------------------------------------

takeBinary :: Table -> Either LogicalSchemaError ByteString
takeBinary = {-# SCC takeBinary #-} \case
  Binary x ->
    Right x
  x ->
    Left $ LogicalExpectedBinary x
{-# INLINE takeBinary #-}

takeArray :: Table -> Either LogicalSchemaError (Boxed.Vector Value)
takeArray = {-# SCC takeArray #-} \case
  Array x ->
    Right x
  x ->
    Left $ LogicalExpectedArray x
{-# INLINE takeArray #-}

takeMap :: Table -> Either LogicalSchemaError (Map Value Value)
takeMap = {-# SCC takeMap #-} \case
  Map x ->
    Right x
  x ->
    Left $ LogicalExpectedMap x
{-# INLINE takeMap #-}

takeInt :: Value -> Either LogicalSchemaError Int64
takeInt = {-# SCC takeInt #-} \case
  Int x ->
    Right x
  x ->
    Left $ LogicalExpectedInt x
{-# INLINE takeInt #-}

takeDouble :: Value -> Either LogicalSchemaError Double
takeDouble = {-# SCC takeDouble #-} \case
  Double x ->
    Right x
  x ->
    Left $ LogicalExpectedDouble x
{-# INLINE takeDouble #-}

takeEnum :: Value -> Either LogicalSchemaError (Tag, Value)
takeEnum = {-# SCC takeEnum #-} \case
  Enum tag x ->
    Right (tag, x)
  x ->
    Left $ LogicalExpectedEnum x
{-# INLINE takeEnum #-}

takeStruct :: Value -> Either LogicalSchemaError (Cons Boxed.Vector Value)
takeStruct = {-# SCC takeStruct #-} \case
  Struct x ->
    Right x
  x ->
    Left $ LogicalExpectedStruct x
{-# INLINE takeStruct #-}

takeNested :: Value -> Either LogicalSchemaError Table
takeNested = {-# SCC takeNested #-} \case
  Nested x ->
    Right x
  x ->
    Left $ LogicalExpectedNested x
{-# INLINE takeNested #-}

takeReversed :: Value -> Either LogicalSchemaError Value
takeReversed = {-# SCC takeReversed #-} \case
  Reversed x ->
    Right x
  x ->
    Left $ LogicalExpectedReversed x
{-# INLINE takeReversed #-}

------------------------------------------------------------------------

false :: Value
false = {-# SCC false #-}
  Enum 0 Unit
{-# INLINE false #-}

true :: Value
true = {-# SCC true #-}
  Enum 1 Unit
{-# INLINE true #-}

none :: Value
none = {-# SCC none #-}
  Enum 0 Unit
{-# INLINE none #-}

some :: Value -> Value
some = {-# SCC some #-}
  Enum 1
{-# INLINE some #-}

left :: Value -> Value
left = {-# SCC left #-}
  Enum 0
{-# INLINE left #-}

right :: Value -> Value
right = {-# SCC right #-}
  Enum 1
{-# INLINE right #-}

pair :: Value -> Value -> Value
pair x y = {-# SCC pair #-}
  Struct $ Cons.from2 x y
{-# INLINE pair #-}

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
compareTag x y = {-# SCC compareTag #-}
  if tagToEnum# (dataToTag# x ># dataToTag# y) then
    GT
  else
    LT
{-# INLINE compareTag #-}
