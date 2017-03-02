{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Table (
    Table(..)
  , Column(..)
  , ColumnIndex(..)

  , TableError(..)
  , ValueError(..)

  , annotation
  , schema
  , rowCount
  , columns

  , encoding
  , encodingColumn

  , rows
  , fromRow
  , fromRowOrDefault

  , empty
  , concat
  , append
  , appendColumn
  , splitAt
  , splitAtColumn
  ) where

import           Control.Monad.State.Strict (MonadState(..))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.State.Strict (StateT, runStateT)

import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString as B
import           Data.Typeable (Typeable)
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P hiding (empty, concat, splitAt)

import qualified X.Data.ByteString.Unsafe as B
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Generic as Generic

import           Zebra.Data.Encoding
import qualified Zebra.Data.Vector.Storable as Storable
import           Zebra.Schema (Schema, Field(..), Variant(..))
import qualified Zebra.Schema as Schema
import           Zebra.Value (Value)
import qualified Zebra.Value as Value


data Table a =
  Table !a !Int !(Boxed.Vector (Column a))
  deriving (Eq, Ord, Show, Generic, Typeable, Functor, Foldable, Traversable)

data Column a =
    ByteColumn !ByteString
  | IntColumn !(Storable.Vector Int64)
  | DoubleColumn !(Storable.Vector Double)
  | ArrayColumn !(Storable.Vector Int64) !(Table a)
    deriving (Eq, Ord, Show, Generic, Typeable, Functor, Foldable, Traversable)

newtype ColumnIndex =
  ColumnIndex {
      unColumnIndex :: Int
    } deriving (Eq, Ord, Show, Num)

data TableError a =
    TableSchemaMismatch !Value !Schema
  | TableRequiredFieldMissing !Schema
  | TableCannotConcatEmpty
  | TableAppendColumnsMismatch !(Column a) !(Column a)
  | TableStructFieldsMismatch !(Boxed.Vector Value) !(Boxed.Vector Field)
  | TableEnumVariantMismatch !Int !Value !(Boxed.Vector Variant)
    deriving (Eq, Ord, Show, Generic, Typeable, Functor, Foldable, Traversable)

data ValueError a =
    ValueExpectedByteColumn !(Column a)
  | ValueExpectedIntColumn !(Column a)
  | ValueExpectedDoubleColumn !(Column a)
  | ValueExpectedArrayColumn !(Column a)
  | ValueStringLengthMismatch !Int !Int
  | ValueListLengthMismatch !Int !Int
  | ValueEnumVariantMismatch !Int !(Boxed.Vector Variant)
  | ValueNoMoreColumns
  | ValueLeftoverColumns !(Boxed.Vector (Column a))
    deriving (Eq, Ord, Show, Generic, Typeable, Functor, Foldable, Traversable)

annotation :: Table a -> a
annotation (Table x _ _) =
  x

schema :: Table Schema -> Schema
schema (Table x _ _) =
  x

rowCount :: Table a -> Int
rowCount (Table _ x _) =
  x

columns :: Table a -> Boxed.Vector (Column a)
columns (Table _ _ x) =
  x

fromRowOrDefault :: Schema -> Maybe' Value -> Either (TableError Schema) (Table Schema)
fromRowOrDefault s = \case
  Nothing' ->
    pure $ defaultTable s
  Just' value ->
    fromRow s value

fromRow :: Schema -> Value -> Either (TableError Schema) (Table Schema)
fromRow vschema =
  case vschema of
    Schema.Byte -> \case
      Value.Byte x ->
        pure . singletonByte $ fromIntegral x
      value ->
        Left $ TableSchemaMismatch value vschema

    Schema.Int -> \case
      Value.Int x ->
        pure . singletonInt $ fromIntegral x
      value ->
        Left $ TableSchemaMismatch value vschema

    Schema.Double -> \case
      Value.Double x ->
        pure $ singletonDouble x
      value ->
        Left $ TableSchemaMismatch value vschema

    Schema.Enum variant0 variants -> \case
      Value.Enum tag x -> do
        (vs0, v1, vs2) <-
            maybeToRight (TableEnumVariantMismatch tag x $ Boxed.cons variant0 variants) $
              Schema.focusVariant tag variant0 variants

        table1 <- fromRow (variantSchema v1) x

        pure . Table vschema 1 $
          columns (singletonInt $ fromIntegral tag) <>
          Boxed.concatMap (columns . defaultTable . variantSchema) vs0 <>
          columns table1 <>
          Boxed.concatMap (columns . defaultTable . variantSchema) vs2

      value ->
        Left $ TableSchemaMismatch value vschema

    Schema.Struct fields -> \case
      Value.Struct xs ->
        fromStruct fields xs
      value ->
        Left $ TableSchemaMismatch value vschema

    Schema.Array Schema.Byte -> \case
      Value.ByteArray x ->
        pure $ singletonByteArray x
      value ->
        Left $ TableSchemaMismatch value vschema

    Schema.Array eschema -> \case
      Value.Array xs
        | Boxed.null xs ->
            pure (singletonEmptyArray eschema)
        | otherwise -> do
            vs0 <- traverse (fromRow eschema) xs
            vs1 <- concat vs0
            pure . Table vschema 1 . Boxed.singleton $
              ArrayColumn (Storable.singleton . fromIntegral $ Boxed.length xs) vs1
      value ->
        Left $ TableSchemaMismatch value vschema

fromStruct :: Boxed.Vector Field -> Boxed.Vector Value -> Either (TableError Schema) (Table Schema)
fromStruct fields values =
  if Boxed.length fields /= Boxed.length values then
    Left $ TableStructFieldsMismatch values fields
  else
    fmap (Table (Schema.Struct fields) 1 . Boxed.concatMap columns) $
      Boxed.zipWithM fromRow (fmap fieldSchema fields) values

------------------------------------------------------------------------

rows :: Schema -> Table a -> Either (ValueError a) (Boxed.Vector Value)
rows vschema table0 =
  consumeColumns table0 $ popValueColumn table0 vschema

consumeColumns :: Table a -> StateT ColumnIndex (Either (ValueError a)) b -> Either (ValueError a) b
consumeColumns table m = do
  (result, ColumnIndex index) <- runStateT m 0
  if index == Boxed.length (columns table) then
    pure result
  else
    Left . ValueLeftoverColumns . Boxed.drop index $ columns table

popColumn :: Table a -> StateT ColumnIndex (Either (ValueError a)) (Column a)
popColumn table = do
  index <- get
  case columns table Boxed.!? unColumnIndex index of
    Just x -> do
      put $ index + 1
      pure x
    Nothing ->
      lift $ Left ValueNoMoreColumns

popByteColumn :: Table a -> StateT ColumnIndex (Either (ValueError a)) ByteString
popByteColumn table =
  popColumn table >>= \case
    ByteColumn xs ->
      pure xs
    x ->
      lift . Left $ ValueExpectedByteColumn x

popIntColumn :: Table a -> StateT ColumnIndex (Either (ValueError a)) (Storable.Vector Int64)
popIntColumn table =
  popColumn table >>= \case
    IntColumn xs ->
      pure xs
    x ->
      lift . Left $ ValueExpectedIntColumn x

popDoubleColumn :: Table a -> StateT ColumnIndex (Either (ValueError a)) (Storable.Vector Double)
popDoubleColumn table =
  popColumn table >>= \case
    DoubleColumn xs ->
      pure xs
    x ->
      lift . Left $ ValueExpectedDoubleColumn x

popArrayColumn ::
  Table a ->
  (Storable.Vector Int64 -> Table a -> Either (ValueError a) b) ->
  StateT ColumnIndex (Either (ValueError a)) b
popArrayColumn table update =
  popColumn table >>= \case
    ArrayColumn ns atable -> do
      lift $ update ns atable
    x ->
      lift . Left $ ValueExpectedArrayColumn x

popValueColumn :: Table a -> Schema -> StateT ColumnIndex (Either (ValueError a)) (Boxed.Vector Value)
popValueColumn table = \case
  Schema.Byte ->
    fmap Value.Byte . Boxed.convert . Storable.unsafeFromByteString <$>
      popByteColumn table

  Schema.Int ->
    fmap Value.Int . Boxed.convert <$>
      popIntColumn table

  Schema.Double ->
    fmap Value.Double . Boxed.convert <$>
      popDoubleColumn table

  Schema.Struct fields ->
    if Boxed.null fields then do
      pure . Boxed.replicate (rowCount table) $ Value.Struct Boxed.empty
    else do
      xss <- traverse (popValueColumn table . fieldSchema) fields
      pure . fmap Value.Struct $ Boxed.transpose xss

  Schema.Enum variant0 variants -> do
    tags <- popIntColumn table
    xss <- Boxed.transpose <$> traverse (popValueColumn table . variantSchema) (Boxed.cons variant0 variants)

    let
      takeTag tag xs = do
        x <- maybeToRight (ValueEnumVariantMismatch tag $ Boxed.cons variant0 variants) $ xs Boxed.!? tag
        pure $ Value.Enum tag x

    lift $
      Boxed.zipWithM takeTag (fmap fromIntegral $ Boxed.convert tags) xss

  Schema.Array Schema.Byte ->
    popArrayColumn table $ \ns atable ->
      consumeColumns atable $ do
        bs <- popByteColumn atable
        lift . fmap (fmap Value.ByteArray) $ restring ns bs

  Schema.Array eschema ->
    popArrayColumn table $ \ns atable ->
      consumeColumns atable $ do
        xs <- popValueColumn atable eschema
        lift . fmap (fmap Value.Array) $ relist ns xs

restring :: Storable.Vector Int64 -> ByteString -> Either (ValueError a) (Boxed.Vector ByteString)
restring ns bs =
  let
    !n =
      fromIntegral $ Storable.sum ns

    !m =
      B.length bs
  in
    if n /= m then
      Left $ ValueStringLengthMismatch n m
    else
      pure . B.unsafeSplits id bs $ Storable.map fromIntegral ns

relist :: Storable.Vector Int64 -> Boxed.Vector b -> Either (ValueError a) (Boxed.Vector (Boxed.Vector b))
relist ns xs =
  let
    !n =
      fromIntegral $ Storable.sum ns

    !m =
      Boxed.length xs
  in
    if n /= m then
      Left $ ValueListLengthMismatch n m
    else
      pure . Generic.unsafeSplits id xs $ Storable.map fromIntegral ns

------------------------------------------------------------------------

encoding :: Table a -> Encoding
encoding =
  Encoding . fmap encodingColumn . Boxed.toList . columns

encodingColumn :: Column a -> ColumnEncoding
encodingColumn = \case
  ByteColumn _ ->
    ByteEncoding
  IntColumn _ ->
    IntEncoding
  DoubleColumn _ ->
    DoubleEncoding
  ArrayColumn _ table ->
    ArrayEncoding $ encoding table

concat :: Boxed.Vector (Table Schema) -> Either (TableError Schema) (Table Schema)
concat xss0 =
  if Boxed.null xss0 then
    Left TableCannotConcatEmpty
  else
    let
      schema0 :: Schema
      schema0 =
        schema $ Boxed.head xss0 -- FIXME check schema

      n :: Int
      n =
        Boxed.sum $ fmap rowCount xss0

      xss :: Boxed.Vector (Boxed.Vector (Column Schema))
      xss =
        fmap columns xss0

      yss =
        Boxed.transpose xss
    in
      fmap (Table schema0 n) $
        traverse (Boxed.fold1M' appendColumn) yss

append :: Table a -> Table a -> Either (TableError a) (Table a)
append (Table s n xs) (Table _s m ys) =
  Table s (n + m) <$> Boxed.zipWithM appendColumn xs ys

appendColumn :: Column a -> Column a -> Either (TableError a) (Column a)
appendColumn x y =
  case (x, y) of
    (ByteColumn xs, ByteColumn ys) ->
      pure $ ByteColumn (xs <> ys)

    (IntColumn xs, IntColumn ys) ->
      pure $ IntColumn (xs <> ys)

    (DoubleColumn xs, DoubleColumn ys) ->
      pure $ DoubleColumn (xs <> ys)

    (ArrayColumn n xs, ArrayColumn m ys) ->
      ArrayColumn (n <> m) <$> append xs ys

    (_, _) ->
      Left $ TableAppendColumnsMismatch x y

splitAt :: Int -> Table a -> (Table a, Table a)
splitAt i0 (Table s n fs) =
  let
    i =
      min n (max 0 i0)

    (as, bs) =
      Boxed.unzip $ Boxed.map (splitAtColumn i) fs
  in
    (Table s i as, Table s (n - i) bs)

splitAtColumn :: Int -> Column a -> (Column a, Column a)
splitAtColumn i =
  \case
    ByteColumn vs
     -> bye ByteColumn $ B.splitAt i vs
    IntColumn vs
     -> bye IntColumn $ Storable.splitAt i vs
    DoubleColumn vs
     -> bye DoubleColumn $ Storable.splitAt i vs
    ArrayColumn len rec
     -> let (len1, len2) = Storable.splitAt i len
            nested_count = fromIntegral $ Storable.sum len1
            (rec1, rec2) = splitAt nested_count rec
        in  (ArrayColumn len1 rec1, ArrayColumn len2 rec2)
  where
   bye f = bimap f f

------------------------------------------------------------------------

emptyByte :: Table Schema
emptyByte =
  Table Schema.Byte 0 . Boxed.singleton $ ByteColumn B.empty

emptyInt :: Table Schema
emptyInt =
  Table Schema.Int 0 . Boxed.singleton $ IntColumn Storable.empty

emptyDouble :: Table Schema
emptyDouble =
  Table Schema.Double 0 . Boxed.singleton $ DoubleColumn Storable.empty

emptyArray :: Table Schema -> Table Schema
emptyArray vs@(Table s _ _) =
  Table (Schema.Array s) 0 . Boxed.singleton $ ArrayColumn Storable.empty vs

empty :: Schema -> Table Schema
empty tschema =
  case tschema of
    Schema.Byte ->
      emptyByte
    Schema.Int ->
      emptyInt
    Schema.Double ->
      emptyDouble
    Schema.Array element ->
      emptyArray $ empty element
    Schema.Struct fields ->
      Table tschema 0 $
        Boxed.concatMap (columns . empty . fieldSchema) fields
    Schema.Enum variant0 variants ->
      Table tschema 0 $
        columns emptyInt <>
        Boxed.concatMap (columns . empty . variantSchema) (Boxed.cons variant0 variants)

singletonByte :: Word8 -> Table Schema
singletonByte =
  Table Schema.Byte 1 . Boxed.singleton . ByteColumn . B.singleton

singletonInt :: Int64 -> Table Schema
singletonInt =
  Table Schema.Int 1 . Boxed.singleton . IntColumn . Storable.singleton

singletonDouble :: Double -> Table Schema
singletonDouble =
  Table Schema.Double 1 . Boxed.singleton . DoubleColumn . Storable.singleton

singletonByteArray :: ByteString -> Table Schema
singletonByteArray bs =
  Table (Schema.Array Schema.Byte) 1 . Boxed.singleton $
    ArrayColumn
      (Storable.singleton . fromIntegral $ B.length bs)
      (Table Schema.Byte (B.length bs) . Boxed.singleton $ ByteColumn bs)

singletonEmptyArray :: Schema -> Table Schema
singletonEmptyArray element =
  Table (Schema.Array element) 1 .
    Boxed.singleton $
    ArrayColumn (Storable.singleton 0) (empty element)

defaultTable :: Schema -> Table Schema
defaultTable tschema =
  case tschema of
    Schema.Byte ->
      singletonByte 0
    Schema.Int ->
      singletonInt 0
    Schema.Double ->
      singletonDouble 0
    Schema.Struct fields ->
      Table tschema 1 $
        Boxed.concatMap (columns . defaultTable . fieldSchema) fields
    Schema.Enum variant0 variants ->
      Table tschema 1 $
        columns (singletonInt 0) <>
        Boxed.concatMap (columns . defaultTable . variantSchema) (Boxed.cons variant0 variants)
    Schema.Array element ->
      singletonEmptyArray element
