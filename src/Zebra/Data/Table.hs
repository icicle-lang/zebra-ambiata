{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Table (
    Table(..)
  , Column(..)

  , TableError(..)
  , ValueError(..)

  , encodingOfTable
  , encodingOfColumn

  , tableOfMaybeValue
  , tableOfValue
  , tableOfStruct

  , valuesOfTable

  , concatTables
  , concatColumns
  , appendTables
  , appendColumns
  , splitAtTable
  , splitAtColumn
  ) where

import           Control.Monad.State.Strict (MonadState(..))
import           Control.Monad.Trans.State.Strict (State, runState)

import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString as B
import           Data.Typeable (Typeable)
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, hoistEither, left)
import qualified X.Data.ByteString.Unsafe as B
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Storable as Storable
import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Encoding
import           Zebra.Data.Fact
import           Zebra.Data.Schema (Schema, Field(..), Variant(..))
import qualified Zebra.Data.Schema as Schema


data Table a =
  Table {
      tableAnnotation :: !a
    , tableRowCount :: !Int
    , tableColumns :: !(Boxed.Vector (Column a))
    } deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

data Column a =
    ByteColumn !ByteString
  | IntColumn !(Storable.Vector Int64)
  | DoubleColumn !(Storable.Vector Double)
  | ArrayColumn !(Storable.Vector Int64) !(Table a)
    deriving (Eq, Ord, Generic, Typeable, Functor, Foldable, Traversable)

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
  | ValueLeftoverColumns !Encoding
    deriving (Eq, Ord, Show, Generic, Typeable, Functor, Foldable, Traversable)

instance Show a => Show (Table a) where
  showsPrec =
    gshowsPrec

instance Show a => Show (Column a) where
  showsPrec =
    gshowsPrec

tableOfMaybeValue :: Schema -> Maybe' Value -> Either (TableError Schema) (Table Schema)
tableOfMaybeValue schema = \case
  Nothing' ->
    pure $ defaultTable schema
  Just' value ->
    tableOfValue schema value

tableOfValue :: Schema -> Value -> Either (TableError Schema) (Table Schema)
tableOfValue schema =
  case schema of
    Schema.Bool -> \case
      Bool x ->
        pure $ singletonBool x
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Byte -> \case
      Byte x ->
        pure . singletonByte $ fromIntegral x
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Int -> \case
      Int x ->
        pure . singletonInt $ fromIntegral x
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Double -> \case
      Double x ->
        pure $ singletonDouble x
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Enum variant0 variants -> \case
      Enum tag x -> do
        Variant _ variant <- maybeToRight (TableEnumVariantMismatch tag x variants) $ Schema.lookupVariant tag variant0 variants
        xtable <- tableOfValue variant x
        pure . Table schema 1 $
          tableColumns (singletonInt $ fromIntegral tag) <>
          tableColumns xtable
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Struct fields -> \case
      Struct values ->
        tableOfStruct fields values
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Array Schema.Byte -> \case
      ByteArray x ->
        pure $ singletonByteArray x
      value ->
        Left $ TableSchemaMismatch value schema

    Schema.Array ischema -> \case
      Array xs -> do
        vs0 <- traverse (tableOfValue ischema) xs
        vs1 <- concatTables vs0
        pure . Table schema 1 . Boxed.singleton $
          ArrayColumn (Storable.singleton . fromIntegral $ Boxed.length xs) vs1
      value ->
        Left $ TableSchemaMismatch value schema

tableOfStruct :: Boxed.Vector Field -> Boxed.Vector Value -> Either (TableError Schema) (Table Schema)
tableOfStruct fields values =
  if Boxed.length fields /= Boxed.length values then
    Left $ TableStructFieldsMismatch values fields
  else
    fmap (Table (Schema.Struct fields) 1 . Boxed.concatMap tableColumns) $
      Boxed.zipWithM tableOfValue (fmap fieldSchema fields) values

------------------------------------------------------------------------

valuesOfTable :: Schema -> Table a -> Either (ValueError a) (Boxed.Vector Value)
valuesOfTable schema table0 =
  evalStateTable table0 $ popValueColumn schema

evalStateTable :: Table a -> EitherT (ValueError a) (State (Table a)) b -> Either (ValueError a) b
evalStateTable table0 m =
  let
    (result, table) =
      runState (runEitherT m) table0
  in
    if Boxed.null $ tableColumns table then
      result
    else
      Left . ValueLeftoverColumns $ encodingOfTable table

popColumn :: EitherT (ValueError a) (State (Table a)) (Column a)
popColumn = do
  Table schema n xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . Table schema n $ Boxed.drop 1 xs
      pure x
    Nothing ->
      left ValueNoMoreColumns

popByteColumn :: EitherT (ValueError a) (State (Table a)) ByteString
popByteColumn =
  popColumn >>= \case
    ByteColumn xs ->
      pure xs
    x ->
      left $ ValueExpectedByteColumn x

popIntColumn :: EitherT (ValueError a) (State (Table a)) (Storable.Vector Int64)
popIntColumn =
  popColumn >>= \case
    IntColumn xs ->
      pure xs
    x ->
      left $ ValueExpectedIntColumn x

popDoubleColumn :: EitherT (ValueError a) (State (Table a)) (Storable.Vector Double)
popDoubleColumn =
  popColumn >>= \case
    DoubleColumn xs ->
      pure xs
    x ->
      left $ ValueExpectedDoubleColumn x

popArrayColumn ::
  (Storable.Vector Int64 -> EitherT (ValueError a) (State (Table a)) b) ->
  EitherT (ValueError a) (State (Table a)) b
popArrayColumn f =
  popColumn >>= \case
    ArrayColumn ns table0 ->
      hoistEither . evalStateTable table0 $ f ns
    x ->
      left $ ValueExpectedArrayColumn x

popBoolColumn :: EitherT (ValueError a) (State (Table a)) (Boxed.Vector Bool)
popBoolColumn =
  fmap (fmap (/= 0) . Boxed.convert) $ popIntColumn

popValueColumn :: Schema -> EitherT (ValueError a) (State (Table a)) (Boxed.Vector Value)
popValueColumn = \case
  Schema.Bool ->
    fmap (fmap Bool) popBoolColumn

  Schema.Byte ->
    fmap (fmap (Byte . fromIntegral) . Boxed.convert . unsafeFromByteString) popByteColumn

  Schema.Int ->
    fmap (fmap (Int . fromIntegral) . Boxed.convert) popIntColumn

  Schema.Double ->
    fmap (fmap Double . Boxed.convert) popDoubleColumn

  Schema.Struct fields ->
    if Boxed.null fields then do
      Table _ n _ <- get
      pure . Boxed.replicate n $ Struct Boxed.empty
    else do
      xss <- traverse (popValueColumn . fieldSchema) fields
      pure . fmap Struct $ Boxed.transpose xss

  Schema.Enum variant0 variants -> do
    tags <- popIntColumn
    xss <- Boxed.transpose <$> traverse (popValueColumn . variantSchema) (Boxed.cons variant0 variants)

    let
      takeTag tag xs = do
        x <- maybeToRight (ValueEnumVariantMismatch tag $ Boxed.cons variant0 variants) $ xs Boxed.!? tag
        pure $ Enum tag x

    hoistEither $
      Boxed.zipWithM takeTag (fmap fromIntegral $ Boxed.convert tags) xss

  Schema.Array Schema.Byte ->
    popArrayColumn $ \ns -> do
      bs <- popByteColumn
      fmap (fmap $ ByteArray) . hoistEither $ restring ns bs

  Schema.Array schema ->
    popArrayColumn $ \ns -> do
      xs <- popValueColumn schema
      fmap (fmap Array) . hoistEither $ relist ns xs

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

encodingOfTable :: Table a -> Encoding
encodingOfTable =
  encodingOfColumns . Boxed.toList . tableColumns

encodingOfColumns :: [Column a] -> Encoding
encodingOfColumns =
  Encoding . fmap encodingOfColumn

encodingOfColumn :: Column a -> ColumnEncoding
encodingOfColumn = \case
  ByteColumn _ ->
    ByteEncoding
  IntColumn _ ->
    IntEncoding
  DoubleColumn _ ->
    DoubleEncoding
  ArrayColumn _ table ->
    ArrayEncoding $ encodingOfTable table

concatTables :: Boxed.Vector (Table Schema) -> Either (TableError Schema) (Table Schema)
concatTables xss0 =
  if Boxed.null xss0 then
    Left TableCannotConcatEmpty
  else
    let
      schema :: Schema
      schema =
        tableAnnotation $ Boxed.head xss0 -- FIXME check schema

      n :: Int
      n =
        Boxed.sum $ fmap tableRowCount xss0

      xss :: Boxed.Vector (Boxed.Vector (Column Schema))
      xss =
        fmap tableColumns xss0

      yss =
        Boxed.transpose xss
    in
      fmap (Table schema n) $
      traverse concatColumns yss

appendTables :: Table a -> Table a -> Either (TableError a) (Table a)
appendTables (Table schema n xs) (Table _schema m ys) =
  -- FIXME check schema?
  Table schema (n + m) <$> Boxed.zipWithM appendColumns xs ys

concatColumns :: Boxed.Vector (Column a) -> Either (TableError a) (Column a)
concatColumns xs =
  if Boxed.null xs then
    Left TableCannotConcatEmpty
  else
    Boxed.fold1M' appendColumns xs

appendColumns :: Column a -> Column a -> Either (TableError a) (Column a)
appendColumns x y =
  case (x, y) of
    (ByteColumn xs, ByteColumn ys) ->
      pure $ ByteColumn (xs <> ys)

    (IntColumn xs, IntColumn ys) ->
      pure $ IntColumn (xs <> ys)

    (DoubleColumn xs, DoubleColumn ys) ->
      pure $ DoubleColumn (xs <> ys)

    (ArrayColumn n xs, ArrayColumn m ys) ->
      ArrayColumn (n <> m) <$> appendTables xs ys

    (_, _) ->
      Left $ TableAppendColumnsMismatch x y

splitAtTable :: Int -> Table a -> (Table a, Table a)
splitAtTable i0 (Table schema n fs) =
  let
    i =
      min n (max 0 i0)

    (as, bs) =
      Boxed.unzip $ Boxed.map (splitAtColumn i) fs
  in
    (Table schema i as, Table schema (n - i) bs)

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
            (rec1, rec2) = splitAtTable nested_count rec
        in  (ArrayColumn len1 rec1, ArrayColumn len2 rec2)
  where
   bye f = bimap f f

------------------------------------------------------------------------

emptyBool :: Table Schema
emptyBool =
  Table Schema.Bool 0 . Boxed.singleton $ IntColumn Storable.empty

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
emptyArray vs@(Table schema _ _) =
  Table (Schema.Array schema) 0 . Boxed.singleton $ ArrayColumn Storable.empty vs

emptyTable :: Schema -> Table Schema
emptyTable schema =
  case schema of
    Schema.Bool ->
      emptyBool
    Schema.Byte ->
      emptyByte
    Schema.Int ->
      emptyInt
    Schema.Double ->
      emptyDouble
    Schema.Array item ->
      emptyArray $ emptyTable item
    Schema.Struct fields ->
      Table schema 0 $
        Boxed.concatMap (tableColumns . emptyTable . fieldSchema) fields
    Schema.Enum variant0 variants ->
      Table schema 0 $
        tableColumns emptyInt <>
        Boxed.concatMap (tableColumns . emptyTable . variantSchema) (Boxed.cons variant0 variants)

singletonBool :: Bool -> Table Schema
singletonBool b =
  Table Schema.Bool 1 . Boxed.singleton . IntColumn . Storable.singleton $
    if b then 1 else 0

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
singletonEmptyArray schema =
  Table (Schema.Array schema) 1 .
    Boxed.singleton $
    ArrayColumn (Storable.singleton 0) (emptyTable schema)

defaultTable :: Schema -> Table Schema
defaultTable schema =
  case schema of
    Schema.Bool ->
      singletonBool False
    Schema.Byte ->
      singletonByte 0
    Schema.Int ->
      singletonInt 0
    Schema.Double ->
      singletonDouble 0
    Schema.Struct fields ->
      Table schema 1 $
        Boxed.concatMap (tableColumns . defaultTable . fieldSchema) fields
    Schema.Enum variant0 variants ->
      Table schema 1 $
        tableColumns (singletonInt 0) <>
        Boxed.concatMap (tableColumns . defaultTable . variantSchema) (Boxed.cons variant0 variants)
    Schema.Array item ->
      singletonEmptyArray item

-- TODO duplicated
unsafeFromByteString :: ByteString -> Storable.Vector Word8
unsafeFromByteString (PS fp off len) =
  Storable.unsafeFromForeignPtr fp off len
