{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Table (
    Table(..)
  , Column(..)

  , TableError(..)
  , ValueError(..)

  , rowsOfTable
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

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Coerce (coerce)
import qualified Data.Text.Encoding as T
import           Data.Typeable (Typeable)

import           GHC.Generics (Generic)

import           P

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, hoistEither, left)
import qualified X.Data.ByteString.Unsafe as B
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Generic as Generic
import qualified X.Data.Vector.Storable as Storable
import           X.Text.Show (gshowsPrec)

import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Fact
import           Zebra.Data.Schema


newtype Table =
  Table {
      tableColumns :: Boxed.Vector Column
    } deriving (Eq, Ord, Generic, Typeable)

data Column =
    ByteColumn !ByteString
  | IntColumn !(Storable.Vector Int64)
  | DoubleColumn !(Storable.Vector Double)
  | ArrayColumn !(Storable.Vector Int64) !Table
    deriving (Eq, Ord, Generic, Typeable)

data TableError =
    TableSchemaMismatch !Value !Schema
  | TableRequiredFieldMissing !Schema
  | TableCannotConcatEmpty
  | TableAppendColumnsMismatch !Column !Column
  | TableStructFieldsMismatch !(Boxed.Vector Value) !(Boxed.Vector FieldSchema)
  | TableEnumVariantMismatch !Int !Value !(Boxed.Vector VariantSchema)
    deriving (Eq, Ord, Show, Generic, Typeable)

data ValueError =
    ValueExpectedByteColumn !Column
  | ValueExpectedIntColumn !Column
  | ValueExpectedDoubleColumn !Column
  | ValueExpectedArrayColumn !Column
  | ValueStringLengthMismatch !Int !Int
  | ValueListLengthMismatch !Int !Int
  | ValueEnumVariantMismatch !Int !(Boxed.Vector VariantSchema)
  | ValueNoMoreColumns
  | ValueLeftoverColumns !Encoding
    deriving (Eq, Ord, Show, Generic, Typeable)

instance Show Table where
  showsPrec =
    gshowsPrec

instance Show Column where
  showsPrec =
    gshowsPrec

tableOfMaybeValue :: Schema -> Maybe' Value -> Either TableError Table
tableOfMaybeValue schema = \case
  Nothing' ->
    pure $ defaultOfSchema schema
  Just' value ->
    tableOfValue schema value

tableOfValue :: Schema -> Value -> Either TableError Table
tableOfValue schema =
  case schema of
    BoolSchema -> \case
      BoolValue False ->
        pure $ singletonInt 0
      BoolValue True ->
        pure $ singletonInt 1
      value ->
        Left $ TableSchemaMismatch value schema

    Int64Schema -> \case
      Int64Value x ->
        pure . singletonInt $ fromIntegral x
      value ->
        Left $ TableSchemaMismatch value schema

    DoubleSchema -> \case
      DoubleValue x ->
        pure $ singletonDouble x
      value ->
        Left $ TableSchemaMismatch value schema

    StringSchema -> \case
      StringValue x ->
        pure . singletonString $ T.encodeUtf8 x
      value ->
        Left $ TableSchemaMismatch value schema

    DateSchema -> \case
      DateValue x ->
        pure . singletonInt . fromIntegral $ fromDay x
      value ->
        Left $ TableSchemaMismatch value schema

    ListSchema ischema -> \case
      ListValue xs -> do
        vs0 <- traverse (tableOfValue ischema) xs
        vs1 <- concatTables vs0
        pure . Table . Boxed.singleton $
          ArrayColumn (Storable.singleton . fromIntegral $ Boxed.length xs) vs1
      value ->
        Left $ TableSchemaMismatch value schema

    StructSchema fields -> \case
      StructValue values ->
        tableOfStruct fields values
      value ->
        Left $ TableSchemaMismatch value schema

    EnumSchema variant0 variants -> \case
      EnumValue tag x -> do
        VariantSchema _ variant <- maybeToRight (TableEnumVariantMismatch tag x variants) $ lookupVariant tag variant0 variants
        xtable <- tableOfValue variant x
        pure . Table $
          tableColumns (singletonInt $ fromIntegral tag) <>
          tableColumns xtable
      value ->
        Left $ TableSchemaMismatch value schema

tableOfStruct :: Boxed.Vector FieldSchema -> Boxed.Vector Value -> Either TableError Table
tableOfStruct fields values =
  if Boxed.null fields then
    pure $ singletonInt 0
  else if Boxed.length fields /= Boxed.length values then
    Left $ TableStructFieldsMismatch values fields
  else
    fmap (Table . Boxed.concatMap tableColumns) $
    Boxed.zipWithM tableOfValue (fmap fieldSchema fields) values

------------------------------------------------------------------------

valuesOfTable :: Schema -> Table -> Either ValueError (Boxed.Vector Value)
valuesOfTable schema table0 =
  withTable table0 $ takeValue schema

withTable :: Table -> EitherT ValueError (State Table) a -> Either ValueError a
withTable table0 m =
  let
    (result, table) =
      runState (runEitherT m) table0
  in
    if Boxed.null $ tableColumns table then
      result
    else
      Left . ValueLeftoverColumns $ encodingOfTable table

takeNext :: EitherT ValueError (State Table) Column
takeNext = do
  Table xs <- get
  case xs Boxed.!? 0 of
    Just x -> do
      put . Table $ Boxed.drop 1 xs
      pure x
    Nothing ->
      left ValueNoMoreColumns

takeByte :: EitherT ValueError (State Table) ByteString
takeByte =
  takeNext >>= \case
    ByteColumn xs ->
      pure xs
    x ->
      left $ ValueExpectedByteColumn x

takeInt :: EitherT ValueError (State Table) (Storable.Vector Int64)
takeInt =
  takeNext >>= \case
    IntColumn xs ->
      pure xs
    x ->
      left $ ValueExpectedIntColumn x

takeDouble :: EitherT ValueError (State Table) (Storable.Vector Double)
takeDouble =
  takeNext >>= \case
    DoubleColumn xs ->
      pure xs
    x ->
      left $ ValueExpectedDoubleColumn x

takeArray ::
  (Storable.Vector Int64 -> EitherT ValueError (State Table) a) ->
  EitherT ValueError (State Table) a
takeArray f =
  takeNext >>= \case
    ArrayColumn ns table0 ->
      hoistEither $ withTable table0 $ f ns
    x ->
      left $ ValueExpectedArrayColumn x

takeBool :: EitherT ValueError (State Table) (Boxed.Vector Bool)
takeBool =
  fmap (fmap (/= 0) . Boxed.convert) $ takeInt

takeValue :: Schema -> EitherT ValueError (State Table) (Boxed.Vector Value)
takeValue = \case
  BoolSchema ->
    fmap (fmap BoolValue) takeBool

  Int64Schema ->
    fmap (fmap (Int64Value . fromIntegral) . Boxed.convert) takeInt

  DoubleSchema ->
    fmap (fmap DoubleValue . Boxed.convert) takeDouble

  StringSchema ->
    takeArray $ \ns -> do
      bs <- takeByte
      fmap (fmap $ StringValue . T.decodeUtf8) . hoistEither $ restring ns bs

  DateSchema ->
    fmap (fmap (DateValue . toDay . fromIntegral) . Boxed.convert) takeInt

  ListSchema schema ->
    takeArray $ \ns -> do
      xs <- takeValue schema
      fmap (fmap ListValue) . hoistEither $ relist ns xs

  StructSchema fields ->
    if Boxed.null fields then do
      xs <- takeInt
      pure $ Boxed.replicate (Storable.length xs) (StructValue Boxed.empty)
    else do
      xss <- traverse (takeValue . fieldSchema) fields
      pure . fmap StructValue $ Boxed.transpose xss

  EnumSchema variant0 variants -> do
    tags <- takeInt
    -- TODO is this transpose correct?
    xss <- Boxed.transpose <$> traverse (takeValue . variantSchema) (Boxed.cons variant0 variants)

    let
      takeTag tag xs = do
        x <- maybeToRight (ValueEnumVariantMismatch tag $ Boxed.cons variant0 variants) $ xs Boxed.!? tag
        pure $ EnumValue tag x

    hoistEither $
      Boxed.zipWithM takeTag (fmap fromIntegral $ Boxed.convert tags) xss

restring :: Storable.Vector Int64 -> ByteString -> Either ValueError (Boxed.Vector ByteString)
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

relist :: Storable.Vector Int64 -> Boxed.Vector a -> Either ValueError (Boxed.Vector (Boxed.Vector a))
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

rowsOfTable :: Table -> Int
rowsOfTable (Table columns) =
  case columns Boxed.!? 0 of
    Nothing ->
      0
    Just (ByteColumn xs) ->
      B.length xs
    Just (IntColumn xs) ->
      Storable.length xs
    Just (DoubleColumn xs) ->
      Storable.length xs
    Just (ArrayColumn xs _) ->
      Storable.length xs

encodingOfTable :: Table -> Encoding
encodingOfTable =
  encodingOfColumns . Boxed.toList . tableColumns

encodingOfColumns :: [Column] -> Encoding
encodingOfColumns =
  Encoding . fmap encodingOfColumn

encodingOfColumn :: Column -> ColumnEncoding
encodingOfColumn = \case
  ByteColumn _ ->
    ByteEncoding
  IntColumn _ ->
    IntEncoding
  DoubleColumn _ ->
    DoubleEncoding
  ArrayColumn _ table ->
    ArrayEncoding $ encodingOfTable table

concatTables :: Boxed.Vector Table -> Either TableError Table
concatTables xss0 =
  let
    xss :: Boxed.Vector (Boxed.Vector Column)
    xss =
      coerce xss0

    yss =
      Boxed.transpose xss
  in
    fmap Table $
    traverse concatColumns yss

appendTables :: Table -> Table -> Either TableError Table
appendTables (Table xs) (Table ys) =
  Table <$> Boxed.zipWithM appendColumns xs ys

concatColumns :: Boxed.Vector Column -> Either TableError Column
concatColumns xs =
  if Boxed.null xs then
    Left TableCannotConcatEmpty
  else
    Boxed.fold1M' appendColumns xs

appendColumns :: Column -> Column -> Either TableError Column
appendColumns x y =
  case (x, y) of
    (ByteColumn xs, ByteColumn ys) ->
      pure $ ByteColumn (xs <> ys)

    (IntColumn xs, IntColumn ys) ->
      pure $ IntColumn (xs <> ys)

    (DoubleColumn xs, DoubleColumn ys) ->
      pure $ DoubleColumn (xs <> ys)

    (ArrayColumn n xs, ArrayColumn m ys) ->
      ArrayColumn (n <> m) <$>
      concatTables (Boxed.fromList [xs, ys])

    (_, _) ->
      Left $ TableAppendColumnsMismatch x y

splitAtTable :: Int -> Table -> (Table, Table)
splitAtTable i (Table fs) =
  let (as,bs) = Boxed.unzip $ Boxed.map (splitAtColumn i) fs
  in  (Table as, Table bs)

splitAtColumn :: Int -> Column -> (Column, Column)
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

emptyByte :: Table
emptyByte =
  Table . Boxed.singleton $ ByteColumn B.empty

emptyInt :: Table
emptyInt =
  Table . Boxed.singleton $ IntColumn Storable.empty

emptyDouble :: Table
emptyDouble =
  Table . Boxed.singleton $ DoubleColumn Storable.empty

emptyArray :: Table -> Table
emptyArray vs =
  Table . Boxed.singleton $ ArrayColumn Storable.empty vs

emptyOfSchema :: Schema -> Table
emptyOfSchema = \case
  BoolSchema ->
    emptyInt
  Int64Schema ->
    emptyInt
  DoubleSchema ->
    emptyDouble
  StringSchema ->
    emptyArray emptyByte
  DateSchema ->
    emptyInt
  ListSchema schema ->
    emptyArray $ emptyOfSchema schema
  StructSchema fields ->
    if Boxed.null fields then
      emptyInt
    else
      Table $ Boxed.concatMap (tableColumns . emptyOfSchema . fieldSchema) fields
  EnumSchema variant0 variants ->
    Table $
      tableColumns emptyInt <>
      Boxed.concatMap (tableColumns . emptyOfSchema . variantSchema) (Boxed.cons variant0 variants)

singletonInt :: Int64 -> Table
singletonInt =
  Table . Boxed.singleton . IntColumn . Storable.singleton

singletonDouble :: Double -> Table
singletonDouble =
  Table . Boxed.singleton . DoubleColumn . Storable.singleton

singletonString :: ByteString -> Table
singletonString bs =
  Table .
  Boxed.singleton $
  ArrayColumn
    (Storable.singleton . fromIntegral $ B.length bs)
    (Table . Boxed.singleton $ ByteColumn bs)

singletonEmptyList :: Table -> Table
singletonEmptyList =
  Table . Boxed.singleton . ArrayColumn (Storable.singleton 0)

defaultOfSchema :: Schema -> Table
defaultOfSchema = \case
  BoolSchema ->
    singletonInt 0
  Int64Schema ->
    singletonInt 0
  DoubleSchema ->
    singletonDouble 0
  StringSchema ->
    singletonString B.empty
  DateSchema ->
    singletonInt 0
  ListSchema schema ->
    singletonEmptyList $ emptyOfSchema schema
  StructSchema fields ->
    if Boxed.null fields then
      singletonInt 0
    else
      Table $ Boxed.concatMap (tableColumns . defaultOfSchema . fieldSchema) fields
  EnumSchema variant0 variants ->
    Table $
      tableColumns (singletonInt 0) <>
      Boxed.concatMap (tableColumns . defaultOfSchema . variantSchema) (Boxed.cons variant0 variants)

--defaultOfFieldSchema :: FieldSchema -> Table
--defaultOfFieldSchema = \case
--  FieldSchema RequiredField schema ->
--    defaultOfSchema schema
--  FieldSchema OptionalField schema ->
--    Table $ tableColumns (singletonInt 0) <> tableColumns (defaultOfSchema schema)
