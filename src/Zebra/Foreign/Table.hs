{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.Foreign.Table (
    CTable(..)
  , tableOfForeign
  , foreignOfTable

  , peekTable
  , pokeTable
  , peekColumn
  , pokeColumn
  ) where

import           Anemone.Foreign.Mempool (Mempool, alloc, calloc)

import           Control.Monad.IO.Class (MonadIO(..))

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable

import           Foreign.Ptr (Ptr)

import           P

import           X.Control.Monad.Trans.Either (EitherT, left)

import           Zebra.Data.Table
import           Zebra.Foreign.Bindings
import           Zebra.Foreign.Util


newtype CTable =
  CTable {
      unCTable :: Ptr C'zebra_table
    }

tableOfForeign :: MonadIO m => CTable -> EitherT ForeignError m Table
tableOfForeign (CTable c_table) =
  peekTable c_table

foreignOfTable :: MonadIO m => Mempool -> Table -> m CTable
foreignOfTable pool table = do
  c_table <- liftIO $ alloc pool
  pokeTable pool c_table table
  pure $ CTable c_table

peekTable :: MonadIO m => Ptr C'zebra_table -> EitherT ForeignError m Table
peekTable c_table = do
  n_rows <- fromIntegral <$> peekIO (p'zebra_table'row_count c_table)
  n_cap <- fromIntegral <$> peekIO (p'zebra_table'row_capacity c_table)
  -- Make sure tests fail if the capacity is wrong
  when (n_cap < n_rows) $ left ForeignTableNotEnoughCapacity

  n_cols <- fromIntegral <$> peekIO (p'zebra_table'column_count c_table)
  c_columns <- peekIO (p'zebra_table'columns c_table)

  fmap (Table n_rows) . peekMany c_columns n_cols $ peekColumn n_rows

pokeTable :: MonadIO m => Mempool -> Ptr C'zebra_table -> Table -> m ()
pokeTable pool c_table (Table n_rows columns) = do
  let
    n_cols =
      Boxed.length columns

  c_columns <- liftIO . calloc pool $ fromIntegral n_cols

  pokeIO (p'zebra_table'row_count c_table) $ fromIntegral n_rows
  pokeIO (p'zebra_table'row_capacity c_table) $ fromIntegral n_rows
  pokeIO (p'zebra_table'column_count c_table) $ fromIntegral n_cols
  pokeIO (p'zebra_table'columns c_table) c_columns

  pokeMany c_columns columns $ pokeColumn pool

peekColumn :: MonadIO m => Int -> Ptr C'zebra_column -> EitherT ForeignError m Column
peekColumn n_rows c_column = do
  typ <- peekIO (p'zebra_column'type c_column)
  case typ of
    C'ZEBRA_BYTE ->
      ByteColumn
        <$> peekByteString n_rows (p'zebra_data'b $ p'zebra_column'data c_column)

    C'ZEBRA_INT ->
      IntColumn
        <$> peekVector n_rows (p'zebra_data'i $ p'zebra_column'data c_column)

    C'ZEBRA_DOUBLE ->
      DoubleColumn
        <$> peekVector n_rows (p'zebra_data'd $ p'zebra_column'data c_column)

    C'ZEBRA_ARRAY ->
      ArrayColumn
        <$> peekOffsetsVector n_rows (p'zebra_data'a'n $ p'zebra_column'data c_column)
        <*> peekTable (p'zebra_data'a'table $ p'zebra_column'data c_column)

    _ ->
      left ForeignInvalidColumnType

pokeColumn :: MonadIO m => Mempool -> Ptr C'zebra_column -> Column -> m ()
pokeColumn pool c_column = \case
  ByteColumn bs -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_BYTE
    pokeByteString pool (p'zebra_data'b $ p'zebra_column'data c_column) bs

  IntColumn xs -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_INT
    pokeVector pool (p'zebra_data'i $ p'zebra_column'data c_column) xs

  DoubleColumn xs -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_DOUBLE
    pokeVector pool (p'zebra_data'd $ p'zebra_column'data c_column) xs

  ArrayColumn ns table -> do
    pokeIO (p'zebra_column'type c_column) C'ZEBRA_ARRAY
    pokeOffsetsVector pool (p'zebra_data'a'n $ p'zebra_column'data c_column) ns
    pokeTable pool (p'zebra_data'a'table $ p'zebra_column'data c_column) table


peekOffsetsVector :: forall m. (MonadIO m) => Int -> Ptr (Ptr Int64) -> m (Storable.Vector Int64)
peekOffsetsVector n_rows p
 | n_rows == 0
 = return Storable.empty
 | otherwise
 = do ns <- peekVector (n_rows + 1) p
      return $ Storable.zipWith (-) (Storable.tail ns) ns

pokeOffsetsVector :: forall m. (MonadIO m) => Mempool -> Ptr (Ptr Int64) -> Storable.Vector Int64 -> m ()
pokeOffsetsVector pool p ns
 = pokeVector pool p (Storable.scanl (+) 0 ns)
