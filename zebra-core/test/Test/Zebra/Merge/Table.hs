{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Merge.Table where

import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import           Data.String (String)
import qualified Data.Vector as Boxed

import           Disorder.Jack (Property, Jack, quickCheckAll)
import           Disorder.Jack ((===), gamble, counterexample, listOfN)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Merge.Table (UnionTableError(..))
import qualified Zebra.Merge.Table as Striped
import           Zebra.Table.Schema (Schema.Table)
import           Zebra.Table.Striped (Table, TableError(..))
import qualified Zebra.Table.Striped as Striped

jFileTable :: Schema.Table -> Jack Striped.Table
jFileTable schema = do
  Right x <- Striped.fromCollection schema <$> jSizedCollection schema
  pure x

jFile :: Schema.Table -> Jack (NonEmpty Striped.Table)
jFile schema = do
  NonEmpty.fromList <$> listOfN 1 10 (jFileTable schema)

unionSimple :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either String (Maybe Striped.Table)
unionSimple xss0 =
  case Striped.unions =<< traverse Striped.unions (fmap Cons.fromNonEmpty xss0) of
    Left (TableValueUnionError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right x ->
      pure $ pure x

unionList :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either String (Maybe Striped.Table)
unionList xss0 =
  case Striped.unionList xss0 of
    Left (UnionValueUnionError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right xs ->
      case Striped.unions (Cons.fromNonEmpty xs) of
        Left (TableValueUnionError _) ->
          pure Nothing
        Left err ->
          Left $ ppShow err
        Right x ->
          pure $ pure x

prop_union_files :: Property
prop_union_files =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList files
    pure $
      fmap normalizeTable x
      ===
      fmap normalizeTable y

return []
tests :: IO Bool
tests =
  $quickCheckAll
