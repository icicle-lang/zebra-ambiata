{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Merge.Table where

import           Data.List.NonEmpty (NonEmpty(..))
import           Data.String (String)
import qualified Data.Vector as Boxed

import           Disorder.Jack (Property, Jack, quickCheckAll)
import           Disorder.Jack ((===), (==>), gamble, counterexample, oneOf, listOfN, sized, chooseInt)

import           P

import           System.IO (IO)
import           System.IO.Unsafe (unsafePerformIO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Merge.Table (UnionTableError(..))
import qualified Zebra.Merge.Table as Merge
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError(..))
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.Stream (Of(..))
import qualified Zebra.X.Stream as Stream

jFileTable :: Schema.Table -> Jack Striped.Table
jFileTable schema = do
  sized $ \size -> do
    n <- chooseInt (0, 20 * (size `div` 5))
    Right x <- Striped.fromLogical schema <$> jLogical schema n
    pure x

jSplits :: Striped.Table -> Jack (NonEmpty Striped.Table)
jSplits x =
  let
    n =
      Striped.length x
  in
    if n == 0 then
      pure $ x :| []
    else do
      ix <- chooseInt (1, min n 20)

      let
        (y, z) =
          Striped.splitAt ix x

      ys <- jSplits z
      pure $ y :| toList ys

jFile :: Schema.Table -> Jack (NonEmpty Striped.Table)
jFile schema = do
  jSplits =<< jFileTable schema

jModSchema :: Schema.Table -> Jack Schema.Table
jModSchema schema =
  oneOf [
      pure schema
    , jExpandedTableSchema schema
    , jContractedTableSchema schema
    ]

unionSimple :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either String (Maybe Striped.Table)
unionSimple xss0 =
  case Striped.merges =<< traverse Striped.merges (fmap Cons.fromNonEmpty xss0) of
    Left (StripedLogicalMergeError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right x ->
      pure $ pure x

unionList :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either String (Maybe Striped.Table)
unionList xss0 =
  case unsafePerformIO . runEitherT . Stream.toList . Merge.unionStriped $ fmap Stream.each xss0 of
    Left (UnionLogicalMergeError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right (xs0 :> ()) ->
      case Cons.fromList xs0 of
        Nothing ->
          Left "Union returned empty stream"
        Just xs ->
          case Striped.unsafeConcat xs of
            Left err ->
              Left $ ppShow err
            Right x ->
              pure $ pure x

prop_union_identity :: Property
prop_union_identity =
  gamble jMapSchema $ \schema ->
  gamble (jFile schema) $ \file0 ->
  either (flip counterexample False) id $ do
    let
      files =
        Cons.from2 file0 (Striped.empty schema :| [])

      Right file =
        Striped.unsafeConcat $
        Cons.fromNonEmpty file0

    x <- first ppShow $ unionList files
    pure $
      Just (normalizeStriped file)
      ===
      fmap normalizeStriped x

prop_union_files_same_schema :: Property
prop_union_files_same_schema =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList files
    pure $
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

prop_union_files_diff_schema :: Property
prop_union_files_diff_schema =
  counterexample "=== Schema ===" $
  gamble jMapSchema $ \schema ->
  counterexample "=== Modified Schemas ===" $
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jModSchema schema)) $ \schemas ->
  isRight (Cons.fold1M' Schema.union schemas) ==>
  counterexample "=== Files ===" $
  gamble (traverse jFile schemas) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList files
    pure $
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

return []
tests :: IO Bool
tests =
  $quickCheckAll
