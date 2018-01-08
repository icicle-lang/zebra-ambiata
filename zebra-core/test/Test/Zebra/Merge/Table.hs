{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Merge.Table where

import           Data.Functor.Identity (runIdentity)
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.String (String)
import qualified Data.Vector as Boxed

import           Disorder.Jack (Property, Jack, quickCheckAll)
import           Disorder.Jack ((===), (==>), gamble, counterexample, oneOf, listOfN, sized, chooseInt)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Viking.Stream (Of(..))
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Merge.Table (UnionTableError(..))
import qualified Zebra.Merge.Table as Merge
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError(..))
import qualified Zebra.Table.Striped as Striped

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

unionList ::
      Merge.Monoidal a
   -> Maybe Merge.MaximumRowSize
   -> Cons Boxed.Vector (NonEmpty Striped.Table)
   -> Either String (Maybe Striped.Table)
unionList m msize xss0 =
  case runIdentity . runEitherT . Stream.toList . Merge.unionStriped m msize $ fmap Stream.each xss0 of
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

    x <- first ppShow $ unionList Merge.valueMonoid Nothing files
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
    y <- first ppShow $ unionList Merge.valueMonoid Nothing files
    pure $
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

prop_union_files_empty :: Property
prop_union_files_empty =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionList Merge.valueMonoid (Just (Merge.MaximumRowSize (-1))) files
    pure $
      Just (Striped.empty schema) === x

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
    y <- first ppShow $ unionList Merge.valueMonoid Nothing files
    pure $
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

prop_measure_empty :: Property
prop_measure_empty =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionList Merge.measureMonoid (Just (Merge.MaximumRowSize (-1))) files
    let
      Right schema0 =
        Merge.monoidSchema Merge.measureMonoid schema
    pure $
      Just (Striped.empty schema0) === x

prop_measure_commutative :: Property
prop_measure_commutative =
  gamble jMapSchema $ \schema ->
  gamble (jFile schema) $ \file0 ->
  gamble (jFile schema) $ \file1 ->
  either (flip counterexample False) id $ do
    let
      files0 =
        Cons.from2 file0 file1

      files1 =
        Cons.from2 file1 file0

    x0 <- first ppShow $ unionList Merge.measureMonoid Nothing files0
    x1 <- first ppShow $ unionList Merge.measureMonoid Nothing files1

    pure $
      x0
      ===
      x1

prop_measure_associative :: Property
prop_measure_associative =
  gamble jMapSchema $ \schema ->
  gamble (jFile schema) $ \file0 ->
  gamble (jFile schema) $ \file1 ->
  gamble (jFile schema) $ \file2 ->
  either (flip counterexample False) id $ do
    let
      files01 =
        Cons.from2 file0 file1

      files2 =
        Cons.from2 file2 (Striped.empty schema :| [])

      files12 =
        Cons.from2 file1 file2

      files0 =
        Cons.from2 file0 (Striped.empty schema :| [])

    Just x01 <- first ppShow $ unionList Merge.measureMonoid Nothing files01
    Just x2  <- first ppShow $ unionList Merge.measureMonoid Nothing files2
    Just x12 <- first ppShow $ unionList Merge.measureMonoid Nothing files12
    Just x0  <- first ppShow $ unionList Merge.measureMonoid Nothing files0

    let
      y0 =
        Cons.from2 (x01 :| []) (x2 :| [])

      y1 =
        Cons.from2 (x0 :| []) (x12 :| [])

    measure0 <- first ppShow $ unionList Merge.summationMonoid Nothing y0
    measure1 <- first ppShow $ unionList Merge.summationMonoid Nothing y1

    pure $
      measure0
      ===
      measure1

return []
tests :: IO Bool
tests =
  $quickCheckAll
