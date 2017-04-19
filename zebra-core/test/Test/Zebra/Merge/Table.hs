{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Merge.Table where

import           Data.Functor.Identity (runIdentity)
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
  Right x <- Striped.fromLogical schema <$> jSizedLogical schema
  pure x

jFile :: Schema.Table -> Jack (NonEmpty Striped.Table)
jFile schema = do
  NonEmpty.fromList <$> listOfN 1 10 (jFileTable schema)

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
  case runIdentity . runEitherT . Stream.toList . Merge.unionStriped $ fmap Stream.each xss0 of
    Left (UnionLogicalMergeError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right (xs0 :> ()) ->
      case Cons.fromList xs0 of
        Nothing ->
          Left "Union returned empty stream"
        Just xs ->
          case Striped.merges xs of
            Left (StripedLogicalMergeError _) ->
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
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

return []
tests :: IO Bool
tests =
  $quickCheckAll
