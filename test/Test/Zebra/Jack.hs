{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- * Zebra.Data.Block
    jBlock

  -- * Zebra.Data.Fact
  , jEntityId
  , jAttributeId
  , jAttributeName
  , jTime
  , jPriority

  -- * Zebra.Data.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Data.Index
  , jIndex
  , jTombstone

  -- * Zebra.Data.Record
  , jRecord
  , jRecord'
  , jField

  -- * Zebra.Data.Schema
  , jSchema
  , jFormat
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import           Data.Thyme.Calendar (Year, Day, YearMonthDay(..), gregorianValid)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Corpus (muppets, southpark)
import           Disorder.Jack (Jack, mkJack, shrinkTowards, sized)
import           Disorder.Jack (elements, arbitrary, chooseInt)
import           Disorder.Jack (oneOf, oneOfRec, listOf, justOf, listOfN)

import           P

import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)

import           Zebra.Data.Block
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Data.Index
import           Zebra.Data.Record
import           Zebra.Data.Schema


jSchema :: Jack Schema
jSchema =
  Schema <$> listOf jFormat

jFormat :: Jack Format
jFormat =
  oneOfRec [
      pure WordFormat
    , pure ByteFormat
    , pure DoubleFormat
    ] [
      ListFormat <$> jSchema
    ]

jEntityId :: Jack EntityId
jEntityId =
  let
    mkEnt name num =
      EntityId $ name <> Char8.pack (printf "-%03d" num)
  in
    mkEnt
      <$> elements southpark
      <*> chooseInt (0, 999)

jAttributeId :: Jack AttributeId
jAttributeId =
  AttributeId <$> chooseInt (0, 10000)

jAttributeName :: Jack AttributeName
jAttributeName =
  AttributeName <$> oneOf [elements muppets, arbitrary]

jTime :: Jack Time
jTime =
  fromDay <$> jDay

jDay :: Jack Day
jDay =
  justOf . fmap gregorianValid $
    YearMonthDay
      <$> jYear
      <*> chooseInt (1, 12)
      <*> chooseInt (1, 31)

jYear :: Jack Year
jYear =
  mkJack (shrinkTowards 2000) $ QC.choose (1600, 3000)

jPriority :: Jack Priority
jPriority =
  Priority <$> chooseInt (0, 100000)

jBlock :: Jack Block
jBlock =
  Block
    <$> jTime
    <*> (Boxed.fromList <$> listOf jEntity)
    <*> (Unboxed.fromList <$> listOf jIndex)
    <*> (Boxed.fromList <$> listOf jRecord)

jEntity :: Jack Entity
jEntity =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10

    mkEntity eid =
      Entity (hash eid) eid
  in
    mkEntity
      <$> jEntityId
      <*> (Boxed.fromList <$> listOf jAttribute)

jAttribute :: Jack Attribute
jAttribute =
  Attribute
    <$> jAttributeId
    <*> chooseInt (0, 1000000)

jIndex :: Jack Index
jIndex =
  Index
    <$> jTime
    <*> jPriority
    <*> jTombstone

jTombstone :: Jack Tombstone
jTombstone =
  elements [
      NotTombstone
    , Tombstone
    ]

jRecord :: Jack Record
jRecord =
  sized $ \size -> do
    n <- chooseInt (0, size)
    jRecord' n

jRecord' :: Int -> Jack Record
jRecord' n =
  sized $ \size ->
    Record . Boxed.fromList <$> listOfN 0 (size `div` 10) (jField n)

jField :: Int -> Jack Field
jField n =
  oneOfRec [
      ByteField . B.pack <$> listOfN n n arbitrary
    , WordField . Storable.fromList <$> listOfN n n arbitrary
    , DoubleField . Storable.fromList <$> listOfN n n arbitrary
    ] [
      sized $ \m -> do
        ms <- listOfN n n $ chooseInt (0, m `div` 10)
        ListField (Storable.fromList . fmap fromIntegral $ ms) <$> jRecord' (sum ms)
    ]
