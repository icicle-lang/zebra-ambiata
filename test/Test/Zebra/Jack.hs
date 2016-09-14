{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- * Zebra.Data.Block
    jBlock

  -- * Zebra.Data.Encoding
  , jEncoding
  , jFieldEncoding
  , jFieldName
  , jFieldObligation

  -- * Zebra.Data.Fact
  , jFact
  , jEntityId
  , jEntityHashId
  , jAttributeId
  , jAttributeName
  , jTime
  , jDay
  , jPriority
  , jValue

  -- * Zebra.Data.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Data.Index
  , jIndex
  , jTombstone

  -- * Zebra.Data.Table
  , jTable
  , jTable'
  , jColumn

  -- * Zebra.Data.Schema
  , jSchema
  , jFormat

  , jMaybe'
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import           Data.Thyme.Calendar (Year, Day, YearMonthDay(..), gregorianValid)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Corpus (muppets, southpark, boats)
import           Disorder.Jack (Jack, mkJack, shrinkTowards, sized)
import           Disorder.Jack (elements, arbitrary, chooseInt, sizedBounded)
import           Disorder.Jack (oneOf, oneOfRec, listOf, listOfN, justOf, maybeOf)

import           P

import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)

import           Zebra.Data.Block
import           Zebra.Data.Encoding
import           Zebra.Data.Entity
import           Zebra.Data.Fact
import           Zebra.Data.Index
import           Zebra.Data.Schema
import           Zebra.Data.Table


jSchema :: Jack Schema
jSchema =
  Schema <$> listOf jFormat

jFormat :: Jack Format
jFormat =
  oneOfRec [
      pure IntFormat
    , pure ByteFormat
    , pure DoubleFormat
    ] [
      ArrayFormat <$> jSchema
    ]

jEncoding :: Jack Encoding
jEncoding =
  oneOfRec [
      pure BoolEncoding
    , pure Int64Encoding
    , pure DoubleEncoding
    , pure StringEncoding
    , pure DateEncoding
    ] [
      StructEncoding . Boxed.fromList <$> listOfN 0 10 ((,) <$> jFieldName <*> jFieldEncoding)
    , ListEncoding <$> jEncoding
    ]

jFieldName :: Jack FieldName
jFieldName =
  FieldName <$> elements boats

jFieldEncoding :: Jack FieldEncoding
jFieldEncoding =
  FieldEncoding <$> jFieldObligation <*> jEncoding

jFieldObligation :: Jack FieldObligation
jFieldObligation =
  elements [RequiredField, OptionalField]

jFact :: Encoding -> AttributeId -> Jack Fact
jFact encoding aid =
  uncurry Fact
    <$> jEntityHashId
    <*> pure aid
    <*> jTime
    <*> jPriority
    <*> (strictMaybe <$> maybeOf (jValue encoding))

jValue :: Encoding -> Jack Value
jValue = \case
  BoolEncoding ->
    BoolValue <$> elements [False, True]
  Int64Encoding ->
    Int64Value <$> sizedBounded
  DoubleEncoding ->
    DoubleValue <$> arbitrary
  StringEncoding ->
    StringValue <$> arbitrary
  DateEncoding ->
    DateValue <$> jDay
  ListEncoding encoding ->
    ListValue . Boxed.fromList <$> listOfN 0 10 (jValue encoding)
  StructEncoding fields ->
    StructValue <$> traverse (jFieldValue . snd) fields

jFieldValue :: FieldEncoding -> Jack (Maybe' Value)
jFieldValue = \case
  FieldEncoding RequiredField encoding ->
    Just' <$> jValue encoding
  FieldEncoding OptionalField encoding ->
    strictMaybe <$> maybeOf (jValue encoding)

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
    <$> (Boxed.fromList <$> listOf jEntity)
    <*> (Unboxed.fromList <$> listOf jIndex)
    <*> (Boxed.fromList <$> listOf jTable)

jEntityHashId :: Jack (EntityHash, EntityId)
jEntityHashId =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> jEntityId

jEntity :: Jack Entity
jEntity =
  uncurry Entity
    <$> jEntityHashId
    <*> (Unboxed.fromList <$> listOf jAttribute)

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

jTable :: Jack Table
jTable =
  sized $ \size -> do
    n <- chooseInt (0, size)
    jTable' n

jTable' :: Int -> Jack Table
jTable' n =
  sized $ \size ->
    Table . Boxed.fromList <$> listOfN 0 (size `div` 10) (jColumn n)

jColumn :: Int -> Jack Column
jColumn n =
  oneOfRec [
      ByteColumn . B.pack <$> listOfN n n arbitrary
    , IntColumn . Storable.fromList <$> listOfN n n arbitrary
    , DoubleColumn . Storable.fromList <$> listOfN n n arbitrary
    ] [
      sized $ \m -> do
        ms <- listOfN n n $ chooseInt (0, m `div` 10)
        ArrayColumn (Storable.fromList . fmap fromIntegral $ ms) <$> jTable' (sum ms)
    ]

jMaybe' :: Jack a -> Jack (Maybe' a)
jMaybe' j =
  oneOfRec [ pure Nothing' ] [ Just' <$> j ]

