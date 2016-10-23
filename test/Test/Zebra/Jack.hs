{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- * Zebra.Data.Block
    jBlock
  , jBlockEntity
  , jBlockAttribute
  , jBlockIndex
  , jTombstone

  -- * Zebra.Data.Core
  , jEntityId
  , jEntityHashId
  , jAttributeId
  , jAttributeName
  , jTime
  , jDay
  , jPriority

  -- * Zebra.Data.Encoding
  , jEncoding
  , jFieldEncoding
  , jFieldName
  , jFieldObligation

  -- * Zebra.Data.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Data.Fact
  , jFact
  , jValue

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
import qualified Data.List as List
import           Data.Thyme.Calendar (Year, Day, YearMonthDay(..), gregorianValid)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Corpus (muppets, southpark, boats)
import           Disorder.Jack (Jack, mkJack, shrinkTowards, sized)
import           Disorder.Jack (elements, arbitrary, choose, chooseInt, sizedBounded)
import           Disorder.Jack (oneOf, oneOfRec, listOf, listOfN, vectorOf, justOf, maybeOf)

import           P

import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Encoding
import           Zebra.Data.Entity
import           Zebra.Data.Fact
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
  Priority <$> choose (0, 100000)

jBlock :: Jack Block
jBlock =
  Block
    <$> (Boxed.fromList <$> listOf jBlockEntity)
    <*> (Unboxed.fromList <$> listOf jBlockIndex)
    <*> (Boxed.fromList <$> listOf jTable)

jEntityHashId :: Jack (EntityHash, EntityId)
jEntityHashId =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> jEntityId

jBlockEntity :: Jack BlockEntity
jBlockEntity =
  uncurry BlockEntity
    <$> jEntityHashId
    <*> (Unboxed.fromList <$> listOf jBlockAttribute)

jBlockAttribute :: Jack BlockAttribute
jBlockAttribute =
  BlockAttribute
    <$> jAttributeId
    <*> chooseInt (0, 1000000)

jBlockIndex :: Jack BlockIndex
jBlockIndex =
  BlockIndex
    <$> jTime
    <*> jPriority
    <*> jTombstone

jEntity :: Jack Entity
jEntity =
  uncurry Entity
    <$> jEntityHashId
    <*> (Boxed.fromList <$> listOf jAttribute)

jAttribute :: Jack Attribute
jAttribute = do
  (ts, ps, bs) <- List.unzip3 <$> listOf ((,,) <$> jTime <*> jPriority <*> jTombstone)
  Attribute
    <$> pure (Storable.fromList ts)
    <*> pure (Storable.fromList ps)
    <*> pure (Storable.fromList bs)
    <*> jTable' (List.length ts)

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
    Table . Boxed.fromList <$> listOfN 1 (max 1 (size `div` 10)) (jColumn n)

jColumn :: Int -> Jack Column
jColumn n =
  oneOfRec [
      ByteColumn . B.pack <$> vectorOf n arbitrary
    , IntColumn . Storable.fromList <$> vectorOf n arbitrary
    , DoubleColumn . Storable.fromList <$> vectorOf n arbitrary
    ] [
      sized $ \m -> do
        ms <- vectorOf n $ chooseInt (0, m `div` 10)
        ArrayColumn (Storable.fromList . fmap fromIntegral $ ms) <$> jTable' (sum ms)
    ]

jMaybe' :: Jack a -> Jack (Maybe' a)
jMaybe' j =
  oneOfRec [ pure Nothing' ] [ Just' <$> j ]

