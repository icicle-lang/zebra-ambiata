{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- * Zebra.Time
    jTime
  , jDate
  , jTimeOfDay

  -- * Zebra.Table.Data
  , jField
  , jFieldName
  , jVariant
  , jVariantName

  -- * Zebra.Serial.Binary
  , jBinaryVersion

  -- * Zebra.Table.Schema
  , jTableSchema
  , jMapSchema
  , jColumnSchema
  , tableSchemaV0
  , columnSchemaV0
  , jExpandedTableSchema
  , jExpandedColumnSchema
  , jContractedTableSchema
  , jContractedColumnSchema

  -- * Zebra.Table.Striped
  , jSizedStriped
  , jStriped
  , jStripedArray
  , jStripedColumn

  -- * Zebra.Table.Logical
  , jSizedLogical
  , jSizedLogical1
  , jLogical
  , jLogicalValue

  , jMaybe'

  -- * Normalization
  , normalizeStriped
  , normalizeLogical
  , normalizeLogicalValue

  -- * x-disorder-jack
  , trippingBoth
  , withList
  , testEither
  , discardLeft
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Char as Char
import           Data.Functor.Identity (Identity(..))
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           Disorder.Corpus (boats, weather)
import           Disorder.Jack (Jack, Property, reshrink, sized)
import           Disorder.Jack (elements, arbitrary, choose, chooseInt, sizedBounded)
import           Disorder.Jack (oneOf, oneOfRec, listOfN, vectorOf, justOf, sublistOf)
import           Disorder.Jack ((===), boundedEnum, property, counterexample, shuffle, discard, suchThat)

import           P

import qualified Prelude as Savage

import           Test.QuickCheck.Instances ()

import           Text.Show.Pretty (ppShow)

import           Viking (Stream, Of)
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Serial.Binary.Data
import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped
import           Zebra.Time

------------------------------------------------------------------------

jDate :: Jack Date
jDate =
  justOf (rightToMaybe . fromDays <$> choose (toDays minBound, toDays maxBound))

jTime :: Jack Time
jTime =
  justOf (rightToMaybe . fromMicroseconds <$> choose (toMicroseconds minBound, toMicroseconds maxBound))

jTimeOfDay :: Jack TimeOfDay
jTimeOfDay =
  toTimeOfDay <$> choose (0, 24 * 60 * 60 * 1000000)

------------------------------------------------------------------------

jField :: Jack a -> Jack (Field a)
jField gen =
  Field <$> jFieldName <*> gen

jFieldName :: Jack FieldName
jFieldName =
  FieldName <$> elements boats

jVariant :: Jack a -> Jack (Variant a)
jVariant gen =
  Variant <$> jVariantName <*> gen

jVariantName :: Jack VariantName
jVariantName =
  VariantName <$> elements weather

------------------------------------------------------------------------

tableSchemaTables :: Schema.Table -> [Schema.Table]
tableSchemaTables = \case
  Schema.Binary _ _ ->
    []
  Schema.Array _ x ->
    columnSchemaTables x
  Schema.Map _ k v ->
    columnSchemaTables k <>
    columnSchemaTables v

tableSchemaColumns :: Schema.Table -> [Schema.Column]
tableSchemaColumns = \case
  Schema.Binary _ _ ->
    []
  Schema.Array _ x ->
    [x]
  Schema.Map _ k v ->
    [k, v]

columnSchemaTables :: Schema.Column -> [Schema.Table]
columnSchemaTables = \case
  Schema.Unit ->
    []
  Schema.Int _ _ ->
    []
  Schema.Double _ ->
    []
  Schema.Enum _ variants ->
    concatMap columnSchemaTables . fmap variantData $
      Cons.toList variants
  Schema.Struct _ fields ->
    concatMap columnSchemaTables . fmap fieldData $
      Cons.toList fields
  Schema.Nested table ->
    [table]
  Schema.Reversed schema ->
    columnSchemaTables schema

columnSchemaColumns :: Schema.Column -> [Schema.Column]
columnSchemaColumns = \case
  Schema.Unit ->
    []
  Schema.Int _ _ ->
    []
  Schema.Double _ ->
    []
  Schema.Enum _ variants ->
    fmap variantData $ Cons.toList variants
  Schema.Struct _ fields ->
    fmap fieldData $ Cons.toList fields
  Schema.Nested table ->
    tableSchemaColumns table
  Schema.Reversed schema ->
    [schema]

-- Strip a schema of features which can't be used in SchemaV0.
tableSchemaV0 :: Schema.Table -> Schema.Table
tableSchemaV0 = \case
  Schema.Binary _ _ ->
    Schema.Binary DenyDefault Encoding.Binary
  Schema.Array _ x ->
    Schema.Array DenyDefault $ columnSchemaV0 x
  Schema.Map _ k v ->
    Schema.Map DenyDefault (columnSchemaV0 k) (columnSchemaV0 v)

columnSchemaV0 :: Schema.Column -> Schema.Column
columnSchemaV0 = \case
  Schema.Unit ->
    Schema.Unit
  Schema.Int _ _ ->
    Schema.Int DenyDefault Encoding.Int
  Schema.Double _ ->
    Schema.Double DenyDefault
  Schema.Enum _ variants ->
    Schema.Enum DenyDefault $ fmap (fmap columnSchemaV0) variants
  Schema.Struct _ fields ->
    Schema.Struct DenyDefault $ fmap (fmap columnSchemaV0) fields
  Schema.Nested table ->
    Schema.Nested $ tableSchemaV0 table
  Schema.Reversed schema ->
    Schema.Reversed $ columnSchemaV0 schema

jTableSchema :: Jack Schema.Table
jTableSchema =
  reshrink tableSchemaTables $
  oneOfRec [
      Schema.Binary <$> jDefault <*> jBinaryEncoding
    ] [
      Schema.Array <$> jDefault <*> jColumnSchema
    , jMapSchema
    ]

jDefault :: Jack Default
jDefault =
  elements [
      DenyDefault
    , AllowDefault
    ]

jBinaryEncoding :: Jack Encoding.Binary
jBinaryEncoding =
  elements [
      Encoding.Binary
    , Encoding.Utf8
    ]

jMapSchema :: Jack Schema.Table
jMapSchema =
  Schema.Map <$> jDefault <*> jColumnSchema <*> jColumnSchema

jColumnSchema :: Jack Schema.Column
jColumnSchema =
  reshrink columnSchemaColumns $
  oneOfRec [
      Schema.Int <$> jDefault <*> jIntEncoding
    , Schema.Double <$> jDefault
    ] [
      Schema.Enum <$> jDefault <*> smallConsUniqueBy variantName (jVariant jColumnSchema)
    , Schema.Struct <$> jDefault <*> smallConsUniqueBy fieldName (jField jColumnSchema)
    , Schema.Nested <$> jTableSchema
    , Schema.Reversed <$> jColumnSchema
    ]

jIntEncoding :: Jack Encoding.Int
jIntEncoding =
  elements [
      Encoding.Int
    , Encoding.Date
    , Encoding.TimeSeconds
    , Encoding.TimeMilliseconds
    , Encoding.TimeMicroseconds
    ]

------------------------------------------------------------------------

jExpandedTableSchema :: Schema.Table -> Jack Schema.Table
jExpandedTableSchema = \case
  Schema.Binary def encoding ->
    pure $ Schema.Binary def encoding
  Schema.Array def x ->
    Schema.Array def <$> jExpandedColumnSchema x
  Schema.Map def k v ->
    Schema.Map def k <$> jExpandedColumnSchema v

jExpandedColumnSchema :: Schema.Column -> Jack Schema.Column
jExpandedColumnSchema = \case
  Schema.Unit ->
    pure Schema.Unit
  Schema.Int def encoding ->
    pure $ Schema.Int def encoding
  Schema.Double def ->
    pure $ Schema.Double def
  Schema.Enum def vs ->
    Schema.Enum def <$> traverse (traverse jExpandedColumnSchema) vs
  Schema.Struct def fs0 -> do
    fs1 <- Cons.toList <$> traverse (traverse jExpandedColumnSchema) fs0
    fs2 <- fmap2 (fmap (Schema.withDefaultColumn AllowDefault)) <$> listOfN 0 3 $ jField jColumnSchema

    let
      fs3 =
        ordNubBy (comparing fieldName) (fs1 <> fs2)

    fs4 <- shuffle fs3
    pure . Schema.Struct def $ Cons.unsafeFromList fs4
  Schema.Nested x ->
    Schema.Nested <$> jExpandedTableSchema x
  Schema.Reversed x ->
    Schema.Reversed <$> jExpandedColumnSchema x

jContractedTableSchema :: Schema.Table -> Jack Schema.Table
jContractedTableSchema = \case
  Schema.Binary def encoding ->
    pure $ Schema.Binary def encoding
  Schema.Array def x ->
    Schema.Array def <$> jContractedColumnSchema x
  Schema.Map def k v ->
    Schema.Map def k <$> jContractedColumnSchema v

jContractedColumnSchema :: Schema.Column -> Jack Schema.Column
jContractedColumnSchema = \case
  Schema.Unit ->
    pure Schema.Unit
  Schema.Int def encoding ->
    pure $ Schema.Int def encoding
  Schema.Double def ->
    pure $ Schema.Double def
  Schema.Enum def vs ->
    Schema.Enum def <$> traverse (traverse jContractedColumnSchema) vs
  Schema.Struct def fs0 -> do
    fs1 <- Cons.toList <$> traverse (traverse jContractedColumnSchema) fs0
    fs2 <- sublistOf fs1 `suchThat` (not . null)
    pure . Schema.Struct def $ Cons.unsafeFromList fs2
  Schema.Nested x ->
    Schema.Nested <$> jContractedTableSchema x
  Schema.Reversed x ->
    Schema.Reversed <$> jContractedColumnSchema x

------------------------------------------------------------------------

tableTables :: Striped.Table -> [Striped.Table]
tableTables = \case
  Striped.Binary _ _ _ ->
    []
  Striped.Array _ x ->
    columnTables x
  Striped.Map _ k v ->
    columnTables k <>
    columnTables v

tableColumns :: Striped.Table -> [Striped.Column]
tableColumns = \case
  Striped.Binary _ _ _ ->
    []
  Striped.Array _ x ->
    [x]
  Striped.Map _ k v ->
    [k, v]

columnTables :: Striped.Column -> [Striped.Table]
columnTables = \case
  Striped.Unit _ ->
    []
  Striped.Int _ _ _ ->
    []
  Striped.Double _ _ ->
    []
  Striped.Enum _ _ variants ->
    concatMap columnTables $
      fmap variantData (Cons.toList variants)
  Striped.Struct _ fields ->
    concatMap columnTables $
      fmap fieldData (Cons.toList fields)
  Striped.Nested _ table ->
    [table]
  Striped.Reversed column ->
    columnTables column

columnColumns :: Striped.Column -> [Striped.Column]
columnColumns = \case
  Striped.Unit _ ->
    []
  Striped.Int _ _ _ ->
    []
  Striped.Double _ _ ->
    []
  Striped.Enum _ _ variants ->
    fmap variantData $ Cons.toList variants
  Striped.Struct _ fields ->
    fmap fieldData $ Cons.toList fields
  Striped.Nested _ table ->
    tableColumns table
  Striped.Reversed column ->
    columnColumns column

jSizedStriped :: Jack Striped.Table
jSizedStriped =
  sized $ \size ->
    jStriped =<< chooseInt (0, size `div` 5)

jStriped :: Int -> Jack Striped.Table
jStriped n =
  reshrink tableTables $
  oneOfRec [
      jStripedBinary n
    ] [
      jStripedArray n
    , jStripedMap n
    ]

jByteString :: Int -> Jack ByteString
jByteString n =
  oneOf [
      Char8.pack
        <$> vectorOf n (fmap Char.chr $ chooseInt (Char.ord 'a', Char.ord 'z'))
    , ByteString.pack
        <$> vectorOf n boundedEnum
    ]

jUtf8 :: Int -> Jack ByteString
jUtf8 n =
  fmap (fixupUtf8 n) $
  oneOf [
      vectorOf n (fmap Char.chr $ chooseInt (Char.ord 'a', Char.ord 'z'))
    , vectorOf n (fmap Char.chr $ chooseInt (Char.ord minBound, Char.ord maxBound))
    ]

fixupUtf8 :: Int -> [Char] -> ByteString
fixupUtf8 n xs =
  let
    bs =
      Text.encodeUtf8 $ Text.pack xs

    m =
      ByteString.length bs
  in
    if m > n then
      fixupUtf8 n $ Savage.init xs -- sorry
    else
      -- pad with trash to make exactly 'n' bytes
      bs <> Char8.replicate (n - m) 'x'

jStripedBinary  :: Int -> Jack Striped.Table
jStripedBinary n = do
  encoding <- jBinaryEncoding
  case encoding of
    Encoding.Binary ->
      Striped.Binary <$> jDefault <*> pure encoding <*> jByteString n
    Encoding.Utf8 ->
      -- FIXME This will work out strangely for tests that have nested binary
      -- FIXME as we might get nonsense when we slice a utf-8 string, but the
      -- FIXME tests which generate striped tables don't care at the moment.
      Striped.Binary <$> jDefault <*> pure encoding <*> jUtf8 n

jStripedArray :: Int -> Jack Striped.Table
jStripedArray n = do
  Striped.Array
    <$> jDefault
    <*> jStripedColumn n

-- FIXME this constructs a corrupt table
jStripedMap :: Int -> Jack Striped.Table
jStripedMap n = do
  Striped.Map
    <$> jDefault
    <*> jStripedColumn n
    <*> jStripedColumn n

jStripedColumn :: Int -> Jack Striped.Column
jStripedColumn n =
  reshrink columnColumns $
  oneOfRec [
      jStripedInt n
    , jStripedDouble n
    ] [
      jStripedEnum n
    , jStripedStruct n
    , jStripedNested n
    , jStripedReversed n
    ]

jStripedInt :: Int -> Jack Striped.Column
jStripedInt n =
  Striped.Int
    <$> jDefault
    <*> jIntEncoding
    <*> (Storable.fromList <$> vectorOf n sizedBounded)

jStripedDouble :: Int -> Jack Striped.Column
jStripedDouble n =
  Striped.Double
    <$> jDefault
    <*> (Storable.fromList <$> vectorOf n arbitrary)

jStripedEnum :: Int -> Jack Striped.Column
jStripedEnum n = do
  sized $ \size -> do
    ntags <- chooseInt (1, 1 + (size `div` 10))
    tags <- Storable.fromList . fmap fromIntegral <$> vectorOf n (chooseInt (0, ntags - 1))
    vs <- Cons.unsafeFromList <$> vectorOf ntags (jVariant . jStripedColumn $ Storable.length tags)
    def <- jDefault
    pure $
      Striped.Enum def tags vs

jStripedStruct :: Int -> Jack Striped.Column
jStripedStruct n =
  Striped.Struct
    <$> jDefault
    <*> smallConsUniqueBy fieldName (jField (jStripedColumn n))

jStripedNested :: Int -> Jack Striped.Column
jStripedNested n =
  sized $ \size -> do
    ns <- Storable.fromList . fmap fromIntegral <$> vectorOf n (chooseInt (0, size `div` 10))
    Striped.Nested ns <$> jStriped (fromIntegral $ Storable.sum ns)

jStripedReversed :: Int -> Jack Striped.Column
jStripedReversed n =
  Striped.Reversed <$> jStripedColumn n

------------------------------------------------------------------------

jSizedLogical :: Schema.Table -> Jack Logical.Table
jSizedLogical schema =
  sized $ \size ->
    jLogical schema =<< chooseInt (0, size `div` 5)

jSizedLogical1 :: Schema.Table -> Jack Logical.Table
jSizedLogical1 schema =
  sized $ \size ->
    jLogical schema =<< chooseInt (1, max 1 (size `div` 5))

jLogical :: Schema.Table -> Int -> Jack Logical.Table
jLogical tschema n =
  case tschema of
    Schema.Binary _ Encoding.Binary ->
      Logical.Binary <$> jByteString n
    Schema.Binary _ Encoding.Utf8 ->
      Logical.Binary <$> jUtf8 n
    Schema.Array _ x ->
      Logical.Array . Boxed.fromList <$> vectorOf n (jLogicalValue x)
    Schema.Map _ k v ->
      Logical.Map . Map.fromList <$> vectorOf n (jMapping k v)

jMapping :: Schema.Column -> Schema.Column -> Jack (Logical.Value, Logical.Value)
jMapping k v =
  (,) <$> jLogicalValue k <*> jLogicalValue v

jTag :: Cons Boxed.Vector (Variant a) -> Jack Tag
jTag xs =
  fromIntegral <$> choose (0, Cons.length xs - 1)

jLogicalValue :: Schema.Column -> Jack Logical.Value
jLogicalValue = \case
  Schema.Unit ->
    pure Logical.Unit

  Schema.Int _ Encoding.Int ->
    Logical.Int <$> sizedBounded

  Schema.Int _ Encoding.Date ->
    Logical.Int . Encoding.encodeDate <$> jDate

  Schema.Int _ Encoding.TimeSeconds ->
    Logical.Int . Encoding.encodeTimeSeconds <$> jTime

  Schema.Int _ Encoding.TimeMilliseconds ->
    Logical.Int . Encoding.encodeTimeMilliseconds <$> jTime

  Schema.Int _ Encoding.TimeMicroseconds ->
    Logical.Int . Encoding.encodeTimeMicroseconds <$> jTime

  Schema.Double _ ->
    Logical.Double <$> arbitrary

  Schema.Enum _ variants -> do
    tag <- jTag variants
    case lookupVariant tag variants of
      Nothing ->
        Savage.error $ renderTagLookupError tag variants
      Just (Variant _ schema) ->
        Logical.Enum tag <$> jLogicalValue schema

  Schema.Struct _ fields ->
    Logical.Struct <$> traverse (jLogicalValue . fieldData) fields

  Schema.Nested tschema ->
    sized $ \size -> do
      fmap Logical.Nested $ jLogical tschema =<< chooseInt (0, size `div` 10)

  Schema.Reversed schema ->
    Logical.Reversed <$> jLogicalValue schema

renderTagLookupError :: Show a => Tag -> Cons Boxed.Vector (Variant a) -> [Char]
renderTagLookupError tag variants =
  "jLogicalValue: internal error, tag not found" <>
  "\n" <>
  "\n  tag = " <> show tag <>
  "\n" <>
  "\n  variants =" <>
  (List.concatMap ("\n    " <>) . List.lines $ ppShow variants) <>
  "\n"

jBinaryVersion :: Jack BinaryVersion
jBinaryVersion =
  elements [BinaryV3]

jMaybe' :: Jack a -> Jack (Maybe' a)
jMaybe' j =
  oneOfRec [ pure Nothing' ] [ Just' <$> j ]

smallConsUniqueBy :: Ord b => (a -> b) -> Jack a -> Jack (Cons Boxed.Vector a)
smallConsUniqueBy f gen =
  sized $ \n ->
    Cons.unsafeFromList . ordNubBy (comparing f) <$> listOfN 1 (1 + (n `div` 10)) gen

------------------------------------------------------------------------

normalizeStriped :: Striped.Table -> Striped.Table
normalizeStriped table =
  let
    Right x =
      Striped.fromLogical (Striped.schema table) . normalizeLogical =<<
      Striped.toLogical table
  in
    x

normalizeLogical :: Logical.Table -> Logical.Table
normalizeLogical = \case
  Logical.Binary bs ->
    Logical.Binary $ ByteString.sort bs
  Logical.Array xs ->
    Logical.Array . Boxed.fromList . List.sort $ Boxed.toList xs
  Logical.Map kvs ->
    Logical.Map $ fmap normalizeLogicalValue kvs

normalizeLogicalValue :: Logical.Value -> Logical.Value
normalizeLogicalValue = \case
  Logical.Unit ->
    Logical.Unit
  Logical.Int x ->
    Logical.Int x
  Logical.Double x ->
    Logical.Double x
  Logical.Enum tag x ->
    Logical.Enum tag (normalizeLogicalValue x)
  Logical.Struct xs ->
    Logical.Struct $ fmap normalizeLogicalValue xs
  Logical.Nested x ->
    Logical.Nested $ normalizeLogical x
  Logical.Reversed x ->
    Logical.Reversed $ normalizeLogicalValue x

------------------------------------------------------------------------

trippingBoth :: (Monad m, Show (m a), Show (m b), Eq (m a)) => (a -> m b) -> (b -> m a) -> a -> Property
trippingBoth to from x =
  let
    original =
      pure x

    intermediate =
      to x

    roundtrip =
      from =<< intermediate
  in
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample "=== Intermediate ===" .
    counterexample (ppShow intermediate) .
    counterexample "" $
      property (original === roundtrip)

withList :: (Stream (Of a) Identity () -> Stream (Of b) (EitherT x Identity) ()) -> [a] -> Either x [b]
withList f =
  runIdentity . runEitherT . Stream.toList_ . f . Stream.each

testEither :: Show a => Either a Property -> Property
testEither =
  either (flip counterexample False . ppShow) property

discardLeft :: Either x a -> a
discardLeft = \case
  Left _ ->
    discard
  Right a ->
    a
