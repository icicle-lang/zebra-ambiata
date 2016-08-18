{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.File where

import qualified Data.Binary.Get as Get
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import           Disorder.Jack
import           Disorder.Core.Run

import           P

import           System.IO (IO)

import           Test.QuickCheck.Instances ()
import           Test.Zebra.Util

import           Zebra.Serial.Array
import           Zebra.Serial.File

jSplits :: Jack (Boxed.Vector B.ByteString)
jSplits = arbitrary

checkRunStreamMany :: (Show a, Eq a) => Get.Get a -> Stream.Stream Stream.Id B.ByteString -> Property
checkRunStreamMany get bss =
  let bs = fold $ Stream.listOfStream bss
      one = runGetEitherConsumeAll (many get) (BL.fromStrict bs)
      alls = listErrs $ Stream.listOfStream $ runStreamMany get bss
  in  first (const ()) one === first (const ()) alls

  where
   listErrs [] = Right []
   listErrs (Left e : _) = Left e
   listErrs (Right r : rs)
    = case listErrs rs of
       Right rs' -> Right (r : rs')
       Left e    -> Left e

checkRunStreamOne :: (Show a, Eq a) => Get.Get a -> Stream.Stream Stream.Id B.ByteString -> Property
checkRunStreamOne get bss =
  let bs = fold $ Stream.listOfStream bss
      expect = Get.runGetOrFail get (BL.fromStrict bs)
      actual = Stream.unId $ runStreamOne get bss
  in  getExpect expect === getActual actual

  where
   getExpect (Left  (leftovers, _, _)) = Left  (BL.toStrict leftovers)
   getExpect (Right (leftovers, _, val)) = Right (BL.toStrict leftovers, val)

   getActual (Left    _, leftovers)      = Left  (fold $ Stream.listOfStream leftovers)
   getActual (Right val, leftovers)      = Right (fold $ Stream.listOfStream leftovers, val)

prop_runStreamOne_getWord64be :: Property
prop_runStreamOne_getWord64be =
  gamble jSplits $ \bss ->
    checkRunStreamOne Get.getWord64be (Stream.streamOfVector bss)

prop_runStreamOne_getByteArray :: Property
prop_runStreamOne_getByteArray =
  gamble jSplits $ \bss ->
    checkRunStreamOne getByteArray (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamOne_getByteArray_filtered :: Property
prop_runStreamOne_getByteArray_filtered =
  gamble jSplits $ \bss ->
    checkRunStreamOne getByteArray (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)


prop_runStreamOne_getWordArray_prefix_size :: Property
prop_runStreamOne_getWordArray_prefix_size =
  gamble jSplits $ \bss ->
    checkRunStreamOne get (Stream.streamOfVector bss)
  where
   get = do
    i <- Get.getWord32be
    getWordArray (fromIntegral i)

prop_runStreamOne_getWordArray_static_size :: Property
prop_runStreamOne_getWordArray_static_size =
  gamble jSplits $ \bss ->
  gamble arbitrary $ \num ->
    checkRunStreamOne (getWordArray $ abs num) (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamOne_getStrings_filtered :: Property
prop_runStreamOne_getStrings_filtered =
  gamble jSplits $ \bss ->
  gamble (chooseInt (0,100)) $ \num ->
    checkRunStreamOne (getStrings num) (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)

prop_runStreamMany_getWord64be :: Property
prop_runStreamMany_getWord64be =
  gamble jSplits $ \bss ->
    checkRunStreamMany Get.getWord64be (Stream.streamOfVector bss)

prop_runStreamMany_getByteArray :: Property
prop_runStreamMany_getByteArray =
  gamble jSplits $ \bss ->
    checkRunStreamMany getByteArray (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamMany_getByteArray_filtered :: Property
prop_runStreamMany_getByteArray_filtered =
  gamble jSplits $ \bss ->
    checkRunStreamMany getByteArray (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)


prop_runStreamMany_getWordArray_prefix_size :: Property
prop_runStreamMany_getWordArray_prefix_size =
  gamble jSplits $ \bss ->
    checkRunStreamMany get (Stream.streamOfVector bss)
  where
   get = do
    i <- Get.getWord32be
    getWordArray (fromIntegral i)

prop_runStreamMany_getWordArray_static_size :: Property
prop_runStreamMany_getWordArray_static_size =
  gamble jSplits $ \bss ->
  gamble arbitrary $ \num ->
    checkRunStreamMany (getWordArray $ abs num) (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamMany_getStrings_filtered :: Property
prop_runStreamMany_getStrings_filtered =
  gamble jSplits $ \bss ->
  gamble (chooseInt (0,100)) $ \num ->
    checkRunStreamMany (getStrings num) (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)

return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal


