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


return []
tests :: IO Bool
tests = $disorderCheckEnvAll TestRunNormal


