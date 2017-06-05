{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Time where

import qualified Data.ByteString.Char8 as Char8
import qualified Data.Time as Time

import           Disorder.Jack

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Time


prop_roundtrip_date_render :: Property
prop_roundtrip_date_render =
  gamble jDate $
    tripping renderDate parseDate

prop_roundtrip_date_days :: Property
prop_roundtrip_date_days =
  gamble jDate $
    tripping toDays fromDays

prop_roundtrip_date_calendar :: Property
prop_roundtrip_date_calendar =
  gamble jDate $
    tripping toCalendarDate fromCalendarDate

prop_roundtrip_time_render :: Property
prop_roundtrip_time_render =
  gamble jTime $
    tripping renderTime parseTime

prop_roundtrip_time_seconds :: Property
prop_roundtrip_time_seconds =
  gamble jTime $ \time0 ->
  let
    Right time =
      fromSeconds (toSeconds time0)
  in
    tripping toSeconds fromSeconds time

prop_roundtrip_time_milliseconds :: Property
prop_roundtrip_time_milliseconds =
  gamble jTime $ \time0 ->
  let
    Right time =
      fromMilliseconds (toMilliseconds time0)
  in
    tripping toMilliseconds fromMilliseconds time

prop_roundtrip_time_microseconds :: Property
prop_roundtrip_time_microseconds =
  gamble jTime $
    tripping toMicroseconds fromMicroseconds

prop_roundtrip_time_calendar :: Property
prop_roundtrip_time_calendar =
  gamble jTime $
    tripping toCalendarTime fromCalendarTime

prop_roundtrip_time_of_day_microsecond :: Property
prop_roundtrip_time_of_day_microsecond =
  gamble jTimeOfDay $
    tripping fromTimeOfDay (Just . toTimeOfDay)

epoch :: Time.UTCTime
epoch =
  Time.UTCTime (Time.fromGregorian 1600 3 1) 0

prop_compare_date_parsing :: Property
prop_compare_date_parsing =
  gamble jDate $ \date ->
    let
      str =
        renderDate date

      theirs :: Maybe Time.Day
      theirs =
        Time.parseTimeM False Time.defaultTimeLocale "%Y-%m-%d" $
        Char8.unpack str

      theirs_days :: Maybe Days
      theirs_days =
        fmap (fromIntegral . Time.toModifiedJulianDay) theirs

      ours :: Maybe Days
      ours =
        fmap toModifiedJulianDay .
        rightToMaybe $
        parseDate str
   in
     counterexample ("render =  " <> Char8.unpack str) $
     counterexample ("theirs =  " <> show theirs) $
     counterexample ("ours   =  " <> show ours) $
       theirs_days === ours

prop_compare_time_parsing :: Property
prop_compare_time_parsing =
  gamble jTime $ \time ->
    let
      str =
        renderTime time

      theirs :: Maybe Time.UTCTime
      theirs =
        Time.parseTimeM False Time.defaultTimeLocale "%Y-%m-%d %H:%M:%S%Q" $
        Char8.unpack str

      theirs_us :: Maybe Microseconds
      theirs_us =
        fmap round .
        fmap (* 1000000) $
        fmap (`Time.diffUTCTime` epoch) theirs

      ours :: Maybe Microseconds
      ours =
        fmap toMicroseconds .
        rightToMaybe $
        parseTime str
   in
     counterexample ("render =  " <> Char8.unpack str) $
     counterexample ("theirs =  " <> show theirs) $
     counterexample ("ours   =  " <> show ours) $
       theirs_us === ours

return []
tests :: IO Bool
tests =
  $quickCheckAll
