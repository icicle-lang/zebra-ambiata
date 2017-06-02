{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Time where

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

return []
tests :: IO Bool
tests =
  $quickCheckAll
