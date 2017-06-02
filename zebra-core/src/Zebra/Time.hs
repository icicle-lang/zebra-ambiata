{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Time (
    Days(..)
  , Seconds(..)
  , Milliseconds(..)
  , Microseconds(..)

  , Date
  , fromDays
  , fromModifiedJulianDay
  , toDays
  , toModifiedJulianDay
  , parseDate
  , renderDate

  , Time
  , fromSeconds
  , fromMilliseconds
  , fromMicroseconds
  , toSeconds
  , toMilliseconds
  , toMicroseconds
  , parseTime
  , renderTime

  , CalendarTime(..)
  , CalendarDate(..)
  , TimeOfDay(..)
  , Year(..)
  , Month(..)
  , Day(..)
  , Hour(..)
  , Minute(..)
  , fromCalendarTime
  , toCalendarTime
  , fromCalendarDate
  , toCalendarDate
  , fromTimeOfDay
  , toTimeOfDay

  , TimeError(..)
  , renderTimeError
  ) where

import qualified Anemone.Parser as Anemone

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Unsafe as ByteString
import qualified Data.Char as Char
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Thyme.Calendar as Thyme
import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import           Data.Word (Word8)

import           Foreign.Storable (Storable)

import           GHC.Generics (Generic)

import qualified Numeric as Numeric

import           P

import           System.Random (Random)

import           Text.Printf (printf)

import           X.Text.Show (gshowsPrec)


newtype Days =
  Days {
      unDays :: Int64
    } deriving (Eq, Ord, Num, Enum, Real, Integral, Generic, Storable, Random)

newtype Seconds =
  Seconds {
      unSeconds :: Int64
    } deriving (Eq, Ord, Num, Enum, Real, Integral, Generic, Storable, Random)

newtype Milliseconds =
  Milliseconds {
      unMilliseconds :: Int64
    } deriving (Eq, Ord, Num, Enum, Real, Integral, Generic, Storable, Random)

newtype Microseconds =
  Microseconds {
      unMicroseconds :: Int64
    } deriving (Eq, Ord, Num, Enum, Real, Integral, Generic, Storable, Random)

-- | A date in the range @[1600-03-01, 3000-01-01)@
--
newtype Date =
  Date {
      unDate :: Days -- ^ Days since 1600-03-01.
    } deriving (Eq, Ord, Generic, Storable)

-- | A time in the range @[1600-03-01 00:00:00, 3000-01-01 00:00:00)@
--
newtype Time =
  Time {
      unTime :: Microseconds -- ^ Microseconds since 1600-03-01.
    } deriving (Eq, Ord, Generic, Storable)

newtype Year =
  Year {
      unYear :: Int64
    } deriving (Eq, Ord, Num, Generic, Storable)

newtype Month =
  Month {
      unMonth :: Int64
    } deriving (Eq, Ord, Num, Generic, Storable)

newtype Day =
  Day {
      unDay :: Int64
    } deriving (Eq, Ord, Num, Generic, Storable)

newtype Hour =
  Hour {
      unHour :: Int64
    } deriving (Eq, Ord, Num, Generic, Storable)

newtype Minute =
  Minute {
      unMinute :: Int64
    } deriving (Eq, Ord, Num, Generic, Storable)

data TimeOfDay =
  TimeOfDay {
      timeHour :: !Hour
    , timeMinute :: !Minute
    , timeSecond :: !Microseconds
    } deriving (Eq, Ord, Generic)

data CalendarDate =
  CalendarDate {
      dateYear :: !Year
    , dateMonth :: !Month
    , dateDay :: !Day
    } deriving (Eq, Ord, Generic)

data CalendarTime =
  CalendarTime {
      calendarDate :: !CalendarDate
    , calendarTime :: !TimeOfDay
    } deriving (Eq, Ord, Generic)

instance Bounded Date where
  minBound =
    Date 0 -- 1600-03-01
  {-# INLINE minBound #-}

  maxBound =
    Date 511279 -- 2999-12-31
  {-# INLINE maxBound #-}

instance Bounded Time where
  minBound =
    Time 0 -- 1600-03-01 00:00:00.000000
  {-# INLINE minBound #-}

  maxBound =
    Time 44174591999999999 -- 2999-12-31 23:59:59.999999
  {-# INLINE maxBound #-}

data TimeError =
    TimeCalendarDateOutOfBounds !CalendarDate
  | TimeCalendarTimeOutOfBounds !CalendarTime
  | TimeDaysOutOfBounds !Days
  | TimeSecondsOutOfBounds !Seconds
  | TimeMillisecondsOutOfBounds !Milliseconds
  | TimeMicrosecondsOutOfBounds !Microseconds
  | TimeDateParseError !Anemone.TimeError
  | TimeDateLeftover !ByteString !ByteString
  | TimeTimeOfDayParseError !ByteString
  | TimeSecondsParseError !ByteString
  | TimeMissingTimeOfDay !ByteString
  | TimeInvalidDateTimeSeparator !Char !ByteString
    deriving (Eq, Ord, Show)

renderTimeError :: TimeError -> Text
renderTimeError = \case
  TimeCalendarDateOutOfBounds date ->
    "Tried to convert illegal date <" <>
    Text.decodeUtf8 (renderCalendarDate date) <>
    ">, " <>
    dateRangeError

  TimeCalendarTimeOutOfBounds time ->
    "Tried to convert illegal time <" <>
    Text.decodeUtf8 (renderCalendarTime time) <>
    ">, " <>
    timeRangeError

  TimeDaysOutOfBounds days ->
    "Tried convert illegal date from days <" <>
    Text.decodeUtf8 (renderDate (Date days)) <>
    ">, " <>
    dateRangeError

  TimeSecondsOutOfBounds seconds ->
    "Tried convert illegal time from seconds <" <>
    Text.decodeUtf8 (renderTime (Time . Microseconds $ unSeconds seconds * 1000000)) <>
    ">, " <>
    timeRangeError

  TimeMillisecondsOutOfBounds ms ->
    "Tried convert illegal time from milliseconds <" <>
    Text.decodeUtf8 (renderTime (Time . Microseconds $ unMilliseconds ms * 1000)) <>
    ">, " <>
    timeRangeError

  TimeMicrosecondsOutOfBounds us ->
    "Tried convert illegal time from microseconds <" <>
    Text.decodeUtf8 (renderTime (Time us)) <>
    ">, " <>
    timeRangeError

  TimeDateParseError err ->
    Anemone.renderTimeError err

  TimeDateLeftover date leftover ->
    "Date <" <>
    Text.decodeUtf8 date <>
    "> was parsed but found unused characters <" <>
    Text.decodeUtf8 leftover <>
    "> at end"

  TimeTimeOfDayParseError bs ->
    "Could not parse <" <>
    Text.decodeUtf8 bs <>
    "> as time of day"

  TimeSecondsParseError bs ->
    "Could not parse <" <>
    Text.decodeUtf8 bs <>
    "> as seconds"

  TimeMissingTimeOfDay bs ->
    "Could not parse <" <>
    Text.decodeUtf8 bs <>
    "> as a time because it was missing the time of day"

  TimeInvalidDateTimeSeparator d bs ->
    "Could not parse <" <>
    Text.decodeUtf8 bs <>
    "> as a time because it had an unregonised date/time separator " <>
    Text.pack (show d) <>
    ", expected either 'T' or ' '"

dateRangeError :: Text
dateRangeError =
  "dates must be in the range <" <>
  Text.decodeUtf8 (renderDate minBound) <>
  "> to <" <>
  Text.decodeUtf8 (renderDate maxBound) <>
  ">"

timeRangeError :: Text
timeRangeError =
  "times must be in the range <" <>
  Text.decodeUtf8 (renderTime minBound) <>
  "> to <" <>
  Text.decodeUtf8 (renderTime maxBound) <>
  ">"

------------------------------------------------------------------------
-- Date

-- | Construct a 'Date' from days since our epoch date, 1600-03-01.
--
fromDays :: Days -> Either TimeError Date
fromDays days =
  let
    !date =
      Date days
  in
    if date >= minBound && date <= maxBound then
      pure date
    else
      Left $ TimeDaysOutOfBounds days
{-# INLINABLE fromDays #-}

toDays :: Date -> Days
toDays (Date days) =
  days
{-# INLINABLE toDays #-}

fromModifiedJulianDay :: Days -> Either TimeError Date
fromModifiedJulianDay mjd =
  fromDays (mjd + 94493)
{-# INLINABLE fromModifiedJulianDay #-}

toModifiedJulianDay :: Date -> Days
toModifiedJulianDay date =
  toDays date - 94493
{-# INLINABLE toModifiedJulianDay #-}

parseDate :: ByteString -> Either TimeError Date
parseDate bs =
  case Anemone.parseDay bs of
    Left err ->
      Left $ TimeDateParseError err
    Right (x, leftover) ->
      if ByteString.null leftover then
        fromModifiedJulianDay . Days . fromIntegral $
          Thyme.toModifiedJulianDay x
      else
        let
          consumed =
            ByteString.length bs - ByteString.length leftover
        in
          Left $ TimeDateLeftover (ByteString.take consumed bs) leftover
{-# INLINABLE parseDate #-}

renderDate :: Date -> ByteString
renderDate =
  renderCalendarDate . toCalendarDate
{-# INLINABLE renderDate #-}

------------------------------------------------------------------------
-- Time

-- | Construct a 'Time' from seconds since our epoch date, 1600-03-01.
--
fromSeconds :: Seconds -> Either TimeError Time
fromSeconds seconds =
  let
    !time =
      Time . Microseconds $ unSeconds seconds * 1000000
  in
    if time >= minBound && time <= maxBound then
      pure time
    else
      Left $ TimeSecondsOutOfBounds seconds
{-# INLINABLE fromSeconds #-}

-- | Construct a 'Time' from milliseconds our epoch date, 1600-03-01.
--
fromMilliseconds :: Milliseconds -> Either TimeError Time
fromMilliseconds ms =
  let
    !time =
      Time . Microseconds $ unMilliseconds ms * 1000
  in
    if time >= minBound && time <= maxBound then
      pure time
    else
      Left $ TimeMillisecondsOutOfBounds ms
{-# INLINABLE fromMilliseconds #-}

-- | Construct a 'Time' from microseconds since our epoch date, 1600-03-01.
--
fromMicroseconds :: Microseconds -> Either TimeError Time
fromMicroseconds us =
  let
    !time =
      Time us
  in
    if time >= minBound && time <= maxBound then
      pure time
    else
      Left $ TimeMicrosecondsOutOfBounds us
{-# INLINABLE fromMicroseconds #-}

toSeconds :: Time -> Seconds
toSeconds (Time us) =
  Seconds (unMicroseconds us `quot` 1000000)
{-# INLINABLE toSeconds #-}

toMilliseconds :: Time -> Milliseconds
toMilliseconds (Time us) =
  Milliseconds (unMicroseconds us `quot` 1000)
{-# INLINABLE toMilliseconds #-}

toMicroseconds :: Time -> Microseconds
toMicroseconds (Time us) =
  us
{-# INLINABLE toMicroseconds #-}

parseTime :: ByteString -> Either TimeError Time
parseTime bs0 =
  case Anemone.parseDay bs0 of
    Left err ->
      Left $ TimeDateParseError err
    Right (days0, bs) ->
      if ByteString.null bs then
        Left $ TimeMissingTimeOfDay bs0
      else
        let
          !days =
            fromIntegral (Thyme.toModifiedJulianDay days0) + 94493

          !us_days =
            Microseconds $ days * 24 * 60 * 60 * 1000000

          !d0 =
            ByteString.unsafeIndex bs 0
        in
          if d0 == upperT || d0 == space then do
            us <- fromTimeOfDay <$> parseTimeOfDay (ByteString.drop 1 bs)
            fromMicroseconds $ us_days + us
          else
            Left $ TimeInvalidDateTimeSeparator (Char.chr $ fromIntegral d0) bs0
{-# INLINABLE parseTime #-}

renderTime :: Time -> ByteString
renderTime =
  renderCalendarTime . toCalendarTime
{-# INLINABLE renderTime #-}

------------------------------------------------------------------------
-- CalendarDate

-- | Create a 'Date' from a Gregorian calendar date.
--
fromCalendarDate :: CalendarDate -> Either TimeError Date
fromCalendarDate calendar@(CalendarDate (Year y0) (Month m0) (Day d)) =
  let
    !y1 =
      y0 - 1600

    !m =
      (m0 + 9) `rem` 12

    !y =
      y1 - m `quot` 10

    !days =
      365 * y +
      y `quot` 4 -
      y `quot` 100 +
      y `quot` 400 +
      (m * 306 + 5) `quot` 10 +
      (d - 1)

    !date =
       Date $ Days days
 in
   if date >= minBound && date <= maxBound then
     pure date
   else
     Left $ TimeCalendarDateOutOfBounds calendar
{-# INLINABLE fromCalendarDate #-}

-- | Create a Gregorian calendar date from a 'Date'.
--
toCalendarDate :: Date -> CalendarDate
toCalendarDate (Date (Days g)) =
  let
    !y0 =
      (10000 * g + 14780) `quot` 3652425

    fromY !yy =
      g - (365 * yy + yy `quot` 4 - yy `quot` 100 + yy `quot` 400)

    !ddd0 =
      fromY y0

    (!y1, !ddd) =
      if ddd0 < 0 then
        -- Date before 1600-03-01, should be impossible if 'Date' was created
        -- using smart constructors.
        (y0 - 1, fromY (y0 - 1))
      else
        (y0, ddd0)

    !mi =
      (100 * ddd + 52) `quot` 3060

    !mm =
      (mi + 2) `rem` 12 + 1

    !y =
      y1 + (mi + 2) `quot` 12

    !dd =
      ddd - (mi * 306 + 5) `quot` 10 + 1
  in
    CalendarDate (Year (y + 1600)) (Month mm) (Day dd)
{-# INLINABLE toCalendarDate #-}

renderCalendarDate :: CalendarDate -> ByteString
renderCalendarDate (CalendarDate y m d) =
  Char8.pack $
    printf "%04d-%02d-%02d" (unYear y) (unMonth m) (unDay d)
{-# INLINABLE renderCalendarDate #-}

------------------------------------------------------------------------
-- TimeOfDay

fromTimeOfDay :: TimeOfDay -> Microseconds
fromTimeOfDay (TimeOfDay (Hour h) (Minute m) (Microseconds us)) =
  let
    !h_us =
      h * 1000000 * 60 * 60

    !m_us =
      m * 1000000 * 60
  in
    Microseconds $! h_us + m_us + us
{-# INLINABLE fromTimeOfDay #-}

toTimeOfDay :: Microseconds -> TimeOfDay
toTimeOfDay (Microseconds us0) =
  let
    !us_per_hour =
      1000000 * 60 * 60

    !us_per_minute =
      1000000 * 60

    (!h, !m_us) =
      us0 `quotRem` us_per_hour

    (!m, !us) =
      m_us `quotRem` us_per_minute
  in
    TimeOfDay (Hour h) (Minute m) (Microseconds us)
{-# INLINABLE toTimeOfDay #-}

upperT :: Word8
upperT =
  0x54 -- T
{-# INLINE upperT #-}

colon :: Word8
colon =
  0x3A -- :
{-# INLINE colon #-}

space :: Word8
space =
  0x20 -- ' '
{-# INLINE space #-}

isDigit :: Word8 -> Bool
isDigit x =
  x >= 0x30 && x <= 0x39
{-# INLINE isDigit #-}

fromDigit :: Word8 -> Int64
fromDigit x =
  fromIntegral (x - 0x30)
{-# INLINE fromDigit #-}

parseTimeOfDay :: ByteString -> Either TimeError TimeOfDay
parseTimeOfDay bs = do
  when (ByteString.length bs < 8) $
    Left (TimeTimeOfDayParseError bs)

  let
    !h0 =
      ByteString.unsafeIndex bs 0 -- H
    !h1 =
      ByteString.unsafeIndex bs 1 -- H

    !d0 =
      ByteString.unsafeIndex bs 2 -- :
    !m0 =
      ByteString.unsafeIndex bs 3 -- M
    !m1 =
      ByteString.unsafeIndex bs 4 -- M

    !d1 =
      ByteString.unsafeIndex bs 5 -- :
    !s0 =
      ByteString.unsafeIndex bs 6 -- S
    !s1 =
      ByteString.unsafeIndex bs 7 -- S

    !valid =
      d0 == colon &&
      d1 == colon &&
      isDigit h0 &&
      isDigit h1 &&
      isDigit m0 &&
      isDigit m1 &&
      isDigit s0 &&
      isDigit s1

  when (not valid) $
    Left $ TimeTimeOfDayParseError bs

  !us <- parseSeconds $ ByteString.drop 6 bs

  let
    !h =
      Hour (fromDigit h0 * 10 + fromDigit h1)

    !m =
      Minute (fromDigit m0 * 10 + fromDigit m1)

  pure $ TimeOfDay h m us
{-# INLINABLE parseTimeOfDay #-}

parseSeconds :: ByteString -> Either TimeError Microseconds
parseSeconds bs =
  case Anemone.parseDouble bs of
    Nothing ->
      Left $ TimeSecondsParseError bs
    Just (us, leftover) ->
      if ByteString.null leftover then
        pure $ Microseconds (round (us * 1000000))
      else
        Left $ TimeSecondsParseError bs
{-# INLINABLE parseSeconds #-}

------------------------------------------------------------------------
-- CalendarTime

fromCalendarTime :: CalendarTime -> Either TimeError Time
fromCalendarTime calendar@(CalendarTime date tod) = do
  Date (Days days) <- fromCalendarDate date

  let
    !d_us =
      days * 1000000 * 60 * 60 * 24

    !us =
      unMicroseconds $! fromTimeOfDay tod

    !time =
      Time . Microseconds $ d_us + us

  if time >= minBound && time <= maxBound then
    pure time
  else
    Left $ TimeCalendarTimeOutOfBounds calendar
{-# INLINABLE fromCalendarTime #-}

toCalendarTime :: Time -> CalendarTime
toCalendarTime (Time (Microseconds us0)) =
  let
    !us_per_day =
      1000000 * 60 * 60 * 24

    (!days, !us) =
      us0 `divMod` us_per_day

    !date =
      Date $ Days days

    !tod =
      Microseconds us
  in
    CalendarTime (toCalendarDate date) (toTimeOfDay tod)
{-# INLINABLE toCalendarTime #-}

renderCalendarTime :: CalendarTime -> ByteString
renderCalendarTime (CalendarTime date tod) =
  let
    CalendarDate (Year year) (Month month) (Day day) =
      date

    TimeOfDay (Hour hour) (Minute minute) (Microseconds us0) =
      tod

    (!secs, !us1) =
      us0 `quotRem` 1000000

    us :: Double
    !us =
      fromIntegral us1 / 1000000.0

    !bs0 =
      Char8.pack $
        printf "%04d-%02d-%02d %02d:%02d:%02d"
          year month day hour minute secs

    !bs1 =
      if us == 0 then
        ByteString.empty
      else
        Char8.pack . drop 1 $
          Numeric.showFFloat Nothing us ""
  in
    bs0 <> bs1
{-# INLINABLE renderCalendarTime #-}

------------------------------------------------------------------------

instance Show Days where
  showsPrec =
    gshowsPrec

instance Show Seconds where
  showsPrec =
    gshowsPrec

instance Show Milliseconds where
  showsPrec =
    gshowsPrec

instance Show Microseconds where
  showsPrec =
    gshowsPrec

instance Show Date where
  showsPrec =
    gshowsPrec

instance Show Time where
  showsPrec =
    gshowsPrec

instance Show Year where
  showsPrec =
    gshowsPrec

instance Show Month where
  showsPrec =
    gshowsPrec

instance Show Day where
  showsPrec =
    gshowsPrec

instance Show Hour where
  showsPrec =
    gshowsPrec

instance Show Minute where
  showsPrec =
    gshowsPrec

instance Show TimeOfDay where
  showsPrec =
    gshowsPrec

instance Show CalendarDate where
  showsPrec =
    gshowsPrec

instance Show CalendarTime where
  showsPrec =
    gshowsPrec

derivingUnbox "Days"
  [t| Days -> Int64 |]
  [| unDays |]
  [| Days |]

derivingUnbox "Seconds"
  [t| Seconds -> Int64 |]
  [| unSeconds |]
  [| Seconds |]

derivingUnbox "Milliseconds"
  [t| Milliseconds -> Int64 |]
  [| unMilliseconds |]
  [| Milliseconds |]

derivingUnbox "Microseconds"
  [t| Microseconds -> Int64 |]
  [| unMicroseconds |]
  [| Microseconds |]

derivingUnbox "Date"
  [t| Date -> Days |]
  [| unDate |]
  [| Date |]

derivingUnbox "Time"
  [t| Time -> Microseconds |]
  [| unTime |]
  [| Time |]

derivingUnbox "Year"
  [t| Year -> Int64 |]
  [| unYear |]
  [| Year |]

derivingUnbox "Month"
  [t| Month -> Int64 |]
  [| unMonth |]
  [| Month |]

derivingUnbox "Day"
  [t| Day -> Int64 |]
  [| unDay |]
  [| Day |]

derivingUnbox "Hour"
  [t| Hour -> Int64 |]
  [| unHour |]
  [| Hour |]

derivingUnbox "Minute"
  [t| Minute -> Int64 |]
  [| unMinute |]
  [| Minute |]

derivingUnbox "CalendarDate"
  [t| CalendarDate -> (Year, Month, Day) |]
  [| \(CalendarDate y m d) -> (y, m, d) |]
  [| \(y, m, d) -> CalendarDate y m d |]

derivingUnbox "TimeOfDay"
  [t| TimeOfDay -> (Hour, Minute, Microseconds) |]
  [| \(TimeOfDay h m s) -> (h, m, s) |]
  [| \(h, m, s) -> TimeOfDay h m s |]

derivingUnbox "CalendarTime"
  [t| CalendarTime -> (CalendarDate, TimeOfDay) |]
  [| \(CalendarTime d t) -> (d, t) |]
  [| \(d, t) -> CalendarTime d t |]
