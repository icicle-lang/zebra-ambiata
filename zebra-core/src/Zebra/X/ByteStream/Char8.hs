{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Zebra.X.ByteStream.Char8 (
    ByteStream

  , rechunkLineEnd

  , module Data.ByteString.Streaming.Char8
  ) where

import qualified Data.ByteString as Strict
import           Data.ByteString.Streaming.Char8
import           Data.ByteString.Streaming.Internal (ByteString(..))

import           P

import           Zebra.X.ByteStream (ByteStream)


-- | Ensures that every chunk in the stream ends on a @'\n'@ line feed character.
--
rechunkLineEnd :: forall m r. Monad m => ByteStream m r -> ByteStream m r
rechunkLineEnd incoming =
  let
    loop dl = \case
      Empty r ->
        let
          !bs =
            fromDList dl []
        in
          if Strict.null bs then
            Empty r
          else
            Chunk bs (Empty r)

      Chunk bs0 bss ->
        case Strict.elemIndexEnd 0x0A bs0 of
          Nothing ->
            loop (dl . (bs0 :)) bss
          Just ix ->
            let
              (!bs1, !bs2) =
                Strict.splitAt (ix + 1) bs0
            in
              Chunk (fromDList dl [bs1]) (loop (bs2 :) bss)

      Go m ->
        Go (fmap (loop dl) m)
  in
    loop id incoming
{-# INLINABLE rechunkLineEnd #-}

fromDList ::
     ([Strict.ByteString] -> [Strict.ByteString])
  -> [Strict.ByteString]
  -> Strict.ByteString
fromDList dl end =
  Strict.concat $! dl end
{-# INLINE fromDList #-}
