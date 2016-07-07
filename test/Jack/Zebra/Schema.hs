{-# LANGUAGE NoImplicitPrelude #-}
module Jack.Zebra.Schema (
    jSchema
  , jFormat
  ) where

import           Disorder.Jack (Jack, listOf, oneOfRec)

import           P

import           Zebra.Schema


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
      ArrayFormat <$> jSchema
    ]
