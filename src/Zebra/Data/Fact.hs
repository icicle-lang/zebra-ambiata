{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Data.Fact (
    Fact(..)
  , renderFact

  , FactRenderError(..)
  , renderFactRenderError
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Text as T
import           Data.Thyme.Format (formatTime)
import           Data.Typeable (Typeable)
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P hiding (some)

import           System.Locale (defaultTimeLocale)

import           Text.Printf (printf)

import           Zebra.Data.Core
import           Zebra.Schema (Schema)
import           Zebra.Value (Value, ValueRenderError)
import qualified Zebra.Value as Value


data Fact =
  Fact {
      factEntityHash :: !EntityHash
    , factEntityId :: !EntityId
    , factAttributeId :: !AttributeId
    , factTime :: !Time
    , factFactsetId :: !FactsetId
    , factValue :: !(Maybe' Value)
    } deriving (Eq, Ord, Show, Generic, Typeable)

data FactRenderError =
    FactValueRenderError !ValueRenderError
  | FactSchemaNotFoundForAttribute !AttributeId
    deriving (Eq, Ord, Show, Generic, Typeable)

renderFactRenderError :: FactRenderError -> Text
renderFactRenderError = \case
  FactValueRenderError err ->
    Value.renderValueRenderError err
  FactSchemaNotFoundForAttribute (AttributeId aid) ->
    "Could not render fact, no schema found for attribute-id: " <> T.pack (show aid)

renderFact :: Boxed.Vector Schema -> Fact -> Either FactRenderError ByteString
renderFact schemas fact = do
  let
    aid =
      factAttributeId fact
    ix =
      fromIntegral $ unAttributeId aid

  schema <- maybeToRight (FactSchemaNotFoundForAttribute aid) (schemas Boxed.!? ix)
  rvalue <- renderMaybeValue schema $ factValue fact

  pure $ Char8.intercalate "|" [
      renderEntityHash $ factEntityHash fact
    , unEntityId $ factEntityId fact
    , renderAttributeId $ factAttributeId fact
    , renderTime $ factTime fact
    , renderFactsetId $ factFactsetId fact
    , rvalue
    ]

renderEntityHash :: EntityHash -> ByteString
renderEntityHash (EntityHash hash) =
  Char8.pack $ printf "0x%08X" hash

renderAttributeId :: AttributeId -> ByteString
renderAttributeId (AttributeId aid) =
  Char8.pack $ printf "attribute=%05d" aid

renderTime :: Time -> ByteString
renderTime =
  Char8.pack . formatTime defaultTimeLocale "%0Y-%m-%d %H:%M:%S" . toUTCTime

renderFactsetId :: FactsetId -> ByteString
renderFactsetId (FactsetId factsetId) =
  Char8.pack $ printf "factset=%08x" factsetId

renderMaybeValue :: Schema -> Maybe' Value -> Either FactRenderError ByteString
renderMaybeValue schema =
  maybe' (pure "NA") (first FactValueRenderError . Value.render schema)
