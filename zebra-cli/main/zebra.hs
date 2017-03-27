{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra_cli
import           DependencyInfo_ambiata_zebra_cli

import           Data.String (String)

import           P

import           System.IO (IO, FilePath)
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either.Exit (orDie)
import           X.Options.Applicative (Parser, Mod, CommandFields)
import qualified X.Options.Applicative as Options

import           Zebra.Command
import           Zebra.Data.Core (ZebraVersion(..))


main :: IO ()
main = do
  IO.hSetBuffering IO.stdout IO.LineBuffering
  IO.hSetBuffering IO.stderr IO.LineBuffering
  Options.cli "zebra" buildInfoVersion dependencyInfo parser run

data Command =
    FileCat ![FilePath] !CatOptions
  | FileFacts !FilePath
  | MergeFiles ![FilePath] !(Maybe FilePath) !MergeOptions
  | UnionFiles ![FilePath] !FilePath
    deriving (Eq, Show)

parser :: Parser Command
parser =
  Options.subparser $ mconcat commands

cmd :: Parser a -> String -> String -> Mod CommandFields a
cmd p name desc =
  Options.command' name desc p

commands :: [Mod CommandFields Command]
commands =
  [ cmd
      (FileCat <$> pInputFiles <*> pCatOptions)
      "cat"
      "Dump all information in a Zebra file."
  , cmd
      (FileFacts <$> pZebraFile)
      "facts"
      "Dump the Zebra file as facts."
  , cmd
      (MergeFiles <$> pInputFiles <*> pMaybeOutputFile <*> pMergeOptions)
      "merge"
      "Merge multiple input files together"
  , cmd
      (UnionFiles <$> pInputFiles <*> pOutputFile)
      "union"
      "Union multiple input files together"
  ]


pZebraFile :: Parser FilePath
pZebraFile =
  Options.argument Options.str $
    Options.metavar "ZEBRA_PATH" <>
    Options.help "Path to a Zebra file"

pInputFiles :: Parser [FilePath]
pInputFiles =
  many $ Options.argument Options.str $ Options.help "Path to a Zebra file"

pMaybeOutputFile :: Parser (Maybe FilePath)
pMaybeOutputFile =
  let
    none =
      Options.flag' Nothing $
        Options.long "no-output" <>
        Options.help "Don't output block file, just print entity id"
  in
    (Just <$> pOutputFile) <|> none

pOutputFile :: Parser FilePath
pOutputFile =
  Options.option Options.str $
    Options.short 'o' <>
    Options.long "output" <>
    Options.metavar "OUTPUT_PATH" <>
    Options.help "Path to a Zebra output file"

pCatOptions :: Parser CatOptions
pCatOptions =
  fmap fixCatOptions $
    CatOptions
      <$> Options.switch (Options.long "header")
      <*> Options.switch (Options.long "block-summary")
      <*> Options.switch (Options.long "entities")
      <*> Options.switch (Options.long "entity-details")
      <*> Options.switch (Options.long "entity-summary")
      <*> Options.switch (Options.long "summary")

fixCatOptions :: CatOptions -> CatOptions
fixCatOptions opts =
  opts {
      catEntities =
        catEntities opts ||
        catEntityDetails opts ||
        catEntitySummary opts
    }

pMergeOptions :: Parser MergeOptions
pMergeOptions =
  MergeOptions
    <$> pGCLimit
    <*> pOutputBlockFacts
    <*> pOutputFormat

pGCLimit :: Parser Double
pGCLimit =
  Options.option Options.auto $
    Options.value 2 <>
    Options.long "gc-gigabytes" <>
    Options.help "Garbage collect input blocks when pool becomes this large. Fractions allowed."

pOutputBlockFacts :: Parser Int
pOutputBlockFacts =
  Options.option Options.auto $
    Options.value 4096 <>
    Options.long "output-block-facts" <>
    Options.help "Minimum number of facts per output block (last block of leftovers will contain fewer)"

pOutputFormat :: Parser ZebraVersion
pOutputFormat =
  fromMaybe ZebraV2 <$> optional (pOutputV2 <|> pOutputV3)

pOutputV2 :: Parser ZebraVersion
pOutputV2 =
  Options.flag' ZebraV2 $
    Options.long "output-v2" <>
    Options.help "Force merge to output files in version 2 format. (default)"

pOutputV3 :: Parser ZebraVersion
pOutputV3 =
  Options.flag' ZebraV3 $
    Options.long "output-v3" <>
    Options.help "Force merge to output files in version 3 format."

run :: Command -> IO ()
run = \case
  FileCat inputs options ->
    orDie id $
      zebraCat inputs options

  FileFacts input ->
    orDie id $
      zebraFacts input

  MergeFiles inputs output options ->
    orDie id $
      zebraMerge inputs output options

  UnionFiles inputs output ->
    orDie id $
      zebraUnion inputs output
