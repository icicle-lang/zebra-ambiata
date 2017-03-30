{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra_cli
import           DependencyInfo_ambiata_zebra_cli

import           Data.List.NonEmpty (NonEmpty(..), some1)
import           Data.String (String)

import           P

import           System.IO (IO, FilePath)
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either.Exit (orDie)
import           X.Options.Applicative (Parser, Mod, CommandFields)
import qualified X.Options.Applicative as Options

import           Zebra.Binary (BinaryVersion(..))
import           Zebra.Command
import           Zebra.Command.Export
import           Zebra.Command.Import


main :: IO ()
main = do
  IO.hSetBuffering IO.stdout IO.LineBuffering
  IO.hSetBuffering IO.stderr IO.LineBuffering
  Options.cli "zebra" buildInfoVersion dependencyInfo parser run

data Command =
    ZebraCat !(NonEmpty FilePath) !CatOptions
  | ZebraFacts !FilePath
  | ZebraMerge !(NonEmpty FilePath) !(Maybe FilePath) !MergeOptions
  | ZebraUnion !(NonEmpty FilePath) !FilePath
  | ZebraImport !Import
  | ZebraExport !Export
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
      (ZebraCat <$> some1 pInputZebra <*> pCatOptions)
      "cat"
      "Dump all information in a zebra file."
  , cmd
      (ZebraFacts <$> pInputZebra)
      "facts"
      "Dump a zebra file as facts."
  , cmd
      (ZebraMerge <$> some1 pInputZebra <*> pMaybeOutputZebra <*> pMergeOptions)
      "merge"
      "Merge multiple input files together."
  , cmd
      (ZebraUnion <$> some1 pInputZebra <*> pOutputZebra)
      "union"
      "Union multiple input files together."
  , cmd
      (ZebraImport <$> pImport)
      "import"
      "Import a zebra json file into a zebra binary file."
  , cmd
      (ZebraExport <$> pExport)
      "export"
      "Export a zebra binary file to a zebra json file."
  ]

pInputZebra :: Parser FilePath
pInputZebra =
  Options.argument Options.str $
    Options.metavar "INPUT_ZEBRA" <>
    Options.help "Path to an input file (in zebra binary format)"

pMaybeOutputZebra :: Parser (Maybe FilePath)
pMaybeOutputZebra =
  let
    none =
      Options.flag' Nothing $
        Options.long "no-output" <>
        Options.help "Don't output block file, just print entity-id"
  in
    (Just <$> pOutputZebra) <|> none

pOutputZebra :: Parser FilePath
pOutputZebra =
  Options.option Options.str $
    Options.short 'o' <>
    Options.long "output" <>
    Options.metavar "OUTPUT_ZEBRA" <>
    Options.help "Write the output to a file (in zebra binary format)"

pImport :: Parser Import
pImport =
 Import
   <$> pInputJson
   <*> pInputSchema
   <*> pOutputZebra

pInputJson :: Parser FilePath
pInputJson =
  Options.option Options.str $
    Options.long "json" <>
    Options.metavar "INPUT_JSON" <>
    Options.help "Path to an input file (in zebra json format)"

pInputSchema :: Parser FilePath
pInputSchema =
  Options.option Options.str $
    Options.long "schema" <>
    Options.metavar "INPUT_SCHEMA" <>
    Options.help "Path to a schema file which describes the input file"

pExport :: Parser Export
pExport =
 Export
   <$> pInputZebra
   <*> fmap (defaultOnEmpty ExportJsonStdout) (many pExportOutput)

defaultOnEmpty :: a -> [a] -> NonEmpty a
defaultOnEmpty def = \case
  [] ->
    def :| []
  x : xs ->
    x :| xs

pExportOutput :: Parser ExportOutput
pExportOutput =
      pExportJson
  <|> pExportJsonStdout
  <|> pExportSchema
  <|> pExportSchemaStdout

pExportJsonStdout :: Parser ExportOutput
pExportJsonStdout =
  Options.flag' ExportJsonStdout $
    Options.long "json-stdout" <>
    Options.help "Write data to stdout (in zebra json format, default)"

pExportJson :: Parser ExportOutput
pExportJson =
  fmap ExportJson .
  Options.option Options.str $
    Options.long "json" <>
    Options.metavar "OUTPUT_JSON" <>
    Options.help "Write data to a file (in zebra json format)"

pExportSchemaStdout :: Parser ExportOutput
pExportSchemaStdout =
  Options.flag' ExportSchemaStdout $
    Options.long "schema-stdout" <>
    Options.help "Write schema to stdout"

pExportSchema :: Parser ExportOutput
pExportSchema =
  fmap ExportSchema .
  Options.option Options.str $
    Options.long "schema" <>
    Options.metavar "OUTPUT_SCHEMA" <>
    Options.help "Write schema to a file"

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

pOutputFormat :: Parser BinaryVersion
pOutputFormat =
  fromMaybe BinaryV2 <$> optional (pOutputV2 <|> pOutputV3)

pOutputV2 :: Parser BinaryVersion
pOutputV2 =
  Options.flag' BinaryV2 $
    Options.long "output-v2" <>
    Options.help "Force merge to output files in version 2 format. (default)"

pOutputV3 :: Parser BinaryVersion
pOutputV3 =
  Options.flag' BinaryV3 $
    Options.long "output-v3" <>
    Options.help "Force merge to output files in version 3 format."

run :: Command -> IO ()
run = \case
  ZebraCat inputs options ->
    orDie id $
      zebraCat inputs options

  ZebraFacts input ->
    orDie id $
      zebraFacts input

  ZebraMerge inputs output options ->
    orDie id $
      zebraMerge inputs output options

  ZebraUnion inputs output ->
    orDie id $
      zebraUnion inputs output

  ZebraImport import_ ->
    orDie renderImportError $
      zebraImport import_

  ZebraExport export ->
    orDie renderExportError $
      zebraExport export
