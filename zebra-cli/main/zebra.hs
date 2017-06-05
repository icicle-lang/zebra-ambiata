{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra_cli
import           DependencyInfo_ambiata_zebra_cli

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (runResourceT)

import           Data.List.NonEmpty (NonEmpty(..), some1)
import           Data.String (String)

import           P

import           System.IO (IO, FilePath)
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either.Exit (orDie)
import           X.Options.Applicative (Parser, Mod, CommandFields)
import qualified X.Options.Applicative as Options

import           Zebra.Command
import           Zebra.Command.Export
import           Zebra.Command.Import
import           Zebra.Command.Merge
import           Zebra.Serial.Binary (BinaryVersion(..))


main :: IO ()
main = do
  IO.hSetBuffering IO.stdout IO.LineBuffering
  IO.hSetBuffering IO.stderr IO.LineBuffering
  Options.cli "zebra" buildInfoVersion dependencyInfo parser run

data Command =
    ZebraImport !Import
  | ZebraExport !Export
  | ZebraMerge !Merge
  -- FIXME cleanup: move to own module like above commands
  | ZebraCat !(NonEmpty FilePath) !CatOptions
  | ZebraFacts !FilePath
  | ZebraFastMerge !(NonEmpty FilePath) !(Maybe FilePath) !MergeOptions
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
      (ZebraImport <$> pImport)
      "import"
      "Import a zebra text file into a zebra binary file."
  , cmd
      (ZebraExport <$> pExport)
      "export"
      "Export a zebra binary file to a zebra text file."
  , cmd
      (ZebraMerge <$> pMerge)
      "merge"
      "Merge multiple zebra binary files together."
  -- FIXME cleanup: move to own module like above commands
  , cmd
      (ZebraCat <$> some1 pInputBinary <*> pCatOptions)
      "cat"
      "Dump all information in a zebra file."
  , cmd
      (ZebraFacts <$> pInputBinary)
      "facts"
      "Dump a zebra file as facts."
  , cmd
      (ZebraFastMerge <$> some1 pInputBinary <*> pMaybeOutputBinary <*> pMergeOptions)
      "fast-merge"
      "Merge multiple input files together using the C merging algorithm."
  ]

pInputBinary :: Parser FilePath
pInputBinary =
  Options.argument Options.str $
    Options.metavar "INPUT_ZEBRA_BINARY" <>
    Options.help "Path to an input file (in zebra binary format)"

pMaybeOutputBinary :: Parser (Maybe FilePath)
pMaybeOutputBinary =
  let
    none =
      Options.flag' Nothing $
        Options.long "no-output" <>
        Options.help "Don't output block file, just print entity-id"
  in
    (Just <$> pOutputBinary) <|> none

pOutputBinary :: Parser FilePath
pOutputBinary =
  Options.option Options.str $
    Options.short 'o' <>
    Options.long "output" <>
    Options.metavar "OUTPUT_ZEBRA_BINARY" <>
    Options.help "Write the output to a file (in zebra binary format)"

pImport :: Parser Import
pImport =
 Import
   <$> pInputJson
   <*> pInputSchema
   <*> ((Just <$> pOutputBinary) <|> pOutputBinaryStdout <|> pure Nothing)

pInputJson :: Parser FilePath
pInputJson =
  Options.argument Options.str $
    Options.metavar "INPUT_ZEBRA_TEXT" <>
    Options.help "Path to an input file (in zebra text format)"

pInputSchema :: Parser FilePath
pInputSchema =
  Options.option Options.str $
    Options.short 's' <>
    Options.long "schema" <>
    Options.metavar "INPUT_ZEBRA_SCHEMA" <>
    Options.help "Path to a schema file which describes the input file"

pOutputBinaryStdout :: Parser (Maybe FilePath)
pOutputBinaryStdout =
  Options.flag' Nothing $
    Options.long "output-stdout" <>
    Options.help "Write the output to stdout (in zebra binary format, default)"

pExport :: Parser Export
pExport =
 Export
   <$> pInputBinary
   <*> fmap (defaultOnEmpty ExportTextStdout) (many pExportOutput)

defaultOnEmpty :: a -> [a] -> NonEmpty a
defaultOnEmpty def = \case
  [] ->
    def :| []
  x : xs ->
    x :| xs

pExportOutput :: Parser ExportOutput
pExportOutput =
      pExportSchema
  <|> pExportSchemaStdout
  <|> pExportJson
  <|> pExportJsonStdout

pExportJsonStdout :: Parser ExportOutput
pExportJsonStdout =
  Options.flag' ExportTextStdout $
    Options.long "output-stdout" <>
    Options.help "Write output to stdout (in zebra text format, default)"

pExportJson :: Parser ExportOutput
pExportJson =
  fmap ExportText .
  Options.option Options.str $
    Options.short 'o' <>
    Options.long "output" <>
    Options.metavar "OUTPUT_ZEBRA_TEXT" <>
    Options.help "Write output to a file (in zebra text format)"

pExportSchemaStdout :: Parser ExportOutput
pExportSchemaStdout =
  Options.flag' ExportSchemaStdout $
    Options.long "schema-stdout" <>
    Options.help "Write schema to stdout"

pExportSchema :: Parser ExportOutput
pExportSchema =
  fmap ExportSchema .
  Options.option Options.str $
    Options.short 's' <>
    Options.long "schema" <>
    Options.metavar "OUTPUT_ZEBRA_SCHEMA" <>
    Options.help "Write schema to a file"

pMerge :: Parser Merge
pMerge =
 Merge
   <$> some1 pInputBinary
   <*> ((Just <$> pOutputBinary) <|> pOutputBinaryStdout <|> pure Nothing)
   <*> pOutputFormat
   <*> pMergeRowsPerBlock

pMergeRowsPerBlock :: Parser MergeRowsPerBlock
pMergeRowsPerBlock =
  fmap MergeRowsPerBlock .
  Options.option Options.auto $
    Options.value 256 <>
    Options.long "rows-per-block" <>
    Options.metavar "ROWS_PER_BLOCK" <>
    Options.help "The maximum numbers of rows to include in each block. (defaults to 256)"

pOutputFormat :: Parser BinaryVersion
pOutputFormat =
  fromMaybe BinaryV3 <$> optional (pOutputV2 <|> pOutputV3)

pOutputV2 :: Parser BinaryVersion
pOutputV2 =
  Options.flag' BinaryV2 $
    Options.long "output-v2" <>
    Options.help "Force merge to output files in version 2 format."

pOutputV3 :: Parser BinaryVersion
pOutputV3 =
  Options.flag' BinaryV3 $
    Options.long "output-v3" <>
    Options.help "Force merge to output files in version 3 format. (default)"

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

run :: Command -> IO ()
run = \case
  ZebraImport import_ ->
    orDie renderImportError . hoist runResourceT $
      zebraImport import_

  ZebraExport export ->
    orDie renderExportError . hoist runResourceT $
      zebraExport export

  ZebraMerge merge ->
    orDie renderMergeError . hoist runResourceT $
      zebraMerge merge

  -- FIXME cleanup: move to own module like above commands

  ZebraCat inputs options ->
    orDie id . hoist runResourceT $
      zebraCat inputs options

  ZebraFacts input ->
    orDie id . hoist runResourceT $
      zebraFacts input

  ZebraFastMerge inputs output options ->
    orDie id . hoist runResourceT $
      zebraFastMerge inputs output options
