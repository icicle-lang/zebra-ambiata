{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra_cli
import           DependencyInfo_ambiata_zebra_cli

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Resource (runResourceT)

import           Data.List.NonEmpty (NonEmpty(..), some1)
import           Data.String (String)

import           GHC.Conc (getNumProcessors, setNumCapabilities)

import           P

import           System.IO (IO, FilePath)
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either.Exit (orDie)
import           X.Options.Applicative (Parser, Mod, CommandFields)
import qualified X.Options.Applicative as Options

import           Zebra.Command.Adapt
import           Zebra.Command.Export
import           Zebra.Command.Import
import           Zebra.Command.Merge
import           Zebra.Command.Summary
import           Zebra.Serial.Binary (BinaryVersion(..))


main :: IO ()
main = do
  n <- getNumProcessors
  setNumCapabilities (min 8 n)
  IO.hSetBuffering IO.stdout IO.LineBuffering
  IO.hSetBuffering IO.stderr IO.LineBuffering
  Options.cli "zebra" buildInfoVersion dependencyInfo parser run

data Command =
    ZebraSummary !Summary
  | ZebraImport !Import
  | ZebraExport !Export
  | ZebraMerge !Merge
  | ZebraAdapt !Adapt
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
      (ZebraSummary <$> pSummary)
      "summary"
      "Generate a summary, with statistics such as row count, for a zebra binary file."
  , cmd
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
  , cmd
      (ZebraAdapt <$> pAdapt)
      "adapt"
      "Adapt a zebra binary file so that it uses a different, but compatible, schema."
  ]

pInputBinary :: Parser FilePath
pInputBinary =
  Options.argument Options.str $
    Options.metavar "INPUT_ZEBRA_BINARY" <>
    Options.help "Path to an input file (in zebra binary format)"

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

pAdaptSchema :: Parser FilePath
pAdaptSchema =
  Options.option Options.str $
    Options.short 's' <>
    Options.long "schema" <>
    Options.metavar "INPUT_ZEBRA_SCHEMA" <>
    Options.help "Path to a schema file which all inputs will be adapted to before processing"

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
   <*> optional pAdaptSchema
   <*> ((Just <$> pOutputBinary) <|> pOutputBinaryStdout <|> pure Nothing)
   <*> pOutputFormat
   <*> pMergeRowsPerBlock
   <*> optional pMergeMaximumRowSize

pMergeRowsPerBlock :: Parser MergeRowsPerBlock
pMergeRowsPerBlock =
  fmap MergeRowsPerBlock .
  Options.option Options.auto $
    Options.value 256 <>
    Options.short 'n' <>
    Options.long "rows-per-block" <>
    Options.metavar "ROWS_PER_BLOCK" <>
    Options.help "The maximum numbers of rows to include in each block. (defaults to 256)"

pMergeMaximumRowSize :: Parser MergeMaximumRowSize
pMergeMaximumRowSize =
  fmap MergeMaximumRowSize .
  Options.option Options.auto $
    Options.short 'm' <>
    Options.long "maximum-row-size" <>
    Options.metavar "MAXIMUM_ROW_SIZE_BYTES" <>
    Options.help "Rows which are larger than this size will be dropped."

pOutputFormat :: Parser BinaryVersion
pOutputFormat =
  fromMaybe BinaryV3 <$> optional pOutputV3

pOutputV3 :: Parser BinaryVersion
pOutputV3 =
  Options.flag' BinaryV3 $
    Options.long "output-v3" <>
    Options.help "Force merge to output files in version 3 format. (default)"

pSummary :: Parser Summary
pSummary =
 Summary
   <$> pInputBinary

pAdapt :: Parser Adapt
pAdapt =
 Adapt
   <$> pInputBinary
   <*> pAdaptSchema
   <*> ((Just <$> pOutputBinary) <|> pOutputBinaryStdout <|> pure Nothing)

run :: Command -> IO ()
run = \case
  ZebraSummary summary ->
    orDie renderSummaryError . hoist runResourceT $
      zebraSummary summary

  ZebraImport import_ ->
    orDie renderImportError . hoist runResourceT $
      zebraImport import_

  ZebraExport export ->
    orDie renderExportError . hoist runResourceT $
      zebraExport export

  ZebraMerge merge ->
    orDie renderMergeError . hoist runResourceT $
      zebraMerge merge

  ZebraAdapt adapt ->
    orDie renderAdaptError . hoist runResourceT $
      zebraAdapt adapt
