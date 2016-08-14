{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra
import           DependencyInfo_ambiata_zebra

import           Zebra.Data.Block
import           Zebra.Serial.Header
import           Zebra.Serial.Block
import           Zebra.Serial.File

import           Options.Applicative

import           P

import qualified Data.Binary.Get as Get
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Map as Map
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Stream as Stream


import           Data.String (String)
import           System.IO
import           Data.IORef
import           System.Exit
import           X.Options.Applicative

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  dispatch parser >>= \sc ->
    case sc of
      VersionCommand ->
        putStrLn buildInfoVersion >> exitSuccess
      DependencyCommand ->
        mapM_ putStrLn dependencyInfo
      RunCommand DryRun c ->
        print c >> exitSuccess
      RunCommand RealRun c ->
        run c

data Command =
    FileDump FilePath
  | FileDumpLazy FilePath
  | FileHeader FilePath
  | MergeFiles [FilePath] (Maybe FilePath)
  deriving (Eq, Show)

parser :: Parser (SafeCommand Command)
parser =
  safeCommand . subparser $ mconcat commands

cmd :: Parser a -> String -> String -> Mod CommandFields a
cmd p name desc =
  command' name desc p

commands :: [Mod CommandFields Command]
commands =
  [ cmd
      (FileDump <$> pZebraFile)
      "dump"
      "Dump all information in a Zebra file."
  , cmd
      (FileDumpLazy <$> pZebraFile)
      "lazy"
      "Lazy IO Dump all information in a Zebra file. For testing streaming version"
  , cmd
      (FileHeader <$> pZebraFile)
      "header"
      "Show header information about a Zebra file"
  , cmd
      (MergeFiles <$> pInputFiles <*> optional pOutputFile)
      "merge"
      "Merge multiple input files together"
  ]


pZebraFile :: Parser FilePath
pZebraFile =
  argument str $
    metavar "ZEBRA_PATH" <>
    help "Path to a Zebra file"

pInputFiles :: Parser [FilePath]
pInputFiles =
  many $ argument str $ help "Path to a Zebra file"

pOutputFile :: Parser FilePath
pOutputFile =
  strOption $
    long "output" <>
    short 'o' <>
    metavar "OUTPUT_PATH" <>
    help "Path to a output file"



run :: Command -> IO ()
run c = case c of
  FileDump f -> do
    stream <- streamOfFile f
    res <- getFile stream
    case res of
     Left err -> do
      putStrLn ("error: " <> show err)
      exitFailure
     Right (r,blocks) -> do
      putStrLn "Header information:"
      putStrLn (show r)
      showBlocks blocks

  FileDumpLazy f -> do
    file <- B.readFile f
    let Right (leftovers, sz, header') = Get.runGetOrFail getHeader (Lazy.fromStrict file)
    print header'
    print sz
    let schema = Boxed.fromList $ Map.elems header'
    let l = Get.runGet (getBlock schema) leftovers
    print (Boxed.length $ blockEntities l)


  FileHeader f -> do
    stream <- streamOfFile f
    res <- getFile stream
    case res of
     Left err -> do
      putStrLn ("error: " <> show err)
      exitFailure
     Right (r,_) -> do
      putStrLn "Header information:"
      putStrLn (show r)

  MergeFiles _ins _out ->
    putStrLn "*implement me*" >> exitFailure

showBlocks :: Stream.Stream IO (Either DecodeError Block) -> IO ()
showBlocks blocks = do
  let int0 = 0 :: Int
  totalBlocks <- newIORef int0
  totalErrors <- newIORef int0
  totalEnts <- newIORef int0
  totalIxs <- newIORef int0

  let go block = do
        num <- readIORef totalBlocks
        modifyIORef totalBlocks (+1)
        putStrLn ("Block " <> show num)

        case block of
         Left err -> do
          modifyIORef totalErrors (+1)
          putStrLn ("\tERROR: " <> show err)

         Right b -> do
          let numEnts = Boxed.length $ blockEntities b
          modifyIORef totalEnts (+numEnts)
          putStrLn ("\tEntities: " <> show numEnts)

          let numIxs = Unboxed.length $ blockIndices b
          modifyIORef totalIxs (+numIxs)
          putStrLn ("\tIndices:  " <> show numIxs)

  Stream.mapM_ go blocks

  numBlocks <- readIORef totalBlocks
  numEnts <- readIORef totalEnts
  numIxs <- readIORef totalIxs
  numErrors <- readIORef totalErrors

  putStrLn ""
  putStrLn "Total:"

  putStrLn ("\tBlocks:   " <> show numBlocks)
  putStrLn ("\tEntities: " <> show numEnts)
  putStrLn ("\tIndices:  " <> show numIxs)
  putStrLn ("\tErrors:   " <> show numErrors)

streamOfFile :: FilePath -> IO (Stream.Stream IO B.ByteString)
streamOfFile fp = do
  handle <- openBinaryFile fp ReadMode
  return $ Stream.Stream getBytes (Just handle)
  where
    getBytes Nothing = return $ Stream.Done
    getBytes (Just handle) = do
      bytes <- B.hGet handle 100 -- (300 * 1024 * 1024)
      case B.null bytes of
       True -> do
        hClose handle
        return $ Stream.Skip Nothing
       False -> do
        return $ Stream.Yield bytes (Just handle)

