{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra
import           DependencyInfo_ambiata_zebra

import           Zebra.Data.Block
import           Zebra.Serial.File
import qualified Zebra.Merge.BlockC as MergeC
import qualified Zebra.Foreign.Entity as FoEntity

import           P

import qualified Data.ByteString as B
import qualified Data.Text as Text
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, left, joinErrors)
import           X.Control.Monad.Trans.Either.Exit (orDie)
import           Control.Monad.Trans.Class (lift)

import           Data.String (String)
import           System.IO (IO, FilePath)
import qualified System.IO as IO
import qualified Data.IORef as IORef
import qualified System.Exit as Exit
import           X.Options.Applicative (Parser, SafeCommand(..), RunType(..), Mod, CommandFields)
import           X.Options.Applicative (dispatch, safeCommand, command', str, metavar, subparser)
import           X.Options.Applicative (help, argument)

main :: IO ()
main = do
  IO.hSetBuffering IO.stdout IO.LineBuffering
  IO.hSetBuffering IO.stderr IO.LineBuffering
  dispatch parser >>= \sc ->
    case sc of
      VersionCommand ->
        IO.putStrLn buildInfoVersion >> Exit.exitSuccess
      DependencyCommand ->
        mapM_ IO.putStrLn dependencyInfo
      RunCommand DryRun c ->
        IO.print c >> Exit.exitSuccess
      RunCommand RealRun c ->
        run c

data Command =
    FileDump FilePath
  | FileHeader FilePath
  | MergeFiles [FilePath]
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
      (FileHeader <$> pZebraFile)
      "header"
      "Show header information about a Zebra file"
  , cmd
      (MergeFiles <$> pInputFiles)
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


run :: Command -> IO ()
run c = case c of
  FileDump f -> orDie id $ do
    (r,blocks) <- getFile' f
    lift $ IO.putStrLn "Header information:"
    lift $ IO.putStrLn (show r)
    showBlocks blocks

  FileHeader f -> orDie id $ do
    (fileheader,_) <- getFile' f
    lift $ IO.putStrLn "Header information:"
    lift $ IO.putStrLn (show fileheader)

  MergeFiles [] -> do
    IO.putStrLn "Merge: No files"
    Exit.exitFailure

  MergeFiles ins@(i:_) -> orDie id $ do
    fileheader <- fst <$> getFile' i
    let getPuller :: FilePath -> IO (EitherT Text IO (Maybe Block))
        getPuller f = do
        streamPuller $ readBlocksCheckHeader fileheader f

    files <- lift $ mapM getPuller (Boxed.fromList ins)

    let pull :: Int -> EitherT Text IO (Maybe Block)
        pull f = files Boxed.! f
    let push e = printEntityInfo e

    joinErrors (Text.pack . show) id $ MergeC.mergeFiles (MergeC.MergeOptions pull push 2000) (Boxed.map fst $ Boxed.indexed $ Boxed.fromList ins)


 where
  getFile' f = do
    stream <- lift $ streamOfFile f
    (h,bs) <- firstT renderDecodeError $ getFile stream
    return (h, Stream.trans (firstT renderDecodeError) bs)

  readBlocksCheckHeader fileheader f = Stream.embed $ do
    (h,bs) <- getFile' f
    when (h /= fileheader) $
      left ("Block error: reading file " <> Text.pack f <> " has different header")
    return bs

  printEntityInfo entity = do
    eid <- firstT (Text.pack . show) $ FoEntity.peekEntityId $ FoEntity.unCEntity entity
    lift $ IO.putStrLn $ show eid


streamPuller :: Stream.Stream (EitherT Text IO) b -> IO (EitherT Text IO (Maybe b))
streamPuller (Stream.Stream loop state0) = do
  stateRef <- IORef.newIORef state0
  return $ go stateRef
 where
  go stateRef = do
    state <- lift $ IORef.readIORef stateRef
    step  <- loop state
    case step of
      Stream.Yield v state' -> do
        lift $ IORef.writeIORef stateRef state'
        return (Just v)
      Stream.Skip state' -> do
        lift $ IORef.writeIORef stateRef state'
        go stateRef
      Stream.Done -> do
        return Nothing
        

showBlocks :: Stream.Stream (EitherT Text IO) Block -> EitherT Text IO ()
showBlocks blocks = do
  let int0 = 0 :: Int
  totalBlocks <- lift $ IORef.newIORef int0
  totalEnts <- lift $ IORef.newIORef int0
  totalIxs <- lift $ IORef.newIORef int0

  let go block = lift $ do
        num <- IORef.readIORef totalBlocks
        IORef.modifyIORef totalBlocks (+1)
        IO.putStrLn ("Block " <> show num)

        let numEnts = Boxed.length $ blockEntities block
        IORef.modifyIORef totalEnts (+numEnts)
        IO.putStrLn ("\tEntities: " <> show numEnts)

        let numIxs = Unboxed.length $ blockIndices block
        IORef.modifyIORef totalIxs (+numIxs)
        IO.putStrLn ("\tIndices:  " <> show numIxs)

  Stream.mapM_ go blocks

  numBlocks <- lift $ IORef.readIORef totalBlocks
  numEnts <- lift $ IORef.readIORef totalEnts
  numIxs <- lift $ IORef.readIORef totalIxs

  lift $ IO.putStrLn ""
  lift $ IO.putStrLn "Total:"

  lift $ IO.putStrLn ("\tBlocks:   " <> show numBlocks)
  lift $ IO.putStrLn ("\tEntities: " <> show numEnts)
  lift $ IO.putStrLn ("\tIndices:  " <> show numIxs)

  return ()

streamOfFile :: FilePath -> IO (Stream.Stream IO B.ByteString)
streamOfFile fp = do
  handle <- IO.openBinaryFile fp IO.ReadMode
  return $ Stream.Stream getBytes (Just handle)
  where
    getBytes Nothing = return $ Stream.Done
    getBytes (Just handle) = do
      bytes <- B.hGet handle (1024*1024)
      case B.null bytes of
       True -> do
        IO.hClose handle
        return $ Stream.Skip Nothing
       False -> do
        return $ Stream.Yield bytes (Just handle)

