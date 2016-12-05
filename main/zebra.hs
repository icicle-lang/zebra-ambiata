{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra
import           DependencyInfo_ambiata_zebra

import qualified Zebra.Data.Block as Block
import qualified Zebra.Data.Entity as Entity
import           Zebra.Serial.File
import qualified Zebra.Merge.BlockC as MergeC
import qualified Zebra.Foreign.Entity as FoEntity

import           P

import qualified Data.ByteString as B
import qualified Data.Text as Text
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Storable as Storable
import qualified X.Data.Vector.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, left, joinErrors, hoistEither)
import           X.Control.Monad.Trans.Either.Exit (orDie)
import           Control.Monad.Trans.Class (lift)

import           Data.String (String)
import           System.IO (IO, FilePath)
import qualified System.IO as IO
import qualified Data.IORef as IORef
import qualified System.Exit as Exit
import           X.Options.Applicative (Parser, SafeCommand(..), RunType(..), Mod, CommandFields)
import           X.Options.Applicative (dispatch, safeCommand, command', str, metavar, subparser)
import           X.Options.Applicative (help, argument, switch, long)
import           Text.Show.Pretty (ppShow)

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
    FileCat FilePath CatOptions
  | MergeFiles [FilePath]
  deriving (Eq, Show)

data CatOptions =
  CatOptions {
    catHeader :: Bool
  , catBlockSummary :: Bool
  , catEntities :: Bool
  , catEntityDetails :: Bool
  , catSummary :: Bool
  } deriving (Eq, Show)

parser :: Parser (SafeCommand Command)
parser =
  safeCommand . subparser $ mconcat commands

cmd :: Parser a -> String -> String -> Mod CommandFields a
cmd p name desc =
  command' name desc p

commands :: [Mod CommandFields Command]
commands =
  [ cmd
      (FileCat <$> pZebraFile <*> pCatOptions)
      "cat"
      "Dump all information in a Zebra file."
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

pCatOptions :: Parser CatOptions
pCatOptions =
  CatOptions
  <$> no_switch (long "no-header")
  <*> no_switch (long "no-block-summary")
  <*> no_switch (long "no-entities")
  <*> no_switch (long "no-entity-details")
  <*> no_switch (long "no-summary")
 where
  no_switch args = not <$> switch args


run :: Command -> IO ()
run c = case c of
  FileCat f opts -> orDie id $ do
    (r,blocks) <- getFile' f
    when (catHeader opts) $ lift $ do
      IO.putStrLn "Header information:"
      IO.putStrLn (ppShow r)
    catBlocks opts blocks

  MergeFiles [] -> do
    IO.putStrLn "Merge: No files"
    Exit.exitFailure

  MergeFiles ins@(i:_) -> orDie id $ do
    fileheader <- fst <$> getFile' i
    let getPuller f = streamPuller $ readBlocksCheckHeader fileheader f

    files <- lift $ mapM getPuller (Boxed.fromList ins)

    let pull f = files Boxed.! f
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
    eid <- firstTshow $ FoEntity.peekEntityId $ FoEntity.unCEntity entity
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
        

catBlocks :: CatOptions -> Stream.Stream (EitherT Text IO) Block.Block -> EitherT Text IO ()
catBlocks opts blocks = do
  let int0 = 0 :: Int
  totalBlocks <- lift $ IORef.newIORef int0
  totalEnts <- lift $ IORef.newIORef int0
  totalIxs <- lift $ IORef.newIORef int0

  let go block = do
        num <- lift $ IORef.readIORef totalBlocks
        let numEnts = Boxed.length $ Block.blockEntities block
        let numIxs = Unboxed.length $ Block.blockIndices block
        lift $ IORef.modifyIORef totalBlocks (+1)
        lift $ IORef.modifyIORef totalEnts (+numEnts)
        lift $ IORef.modifyIORef totalIxs (+numIxs)

        when (catBlockSummary opts || catEntities opts) $ lift $ do
          IO.putStrLn ("Block " <> show num)

        when (catEntities opts) $ do
          entities <- firstTshow $ hoistEither $ Block.entitiesOfBlock block
          mapM_ (catEntity opts) entities

        when (catBlockSummary opts) $ lift $ do
          IO.putStrLn ("  Entities: " <> show numEnts)
          IO.putStrLn ("  Indices:  " <> show numIxs)

  Stream.mapM_ go blocks

  numBlocks <- lift $ IORef.readIORef totalBlocks
  numEnts <- lift $ IORef.readIORef totalEnts
  numIxs <- lift $ IORef.readIORef totalIxs

  when (catSummary opts) $ lift $ do
    IO.putStrLn ""
    IO.putStrLn "Total:"
    IO.putStrLn ("  Blocks:   " <> show numBlocks)
    IO.putStrLn ("  Entities: " <> show numEnts)
    IO.putStrLn ("  Indices:  " <> show numIxs)

  return ()

catEntity :: CatOptions -> Entity.Entity -> EitherT Text IO ()
catEntity opts entity = lift $ do
  IO.putStrLn ("    " <> show (Entity.entityId entity) <> " (" <> show (Entity.entityHash entity) <> ")")

  let counts = Boxed.map (Storable.length . Entity.attributeTime)
             $ Entity.entityAttributes entity
  let times  = Boxed.concatMap (Boxed.convert . Entity.attributeTime)
             $ Entity.entityAttributes entity
  when (catEntityDetails opts) $ do
    IO.putStrLn ("      Min time: " <> show (Boxed.minimum times))
    IO.putStrLn ("      Max time: " <> show (Boxed.maximum times))
    IO.putStrLn ("      Min #facts per attribute: " <> show (Boxed.minimum counts))
    IO.putStrLn ("      Max #facts per attribute: " <> show (Boxed.maximum counts))
    IO.putStrLn ("      Total #facts: " <> show (Boxed.sum counts))


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

firstTshow :: (Functor m, Show e) => EitherT e m a -> EitherT Text m a
firstTshow = firstT (Text.pack . show)
