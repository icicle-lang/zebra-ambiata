{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           BuildInfo_ambiata_zebra
import           DependencyInfo_ambiata_zebra

import qualified Zebra.Data.Block as Block
import qualified Zebra.Data.Core as Core
import qualified Zebra.Data.Entity as Entity
import qualified Zebra.Data.Fact as Fact
import qualified Zebra.Data.Schema as Schema
import qualified Zebra.Serial as Serial
import qualified Zebra.Serial.File as Serial
import qualified Zebra.Merge.BlockC as Merge
import qualified Zebra.Merge.Puller as MergePuller
import qualified Zebra.Foreign.Block as FoBlock
import qualified Zebra.Foreign.Entity as FoEntity

import           P

import qualified Anemone.Foreign.Mempool as Mempool
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Builder as Builder
import qualified Data.Map as Map
import qualified Data.List as List
import qualified Data.Text as Text
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed
import qualified X.Data.Vector.Storable as Storable
import qualified X.Data.Vector.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, joinErrors, hoistEither, bracketEitherT', left)
import           X.Control.Monad.Trans.Either.Exit (orDie)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Class (lift)

import           Data.String (String)
import           System.IO (IO, FilePath)
import qualified System.IO as IO
import qualified Data.IORef as IORef
import qualified System.Exit as Exit
import           X.Options.Applicative (Parser, SafeCommand(..), RunType(..), Mod, CommandFields)
import qualified X.Options.Applicative as Options
import           Text.Show.Pretty (ppShow)

main :: IO ()
main = do
  IO.hSetBuffering IO.stdout IO.LineBuffering
  IO.hSetBuffering IO.stderr IO.LineBuffering
  Options.dispatch parser >>= \sc ->
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
    FileCat [FilePath] CatOptions
  | FileFacts FilePath
  | MergeFiles [FilePath] FilePath MergeOptions
  deriving (Eq, Show)

data CatOptions =
  CatOptions {
    catHeader :: Bool
  , catBlockSummary :: Bool
  , catEntities :: Bool
  , catEntityDetails :: Bool
  , catEntitySummary :: Bool
  , catSummary :: Bool
  } deriving (Eq, Show)

data MergeOptions =
  MergeOptions {
    mergeGcEvery :: Int
  , mergeOutputBlockFacts :: Int
  , mergeNoOutput :: Bool
  } deriving (Eq, Show)

parser :: Parser (SafeCommand Command)
parser =
  Options.safeCommand . Options.subparser $ mconcat commands

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
      (MergeFiles <$> pInputFiles <*> pOutputFile <*> pMergeOptions)
      "merge"
      "Merge multiple input files together"
  ]


pZebraFile :: Parser FilePath
pZebraFile =
  Options.argument Options.str $
    Options.metavar "ZEBRA_PATH" <>
    Options.help "Path to a Zebra file"

pInputFiles :: Parser [FilePath]
pInputFiles =
  many $ Options.argument Options.str $ Options.help "Path to a Zebra file"

pOutputFile :: Parser FilePath
pOutputFile =
  Options.option Options.str $
    Options.short 'o' <>
    Options.long "output" <>
    Options.metavar "OUTPUT_PATH" <>
    Options.help "Path to a Zebra output file"

pCatOptions :: Parser CatOptions
pCatOptions =
  fixCatOptions
  <$> (CatOptions
  <$> Options.switch (Options.long "header")
  <*> Options.switch (Options.long "block-summary")
  <*> Options.switch (Options.long "entities")
  <*> Options.switch (Options.long "entity-details")
  <*> Options.switch (Options.long "entity-summary")
  <*> Options.switch (Options.long "summary")
  )

fixCatOptions :: CatOptions -> CatOptions
fixCatOptions opts
 = opts
 { catEntities = catEntities opts || catEntityDetails opts || catEntitySummary opts }

pMergeOptions :: Parser MergeOptions
pMergeOptions =
  MergeOptions
  <$> Options.option Options.auto
    (Options.value 200
    <> Options.long "gc-entity-count"
    <> Options.help "Garbage collect input after every Nth entity. Must be >1")
  <*> Options.option Options.auto
    (Options.value 4096
    <> Options.long "output-block-facts"
    <> Options.help "Minimum number of facts per output block (last block of leftovers will contain fewer)")
  <*> Options.switch
    (Options.long "no-output"
    <> Options.help "Don't output block file, just print entity id")



run :: Command -> IO ()
run c = case c of
  FileCat fs opts -> orDie id $ forM_ fs $ \f -> do
    (header,blocks) <- firstTshow $ Serial.fileOfFilePath f
    when (catHeader opts) $ lift $ do
      IO.putStrLn "Header information:"
      IO.putStrLn (ppShow header)
    catBlocks opts blocks

  FileFacts f -> orDie id $ do
    (header,blocks) <- firstTshow $ Serial.fileOfFilePath f
    catFacts (Map.elems header) blocks

  MergeFiles [] _ _ -> do
    IO.putStrLn "Merge: No files"
    Exit.exitFailure

  MergeFiles ins@(in1:_) out opts -> orDie id $ do
    (puller, pullids) <- firstTshow $ MergePuller.blockChainPuller (Boxed.fromList ins)
    let withPusher | mergeNoOutput opts
                   = withPrintPusher
                   | otherwise
                   = withOutputPusher opts in1 out
 
    withPusher $ \pusher -> do
    let runOpts = Merge.MergeOptions (firstTshow . puller) pusher $ mergeGcEvery opts
    joinErrors (Text.pack . show) id $ Merge.mergeBlocks runOpts pullids

withPrintPusher :: ((FoEntity.CEntity -> EitherT Text IO ()) -> EitherT Text IO ())
  -> EitherT Text IO ()

withPrintPusher runWith = do
  let pusher e = do
      eid     <- firstTshow $ FoEntity.peekEntityId $ FoEntity.unCEntity e
      lift $ IO.putStrLn (show eid)

  runWith pusher


withOutputPusher :: MergeOptions -> FilePath -> FilePath
  -> ((FoEntity.CEntity -> EitherT Text IO ()) -> EitherT Text IO ())
  -> EitherT Text IO ()

withOutputPusher opts inputfile outputfile runWith = do
  (fileheader,_) <- firstTshow $ Serial.fileOfFilePath inputfile
  outfd <- lift $ IO.openBinaryFile outputfile IO.WriteMode
  lift $ Builder.hPutBuilder outfd (Serial.bHeader fileheader)

  pool0    <- lift $ Mempool.create

  let freePool poolRef = do
      pool <- lift $ IORef.readIORef poolRef
      lift $ Mempool.free pool

  blockRef <- lift $ IORef.newIORef Nothing
  bracketEitherT' (lift $ IORef.newIORef pool0) freePool $ \poolRef -> do

  let checkPurge block = do
      is <- FoBlock.peekBlockRowCount (FoBlock.unCBlock block)
      return (is >= mergeOutputBlockFacts opts)

  let doPurge block = do
      pool <- lift $ IORef.readIORef poolRef
      outblock <- firstTshow $ FoBlock.blockOfForeign block
      lift $ Builder.hPutBuilder outfd (Serial.bBlock outblock)
      pool' <- lift $ Mempool.create
      lift $ IORef.writeIORef poolRef pool'
      lift $ Mempool.free pool
      lift $ IORef.writeIORef blockRef Nothing

  let pusher e = do
      pool    <- lift $ IORef.readIORef poolRef
      block   <- lift $ IORef.readIORef blockRef
      block'  <- firstTshow $ FoBlock.appendEntityToBlock pool e block
      ifpurge <- checkPurge block'
      if ifpurge
        then doPurge block'
        else lift $ IORef.writeIORef blockRef (Just block')

  runWith pusher

  mblock  <- lift $ IORef.readIORef blockRef
  -- This final purge will actually create a new Mempool, but it will be freed by the bracket anyway
  maybe (return ()) doPurge mblock
  lift $ IO.hClose outfd

catFacts :: [Schema.Schema] -> Stream.Stream (EitherT Serial.DecodeError IO) Block.Block -> EitherT Text IO ()
catFacts schemas blocks =
  case traverse Schema.recoverEncodingOfSchema schemas of
    Nothing ->
      left "failed to recover encodings from schemas for fact output"
    Just encodings0 -> do
      let
        encodings =
          Boxed.fromList encodings0

        go block = do
          facts <- firstTshow . hoistEither $ Block.factsOfBlock encodings block
          Boxed.mapM_ (liftIO . Char8.putStrLn . Fact.renderFact) facts

        blocks' =
          Stream.trans (firstT Serial.renderDecodeError) blocks

      Stream.mapM_ go blocks'

catBlocks :: CatOptions -> Stream.Stream (EitherT Serial.DecodeError IO) Block.Block -> EitherT Text IO ()
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
          IO.putStrLn ("  Facts:    " <> show numIxs)

  let blocks' = Stream.trans (firstT Serial.renderDecodeError) blocks
  Stream.mapM_ go blocks'

  numBlocks <- lift $ IORef.readIORef totalBlocks
  numEnts <- lift $ IORef.readIORef totalEnts
  numIxs <- lift $ IORef.readIORef totalIxs

  when (catSummary opts) $ lift $ do
    IO.putStrLn ""
    IO.putStrLn "Total:"
    IO.putStrLn ("  Blocks:   " <> show numBlocks)
    IO.putStrLn ("  Entities: " <> show numEnts)
    IO.putStrLn ("  Facts:    " <> show numIxs)

  return ()

catEntity :: CatOptions -> Entity.Entity -> EitherT Text IO ()
catEntity opts entity = lift $ do
  IO.putStrLn ("    " <> show (Entity.entityId entity) <> " (" <> show (Entity.entityHash entity) <> ")")

  when (catEntityDetails opts) $ catEntityFacts entity

  let facts' = Boxed.filter ((>0) . Storable.length . Entity.attributeTime)
             $ Entity.entityAttributes entity
  let counts = Boxed.map (Storable.length . Entity.attributeTime) facts'
  let times  = Boxed.concatMap (Boxed.convert . Entity.attributeTime) facts'


  let showTime = show . Core.toDay
  when (catEntitySummary opts) $ do
    IO.putStrLn ("      Times:               " <> showRange showTime times)
    IO.putStrLn ("      Facts per attribute: " <> showRange show counts)
    IO.putStrLn ("      Facts:               " <> show (Boxed.sum counts))

 where
  showRange showf inps
   | Boxed.null inps
   = "(empty)"
   | otherwise
   = showf (Boxed.minimum inps) <> "..." <> showf (Boxed.maximum inps)

catEntityFacts :: Entity.Entity -> IO ()
catEntityFacts entity = do
  IO.putStrLn ("      Values:")
  let facts' = Boxed.filter ((>0) . Storable.length . Entity.attributeTime . snd)
             $ Boxed.indexed
             $ Entity.entityAttributes entity
  mapM_ (\(ix,attr) -> putIndented 8 (show ix <> ": " <> ppShow attr)) facts'

putIndented :: Int -> String -> IO ()
putIndented indents str =
  mapM_ (IO.putStrLn . (List.replicate indents ' ' <>)) $ List.lines str

firstTshow :: (Functor m, Show e) => EitherT e m a -> EitherT Text m a
firstTshow = firstT (Text.pack . show)
