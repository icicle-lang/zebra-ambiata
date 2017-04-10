{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Command (
    CatOptions(..)
  , zebraCat
  , zebraFacts

  , MergeOptions(..)
  , zebraMerge
  , zebraUnion
  ) where

import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Resource (MonadResource)
import qualified Control.Monad.Trans.Resource as Resource

import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as Char8
import qualified Data.IORef as IORef
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map.Strict as Map
import           Data.String (String)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text

import           P

import           System.IO (IO, FilePath, stderr)
import qualified System.IO as IO

import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (EitherT, joinEitherT, hoistEither)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Cons as Cons
import qualified X.Data.Vector.Storable as Storable
import qualified X.Data.Vector.Stream as Stream
import qualified X.Data.Vector.Unboxed as Unboxed

import qualified Zebra.Factset.Block as Block
import qualified Zebra.Factset.Data as Core
import qualified Zebra.Factset.Entity as Entity
import qualified Zebra.Factset.Fact as Fact
import qualified Zebra.Factset.Table as Block
import qualified Zebra.Foreign.Block as Foreign
import qualified Zebra.Foreign.Entity as Foreign
import qualified Zebra.Merge.BlockC as Merge
import qualified Zebra.Merge.Puller.File as Merge
import qualified Zebra.Merge.Table as Merge
import           Zebra.Serial.Binary (BinaryVersion(..))
import qualified Zebra.Serial.Binary as Binary
import qualified Zebra.Serial.Binary.File as Binary
import qualified Zebra.Table.Schema as Schema


data CatOptions =
  CatOptions {
      catHeader :: !Bool
    , catBlockSummary :: !Bool
    , catEntities :: !Bool
    , catEntityDetails :: !Bool
    , catEntitySummary :: !Bool
    , catSummary :: !Bool
    } deriving (Eq, Show)

data MergeOptions =
  MergeOptions {
      mergeGcGigabytes :: !Double
    , mergeOutputBlockFacts :: !Int
    , mergeOutputFormat :: !BinaryVersion
    } deriving (Eq, Show)

zebraCat :: MonadResource m => NonEmpty FilePath -> CatOptions -> EitherT Text m ()
zebraCat fs opts =
  forM_ fs $ \f -> do
    (header, blocks) <- firstT Binary.renderFileError $ Binary.readBlocks f
    when (catHeader opts) . liftIO $ do
      IO.putStrLn "Header information:"
      IO.putStrLn (ppShow header)
    catBlocks opts blocks

zebraFacts :: MonadResource m => FilePath -> EitherT Text m ()
zebraFacts f = do
  (header, blocks) <- firstT Binary.renderFileError $ Binary.readBlocks f
  attributes <- firstT Block.renderBlockTableError . hoistEither $ Binary.attributesOfHeader header
  catFacts (Map.elems attributes) blocks

zebraMerge :: MonadResource m => NonEmpty FilePath -> Maybe FilePath -> MergeOptions -> EitherT Text m ()
zebraMerge ins outputPath opts = do
  (puller, pullids) <- firstTshow $ Merge.blockChainPuller (Boxed.fromList $ NonEmpty.toList ins)
  let withPusher = case outputPath of
        Just out -> withOutputPusher opts (NonEmpty.head ins) out
        Nothing  -> withPrintPusher

  withPusher $ \pusher ->
    let
      runOpts =
        Merge.MergeOptions (firstT Binary.renderFileError . puller) pusher $
          truncate (mergeGcGigabytes opts * 1024 * 1024 * 1024)
    in
      joinEitherT id . firstTshow $ Merge.mergeBlocks runOpts pullids

zebraUnion :: MonadResource m => NonEmpty FilePath -> FilePath -> EitherT Text m ()
zebraUnion inputs output =
  firstT (Text.pack . ppShow) $
    Merge.unionFile (Cons.fromNonEmpty inputs) output

withPrintPusher ::
     MonadResource m
  => ((Foreign.CEntity -> EitherT Text m ()) -> EitherT Text m ())
  -> EitherT Text m ()
withPrintPusher runWith = do
  let pusher e = do
      eid     <- firstTshow $ Foreign.peekEntityId $ Foreign.unCEntity e
      liftIO $ IO.putStrLn (show eid)

  runWith pusher

withOutputPusher ::
     MonadResource m
  => MergeOptions
  -> FilePath
  -> FilePath
  -> ((Foreign.CEntity -> EitherT Text m ()) -> EitherT Text m ())
  -> EitherT Text m ()
withOutputPusher opts inputfile outputfile runWith = do
  (header0, _) <- firstT Binary.renderFileError $ Binary.readBlocks inputfile
  attributes <- firstT Block.renderBlockTableError . hoistEither $ Binary.attributesOfHeader header0
  let header = Binary.headerOfAttributes (mergeOutputFormat opts) attributes

  (closeOutfd, outfd) <- firstTshow $ Binary.openFile outputfile IO.WriteMode
  liftIO $ Builder.hPutBuilder outfd $ Binary.bHeader header

  pool0    <- liftIO $ Mempool.create

  let freePool poolRef = do
      pool <- IORef.readIORef poolRef
      Mempool.free pool

  blockRef <- liftIO $ IORef.newIORef Nothing
  (poolKey, poolRef) <- Resource.allocate (IORef.newIORef pool0) freePool

  let checkPurge block = do
      is <- Foreign.peekBlockRowCount (Foreign.unCBlock block)
      return (is >= mergeOutputBlockFacts opts)

  let doPurge block = do
      pool <- liftIO $ IORef.readIORef poolRef
      outblock <- firstTshow $ Foreign.blockOfForeign block
      builder <- firstT Binary.renderBinaryEncodeError . hoistEither $ Binary.bBlock header outblock
      liftIO $ Builder.hPutBuilder outfd builder
      pool' <- liftIO $ Mempool.create
      liftIO $ IORef.writeIORef poolRef pool'
      liftIO $ Mempool.free pool
      liftIO $ IORef.writeIORef blockRef Nothing

  let pusher e = do
      pool    <- liftIO $ IORef.readIORef poolRef
      block   <- liftIO $ IORef.readIORef blockRef
      block'  <- firstTshow $ Foreign.appendEntityToBlock pool e block
      ifpurge <- checkPurge block'
      if ifpurge
        then doPurge block'
        else liftIO $ IORef.writeIORef blockRef (Just block')

  runWith pusher

  mblock  <- liftIO $ IORef.readIORef blockRef
  -- This final purge will actually create a new Mempool, but it will be freed by the bracket anyway
  maybe (return ()) doPurge mblock

  Resource.release poolKey
  firstTshow closeOutfd

catFacts :: MonadIO m => [Schema.Column] -> Stream.Stream (EitherT Binary.FileError m) Block.Block -> EitherT Text m ()
catFacts schemas0 blocks =
  let
    schemas =
      Boxed.fromList schemas0

    putFact fact =
      case Fact.render schemas fact of
        Left err ->
          Text.hPutStrLn stderr $ Fact.renderFactRenderError err
        Right bs ->
          Char8.putStrLn bs

    go block = do
      facts <- firstTshow . hoistEither $ Block.factsOfBlock block
      Boxed.mapM_ (liftIO . putFact) facts

    blocks' =
      Stream.trans (firstT Binary.renderFileError) blocks
  in
    Stream.mapM_ go blocks'

catBlocks :: MonadIO m => CatOptions -> Stream.Stream (EitherT Binary.FileError m) Block.Block -> EitherT Text m ()
catBlocks opts blocks = do
  let int0 = 0 :: Int
  totalBlocks <- liftIO $ IORef.newIORef int0
  totalEnts <- liftIO $ IORef.newIORef int0
  totalIxs <- liftIO $ IORef.newIORef int0

  let go block = do
        num <- liftIO $ IORef.readIORef totalBlocks
        let numEnts = Boxed.length $ Block.blockEntities block
        let numIxs = Unboxed.length $ Block.blockIndices block
        liftIO $ IORef.modifyIORef totalBlocks (+1)
        liftIO $ IORef.modifyIORef totalEnts (+numEnts)
        liftIO $ IORef.modifyIORef totalIxs (+numIxs)

        when (catBlockSummary opts || catEntities opts) . liftIO $ do
          IO.putStrLn ("Block " <> show num)

        when (catEntities opts) $ do
          entities <- firstTshow . hoistEither $ Block.entitiesOfBlock block
          mapM_ (catEntity opts) entities

        when (catBlockSummary opts) . liftIO $ do
          IO.putStrLn ("  Entities: " <> show numEnts)
          IO.putStrLn ("  Facts:    " <> show numIxs)

  let blocks' = Stream.trans (firstT Binary.renderFileError) blocks
  Stream.mapM_ go blocks'

  numBlocks <- liftIO $ IORef.readIORef totalBlocks
  numEnts <- liftIO $ IORef.readIORef totalEnts
  numIxs <- liftIO $ IORef.readIORef totalIxs

  when (catSummary opts) . liftIO $ do
    IO.putStrLn ""
    IO.putStrLn "Total:"
    IO.putStrLn ("  Blocks:   " <> show numBlocks)
    IO.putStrLn ("  Entities: " <> show numEnts)
    IO.putStrLn ("  Facts:    " <> show numIxs)

  return ()

catEntity :: MonadIO m => CatOptions -> Entity.Entity -> EitherT Text m ()
catEntity opts entity = liftIO $ do
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
