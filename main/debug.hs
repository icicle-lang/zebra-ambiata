{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}


import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Segv (withSegv)

import           Control.Exception (assert)
import           Control.Monad.Catch (bracket)

import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           P

import           System.IO (IO)


import           Text.Show.Pretty (ppShow)

import           X.Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT)

import           Zebra.Data.Block
import           Zebra.Data.Core
import           Zebra.Data.Entity
import           Zebra.Foreign.Block
import           Zebra.Foreign.Entity
import           Zebra.Foreign.Util
import           Zebra.Data.Table

main :: IO ()
main =
  withSegv (ppShow testBlock) $ do
    block' <- runEitherT ( foreignEntitiesOfBlock' testBlock >>= foreignBlockOfEntities')
    let expect
          | Boxed.null (blockEntities testBlock)
          = Nothing
          | otherwise
          = Just testBlock
    assert (Right expect == block') $ pure ()

foreignEntitiesOfBlock' :: Block -> EitherT ForeignError IO (Boxed.Vector Entity)
foreignEntitiesOfBlock' block =
  EitherT .
  bracket Mempool.create Mempool.free $ \pool ->
  runEitherT $ do
    fblock <- foreignOfBlock pool block
    fentities <- foreignEntitiesOfBlock pool fblock
    traverse entityOfForeign fentities

foreignBlockOfEntities' :: Boxed.Vector Entity -> EitherT ForeignError IO (Maybe Block)
foreignBlockOfEntities' entities =
  EitherT .
  bracket Mempool.create Mempool.free $ \pool ->
  runEitherT $ do
    fentities <- traverse (foreignOfEntity pool) entities
    fblock <- foldM (\fb fe -> Just <$> appendEntityToBlock pool fe fb) Nothing fentities
    traverse blockOfForeign fblock


testBlock :: Block
testBlock = Block
  { blockEntities = Boxed.fromList
      [ BlockEntity
          (EntityHash 2)
          (EntityId "stan-585")
              (Unboxed.fromList [ BlockAttribute (AttributeId 2) 1 ])
      , BlockEntity
          (EntityHash 5)
          (EntityId "token-453")
              (Unboxed.fromList [ BlockAttribute (AttributeId 3) 1 ])
      ]
  , blockIndices = Unboxed.fromList []
  , blockTables = Boxed.fromList
      [ Table $ Boxed.fromList [ IntColumn $ Storable.fromList [] ]
      , Table $ Boxed.fromList [ IntColumn $ Storable.fromList [ 2 , 1 ] ]
      , Table $ Boxed.fromList [ IntColumn $ Storable.fromList [ 0 , 0 ] ]
      , Table $ Boxed.fromList
          [ ArrayColumn
              (Storable.fromList [ 7 , 0 , 2 ])
              (Table
                 (Boxed.fromList
                 [ ArrayColumn
                     (Storable.fromList [ 0 , 7 , 1 , 0 , 0 , 9 , 9 , 0 ])
                     (Table $ Boxed.fromList
                        [ ArrayColumn
                            (Storable.fromList [ 3, 1, 2, 0, 4, 3, 2, 2, 3, 1, 1, 4, 2, 0, 4, 0, 0, 0, 4, 4, 4, 3, 3, 0, 3, 1 ])
                            (Table $ Boxed.fromList [ ByteColumn "boring" ])
                        ])
                 ])
               )
          ]
      ]
  }
