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
          (EntityHash 3)
          (EntityId "timmy-893")
              (Unboxed.fromList [ BlockAttribute (AttributeId 2) 1 ])
      , BlockEntity
          (EntityHash 5)
          (EntityId "token-453")
              (Unboxed.fromList [ BlockAttribute (AttributeId 3) 1 ])
      , BlockEntity
          (EntityHash 6)
          (EntityId "timmy-697")
              (Unboxed.fromList [ BlockAttribute (AttributeId 3) 1 ])
      , BlockEntity
          (EntityHash 7)
          (EntityId "chef-687")
              (Unboxed.fromList [ BlockAttribute (AttributeId 1) 1 ])
      , BlockEntity
          (EntityHash 8)
          (EntityId "timmy-479")
              (Unboxed.fromList [ BlockAttribute (AttributeId 1) 1 ])
      , BlockEntity
          (EntityHash 9)
          (EntityId "timmy-011")
              (Unboxed.fromList [ BlockAttribute  (AttributeId 3) 1 ])
      ]
  , blockIndices = Unboxed.fromList
      [ BlockIndex
          (Time 40250995200)
          (Priority 57532)
          Tombstone
      , BlockIndex
          (Time 37858060800)
          (Priority 98097)
          Tombstone
      , BlockIndex
          (Time 39777177600)
          (Priority 47403)
          NotTombstone
      , BlockIndex
          (Time 20641132800)
          (Priority 87729)
          Tombstone
      , BlockIndex
          (Time 9343468800)
          (Priority 66902)
          NotTombstone
      , BlockIndex
          (Time 7296307200)
          (Priority 78104)
          NotTombstone
      , BlockIndex
          (Time 33686150400)
          (Priority 77789)
          NotTombstone
      ]
  , blockTables = Boxed.fromList
      [ Table $ Boxed.fromList [ IntColumn $ Storable.fromList [] ]
      , Table $ Boxed.fromList [ IntColumn $ Storable.fromList [ 2 , 1 ] ]
      , Table $ Boxed.fromList
          [ IntColumn $ Storable.fromList [ 0 , 0 ]
          , DoubleColumn $ Storable.fromList [ 0.0 , 0.0 ]
          , IntColumn $ Storable.fromList [ 0 , 0 ]
          , IntColumn $ Storable.fromList [ 0 , 0 ]
          , IntColumn $ Storable.fromList [ 0 , 0 ]
          , ArrayColumn (Storable.fromList [ 0 , 0 ]) (Table $ Boxed.fromList [ ByteColumn "" ])
          ]
      , Table $ Boxed.fromList
          [ ArrayColumn
              (Storable.fromList [ 7 , 0 , 2 ])
              (Table
                 (Boxed.fromList [ IntColumn $ Storable.fromList [ 1 , 1 , 1 , 0 , 1 , 1 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList
                     [ 41037840000
                     , 30966710400
                     , 5987260800
                     , 0
                     , 22897036800
                     , 7777036800
                     , 31054492800
                     , 26730432000
                     , 0
                     ]
                 , IntColumn $ Storable.fromList
                     [ 20614608000
                     , 21400934400
                     , 27543196800
                     , 4660243200
                     , 17314300800
                     , 36704448000
                     , 22532342400
                     , 21505651200
                     , 28877472000
                     ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 0 , 1 , 1 , 1 , 1 , 1 , 1 ]
                 , DoubleColumn $ Storable.fromList
                     [ 0.0
                     , 5.321593006929769
                     , 0.0
                     , 3.767846204098485
                     , 2.766696712680897
                     , 67.75616960230039
                     , -1.5069604042010043
                     , -6.675312781135853
                     , 16.365353925539843
                     ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 1 , 1 , 0 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 1 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList
                     [ 0 , 0 , 28961452800 , 32683737600 , 0 , 0 , 0 , 14235609600 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ])
                     (Table $ Boxed.fromList [ ByteColumn "oc" ])
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , -2 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 1 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 0 , 0 , 0 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 0 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 2 , 3 , 0 , 0 , 0 , 0 , 1 , 0 ])
                     (Table $ Boxed.fromList [ ByteColumn "\195\128@\195\145%" ])
                 , IntColumn $ Storable.fromList [ 0 , -1 , -2 , 0 , 0 , 0 , 0 , -2 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , DoubleColumn $ Storable.fromList
                     [ 0.0
                     , 0.0
                     , -8.160914710015918
                     , 0.0
                     , 0.0
                     , 0.0
                     , 0.0
                     , -16.52163897889858
                     , 0.0
                     ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 0 , 0 , 0 , 0 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 2 , 0 , 0 , 0 , 0 , -1 , 0 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 0 , 4 , 0 , 0 , 0 , 3 , 1 , 0 ])
                     (Table  $ Boxed.fromList [ ByteColumn "\DC2\194\149\&4\\\195\148F" ])
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 0 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 0 , 2 , 0 , 0 , 0 , 0 , 0 , 0 ])
                     (Table  $ Boxed.fromList [ ByteColumn "j\SYN" ])
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 0 , 0 , 0 , 1 , 1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 1 , 3 , 0 ])
                     (Table  $ Boxed.fromList [ ByteColumn "{A\195\158Q" ])
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 0 , 0 , 0 , 1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 0 , 2 , 0 , 0 , 0 , 1 , 2 , 0 ])
                     (Table  $ Boxed.fromList [ ByteColumn "|\SOj/\v" ])
                 , ArrayColumn
                     (Storable.fromList [ 0 , 7 , 4 , 1 , 0 , 0 , 9 , 9 , 0 ])
                     (Table $ Boxed.fromList
                        [ ArrayColumn
                            (Storable.fromList [ 3
                            , 1
                            , 2
                            , 0
                            , 4
                            , 3
                            , 2
                            , 2
                            , 3
                            , 1
                            , 1
                            , 4
                            , 2
                            , 0
                            , 4
                            , 0
                            , 0
                            , 0
                            , 4
                            , 4
                            , 4
                            , 3
                            , 3
                            , 0
                            , 3
                            , 1
                            , 1
                            , 3
                            , 3
                            , 2
                            ])
                            (Table $ Boxed.fromList
                               [ ByteColumn
                                   "9\195\187\\\195\188\195\157\v.\195\175WFmd\NUL\FS- N\ACKl\194\137|7R\194\164\ESC\tM.\194\133\195\179^*\194\185\195\161v\DLEnct1?~CX\t\195\179F;\ESCO\194\186"
                               ])
                        ])
                 , IntColumn $ Storable.fromList [ 0 , 1 , 0 , 0 , 1 , 0 , 1 , 0 , 0 ]
                 , IntColumn $ Storable.fromList
                     [ 0 , 22848652800 , 0 , 0 , 34321968000 , 0 , 27410140800 , 0 , 0 ]
                 , IntColumn $ Storable.fromList
                     [ 21746448000
                     , 11308032000
                     , 43378243200
                     , 34832419200
                     , 41587257600
                     , 13812422400
                     , 9640252800
                     , 1878249600
                     , 15249168000
                     ]
                 , IntColumn $ Storable.fromList [ 1 , 0 , 1 , 1 , 1 , 1 , 1 , 1 , 1 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 0 , 1 , 1 , 0 , 1 , 1 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 2 , 0 , 2 , 0 , 0 , 0 , 0 ]
                 , IntColumn $ Storable.fromList [ 1 , 0 , 1 , 0 , 1 , 0 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList
                     [ 26873856000
                     , 0
                     , 27526867200
                     , 0
                     , 35738668800
                     , 0
                     , 34568899200
                     , 4446057600
                     , 0
                     ]
                 , IntColumn $ Storable.fromList [ 1 , 0 , 0 , 0 , 1 , 0 , 1 , 1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 10 , 0 , 5 , 7 , 10 , 3 , 8 , 10 , 3 ])
                     (Table
                        (Boxed.fromList [ IntColumn $ Storable.fromList
                            [ -1
                            , 1
                            , 1
                            , 1
                            , 2
                            , 0
                            , 1
                            , 1
                            , -2
                            , 1
                            , -1
                            , 2
                            , 1
                            , -2
                            , 1
                            , 0
                            , -1
                            , 0
                            , 1
                            , -1
                            , 1
                            , 2
                            , 2
                            , -1
                            , -2
                            , -1
                            , -2
                            , -1
                            , -2
                            , 1
                            , 1
                            , 0
                            , -2
                            , 2
                            , 1
                            , 2
                            , -2
                            , -1
                            , -2
                            , -1
                            , -1
                            , 1
                            , -1
                            , -1
                            , 2
                            , 2
                            , 0
                            , -2
                            , 0
                            , 1
                            , 0
                            , 0
                            , -1
                            , 0
                            , 1
                            , -1
                            ]
                        ]))
                 , IntColumn $ Storable.fromList [ 1 , 0 , 1 , 1 , 1 , 0 , 1 , 1 , 1 ]
                 , IntColumn $ Storable.fromList [ -2 , 0 , -1 , -2 , 1 , 0 , 2 , 2 , -2 ]
                 , IntColumn $ Storable.fromList [ 1 , 0 , 1 , 1 , 1 , 1 , 1 , 0 , 1 ]
                 , IntColumn $ Storable.fromList
                     [ 6023289600
                     , 0
                     , 1831680000
                     , 17829244800
                     , 20677766400
                     , 19159718400
                     , 13995072000
                     , 0
                     , 12885609600
                     ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 1 , 1 , 1 , 1 , 1 , 1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 0 , 0 , 1 , 5 , 0 , 4 , 0 , 5 , 0 ])
                     (Table
                        (Boxed.fromList [ ByteColumn "i\195\146\\\195\188\195\147\a\SO\194\168\195\144\DC3"
                        ]))
                 , DoubleColumn $ Storable.fromList
                     [ -4.266930698687289
                     , 11.49848777335629
                     , 11.500638298423343
                     , 0.7487354942187031
                     , -2.3317130388316514
                     , 0.35888299719263345
                     , 2.010090078042181
                     , 3.28089671822586
                     , 3.8611669065398093
                     ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 1 , 0 , 0 , 0 , 1 , 0 , 0 ]
                 , DoubleColumn $ Storable.fromList
                     [ 0.0
                     , -0.31470501564117115
                     , -9.922383257775492
                     , 0.0
                     , 0.0
                     , 0.0
                     , 2.9621811501880018
                     , 0.0
                     , 0.0
                     ]
                 , IntColumn $ Storable.fromList [ 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 1 , 0 , 1 , 1 , 1 , 0 , 1 , 0 , 0 ]
                 , DoubleColumn $ Storable.fromList
                     [ -3.850656727153387
                     , 0.0
                     , 47.17306284317328
                     , 4.331670692800078
                     , 5.511367313998637
                     , 0.0
                     , -1.4896024005878132
                     , 0.0
                     , 0.0
                     ]
                 , IntColumn $ Storable.fromList [ 1 , 0 , 1 , 1 , 1 , 1 , 1 , 0 , 0 ]
                 , IntColumn $ Storable.fromList [ 2 , 0 , -2 , 2 , 0 , 2 , 0 , 0 , 0 ]
                 , IntColumn $ Storable.fromList [ 2 , 1 , 0 , 0 , -1 , 0 , 0 , 1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 1 , 2 , 0 , 1 , 0 , 5 , 2 , 0 , 0 ])
                     (Table (Boxed.fromList [ ByteColumn "\a\194\163\RS\NAK\194\187\194\180\195\187" ]))
                 , IntColumn $ Storable.fromList [ 1 , 1 , 1 , 1 , 1 , 1 , 1 , 0 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 0 , 0 , 0 , 1 , 1 , 1 , 0 , 0 ]
                 , IntColumn $ Storable.fromList [ 1 , 1 , 1 , 1 , 1 , 0 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 1 , -1 , 0 , 2 , -1 , 0 , -2 , -1 , 0 ]
                 , ArrayColumn
                     (Storable.fromList [ 3 , 2 , 2 , 2 , 1 , 0 , 2 , 0 , 1 ])
                     (Table $ Boxed.fromList [ ByteColumn "^6E\194\172\\\DLE\195\166o\194\176\SUB" ])
                 , IntColumn $ Storable.fromList
                     [ 44061667200
                     , 22749897600
                     , 35398080000
                     , 11167632000
                     , 14754009600
                     , 2398550400
                     , 21373718400
                     , 7297171200
                     , 7110892800
                     ]
                 , IntColumn $ Storable.fromList [ 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 0 ]
                 , IntColumn $ Storable.fromList [ 0 , 1 , 0 , 0 , 1 , 0 , 1 , 1 , 0 ]
                 , DoubleColumn $ Storable.fromList
                     [ -1.9145293498293126
                     , -3.627493224139408
                     , -0.6443023755156849
                     , 4.140731143652077
                     , 4.595381953548792
                     , -2.2023677202670435
                     , -9.177276058638947e-2
                     , -11.720061807420562
                     , -1.3069479596356866
                     ]
                 , IntColumn $ Storable.fromList
                     [ 40963017600
                     , 22538649600
                     , 1005523200
                     , 28941753600
                     , 8615894400
                     , 8094816000
                     , 22819622400
                     , 38686982400
                     , 9719136000
                     ]
                 ])
               )
          ]
      , Table $ Boxed.fromList [ DoubleColumn (Storable.empty) ]
      ]
  }
