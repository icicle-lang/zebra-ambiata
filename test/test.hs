import           Disorder.Core.Main

import qualified Test.Zebra.Data.Block
import qualified Test.Zebra.Data.Core
import qualified Test.Zebra.Foreign.Block
import qualified Test.Zebra.Foreign.Entity
import qualified Test.Zebra.Foreign.Merge
import qualified Test.Zebra.Foreign.Table
import qualified Test.Zebra.Merge.Entity
import qualified Test.Zebra.Schema
import qualified Test.Zebra.Serial.Array
import qualified Test.Zebra.Serial.Block
import qualified Test.Zebra.Serial.File
import qualified Test.Zebra.Serial.Header
import qualified Test.Zebra.Serial.Table
import qualified Test.Zebra.Table

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Data.Block.tests
    , Test.Zebra.Data.Core.tests
    , Test.Zebra.Foreign.Block.tests
    , Test.Zebra.Foreign.Entity.tests
    , Test.Zebra.Foreign.Merge.tests
    , Test.Zebra.Foreign.Table.tests
    , Test.Zebra.Merge.Entity.tests
    , Test.Zebra.Schema.tests
    , Test.Zebra.Serial.Array.tests
    , Test.Zebra.Serial.Block.tests
    , Test.Zebra.Serial.File.tests
    , Test.Zebra.Serial.Header.tests
    , Test.Zebra.Serial.Table.tests
    , Test.Zebra.Table.tests
    ]
