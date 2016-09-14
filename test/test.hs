import           Disorder.Core.Main

import qualified Test.Zebra.Data.Block
import qualified Test.Zebra.Data.Fact
import qualified Test.Zebra.Data.Schema
import qualified Test.Zebra.Data.Table
import qualified Test.Zebra.Data.Table.Mutable
import qualified Test.Zebra.Merge.Entity
import qualified Test.Zebra.Serial.Array
import qualified Test.Zebra.Serial.Block
import qualified Test.Zebra.Serial.File
import qualified Test.Zebra.Serial.Header

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Data.Block.tests
    , Test.Zebra.Data.Fact.tests
    , Test.Zebra.Data.Schema.tests
    , Test.Zebra.Data.Table.tests
    , Test.Zebra.Data.Table.Mutable.tests
    , Test.Zebra.Merge.Entity.tests
    , Test.Zebra.Serial.Array.tests
    , Test.Zebra.Serial.Block.tests
    , Test.Zebra.Serial.File.tests
    , Test.Zebra.Serial.Header.tests
    ]
