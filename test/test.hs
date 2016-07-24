import           Disorder.Core.Main

import qualified Test.Zebra.Data.Block
import qualified Test.Zebra.Data.Fact
import qualified Test.Zebra.Data.Record
import qualified Test.Zebra.Data.Schema
import qualified Test.Zebra.Serial.Array
import qualified Test.Zebra.Serial.Block
import qualified Test.Zebra.Serial.Header
import qualified Test.Zebra.Merge.Entity

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Data.Block.tests
    , Test.Zebra.Data.Fact.tests
    , Test.Zebra.Data.Record.tests
    , Test.Zebra.Data.Schema.tests
    , Test.Zebra.Serial.Array.tests
    , Test.Zebra.Serial.Block.tests
    , Test.Zebra.Serial.Header.tests
    , Test.Zebra.Merge.Entity.tests
    ]
