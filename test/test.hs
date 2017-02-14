import           Disorder.Core.Main

import qualified Test.Zebra.Data.Block
import qualified Test.Zebra.Data.Core
import qualified Test.Zebra.Data.Encoding
import qualified Test.Zebra.Data.Table
import qualified Test.Zebra.Data.Table.Mutable
import qualified Test.Zebra.Foreign.Block
import qualified Test.Zebra.Foreign.Entity
import qualified Test.Zebra.Foreign.Merge
import qualified Test.Zebra.Foreign.Table
import qualified Test.Zebra.Merge.Entity
import qualified Test.Zebra.Serial.Array
import qualified Test.Zebra.Serial.Block
import qualified Test.Zebra.Serial.File
import qualified Test.Zebra.Serial.Header

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Data.Block.tests
    , Test.Zebra.Data.Core.tests
    , Test.Zebra.Data.Encoding.tests
    , Test.Zebra.Data.Table.Mutable.tests
    , Test.Zebra.Data.Table.tests
    , Test.Zebra.Foreign.Block.tests
    , Test.Zebra.Foreign.Entity.tests
    , Test.Zebra.Foreign.Merge.tests
    , Test.Zebra.Foreign.Table.tests
    , Test.Zebra.Merge.Entity.tests
    , Test.Zebra.Serial.Array.tests
    , Test.Zebra.Serial.Block.tests
    , Test.Zebra.Serial.File.tests
    , Test.Zebra.Serial.Header.tests
    ]
