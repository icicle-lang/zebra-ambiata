import           Disorder.Core.Main

import qualified Test.Zebra.Binary.Array
import qualified Test.Zebra.Binary.Block
import qualified Test.Zebra.Binary.File
import qualified Test.Zebra.Binary.Header
import qualified Test.Zebra.Binary.Table
import qualified Test.Zebra.Data.Block
import qualified Test.Zebra.Data.Core
import qualified Test.Zebra.Foreign.Block
import qualified Test.Zebra.Foreign.Entity
import qualified Test.Zebra.Foreign.Merge
import qualified Test.Zebra.Foreign.Table
import qualified Test.Zebra.Json.Schema
import qualified Test.Zebra.Json.Table
import qualified Test.Zebra.Json.Value
import qualified Test.Zebra.Merge.Entity
import qualified Test.Zebra.Table
import qualified Test.Zebra.Text.Schema
import qualified Test.Zebra.Text.Table
import qualified Test.Zebra.Text.Value
import qualified Test.Zebra.Value

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Binary.Array.tests
    , Test.Zebra.Binary.Block.tests
    , Test.Zebra.Binary.File.tests
    , Test.Zebra.Binary.Header.tests
    , Test.Zebra.Binary.Table.tests
    , Test.Zebra.Data.Block.tests
    , Test.Zebra.Data.Core.tests
    , Test.Zebra.Foreign.Block.tests
    , Test.Zebra.Foreign.Entity.tests
    , Test.Zebra.Foreign.Merge.tests
    , Test.Zebra.Foreign.Table.tests
    , Test.Zebra.Json.Schema.tests
    , Test.Zebra.Json.Table.tests
    , Test.Zebra.Json.Value.tests
    , Test.Zebra.Merge.Entity.tests
    , Test.Zebra.Table.tests
    , Test.Zebra.Text.Schema.tests
    , Test.Zebra.Text.Table.tests
    , Test.Zebra.Text.Value.tests
    , Test.Zebra.Value.tests
    ]
