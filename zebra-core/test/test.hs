import           Disorder.Core.Main

import qualified Test.Zebra.Factset.Block
import qualified Test.Zebra.Factset.Data
import qualified Test.Zebra.Foreign.Block
import qualified Test.Zebra.Foreign.Entity
import qualified Test.Zebra.Foreign.Merge
import qualified Test.Zebra.Foreign.Table
import qualified Test.Zebra.Merge.Entity
import qualified Test.Zebra.Merge.Table
import qualified Test.Zebra.Serial.Binary.Array
import qualified Test.Zebra.Serial.Binary.Block
import qualified Test.Zebra.Serial.Binary.File
import qualified Test.Zebra.Serial.Binary.Header
import qualified Test.Zebra.Serial.Binary.Logical
import qualified Test.Zebra.Serial.Binary.Striped
import qualified Test.Zebra.Serial.Binary.Table
import qualified Test.Zebra.Serial.Json.Logical
import qualified Test.Zebra.Serial.Json.Schema
import qualified Test.Zebra.Serial.Json.Striped
import qualified Test.Zebra.Serial.Text.Logical
import qualified Test.Zebra.Serial.Text.Schema
import qualified Test.Zebra.Serial.Text.Striped
import qualified Test.Zebra.Table.Logical
import qualified Test.Zebra.Table.Striped
import qualified Test.Zebra.Time

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Factset.Block.tests
    , Test.Zebra.Factset.Data.tests
    , Test.Zebra.Foreign.Block.tests
    , Test.Zebra.Foreign.Entity.tests
    , Test.Zebra.Foreign.Merge.tests
    , Test.Zebra.Foreign.Table.tests
    , Test.Zebra.Merge.Table.tests
    , Test.Zebra.Merge.Entity.tests
    , Test.Zebra.Serial.Binary.Array.tests
    , Test.Zebra.Serial.Binary.Block.tests
    , Test.Zebra.Serial.Binary.File.tests
    , Test.Zebra.Serial.Binary.Header.tests
    , Test.Zebra.Serial.Binary.Logical.tests
    , Test.Zebra.Serial.Binary.Striped.tests
    , Test.Zebra.Serial.Binary.Table.tests
    , Test.Zebra.Serial.Json.Logical.tests
    , Test.Zebra.Serial.Json.Schema.tests
    , Test.Zebra.Serial.Json.Striped.tests
    , Test.Zebra.Serial.Text.Logical.tests
    , Test.Zebra.Serial.Text.Schema.tests
    , Test.Zebra.Serial.Text.Striped.tests
    , Test.Zebra.Table.Logical.tests
    , Test.Zebra.Table.Striped.tests
    , Test.Zebra.Time.tests
    ]
