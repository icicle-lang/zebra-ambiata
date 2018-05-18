import           Disorder.Core.Main

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
import qualified Test.Zebra.Serial.Json.Util
import qualified Test.Zebra.Serial.Text.Logical
import qualified Test.Zebra.Serial.Text.Schema
import qualified Test.Zebra.Serial.Text.Striped
import qualified Test.Zebra.Table.Logical
import qualified Test.Zebra.Table.Striped
import qualified Test.Zebra.Time

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Merge.Table.tests
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
    , Test.Zebra.Serial.Json.Util.tests
    , Test.Zebra.Serial.Text.Logical.tests
    , Test.Zebra.Serial.Text.Schema.tests
    , Test.Zebra.Serial.Text.Striped.tests
    , Test.Zebra.Table.Logical.tests
    , Test.Zebra.Table.Striped.tests
    , Test.Zebra.Time.tests
    ]
