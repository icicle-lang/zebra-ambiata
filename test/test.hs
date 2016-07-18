import           Disorder.Core.Main

import qualified Test.Zebra.Data.Schema
import qualified Test.Zebra.Serial.Array
import qualified Test.Zebra.Serial.Block
import qualified Test.Zebra.Serial.Header

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Data.Schema.tests
    , Test.Zebra.Serial.Array.tests
    , Test.Zebra.Serial.Block.tests
    , Test.Zebra.Serial.Header.tests
    ]
