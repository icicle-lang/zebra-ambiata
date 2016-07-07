import           Disorder.Core.Main

import qualified Test.Zebra.Array
import qualified Test.Zebra.Header
import qualified Test.Zebra.Schema

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Array.tests
    , Test.Zebra.Header.tests
    , Test.Zebra.Schema.tests
    ]
