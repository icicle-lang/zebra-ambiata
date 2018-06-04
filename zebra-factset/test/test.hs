import           Disorder.Core.Main

import qualified Test.Zebra.Factset.Block
import qualified Test.Zebra.Factset.Data

main :: IO ()
main =
  disorderMain [
      Test.Zebra.Factset.Block.tests
    , Test.Zebra.Factset.Data.tests
    ]
