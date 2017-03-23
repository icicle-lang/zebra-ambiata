module BuildInfo_ambiata_zebra where
import Prelude
data RuntimeBuildInfo = RuntimeBuildInfo { buildVersion :: String, timestamp :: String, gitVersion :: String }
buildInfo :: RuntimeBuildInfo
buildInfo = RuntimeBuildInfo "0.0.1" "20170317224939" "3684bc6"
buildInfoVersion :: String
buildInfoVersion = "0.0.1-20170317224939-3684bc6"
