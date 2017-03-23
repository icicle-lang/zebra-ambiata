module BuildInfo_ambiata_zebra_core where
import Prelude
data RuntimeBuildInfo = RuntimeBuildInfo { buildVersion :: String, timestamp :: String, gitVersion :: String }
buildInfo :: RuntimeBuildInfo
buildInfo = RuntimeBuildInfo "0.0.1" "20170323143556" "441f9c3-M"
buildInfoVersion :: String
buildInfoVersion = "0.0.1-20170323143556-441f9c3-M"
