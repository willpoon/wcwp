while read tabname
do 
grep -i $tabname *.tcl > /dev/null
ret_code=$?
echo $tabname~$ret_code
done<<!
CL_20100629
DIM_21003_IP_TYPE
!

db2 runstats on table bass1.G_S_04002_DAY_TD with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04005_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21008_TO_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04018_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06031_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06032_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02063_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04010_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04009_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04008_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04012_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04011_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22073_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22012_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22302_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22301_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02061_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02060_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02059_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02058_DAY with distribution and detailed indexes all 
#db2 runstats on table bass1.G_S_22073_DAY_TEST with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22201_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02053_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02008_DAY_T with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_T with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21016_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21002_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22104_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22102_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22202_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22203_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02013_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04016_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22038_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04015_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21006_TO_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21009_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21005_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21001_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_02007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04017_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04014_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04006_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22084_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02023_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02022_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04018_DAY_FLOWS with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04017_DAY_TD with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_06001_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02064_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02062_DAY_20110317REPAIR with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02062_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02061_DAY_0317REPAIR with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21003_TO_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02059_DAY_20110321FIX1340 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02059_DAY_0315MODIFY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02057_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02056_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02055_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02054_DAY_BLACK with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02054_DAY_0317_1220REPAIR with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02054_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TMP with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TD2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02011_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02008_DAY_TAKE with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TD1 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_TAKE with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_CREATE_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_CREATE with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_create_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_create with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_FLOWS with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01006_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01002_DAY with distribution and detailed indexes all 

-----
db2 runstats on table bass1.G_S_22083_MONTH_4 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22083_MONTH_3 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22083_MONTH_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22083_MONTH_1 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03004_MONTH_TD with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21003_MONTH_TD with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21003_MONTH_TMP with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TD with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22081_MONTH_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22081_MONTH_1 with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02021_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02021_MONTH_TEMP2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02021_MONTH_TEMP1 with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02019_MONTH_4 with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02019_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02019_MONTH_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02019_MONTH_1 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04005_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21003_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21008_TO_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21008_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04018_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_02047_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02052_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02049_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_21020_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06031_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06032_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22305_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02063_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03012_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03004_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04010_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04009_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04008_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04012_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04011_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22073_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22012_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22304_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22303_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03018_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03017_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22302_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22301_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02061_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02060_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02059_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02058_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22073_DAY_TEST with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22201_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02053_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06029_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02053_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02008_DAY_T with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_T with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21016_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21002_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22013_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22106_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22105_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22104_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22103_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22102_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22101_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22202_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22203_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03005_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02013_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03016_MONTH_LS with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22036_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22056_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22055_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22052_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22050_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_05003_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04016_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22072_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22038_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04015_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22032_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03016_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22043_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03002_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03002_MONTH_TYM with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22040_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22042_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22041_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22033_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_03015_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21010_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_05002_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_05001_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22025_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21013_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21014_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21011_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21015_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21012_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21020_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22009_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06012_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06011_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06001_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02022_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22039_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21006_TO_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21009_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21005_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21001_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_02007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21006_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22021_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_03007_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04017_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_03003_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_03002_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_03001_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02023_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02005_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_01006_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04014_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04006_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04004_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22085_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22084_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22083_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02023_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22081_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02022_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22065_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22064_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22063_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02020_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22062_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22061_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_02007_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22049_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02018_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.INT_02011_SNAPSHOT with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02016_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02015_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02014_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02006_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04018_DAY_FLOWS with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04017_DAY_TD with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_06001_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02064_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02062_DAY_20110317REPAIR with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02062_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02061_DAY_0317REPAIR with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_21003_TO_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06023_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02059_DAY_20110321FIX1340 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02059_DAY_0315MODIFY with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06022_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06021_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02057_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02056_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02055_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02054_DAY_BLACK with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02054_DAY_0317_1220REPAIR with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02054_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TMP with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_02017_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TD2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02011_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02008_DAY_TAKE with distribution and detailed indexes all 
db2 runstats on table bass1.G_I_06002_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_TD1 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_TAKE with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22401_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_CREATE_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_CREATE with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_create_2 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_02004_DAY_create with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_04002_DAY_FLOWS with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01007_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01006_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01005_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01004_TMP4 with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01004_TMP3 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22307_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01004_TMP12 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22306_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01004_TMP10 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22204_MONTH_TMP3 with distribution and detailed indexes all 
db2 runstats on table bass1.G_S_22204_MONTH with distribution and detailed indexes all 
db2 runstats on table bass1.G_A_01002_DAY with distribution and detailed indexes all 
db2 runstats on table bass1.DIM_21003_IP_TYPE with distribution and detailed indexes all 