#purpose:a short-cut to get  table structure
. $cfgroot/inc/db2v95.cfg.inc
db2 "connect to $database user $user using $pass"
db2 "describe table $1"


