#!/bin/bash
#http://bytes.com/groups/ibm-db2/468129-db2-shell-script
. $cfgroot/inc/db2v95.cfg.inc
db2 -x "CONNECT TO ${database}" >/dev/null
if [[ $? -ge 1 ]]; then
echo "Failed to connect to ${database}"
exit 1
fi
