#!/usr/bin/ksh
#author : panzw 
. $PZW_INC/common.ksh
exp_alltab="
EXPORT TO /home/zhengfm/panzw/outdata/acrm_ref_tr_all.csv.txt OF DEL 
modified by nochardel 
striplzeros 
decplusblank 
select          
                        rtrim(tabschema)
        ||'.'||         tabname
        ,               card
        ,               colcount
        ,               npages
        ,               fpages
        ,               create_time
        ,               stats_time
        ,               tbspace 
from            syscat.tables
order by 1
with ur;
"
db2_exec_sql "$exp_alltab"
