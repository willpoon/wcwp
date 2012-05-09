#!/bin/sh


IS_RUN=`ps -ef | grep vgopMaintest | grep -v grep | wc -l`
if [ $# -eq 0 ] ; then
         if [ $IS_RUN -ge 1 ]; then
               echo "process_cc is already running..."  
         else
               echo "now start process_cc..."
		cd /bassdb1/etl/L/vgop/vgop_interface_zhouc/bin
		CLASSPATH=.:/bassdb1/etl/L/vgop/vgop_interface_zhouc/lib/commons-net-1.4.1.jar:/bassdb1/etl/L/vgop/vgop_interface_zhouc/lib/jakarta-oro-2.0.8.jar
		export CLASSPATH
		LANG=zh_CN.GBK;export NLS_LANG
		LC_ALL=zh_CN.GBK;export LC_ALL
		nohup /usr/j2se/bin/java vgopMaintest &
         fi
elif [ $# -eq 1 ] ; then
    if [ $1 = "start" ] ; then
         if [ $IS_RUN -ge 1 ]; then
               echo "process_cc is already running..."  
         else
               echo "now start process_cc..."
		cd /bassdb1/etl/L/vgop/vgop_interface_zhouc/bin
		CLASSPATH=.:/bassdb1/etl/L/vgop/vgop_interface_zhouc/lib/commons-net-1.4.1.jar:/bassdb1/etl/L/vgop/vgop_interface_zhouc/lib/jakarta-oro-2.0.8.jar
		export CLASSPATH
		LANG=zh_CN.GBK;export NLS_LANG
		LC_ALL=zh_CN.GBK;export LC_ALL
		nohup /usr/j2se/bin/java vgopMaintest &
         fi
    elif [ $1 = "stop" ] ; then  
               cd /bassdb1/etl/L/vgop/vgop_interface_zhouc/bin
               touch stop_vgop_flag_test
    else
         echo "please use:process_cc.sh start[stop]"  
    fi	   
else 
     echo "please use:process_cc.sh start[stop]"  
fi
