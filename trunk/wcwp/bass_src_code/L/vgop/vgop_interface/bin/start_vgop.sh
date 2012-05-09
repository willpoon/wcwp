cd /bassdb1/etl/L/vgop/vgop_interface/bin
CLASSPATH=.:/bassdb1/etl/L/vgop/vgop_interface/lib/commons-net-1.4.1.jar:/bassdb1/etl/L/vgop/vgop_interface/lib/jakarta-oro-2.0.8.jar
export CLASSPATH

LANG=zh_CN.GBK;         export NLS_LANG
LC_ALL=zh_CN.GBK; export LC_ALL

nohup /usr/j2se/bin/java vgopMain &
