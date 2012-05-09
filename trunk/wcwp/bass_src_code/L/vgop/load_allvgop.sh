#!/bin/ksh
#*******************************************************
#函数名：load_allvgop.sh
#功  能：获取VGOP接口信息，并加载入库
#参  数：
#author: liwei
#日  期: 2011-4-20 
#        2012-02-14修改by xiebq
#输  出: nohup load_allvgop.sh 20110401 &
#*******************************************************

    echo "Begining:`date`"
    if [ $# -lt 1 ]
    then
          echo "Please input date's argument..."
          echo "Format like this: load_allvgop.sh YYYYMMDD"
          exit 1
    fi
    time_id=$1
    #配置信息
    WORK_PATH=/bassdb1/etl/L/vgop
    DATA_PATH=${WORK_PATH}/backup
    LOG_PATH=${WORK_PATH}/liwei/log
    
    #取vgop接口文件名
    cd ${DATA_PATH}/${time_id}
    ls -C1 *.dat > ${WORK_PATH}/liwei/vgop_ifile.txt
    
    while read vgop_ilist
    do
        #echo $vgop_ilist
        ${WORK_PATH}/load_vgop_dmkdb.sh $vgop_ilist >> ${LOG_PATH}/${time_id}.log
    
    done < ${WORK_PATH}/liwei/vgop_ifile.txt
    
    echo "Ended:`date`"
    