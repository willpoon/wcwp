#!/bin/ksh
################################################################################
# 模块: load.sh
# 描述: 根据一个数据库或表名来卸载数据并生成对应的数据文件
#
# 参数 1 table= 表名
# 参数 2 infile= 文件名
# 参数 3 replaceflag=是否清空表 I ：直接插入 ； R ：清空表再插入
#
# 作者 Lille Jam
#
# 修改记录
# 日期        修改人      修改描述
# 20060901    Lille Jam   新建脚本
# 20060909    Lille Jam   加入判断表是否存在功能
#                         加入质量控制功能,允许1%错误范围
# 20060930    Lille Jam   加入判断文件是否为空功能,如果文件为空,则不执行,直接返回.
################################################################################

################################################################################
# 模块: generate_control_file
# 描述: 根据一个表名生成该表的控制文件
# 参数:  表名
# 参数:  文件名
################################################################################
generate_control_file()
{

        table=$1
        infile=$2
        replaceflag=$3
        tmstamp=$4

        userid="pzw/oracle"



        case  ${replaceflag} in
                R) replacename=REPLACE ;;
                *) replacename=APPEND   ;;
        esac

        badfile="./${table}${tmstamp}.bad"



        lv_temp="./${table}${tmstamp}.test"
        lv_temp1="./${table}${tmstamp}.test1"
        lv_temp2="./${table}${tmstamp}.test2"
        lv_control="./${table}${tmstamp}.ctl"
echo $lv_control

        ## 执行下载操作
        sqlplus -s $userid<<EOF >/dev/null
                spool ${lv_temp};
                desc ${table};
                spool off;
                exit;
EOF

        if [ "$?" -ne 0 ]
        then
                echo "Error:sqlplus ${userid} error in generate control file for table ${table} !"
                echo "please check userid and passwd or oracle_sid."
                exit
        fi
        ##确定表是否存在
        str_tmp=`grep 'ERROR' ${lv_temp}|sed 's/ //g'`

        if [ "${str_tmp}" != '' ]
        then
              echo "${table} not exist!"
              return 1
        fi


        if [ -f ${lv_temp} ]
        then
                cat ${lv_temp}|grep -v "^SQL>" |grep -v " Name " |grep -v " -------" |awk '{print $1}' > ${lv_temp1}
                lv_line_num=`cat ${lv_temp1} | wc -l`
                lv_line_num=`expr ${lv_line_num} - 2`
                lv_index=0

                rm -f ${lv_temp2}
                for lineinfo in `cat ${lv_temp1}`
                do
                        if [ ${lv_index} -eq ${lv_line_num} ]
                        then
                                echo "${lineinfo}" >> ${lv_temp2}
                        else
                                echo "${lineinfo}," >> ${lv_temp2}
                                lv_index=`expr ${lv_index} + 1`
                        fi
                done
        else
                echo "$0 error :not find ${lv_temp} file."
                exit
        fi

        lv_str="LOAD DATA INFILE '${infile}' BADFILE '${badfile}' ${replacename} INTO TABLE ${table} FIELDS TERMINATEd BY '|' TRAILING NULLCOLS"
        echo ${lv_str} > ${lv_control}
        echo "(" >> ${lv_control}
        cat ${lv_temp2} >> ${lv_control}
        echo ")" >> ${lv_control}

        rm -f ${lv_temp}
        rm -f ${lv_temp1}
        rm -f ${lv_temp2}

        return 0
}

################################################################################
## 主程序入口

lv_no=$#

case ${lv_no} in
2)
        table=$1
        infile=$2
        replaceflag="I";
        ;;
3)
        table=$1
        infile=$2
        replaceflag=$3;
        ;;
*)
        echo "Usage: $0 <table> <infile> <[replaceflag]>"
        exit
        ;;
esac

dss_load_log="./dss_load.log"

fileline=`wc -l ${infile}|awk '{print $1}'`
tmpfile="./${table}.txt"
`sed  's/"//g' ${infile}  >${tmpfile} `
`cat ${tmpfile}>${infile}`
echo ${fileline}
if [ ${fileline} -eq 0 ]
then
     echo "$file $table : line 0 ">>${dss_load_log}
     exit 1
fi
  


timestamp=`date +%Y%m%d%I%M%S`

##echo ${table}
##echo ${infile}
##echo ${replaceflag}

ctlfile="./${table}${timestamp}.ctl"
logfile="./${table}${timestamp}.log"
badfile="./${table}${timestamp}.bad"

generate_control_file ${table} ${infile} ${replaceflag} ${timestamp}

if [ $? -ne 0 ]
then
        exit 1
fi
userid="pzw/oracle"



##################################################################################
#### 生成执行脚本文件
##
###lv_rows=10000
###lv_bindsize=8192000
###lv_readsize=8192000
####echo "sqlldr ${userid} control=${table}.ctl rows=${lv_rows} bindsize=${lv_bindsize} readsize=${lv_readsize} log=log_${table}.log bad=bad_${table}.bad direct=true" > load_${table}.sh
##
echo "sqlldr ${userid} control=${ctlfile}  log=${logfile} bad=${badfile} direct=true"> load_${table}.sh

sqlldr ${userid} control=${ctlfile}  log=${logfile} bad=${badfile} >/dev/null

rt=$?

let rej_cnt=`cat ${logfile}|grep "Total logical records rejected"|awk '{print $5}'`
let total_cnt=`cat ${logfile}|grep "Total logical records read"|awk '{print $5}'`
let may_err_cnt=$((${total_cnt}/100+2))
echo "$infile $table: total_cnt ${total_cnt};may_err_cnt ${may_err_cnt}; rej_cnt ${rej_cnt}"
##看拒绝记录行是否在允许范围内
if [ ${rt} -ne 0 ]
then

        if [ ${rej_cnt} -lt ${may_err_cnt} ]
        then
                rt=0
        else
                rt=1
        fi
fi
echo ${rt}
exit ${rt}
