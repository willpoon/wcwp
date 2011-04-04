#!/bin/sh


#获取参数
interface_code=$1
cycle=$2
cnt=$3
echo cnt=$cnt

#指定工作目录
workpath=/bassapp/bass2/panzw2/mtbin/data
#接口文件名称
s_file_name=${workpath}/${interface_code}${cycle}000000.AVL
#校验文件名称
chk_file_name=${workpath}/${interface_code}${cycle}000000.CHK

#压缩接口文件
compress -f ${s_file_name}

#获取文件长度
file_length=`ls -l ${s_file_name}.Z | awk '{print $5}'`
#获取文件生成时间
file_time=`date '+%Y%m%d%H%M%S'`

#生成接口文件
echo "${interface_code}${cycle}000000.AVL.Z\$${file_length}\$${cnt}\$${cycle}\$${file_time}" > ${chk_file_name}