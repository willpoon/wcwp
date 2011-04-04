#!/bin/sh


#��ȡ����
interface_code=$1
cycle=$2
cnt=$3
echo cnt=$cnt

#ָ������Ŀ¼
workpath=/bassapp/bass2/panzw2/mtbin/data
#�ӿ��ļ�����
s_file_name=${workpath}/${interface_code}${cycle}000000.AVL
#У���ļ�����
chk_file_name=${workpath}/${interface_code}${cycle}000000.CHK

#ѹ���ӿ��ļ�
compress -f ${s_file_name}

#��ȡ�ļ�����
file_length=`ls -l ${s_file_name}.Z | awk '{print $5}'`
#��ȡ�ļ�����ʱ��
file_time=`date '+%Y%m%d%H%M%S'`

#���ɽӿ��ļ�
echo "${interface_code}${cycle}000000.AVL.Z\$${file_length}\$${cnt}\$${cycle}\$${file_time}" > ${chk_file_name}