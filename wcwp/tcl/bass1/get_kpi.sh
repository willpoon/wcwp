#***************************************************************************************************
# **  �ļ����ƣ�	get_kpi.sh
# **  ����������	���b_13100_${date}_${ftpnumber}.verf��ĳһ�У�ĳһ�е�ֵ
# **  �����ߣ�		����
# **  �����		
# **  �����		
# **  �������ڣ�	2007��4��10��
# **  �޸���־��
# **  �޸�����		�޸���		�޸�����
#***************************************************************************************************
#����ֵ
#-1 ��ʾ�ļ�������
#-2 ��ʾ��������������ļ�����
#**********************************************************************
#gt >
#lt <
#*************************************************************************


if [ $# -lt 2 ]
then
        echo "Usage:ָ��.sh  date(yyyymmdd)��yyyymm��;row_number(��������������);id(����������С��4)"
        exit 1
fi
date=$1
row_number=$2
id=$3


objdir="/bassapp/backapp/data/bass1/report/report_${date}/"

#echo $objdir
#---1 �ж��ļ��Ƿ����
count=0
ftpnumber=`echo "$count"|awk '{if (length($1)==1) $1=0$1;print $1}'`
length=`echo "$date"|awk '{print length($0)}'`
#echo $length
if test $length -eq 8
then
midfile="b_13100_${date}_${ftpnumber}.verf"
else
midfile="b_13100_${date}_p_${ftpnumber}.verf"
fi
#echo $midfile

cd $objdir

aimfile="mm"
while [ $count -lt 100 ]
do
if test -r "$midfile"
then
 aimfile=$midfile
fi
 count=`expr $count + 1`
 ftpnumber=`echo "$count"|awk '{if (length($1)==1) $1=0$1;print $1}'`
 if test $length -eq 8
  then
    midfile="b_13100_${date}_${ftpnumber}.verf"
  else
    midfile="b_13100_${date}_p_${ftpnumber}.verf"
 fi
 #echo "$ftpnumber"
done
#echo $aimfile
if [ $aimfile = "mm" ]
  then
  echo "û��ȡ����Ӧ���ļ�"
  echo -1
  exit
fi

#----2 ȡ���ļ�������
NN=`wc -l "$aimfile" | awk '{print $1}'`
if test $row_number -gt $NN
then
 echo "��������������ļ�����"
 echo -2
 exit
fi
#----3 �����Ӧ������������ֵ
row=`awk 'NR =="'$row_number'" {print $"'$id'"}' "$aimfile"`
echo $row