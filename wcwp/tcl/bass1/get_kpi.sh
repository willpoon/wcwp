#***************************************************************************************************
# **  文件名称：	get_kpi.sh
# **  功能描述：	获得b_13100_${date}_${ftpnumber}.verf中某一行，某一列的值
# **  创建者：		王琦
# **  输入表：		
# **  输出表：		
# **  创建日期：	2007年4月10日
# **  修改日志：
# **  修改日期		修改人		修改内容
#***************************************************************************************************
#返回值
#-1 表示文件不存在
#-2 表示输入的行数超过文件行数
#**********************************************************************
#gt >
#lt <
#*************************************************************************


if [ $# -lt 2 ]
then
        echo "Usage:指标.sh  date(yyyymmdd)或（yyyymm）;row_number(行数，输入整数);id(列数，必须小于4)"
        exit 1
fi
date=$1
row_number=$2
id=$3


objdir="/bassapp/backapp/data/bass1/report/report_${date}/"

#echo $objdir
#---1 判断文件是否存在
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
  echo "没有取得相应的文件"
  echo -1
  exit
fi

#----2 取得文件的行数
NN=`wc -l "$aimfile" | awk '{print $1}'`
if test $row_number -gt $NN
then
 echo "输入的行数超过文件行数"
 echo -2
 exit
fi
#----3 获得相应行数，列数的值
row=`awk 'NR =="'$row_number'" {print $"'$id'"}' "$aimfile"`
echo $row