#date_offset.sh 
usage() 
{ 
echo "使用方法:" 
echo " date_offset.sh YYYY/MM/DD offset" 
echo "\n举例:" 
echo " date_offset.sh 2001/10/01 -1 返回: 2001/09/30" 
echo " date_offset.sh 2001/10/01 5 返回: 2001/10/06\n" 
exit 2 
} 

yesterday() 
{ 
YEAR=`echo $1|awk -F "/" '{print $1}'` 
MONTH=`echo $1|awk -F "/" '{print $2}'` 
DAY=`echo $1|awk -F "/" '{print $3}'` 
if [ $DAY -eq 1 ] 
then 
if [ $MONTH -eq 1 ] 
then 
YEAR=`expr $YEAR - 1` 
MONTH=12 
else 
MONTH=`expr $MONTH - 1` 
fi 
DAY=`echo \`cal $MONTH $YEAR\`|tail -n1|awk '{print $NF}'` 
else 
DAY=`expr $DAY - 1` 
fi 
echo "$YEAR $MONTH $DAY"|awk '{if (length($2)==1) $2=0$2;if (length($3)==1) $3=0$3;printf "%s/%s/%s",$1,$2,$3}' 
} 

tomorrow() 
{ 
YEAR=`echo $1|awk -F "/" '{print $1}'` 
MONTH=`echo $1|awk -F "/" '{print $2}'` 
DAY=`echo $1|awk -F "/" '{print $3}'` 
LASTDAY=`echo \`cal $MONTH $YEAR\`|tail -n1|awk '{print $NF}'` 
if [ $DAY -eq $LASTDAY ] 
then 
if [ $MONTH -eq 12 ] 
then 
YEAR=`expr $YEAR + 1` 
MONTH=1 
else 
MONTH=`expr $MONTH + 1` 
fi 
DAY=1 
else 
DAY=`expr $DAY + 1` 
fi 
echo "$YEAR $MONTH $DAY"|awk '{if (length($2)==1) $2=0$2;if (length($3)==1) $3=0$3;printf "%s/%s/%s",$1,$2,$3}' 
} 

# 检查参数数目 
if [ $# -ne 2 ] 
then 
echo "\n调用出错: 参数数目不对!\n" 
usage 
fi 

# 检查参数1长度 
if [ `expr length $1` -ne 10 ] 
then 
echo "\n调用出错: 日期格式不正确!\n" 
usage 
fi 

TMP_YEAR=`echo $1|awk -F "/" '{print $1}'` 
TMP_MONTH=`echo $1|awk -F "/" '{print $2}'` 
TMP_DAY=`echo $1|awk -F "/" '{print $3}'` 

if ! expr $TMP_YEAR + $TMP_MONTH + $TMP_DAY >/dev/null 2>&1 
then 
echo "\n调用出错: 日期格式不正确!\n" 
usage 
fi 

if [ `expr length $TMP_YEAR` -ne 4 ] 
then 
echo "\n调用出错: 日期格式不正确!\n" 
usage 
fi 

if [ $TMP_MONTH -lt 1 ] || [ $TMP_MONTH -gt 12 ] 
then 
echo "\n调用出错: 日期格式不正确!\n" 
usage 
fi 

LAST_DAY=`echo \`cal $TMP_MONTH $TMP_YEAR\`|tail -n1|awk '{print $NF}'` 
if [ $TMP_DAY -lt 1 ] || [ $TMP_DAY -gt $LAST_DAY ] 
then 
echo "\n调用出错: 日期格式不正确!\n" 
usage 
fi 

# 检查参数2是否为数值 
expr $2 + 0 >/dev/null 2>&1 
if [ ! $? ] 
then 
echo "\n调用出错: 日期偏移量应为整数值!\n" 
usage 
fi 

TMP_DATE=$1 
if [ $2 -lt 0 ] 
then 
INC=-1 
COUNT=$2 
else 
INC=1 
COUNT=`expr 0 - $2` 
fi 
while [ $COUNT -lt 0 ] 
do 
if [ $INC -gt 0 ] 
then 
TMP_DATE=`tomorrow $TMP_DATE` 
else 
TMP_DATE=`yesterday $TMP_DATE` 
fi 
COUNT=`expr $COUNT + 1` 
done 
echo $TMP_DATE 
#在要SCO 5.0.5下测试通过。 

