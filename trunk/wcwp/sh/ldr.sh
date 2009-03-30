if [ $# -eq 1 ] 
then
sqlldr zhengfengmei/oracle123@gzdm control=$1 log=ora.log direct=true readsize=20971520 rows=2000000;
else
echo "ldr <ctl_file>"
exit 1
fi
