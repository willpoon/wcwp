set -A city CONGHUA GZEAST GZMIDDLE GZNORTH GZWEST HUADU NANSHA PANYU ZENGCHENG

set -A sign CH GZE GZC GZN GZW HD NS PY ZC

if [ $# -eq 1 ] 
then
mon=$1 
echo $mon
else 
echo "usage sh $0 month"
exit 1
fi

for i in 0 1 2 3 4 5 6 7 8 


do
#echo cp *_${mon}.${sign[$i]}* /data/itc/XIAOBI/${city[$i]}/KPI/${mon}/;
#ls  /data/itc/XIAOBI/${city[$i]}/KPI/${mon}/;

ftp -inv 10.244.28.38<<EOF
user zhengfm itc@gmcc
cd /data/itc/XIAOBI/${city[$i]}/group/${mon}/
lcd /data/ftp_gzyd/dir0/${city[$i]}/group/${mon}/
binary
prompt
mput *_${mon}.${sign[$i]}*
bye
EOF
done
ls  /data/itc/XIAOBI/${city[$i]}/KPI/${mon}/;
