if [ $# -ne 1 ] ; then 
echo "$0 tabname"
exit
fi

base_dir=/bassapp/bihome/panzw/tmp
conn(){
db2 terminate
mydb=BASSDB
my_pass=bass1
db2 connect to ${mydb} user bass1 using ${my_pass} > /dev/null
}

conn
db2 "export to ${base_dir}/$1`date +%Y%m%d`.txt of del \
MODIFIED BY nochardel coldel$  STRIPLZEROS decplusblank \
select * from $1 "
db2 connect reset
db2 terminate
