base_dir=/bassapp/bass2/panzw2
conn(){
db2 terminate
my_pass=bass1
db2 connect to BASSDB user bass1 using ${my_pass} > /dev/null
}

conn
db2 "export to /bassapp/bass2/panzw2/data/$1.txt of del \
MODIFIED BY nochardel coldel$ \
select * from $1";

