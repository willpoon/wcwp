bass1_gzip()
{
if [ $# -ne 1 ];then 
echo "bass1_gzip yyyymm"
return -1
fi
dt=$1
PATH=/bassapp/backapp/data/bass1/export
cd ${PATH}
LS="/usr/bin/ls"
GREP="/usr/bin/grep"
AWK="/usr/bin/awk"
GZIP="/usr/bin/gzip"
TAR="/usr/sbin/tar"
RM="/usr/bin/rm"
pwd
for dir in `${LS} -lh |${GREP} "export_[201[0-9][[0-1][0-2]" |${GREP} ^d |${GREP} $dt |${AWK} '{print $9}'`
do
echo $dir
if [ ! -f $dir.tar.gz ];then
	${TAR} cvf - $dir | $GZIP -qc > $dir.tar.gz
	if [ $? -eq 0 ];then 
	echo "gzip complete!"
	${RM} -rf $dir 
	echo "RM $dir  complete!"
	fi
fi	
done

return 0
}





#tar cvf - /bassapp/backapp/data/bass1/export/export_20090601 | gzip -qc > export_20090601.tar.gz


