##############################################################
#author : panzhiwei
#功能：重新生成21003 verf 文件
#ref data path:/bassapp/backapp/data/bass1/export/export_201104
#run@app
#do not need to care about the exec-path
##############################################################

printflist(){
	
	if [ $# -ne 2 ];then
	echo "$0 <yyyymm> <unit_code>"
	exit
	fi
	base_path=/bassapp/backapp/data/bass1/export/export_${yyyymm}
	unit_code=$2
	ls -1 ${base_path}/*${unit_code}*.dat|nawk -F"/" '{print $NF}'
	if [ $? -ne 0 ];then 
	echo "something wrongs!"
	return -1
	fi
	return 0
}


	if [ $# -ne 1 ];then
	echo "$0 <verf_file>"
	exit 
	fi
  verf_file=$1
	yyyymm=`echo $verf_file|nawk -F"_" '{print $3}'`
	unit_code=`echo $verf_file|nawk -F"_" '{print $4}'`
	tmpdir="/bassapp/bihome/panzw/tmp"

printflist ${yyyymm} ${unit_code}

if [ $? -ne 0 ];then
echo "printflist fail to run!"
exit 
fi

while read single_filename
do
	filename="$single_filename"
	#
	full_filename=${base_path}/${filename}
	#
	filesize=`ls -l ${full_filename}|awk '{print $5}'`
	#
	fileline=`wc -l ${full_filename}|awk '{print $1}'`
	#
	data_dtm=${yyyymm}
	#
	filetimestamp=`echo "puts [file mtime ${full_filename}]"|tclsh|awk -F'\r' '{print $1}'`
	export filetimestamp
	file_mtm=`perl -MPOSIX -e '$x=$ENV{"filetimestamp"}; @y=strftime("%Y%m%d%H%M%S", localtime($x)); print "@y\n"'`
	#
	echo |nawk 	 -v v_filename=${filename} \
			   -v v_filesize=${filesize} \
			   -v v_fileline=${fileline} \
			   -v v_data_dtm=${data_dtm} \
			   -v v_file_mtm=${file_mtm} \
	'{printf "%-40s%-20s%-20s%-8s%-14s"\
						,v_filename\
						,v_filesize\
						,v_fileline\
						,v_data_dtm\
						,v_file_mtm"\n"
	 }
	'>>${tmpdir}/.tmp.${verf_file}
done<<!
`printflist ${yyyymm} ${unit_code}`
!


	sed "s/$/`echo \\\r`/g" ${tmpdir}/.tmp.${verf_file} > ${tmpdir}/t_${verf_file}
	cp ${tmpdir}/t_${verf_file} ${base_path}/t_${verf_file}
	cat ${tmpdir}/.tmp.${verf_file}
	cat ${tmpdir}/t_${verf_file}
	rm ${tmpdir}/.tmp.${verf_file}
	rm ${tmpdir}/t_${verf_file}


exit

cat>/dev/null<<!
$ cat  ../export_201105/s_13100_201105_21003_00.verf
s_13100_201105_21003_00_001.dat         1875000000          7500000             201105  20110606162723
s_13100_201105_21003_00_002.dat         1703654750          6814619             201105  20110606162723
$ cat s_13100_201106_21003_00.verf
s_13100_201106_21003_00_001.dat         3715268750          14861075            201106  20110703151610
!