splfile(){
#function:split large file
#author:panzhiwei
#run:splfile [filename] [linecount]
#example:splfile s_13100_201102_21003_00_001 900000
if [ $# -ne 2 ];then 
echo splfile [filename] [linecount]
return 2
fi
#接口文件名作为参数(不带后缀)
filename=$1
linecount=$2
#去掉最后的顺序号
file_name_=`echo ${filename} |awk '{print substr($1,1,length($1)-1)}'`
#backup
if [ -f ${filename}.dat ];then 
echo ...backup ${filename}.dat to ${filename}.bak...
mv ${filename}.dat ${filename}.bak
else
echo no such file : ${filename}.dat
return 2
fi
#分割
echo ...spliting...
split -$linecount ${filename}.bak
echo ...split complete!
seq_no=1
#rename 
for split_unit in `ls xa*`
	do
	echo ...rename $split_unit to $file_name_$seq_no.dat
	mv $split_unit $file_name_$seq_no.dat
	seq_no=`expr $seq_no + 1`
	if [ $seq_no -gt 9 ];then
		echo ...is the file that large? check it!
		return 2
	fi
done
ls -lrt $file_name_*dat
echo ...all  complete!
}



