inputfile=svnLs_20101231.txt
sed -e 's///g' ${inputfile}|awk  'BEGIN{FS="|";OFS=","}{ if ( ($0 !~ /十一月/) && ($0 !~ /十二月/) ) {print substr($1,1,8),substr($1,8,12),substr($1,30,12),substr($1,42);}
else {print substr($1,1,8),substr($1,8,12),substr($1,30,14),substr($1,44);}
}'|nawk '
#########>>>>>>>>>>前处理start
BEGIN{FS=",";OFS=",";
print "修订日期","版本","用户","文件路径"
}
#########<<<<<<<<<<前处理end


#########>>>>>>>>>>函数定义start
function transmm (col) {
	 sub(/十二月/, "12", col);
	 sub(/十一月/, "11", col);
	 sub(/十月/, "10"  , col);
	 sub(/九月/, "09"  , col);
	 sub(/八月/, "08"  , col);
	 sub(/七月/, "07"  , col);
	 sub(/六月/, "06"  , col);
	 sub(/五月/, "05"  , col);
	 sub(/四月/, "04"  , col);
	 sub(/三月/, "03"  , col);
	 sub(/二月/, "02"  , col);
	 sub(/一月/, "01"  , col);
	 return col;
}


function transyyyymm(ver,col){
	if ($1 < 2708 ){
		col3="2010 "transmm(col)
		return col3
	}
	else {
		col3="2011 "transmm(col)
		return col3	
	}
}


#########<<<<<<<<<<函数定义end

#########>>>>>>>>>>processing start
{
	print transyyyymm($1,$3),$1,$2,$4
}
#########<<<<<<<<<<processing end

##program end

' > rs_ls-vR.csv

