inputfile=svnLs_20101231.txt
sed -e 's///g' ${inputfile}|awk  'BEGIN{FS="|";OFS=","}{ if ( ($0 !~ /ʮһ��/) && ($0 !~ /ʮ����/) ) {print substr($1,1,8),substr($1,8,12),substr($1,30,12),substr($1,42);}
else {print substr($1,1,8),substr($1,8,12),substr($1,30,14),substr($1,44);}
}'|nawk '
#########>>>>>>>>>>ǰ����start
BEGIN{FS=",";OFS=",";
print "�޶�����","�汾","�û�","�ļ�·��"
}
#########<<<<<<<<<<ǰ����end


#########>>>>>>>>>>��������start
function transmm (col) {
	 sub(/ʮ����/, "12", col);
	 sub(/ʮһ��/, "11", col);
	 sub(/ʮ��/, "10"  , col);
	 sub(/����/, "09"  , col);
	 sub(/����/, "08"  , col);
	 sub(/����/, "07"  , col);
	 sub(/����/, "06"  , col);
	 sub(/����/, "05"  , col);
	 sub(/����/, "04"  , col);
	 sub(/����/, "03"  , col);
	 sub(/����/, "02"  , col);
	 sub(/һ��/, "01"  , col);
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


#########<<<<<<<<<<��������end

#########>>>>>>>>>>processing start
{
	print transyyyymm($1,$3),$1,$2,$4
}
#########<<<<<<<<<<processing end

##program end

' > rs_ls-vR.csv

