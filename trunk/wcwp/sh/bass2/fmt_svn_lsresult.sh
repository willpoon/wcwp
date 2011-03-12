outfile=svnList`date +%Y%m%d`.csv
awk  'BEGIN{FS="|";OFS=","}{ if ( ($0 !~ /十一月/) && ($0 !~ /十二月/) ) {print substr($1,1,8),substr($1,8,12),substr($1,30,12),substr($1,42);}
else {print substr($1,1,8),substr($1,8,12),substr($1,30,14),substr($1,44);}
}' $1 >  $outfile
echo cat  $outfile

