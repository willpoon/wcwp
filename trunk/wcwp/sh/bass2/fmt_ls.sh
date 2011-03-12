outfile=svnList`date +%Y%m%d`.csv
awk  'BEGIN{FS="|";OFS=","}{print substr($1,1,8),substr($1,8,12),substr($1,30,14),substr($1,44)}' $1 >  $outfile
echo $outfile
