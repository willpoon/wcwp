outfile=svnList`date +%Y%m%d`.csv
awk  'BEGIN{FS="|";OFS=","}{print substr($1,1,8),substr($1,9,12),substr($1,30,12),substr($1,45)}' $1 >  $outfile
echo $outfile
