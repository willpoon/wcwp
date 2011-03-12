outfile=svnUpdateList`date +%Y%m%d`.csv
awk  'BEGIN{FS="|";OFS=","}{print substr($1,1,4),substr($1,4)}' $1 >  $outfile
echo $outfile
