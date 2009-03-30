#max field length calc
awk 'BEGIN{ FS=",";OFS="," }
{
	for ( i=1 ; i <= NF ; i++ ) {
      if (max[i]<length($i))
      max[i]=length($i)
 }

END{
    for ( i=1 ; i <= NF ; i++ ) {
      print max[i]
 }
	
}

