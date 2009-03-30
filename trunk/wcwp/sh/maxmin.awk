BEGIN{
FS=v_del ; OFS=","
 }
{
  for ( i = 1 ; i <= NF ; i++ ){
	max[i]=0;
	min[i] = 10000;
     if ( max[i] < length($i) ){
      max[i] = length($i)
	 }
     if ( min[i] > length($i) ){
      min[i] = length($i)
     }
  }
}
END {
for ( i = 1 ; i <= NF ; i++ )
    print "len_of_field_"i" -  max:  (" max[i] ") min:  (" min[i] ")"
}
