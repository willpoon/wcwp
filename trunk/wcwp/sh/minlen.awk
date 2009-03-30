BEGIN{
FS=v_del ; OFS=","
 }
{
  for ( i = 1 ; i <= NF ; i++ ){
	min[i] = 10000;
     if ( min[i] > length($i) )
      min[i] = length($i)
  }
}
END {
for ( i = 1 ; i <= NF ; i++ )
    print "field_"i"_min_len  (" min[i] ")"
}
