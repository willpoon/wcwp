BEGIN{
FS=v_del ; OFS=","
 }
{
  for ( i = 1 ; i <= NF ; i++ )
     if ( max[i] < length($i) )
      max[i] = length($i)
}
END {
for ( i = 1 ; i <= NF ; i++ )
    print "field_"i"_max_len: " max[i]
}
