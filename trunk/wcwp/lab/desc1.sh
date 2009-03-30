#run _desc.sh & get fields
#sh _desc.sh $1>desc.tmp
#cat desc.tmp|sed '/^$/d'|sed -e '1,7d;$d'>desc.tmp
#cat desc.tmp|awk 'BEGIN { FS=" "}{ printf ",%20s	%s%s\n" ,$1,$3,$4 }'
sh describe.sh $1|sed '/^$/d'|sed -e '1,7d;$d'|awk 'BEGIN { FS=" "}{ printf ",%-30s	\n",$1 }'
