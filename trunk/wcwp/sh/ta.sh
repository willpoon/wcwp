#!/usr/bin/ksh
if [ $# -eq 2 ] 
then
v_filename=$1
remote_dir=$2
echo $v_filename will be uploaded to $remote_dir on data mart!
else 
echo "usage: sh a2dup filename destination_of_remote_path"
echo "just type subdirname of '~' dir and make sure with a '/' before the dirname"
exit 1
fi

current_workdir=$PWD

ftp -inv 10.243.216.211<<EOF
user zhengfm tds2008
binary
lcd $current_workdir
cd ~$remote_dir
prompt
mput $v_filename
bye
EOF
