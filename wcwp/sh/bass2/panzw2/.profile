
my_pass=`/bassapp/backapp/src/C_FUNCTION/decode  0312004131`
base_dir=/bassapp/bass2/panzw2
alias lt='ls -alrt'
alias llog='$base_dir/ViewLoadLog_testdb.sh'
alias conn='db2 connect to bassdb user bass2 using ${my_pass}'
alias term='db2 terminate'
alias desc='db2 describe table '
set -o vi

