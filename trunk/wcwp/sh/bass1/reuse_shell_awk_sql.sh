##/bassapp/bihome/panzw/bin
## bass1_new_cfg.sh
##包含$2,将\t 替换成 "_"
cat <<!> new_interface.txt 
s_XXXXX_yyyymm_22057_XX_XXX.dat_疑似养卡渠道名单
s_XXXXX_yyyymm_22058_XX_XXX.dat_已确认养卡渠道名单
s_XXXXX_yyyymm_22059_XX_XXX.dat_疑似窜卡渠道名单
s_XXXXX_yyyymm_22060_XX_XXX.dat_已确认窜卡渠道名单
!

echo "\n\n"
## print A/I/S  , UNIT_CODE  , COARSE 
nawk -F"_" '{
if ( length($3) == 6 ) {
	print $4,toupper($1),$3,"M"
}
else 
	if ( length($3) == 8 ) 
	{
		print $4,toupper($1),$3,"D"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"

##  print table_name from interface _file_name
nawk -F"_" '{
if ( length($3) == 6 ) {
	print toupper("G_"$1"_"$4"_MONTH")
}
else 
	if ( length($3) == 8 ) 
	{
		print toupper("G_"$1"_"$4"_DAY")	
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 

echo "\n\n"

##  print tcl file name  from interface _file_name


nawk -F"_" '{
if ( length($3) == 6 ) {
	print toupper("G_"$1"_"$4"_MONTH")".tcl"
}
else 
	if ( length($3) == 8 ) 
	{
		print toupper("G_"$1"_"$4"_DAY")".tcl"	
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"

##  print control_code  from interface _file_name


nawk -F"_" '{
if ( length($3) == 6 ) {
	print "BASS1_"toupper("G_"$1"_"$4"_MONTH")".tcl"
}
else 
	if ( length($3) == 8 ) 
	{
		print "BASS1_"toupper("G_"$1"_"$4"_DAY")".tcl"	
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"

##  print EXP control_code  from interface _file_name


nawk -F"_" '{
if ( length($3) == 6 ) {
	print "BASS1_EXP_"toupper("G_"$1"_"$4"_MONTH")
}
else 
	if ( length($3) == 8 ) 
	{
		print "BASS1_EXP_"toupper("G_"$1"_"$4"_DAY")
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"

##  print drop   from interface _file_name

nawk -F"_" '{
if ( length($3) == 6 ) {
	print "DROP TABLE BASS1."toupper("G_"$1"_"$4"_MONTH")" ;"
}
else 
	if ( length($3) == 8 ) 
	{
		print "DROP TABLE BASS1."toupper("G_"$1"_"$4"_DAY")" ;"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 

echo "\n\n"

##ALTER TABLE  LOCKSIZE ROW APPEND OFF NOT VOLATILE;


nawk -F"_" '{
if ( length($3) == 6 ) {
	print "ALTER TABLE BASS1."toupper("G_"$1"_"$4"_MONTH")"  LOCKSIZE ROW APPEND OFF NOT VOLATILE;"
}
else 
	if ( length($3) == 8 ) 
	{
		print "ALTER TABLE BASS1."toupper("G_"$1"_"$4"_DAY")"  LOCKSIZE ROW APPEND OFF NOT VOLATILE;"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 



echo "\n\n"

###app.sch_control_map

nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "delete from  app.sch_control_map where PROGRAM_NAME = " q toupper("G_"$1"_"$4"_MONTH")".tcl" q ";"
}
else 
	if ( length($3) == 8 ) 
	{
		print "delete from  app.sch_control_map where PROGRAM_NAME = " q toupper("G_"$1"_"$4"_DAY")".tcl" q ";"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 

echo "\n\n"


nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into app.sch_control_map values (2," \
	q \
	toupper("G_"$1"_"$4"_MONTH")".tcl" \
	q \
	" , " \
	q \
	"BASS1_"toupper("G_"$1"_"$4"_MONTH")".tcl"  \
	q \
	") ;"
}
else 
	if ( length($3) == 8 ) 
	{
	print "insert into app.sch_control_map values (2," \
	q \
	toupper("G_"$1"_"$4"_DAY")".tcl" \
	q \
	" , " \
	q \
	"BASS1_"toupper("G_"$1"_"$4"_DAY")".tcl"  \
	q \
	") ;"
}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 







echo "\n\n"

###app.sch_control_map EXP

nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "delete from  app.sch_control_map where PROGRAM_NAME = " q "EXP_"toupper("G_"$1"_"$4"_MONTH") q ";"
}
else 
	if ( length($3) == 8 ) 
	{
		print "delete from  app.sch_control_map where PROGRAM_NAME = " q "EXP_"toupper("G_"$1"_"$4"_DAY") q ";"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"

nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into app.sch_control_map values (2," \
	q \
	"EXP_"toupper("G_"$1"_"$4"_MONTH") \
	q \
	" , " \
	q \
	"BASS1_""EXP_"toupper("G_"$1"_"$4"_MONTH")  \
	q \
	") ;"
}
else 
	if ( length($3) == 8 ) 
	{
	print "insert into app.sch_control_map values (2," \
	q \
	"EXP_"toupper("G_"$1"_"$4"_DAY") \
	q \
	" , " \
	q \
	"BASS1_""EXP_"toupper("G_"$1"_"$4"_DAY")  \
	q \
	") ;"
}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"


## bass1.int_program_data 

nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "delete from   bass1.int_program_data where PROGRAM_NAME = " q toupper("G_"$1"_"$4"_MONTH")".tcl" q ";"
}
else 
	if ( length($3) == 8 ) 
	{
		print "delete from  bass1.int_program_data where PROGRAM_NAME = " q toupper("G_"$1"_"$4"_DAY")".tcl" q ";"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"


nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE" \
	" , " \
	q \
	toupper("G_"$1"_"$4"_MONTH")".tcl" \
	q  \
	" , " \
	q \
	toupper("G_"$1"_"$4"_MONTH")".BASS1" \
	q \
	" , " \
	q \
	toupper("G_"$1"_"$4"_MONTH")"_e" \
	q \
	" , " \
	q \
	toupper("G_"$1"_"$4"_MONTH")"_f" \
	q \
" from bass1.int_program_data where PROGRAM_NAME = " q "G_S_22204_MONTH.tcl" q ";"
}
else 
	if ( length($3) == 8 ) 
	{
	
	print "insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE" \
	" , " \
	q \
	toupper("G_"$1"_"$4"_DAY")".tcl" \
	q  \
	" , " \
	q \
	toupper("G_"$1"_"$4"_DAY")".BASS1" \
	q \
	" , " \
	q \
	toupper("G_"$1"_"$4"_DAY")"_e" \
	q \
	" , " \
	q \
	toupper("G_"$1"_"$4"_DAY")"_f" \
	q \
" from bass1.int_program_data where PROGRAM_NAME = " q "G_S_22204_MONTH.tcl" q ";"

}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


## app.g_unit_info

echo "\n\n"

nawk -F"_" -v q="'" '{
	print "delete from  app.g_unit_info where unit_code = " q $4 q ";"
}' new_interface.txt 


echo "\n\n"


nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into app.g_unit_info values (" \
	q $4 q \
	" , " 1 \
	" , "  q $7 q \
	" , "  q "bass1."tolower("G_"$1"_"$4"_MONTH") q \
	" , "  1 \
	" , "  0 \
	" , "  q "x" q \
	");"
}
else 
	if ( length($3) == 8 ) 
	{
	print "insert into app.g_unit_info values (" \
	q $4 q \
	" , " 0 \
	" , "  q $7 q \
	" , "  q "bass1."tolower("G_"$1"_"$4"_DAY") q \
	" , "  1 \
	" , "  0 \
	" , "  q "x" q \
	");"
}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 



###app.sch_control_before
 echo "\n\n"


nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into app.sch_control_before values \n " \
	"(" q "BASS1_"toupper("G_"$1"_"$4"_MONTH")".tcl" q  \
	" , " \
	q q ")\n" \
	";"
}
else 
	if ( length($3) == 8 ) 
	{
	print "insert into app.sch_control_before values \n " \
	"(" q "BASS1_"toupper("G_"$1"_"$4"_DAY")".tcl" q  \
	" , " \
	q q ")\n" \
	";"
}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


###app.sch_control_before exp
echo "\n\n"


nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into app.sch_control_before values \n " \
	"(" q "BASS1_EXP_"toupper("G_"$1"_"$4"_MONTH") q  \
	" , " \
	q "BASS1_"toupper("G_"$1"_"$4"_MONTH")".tcl" q ")\n" \
	";"
}
else 
	if ( length($3) == 8 ) 
	{
	print "insert into app.sch_control_before values \n " \
	"(" q "BASS1_EXP_"toupper("G_"$1"_"$4"_DAY") q  \
	" , " \
	q "BASS1_"toupper("G_"$1"_"$4"_DAY")".tcl" q ")\n" \
	";"
}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 

###delete from app.sch_control_task 
echo "\n\n"

nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "delete from  app.sch_control_task where control_code in ( " q "BASS1_"toupper("G_"$1"_"$4"_MONTH")".tcl" q "," q "BASS1_EXP_"toupper("G_"$1"_"$4"_MONTH") q ");"
}
else 
	if ( length($3) == 8 ) 
	{
	print "delete from  app.sch_control_task where control_code in ( " q "BASS1_"toupper("G_"$1"_"$4"_DAY")".tcl" q "," q "BASS1_EXP_"toupper("G_"$1"_"$4"_DAY") q ");"
	}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


###insert into app.sch_control_task  tcl
echo "\n\n"

nawk -F"_" -v q="'" '{
if ( length($3) == 8 ) {
	print "insert into app.sch_control_task values(" \
	q \
	"BASS1_"toupper("G_"$1"_"$4"_DAY")".tcl" \
	q \
	" , " \
	1 \
	" , " \
	2  \
	" , " \
	q \
	"int -s "toupper("G_"$1"_"$4"_DAY")".tcl" \
	q \
	" ,10000,-1," \
	q \
	$7 \
	q \
	" , " \
	q \
	"app" \
	q \
	" , " \
	q \
	"BASS1" \
	q \
	" , " \
	1 \
	" , " \
	q \
	"/bassapp/bass1/tcl/" \
	q \
	");"
}
else 
	if ( length($3) == 6 ) 
	{
	
		print "insert into app.sch_control_task values(" \
	q \
	"BASS1_"toupper("G_"$1"_"$4"_MONTH")".tcl" \
	q \
	" , " \
	2 \
	" , " \
	2  \
	" , " \
	q \
	"int -s "toupper("G_"$1"_"$4"_MONTH")".tcl" \
	q \
	" ,-1,-1," \
	q \
	$7 \
	q \
	" , " \
	q \
	"app" \
	q \
	" , " \
	q \
	"BASS1" \
	q \
	" , " \
	1 \
	" , " \
	q \
	"/bassapp/bass1/tcl/" \
	q \
	");"

}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 


echo "\n\n"


###insert into app.sch_control_task  exp



nawk -F"_" -v q="'" '{
if ( length($3) == 8 ) {
	print "insert into app.sch_control_task values(" \
	q \
	"BASS1_EXP_"toupper("G_"$1"_"$4"_DAY") \
	q \
	" , " \
	1 \
	" , " \
	2  \
	" , " \
	q \
	"bass1_export bass1."tolower("G_"$1"_"$4"_DAY")" YESTERDAY()" \
	q \
	" ,10000,-1," \
	q \
	"EXPORT_of "$7 \
	q \
	" , " \
	q \
	"app" \
	q \
	" , " \
	q \
	"BASS1" \
	q \
	" , " \
	1 \
	" , " \
	q \
	"/bassapp/backapp/bin/bass1_export/" \
	q \
	");"
}
else 
	if ( length($3) == 6 ) 
	{
	
		print "insert into app.sch_control_task values(" \
	q \
	"BASS1_EXP_"toupper("G_"$1"_"$4"_MONTH") \
	q \
	" , " \
	2 \
	" , " \
	2  \
	" , " \
	q \
	"bass1_export bass1."tolower("G_"$1"_"$4"_MONTH")" LASTMONTH()" \
	q \
	" ,-1,-1," \
	q \
	"EXPORT_of "$7 \
	q \
	" , " \
	q \
	"app" \
	q \
	" , " \
	q \
	"BASS1" \
	q \
	" , " \
	1 \
	" , " \
	q \
	"/bassapp/backapp/bin/bass1_export/" \
	q \
	");"

}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 
