##/bassapp/bihome/panzw/bin
## bass1_new_cfg.sh

cat <<!> new_interface.txt 
s_XXXXX_yyyymmdd_02024_XX_XXX.dat
s_XXXXX_yyyymmdd_02025_XX_XXX.dat
s_XXXXX_yyyymmdd_04019_XX_XXX.dat
s_XXXXX_yyyymm_22048_XX_XXX.dat
s_XXXXX_yyyymm_22057_XX_XXX.dat
s_XXXXX_yyyymm_22058_XX_XXX.dat
s_XXXXX_yyyymm_22059_XX_XXX.dat
s_XXXXX_yyyymm_22060_XX_XXX.dat
i_XXXXX_yyyymm_06001_XX_XXX.dat
i_XXXXX_yyyymm_06002_XX_XXX.dat
i_XXXXX_yyyymm_02031_XX_XXX.dat
i_XXXXX_yyyymm_02032_XX_XXX.dat
i_XXXXX_yyyymm_02033_XX_XXX.dat
i_XXXXX_yyyymm_02034_XX_XXX.dat
i_XXXXX_yyyymm_02035_XX_XXX.dat
s_XXXXX_yyyymm_22401_XX_XXX.dat
s_XXXXX_yyyymm_22402_XX_XXX.dat
s_XXXXX_yyyymm_22403_XX_XXX.dat
s_XXXXX_yyyymm_22404_XX_XXX.dat
i_XXXXX_yyyymm_22405_XX_XXX.dat
i_XXXXX_yyyymm_22406_XX_XXX.dat
!



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


nawk -F"_" -v q="'" '{
	print "delete from  app.g_unit_info where unit_code = " q $4 q ";"
}' new_interface.txt 




nawk -F"_" -v q="'" '{
if ( length($3) == 6 ) {
	print "insert into app.g_unit_info values (" \
	q $4 q \
	" , " 0 \
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
	" , "  q "bass1."tolower("G_"$1"_"$4"_DAY") q \
	" , "  1 \
	" , "  0 \
	" , "  q "x" q \
	");"
}
	else { print "error file_name format!! check it!!\n" }
}' new_interface.txt 



###app.sch_control_before
 

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
	"xxxxxxxxxxx" \
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
	"xxxxxxxxxxx" \
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
	"bass1_export "tolower("G_"$1"_"$4"_DAY")" YESTERDAY()" \
	q \
	" ,10000,-1," \
	q \
	"EXPORT_of xxxxxxxxxxx" \
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
	"bass1_export "tolower("G_"$1"_"$4"_MONTH")" LASTMONTH()" \
	q \
	" ,-1,-1," \
	q \
	"EXPORT_of xxxxxxxxxxx" \
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
