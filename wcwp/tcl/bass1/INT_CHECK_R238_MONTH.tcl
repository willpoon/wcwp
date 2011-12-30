######################################################################################################
#程序名称：	INT_CHECK_R238_MONTH.tcl
#校验接口：	03017
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        global app_name
        set app_name "INT_CHECK_R238_MONTH.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R238','R239','R240','R241'
 	  				,'R242','R243','R246','R247','R250','R251','R252','R253','R254')
			"

		exec_sql $sql_buff


#R238			新增	月	09_渠道积分	在06022中的渠道标识必须存在于06021中	06022中的实体渠道标识都在06021的实体渠道标识中	0.05		


set sql_buff "
	select count(*) from bass1.g_i_06022_month
	where channel_id not in 
	(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month )
	  and time_id =$op_month
"

chkzero2 $sql_buff "R238 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R238',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


#R239			新增	月	09_渠道积分	在06021中正常运营的非社会代理网点的渠道标识必须存在于06022中	在06021中正常运营的非社会代理网点的渠道的实体渠道标识应存在于06022中	0.05		



set sql_buff "
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06022_month where time_id =$op_month)
  and time_id =$op_month
  and channel_type <>'3'
  and channel_status='1'
"

chkzero2 $sql_buff "R239 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
  puts $RESULT_VAL
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R239',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



#R240			新增	月	09_渠道积分	在06023中的渠道标识必须存在于06021中	06023中的实体渠道标识应存在于06021的实体渠道标识中	0.05		


set sql_buff "
select count(*) from bass1.g_i_06023_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month )
  and time_id =$op_month
"

chkzero2 $sql_buff "R240 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	puts $RESULT_VAL

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R240',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff

  

#R241			新增	月	09_渠道积分	在06021中正常运营的渠道标识必须存在于06023中	06021中正常运营的渠道的实体渠道标识都应存在于06023的实体渠道标识中	0.05		



set sql_buff "
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06023_month where time_id =$op_month)
  and time_id =$op_month
  and channel_status='1'
"

chkzero2 $sql_buff "R241 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R241',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff


  
#R242			新增	月	09_渠道积分	在22061中的渠道标识必须存在于06021中	22061中的实体渠道标识都应存在于06021的实体渠道标识中	0.05		




set sql_buff "

select count(*) from bass1.g_s_22061_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month )
  and time_id =$op_month
  
"

chkzero2 $sql_buff "R242 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R242',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



#R243			新增	月	09_渠道积分	在06021中正常运营的非社会代理网点的渠道标识必须存在于22061中	06021中正常运营的非社会代理网点的渠道的实体渠道标识应存在于22061中	0.05		



set sql_buff "


select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_s_22061_month where time_id =$op_month)
  and time_id =$op_month
  and channel_type<>'3'
  and channel_status='1'
  
"

chkzero2 $sql_buff "R243 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R243',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



  
#R246			新增	月	09_渠道积分	在22063中的渠道标识必须存在于06021中非自营厅渠道标识中	22063中的实体渠道标识应存在于06021中非自营厅的实体渠道标识中	0.05		



set sql_buff "


select count(*) from bass1.g_s_22063_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
  and time_id =$op_month
  
"

chkzero2 $sql_buff "R246 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R246',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



  
#R247			新增	月	09_渠道积分	22063中不能存在自营厅的渠道标识	22063中不应存在自营厅的渠道标识	0.05		




set sql_buff "


select count(*) from bass1.g_s_22063_month
where channel_id in
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type='1')
  and time_id =$op_month

  
"

chkzero2 $sql_buff "R247 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R247',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff





#R250			新增	月	09_渠道积分	06021中基础类型[普通自营厅+品牌店+旗舰店] = 实体渠道类型[自营厅+委托经营厅]	06021中基础类型为普通自营厅、品牌店和旗舰店的实体渠道个数应等于实体渠道类型为自营厅和委托经营厅的实体渠道个数	0.05		

#select channel_id  from    
#BASS1.G_I_06021_MONTH 
#where time_id = 201105 and CHANNEL_STATUS = '1' 
#and   channel_type in ('1','2')
#except
#select channel_id  val1 from    
#BASS1.G_I_06021_MONTH 
#where time_id = 201105 and CHANNEL_STATUS = '1' 
#and CHANNEL_B_TYPE in ('1','2','3')

#02		实体渠道类型	此字段仅取以下分类：
#1：自营厅
#2：委托经营厅
#3：社会代理网点
#4：24小时自助营业厅	
#
#11		渠道基础类型	此字段仅取以下分类：
#1：普通自营厅（自营厅、委托经营厅填写）
#2：品牌店（自营厅、委托经营厅填写）
#3：旗舰店（自营厅、委托经营厅填写）
#4：24小时自助营业厅
#5：指定专营店（社会代理网点填写）
#6：特约代理点（社会代理网点填写）
#CHANNEL_ID
#100000930                               
#100000845                               
#10320010                                
#100000844                               
#这几个渠道是在营业厅中的空中充值点。105001	10500101	自营厅空中充
#由于在自营厅里面，实体渠道类型 = 1 自营厅 
#渠道基础类型 = 1 
#update BASS1.G_I_06021_MONTH  
#set CHANNEL_B_TYPE = '1'
#where time_id = 201105 
#and CHANNEL_ID in 
#(
#'100000844'
#,'100000845'
#,'100000930'
#,'10320010'   
#)


set sql_buff "

SELECT  ( val1 - val2 ) val 
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('1','2','3')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('1','2')
) b 

"

chkzero2 $sql_buff "R250 not pass! "



set sql_buff "
SELECT   val1 , val2 
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('1','2','3')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('1','2')
) b 

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   
   
	set RESULT_VAL 0
	set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1 - $RESULT_VAL2  ]]
	puts $RESULT_VAL
	
		if {  ${RESULT_VAL} != 0 } {
		set grade 2
	        set alarmcontent " R250 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R250',$RESULT_VAL1,$RESULT_VAL2,0,0) 
		"
		exec_sql $sql_buff



#R251			新增	月	09_渠道积分	06021中基础类型[24小时自助营业厅] = 实体渠道类型[24小时自助厅]	06021中基础类型为24小时自助营业厅店的实体渠道个数应等于实体渠道类型为24小时自助厅的实体渠道个数	0.05		


set sql_buff "

SELECT  ( val1 - val2 ) val 
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('4')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('4')
) b 

"

chkzero2 $sql_buff "R251 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R251',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff







#R252			新增	月	09_渠道积分	06021中基础类型[指定专营店+特约代理店] = 实体渠道类型[社会代理网点]	06021中基础类型为指定专营店和特约代理店的实体渠道个数应等于实体渠道类型为社会代理网的实体渠道个数	0.05		



set sql_buff "

SELECT  ( val1 - val2 ) val 
from 
(

select count(0) val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in  ('5','6')

)  a
,(

select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('3')
) b 

"


chkzero2 $sql_buff "R252 not pass! "



set sql_buff "
SELECT  val1 , val2 
from 
(

select count(0) val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in  ('5','6')

)  a
,(

select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('3')
) b 

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   
   
	set RESULT_VAL 0
	set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1 - $RESULT_VAL2  ]]
	puts $RESULT_VAL
	
		if {  ${RESULT_VAL} != 0 } {
		set grade 2
	        set alarmcontent " R252 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R252',$RESULT_VAL1,$RESULT_VAL2,0,0) 
		"
		exec_sql $sql_buff








#R253			新增	月	09_渠道积分	06021中非社会代理网点渠道星级为空	06021中非社会代理网点的渠道星级应为空	0.05		



set sql_buff "

select count(*) from
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =$op_month
  and channel_status='1'
  and channel_type<>'3'
) aa
where channel_star <>''
"

chkzero2 $sql_buff "R253 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R253',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff





#R254			新增	月	09_渠道积分	06021中社会代理网点渠道星级不能等于空	06021中社会代理网点渠道星级不能等于空	0.05		
 
 

set sql_buff "


 select count(*) from
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =$op_month
  and channel_status='1'
  and channel_type='3'
) aa
where channel_star=''

"

chkzero2 $sql_buff "R254 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R254',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff


  	
  
      	
	return 0
}
