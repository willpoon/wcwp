######################################################################################################
#接口名称：TD客户的通话情况月汇总
#接口编码：22205
#接口说明：记录每月的TD客户的通话情况汇总信息
#程序名称: G_S_22205_MONTH.tcl
#功能描述: 生成22205的数据
#运行粒度: 月
#源    表：1.bass2.dw_product_td_yyyymm 2.bass2.dw_call_yyyymm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：xiahuaxue
#编写时间：2009-03-09
#问题记录：
#修改历史: liuzhilong 1.6.0规范增加最后三个字段. 目前无此业务.填0
#          liuzhilong 20090911 去除where 条件  char(drtype_id) not in ('1700','9901','9902','9903')
#          2010-01-24 修改客户到达数据、在网客户数的口径 userstatus_id in (1,2,3,6,8)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        puts $op_month
              

        #本月第一天 yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22205_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
 


#清空临时表
	set sql_buff "ALTER TABLE bass1.g_s_22205_month_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	puts $sql_buff
  exec_sql $sql_buff

#向临时表插入数据
	set sql_buff "insert into bass1.g_s_22205_month_mid
                select user_id,call_duration_s
                from bass2.dw_call_$op_month
                where roamtype_id=0
                  and tolltype_id in(1,2,101,102,120)
                  and ( MNS_TYPE=0 or MNS_TYPE is null)"
	puts $sql_buff
  exec_sql $sql_buff           
	             
#TD_BILL_DURATION              TD手机客户的总计费时长  

	set sql_buff " select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month

group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id   "
    
  puts $sql_buff
  set TD_BILL_DURATION [get_single $sql_buff]	



               
#TD_NB_BILL_DURATION           TD手机客户的本地基本通话计费时长    


	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where tolltype_id=0

and roamtype_id=0
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id  "
    
  puts $sql_buff
  set TD_NB_BILL_DURATION [get_single $sql_buff]	



   
#TD_NDL_BILL_DURATION          TD手机客户的本地国内长途计费时长    

	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from
(
select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
where roamtype_id=0

and tolltype_id in(1,2,101,102,120)
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_NDL_BILL_DURATION [get_single $sql_buff]	


   
#TD_NIL_BILL_DURATION          TD手机客户的本地国际长途计费时长  

	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from
(
select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
where roamtype_id=0

and tolltype_id in(3,4,5,6,7,8,9,10,11,12,13,99,103,104,105,106,107,108,109,110,111,112,113,999)
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_NIL_BILL_DURATION [get_single $sql_buff]	
  
       
#TD_ROAM_BILL_DURATION         TD手机客户的漫游计费时长   
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where roamtype_id<>0

group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_ROAM_BILL_DURATION [get_single $sql_buff]	
              
#TD_TNET_BILL_DURATION         TD手机客户在T网上的计费时长          
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where MNS_TYPE=1

group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_TNET_BILL_DURATION [get_single $sql_buff]	
    
#TD_TNB_BILL_DURATION          TD手机客户在T网上的本地基本通话计费时长
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where tolltype_id=0

and roamtype_id=0
and MNS_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_TNB_BILL_DURATION [get_single $sql_buff]	
  
#TD_TNDL_BILL_DURATION         TD手机客户在T网上的本地国内长途计费时长
	set sql_buff " select value(char(sum(a.call_duration_s)),'0') from
(
select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
where roamtype_id=0

and tolltype_id in(1,2,101,102,120)
and MNS_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id    "
    
  puts $sql_buff
  set TD_TNDL_BILL_DURATION [get_single $sql_buff]	
  
#TD_TNIL_BILL_DURATION         TD手机客户在T网上的本地国际长途计费时长
	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from
(
select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
where roamtype_id=0

and tolltype_id in(3,4,5,6,7,8,9,10,11,12,13,99,103,104,105,106,107,108,109,110,111,112,113,999)
and MNS_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_TNIL_BILL_DURATION [get_single $sql_buff]	
  
#TD_TNET_ROAM_BILL_DURATION    TD手机客户在T网上的漫游计费时长        
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where roamtype_id<>0

and MNS_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_TNET_ROAM_BILL_DURATION [get_single $sql_buff]	
  
#TD_GNET_BILL_DURATION         TD手机客户在G网上的计费时长          
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where MNS_TYPE=0 or MNS_TYPE is null

group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_GNET_BILL_DURATION [get_single $sql_buff]	
    
#TD_GNET_NB_BILL_DURATION      TD手机客户在G网上的本地基本通话计费时长
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where tolltype_id=0

and roamtype_id=0
and ( MNS_TYPE=0 or MNS_TYPE is null)
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_GNET_NB_BILL_DURATION [get_single $sql_buff]	
  
#TD_GNET_NDL_BILL_DURATION     TD手机客户在G网上的本地国内长途计费时长
#	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from
#(
#select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
#where roamtype_id=0
#and tolltype_id in(1,2,101,102,120)
#and ( MNS_TYPE=0 or MNS_TYPE is null)
#group by user_id
#) a
#inner join  (select distinct user_id from bass2.dw_product_td_$op_month where td_user_mark=1 and td_3gcard_mark=0
# and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)) b
#    on a.user_id=b.user_id "



	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from                                               
(                                                                                                                 
select user_id,sum(call_duration_s) call_duration_s from bass1.g_s_22205_month_mid                                                                           
group by user_id                                                                                                  
) a                                                                                                               
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b                                                 
    on a.user_id=b.user_id "                                                                                      

   
  puts $sql_buff
  set TD_GNET_NDL_BILL_DURATION [get_single $sql_buff]	
  
#TD_GNET_NIL_BILL_DURATION     TD手机客户在G网上的本地国际长途计费时长
	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from
(
select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
where roamtype_id=0

and tolltype_id in(3,4,5,6,7,8,9,10,11,12,13,99,103,104,105,106,107,108,109,110,111,112,113,999)
and ( MNS_TYPE=0 or MNS_TYPE is null)
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_GNET_NIL_BILL_DURATION [get_single $sql_buff]	
  
#TD_GNET_ROAM_DURATION         TD手机客户在G网上的漫游计费时长  
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where roamtype_id<>0

and ( MNS_TYPE=0 or MNS_TYPE is null)
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set TD_GNET_ROAM_DURATION [get_single $sql_buff]	
        
#VMOBILE_BILL_DURATION         可视电话计费时长          
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where VIDEO_TYPE=1

group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id  "
    
  puts $sql_buff
  set VMOBILE_BILL_DURATION [get_single $sql_buff]	
               
#VMOBILE_NB_BILL_DURATION      可视电话本地基本通话计费时长   
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where tolltype_id=0

and roamtype_id=0
and VIDEO_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id  "
    
  puts $sql_buff
  set VMOBILE_NB_BILL_DURATION [get_single $sql_buff]	
          
#VMOBILE_NDL_BILL_DURATION     可视电话本地国内长途计费时长    
	set sql_buff "select value(char(sum(a.call_duration_s)),'0') from
(
select user_id,sum(call_duration_s) call_duration_s from bass2.dw_call_$op_month
where roamtype_id=0

and tolltype_id in(1,2,101,102,120)
and VIDEO_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id "
    
  puts $sql_buff
  set VMOBILE_NDL_BILL_DURATION [get_single $sql_buff]	
         
#VMOBILE_ROAM_BILL_DURATION    可视电话漫游计费时长         
	set sql_buff "select value(char(sum(a.call_duration_m)),'0') from
(
select user_id,sum(call_duration_m) call_duration_m from bass2.dw_call_$op_month
where roamtype_id<>0

and VIDEO_TYPE=1
group by user_id
) a
inner join  (select distinct user_id from bass2.dw_product_td_$op_month where userstatus_id in (1,2,3,6,8)
  and usertype_id in (1,2,9)
  and test_mark=0
  and not td_3gcard_mark = 1
  and not td_2gcard_mark = 1
  and not (td_booksprom_mark = 1 and td_call_mark = 0 and td_term_mark = 0 and td_addon_month_mark = 0)
  and td_user_mark=1) b
    on a.user_id=b.user_id  "
    
  puts $sql_buff
  set VMOBILE_ROAM_BILL_DURATION [get_single $sql_buff]	

  set handle [aidb_open $conn]
  #插入 新增客户数  
	set sql_buff "insert into bass1.g_s_22205_month
                values( $op_month,
                        '$op_month',
                        '$TD_BILL_DURATION'            ,
                        '$TD_NB_BILL_DURATION'         ,
                        '$TD_NDL_BILL_DURATION'        ,
                        '$TD_NIL_BILL_DURATION'        ,
                        '$TD_ROAM_BILL_DURATION'       ,
                        '$TD_TNET_BILL_DURATION'       ,
                        '$TD_TNB_BILL_DURATION'        ,
                        '$TD_TNDL_BILL_DURATION'       ,
                        '$TD_TNIL_BILL_DURATION'       ,
                        '$TD_TNET_ROAM_BILL_DURATION'  ,
                        '$TD_GNET_BILL_DURATION'       ,
                        '$TD_GNET_NB_BILL_DURATION'    ,
                        '$TD_GNET_NDL_BILL_DURATION'   ,
                        '$TD_GNET_NIL_BILL_DURATION'   ,
                        '$TD_GNET_ROAM_DURATION'       ,
                        '$VMOBILE_BILL_DURATION'       ,
                        '$VMOBILE_NB_BILL_DURATION'    ,
                        '$VMOBILE_NDL_BILL_DURATION'   ,
                        '$VMOBILE_ROAM_BILL_DURATION' ,
                        '0'                              ,
                        '0'                              ,
                        '0'                              ) "   
    puts $sql_buff 
      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}                           
 
	return 0
}


#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------



