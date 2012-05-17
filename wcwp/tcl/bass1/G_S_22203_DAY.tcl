######################################################################################################
#接口名称：使用TD网络的客户数据流量日汇总
#接口编码：22203
#接口说明：记录每日使用TD网络的客户的数据流量信息
#程序名称: G_S_22203_DAY.tcl
#功能描述: 生成22203的数据
#运行粒度: 日
#源    表：1.bass2.dw_product_td_yyyymmdd 2.BASS2.DW_NEWBUSI_GPRS_YYYYMMDD
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：xiahuaxue
#编写时间：2009-03-02
#问题记录：
#修改历史: liuzhilong 20090903 统计流量时去掉where bill_mark=1条件
#          2010-01-24 修改客户到达数据、在网客户数的口径 userstatus_id in (1,2,3,6,8)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22203_day where time_id=$timestamp"
	puts $sql_buff
  exec_sql $sql_buff
       

  #20090903 统计流量时去掉了where bill_mark=1条件 
	set sql_buff "insert into BASS1.G_S_22203_DAY values
                 ($timestamp,           
                 '$timestamp',          
                 (
select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$timestamp  
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$timestamp where (td_call_mark =1
                            or td_gprs_mark =1
                            or td_addon_mark=1)
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)
 and test_mark=0
  ) b
    on a.user_id=b.user_id
),
(

select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$timestamp
  where    MNS_TYPE=1
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$timestamp where (td_call_mark =1
                            or td_gprs_mark =1
                            or td_addon_mark=1)
 and userstatus_id in (1,2,3,6,8) and usertype_id in (1,2,9)
 and test_mark=0
  ) b
    on a.user_id=b.user_id
    
),
(    

select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$timestamp
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$timestamp
 where (td_call_mark =1
        or td_gprs_mark =1
        or td_addon_mark=1)
 and userstatus_id in (1,2,3,6,8)
 and usertype_id in (1,2,9)
 and (td_2gcard_mark=1
  or td_3gcard_mark=1)
  and test_mark=0
  
 
 ) b
    on a.user_id=b.user_id
    
 ),
(   

select value(char(sum(a.td_gprs_flow)/1024/1024),'0') from
(
select user_id,sum(UPFLOW1+ UPFLOW2+ DOWNFLOW1+ DOWNFLOW2) td_gprs_flow
  from BASS2.DW_NEWBUSI_GPRS_$timestamp
  where   MNS_TYPE=1
group by user_id  
) a
inner join  (select distinct user_id from bass2.dw_product_td_$timestamp
 where (td_call_mark =1
        or td_gprs_mark =1
        or td_addon_mark=1)
 and userstatus_id in (1,2,3,6,8)
 and usertype_id in (1,2,9)
 and (td_2gcard_mark=1
  or td_3gcard_mark=1)
 and test_mark=0
  
 ) b
    on a.user_id=b.user_id
 )   
                 
                  )
with ur;"
                        
  puts $sql_buff
  exec_sql $sql_buff
  

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



