######################################################################################################
#接口名称：增值业务订购关系
#接口编码：02053
#接口说明：上报飞信、全网手机报、全网139手机邮箱业务的订购关系
#程序名称: G_I_02053_MONTH.tcl
#功能描述: 生成02053的数据
#运行粒度: 日
#源    表：1.bass2.dw_product_yyyymmdd
#          2.bass2.DWD_PRODUCT_REGSP_yyyymmdd
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-05-24
#问题记录：bill_flag 字段取值有问题
#修改历史: 
#加入02053086	日期范围错误。失效日期早于生效日期 的校验。
#  2011-05-12 11:22:23 panzhiwei  清除垃圾数据 ：失效日期早于生效日期
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
        set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	
        #上月最后一天
        #set last_day_month [GetLastDay [string range $p_timestamp 0 5]01]
        set last_day_month [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
  
        puts $last_day_month
		set app_name "G_I_02053_MONTH.tcl"        
  
  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02053_month where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
          
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02053_month
	select k.TIME_ID, k.PRODUCT_NO, k.PRODUCT_NO_3, k.BUSI_TYPE, k.SP_CODE, 
                      k.SERV_CODE, k.STS, k.VALID_DATE, k.EXPIRE_DATE, k.BILL_FLAG, k.REMARK, case when int(k.apply_type) between 1 and 14 then char(k.apply_type) else '00'  end  APPLY_TYPE
               from
               (
                select $op_month TIME_ID,
                       b.product_no,
                       a.id_value PRODUCT_NO_3,
                       case when a.busi_type='115' then '09'
                            when a.busi_type='119' then '15'
                            when a.busi_type='130' then '14'
                       end BUSI_TYPE,
                       char(a.sp_code) SP_CODE,
                       a.serv_code,
                       case when a.sts=1 then '0'
                            when a.sts in(2,5,6) then '1'
                            when a.sts=3 then '2'
                       end STS,
                       substr(char(a.valid_date),1,4)||substr(char(a.valid_date),6,2)||substr(char(a.valid_date),9,2) VALID_DATE,
                       substr(char(a.expire_date),1,4)||substr(char(a.expire_date),6,2)||substr(char(a.expire_date),9,2) EXPIRE_DATE,
                       case when a.bill_flag=0 then '0'
				                   when a.bill_flag=1 then '1'
					               	 when a.bill_flag=2 then '2'
					               	 when a.bill_flag=3 then '3'
					               	 when a.bill_flag=4 then '4'
					                 else '0'
				               end bill_flag,
                       case when (a.remark is null or a.remark like '%老系统导入%' ) then '1'
                            when lower(a.remark) like '%cboss%' then '2'
                            else '3'
                       end REMARK,
                       case when a.apply_type=1 then '01'
						     when a.apply_type=2 then '02'
						     when a.apply_type=3 then '03'
						     when a.apply_type=4 then '04'
						     when a.apply_type=5 then '05'
						     when a.apply_type=6 then '06'
						     when a.apply_type=7 then '07'
						     when a.apply_type=8 then '08'
						     when a.apply_type=9 then '09'
						     when a.apply_type between 10 and 14 then char(a.apply_type)
						else '00'  
					   end apply_type,
                       row_number()over(partition by b.PRODUCT_NO, a.id_value, case when a.busi_type='115' then '09'
                            when a.busi_type='119' then '15'
                            when a.busi_type='130' then '14'
                       end, a.SP_CODE, a.SERV_CODE order by $p_timestamp desc) row_id            
                from bass2.DWD_PRODUCT_REGSP_$last_day_month a
                 inner join (select distinct USER_ID  from BASS2.DW_I_USER_RADIUS_ORDER_$op_month) c
                  on a.user_id=c.USER_ID                
                 left outer join bass2.dw_product_$last_day_month b
                  on a.user_id=b.user_id
                where a.busi_type in ('115','119','130')
                  and a.sts=1 
                   ) k   
              where k.row_id=1 
               with ur"   
                        

        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle

#2011-05-12 11:22:23 清除垃圾数据 ：失效日期早于生效日期
	set sql_buff "
	 update  bass1.g_i_02053_month 
		 set EXPIRE_DATE = VALID_DATE
		 where time_id = $op_month
		 and 
		 VALID_DATE> EXPIRE_DATE
		 with ur
 "
 exec_sql $sql_buff
 

	set sql_buff "
		select count(0) from g_i_02053_month
		  where time_id = $op_month
		 and 
		 VALID_DATE> EXPIRE_DATE
		 with ur
  "
set RESULT_VAL [get_single $sql_buff]

	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent "日期范围错误,失效日期早于生效日期!"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }
	   
	return 0
}