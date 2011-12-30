######################################################################################################
#接口名称：MAS业务收入情况
#接口编码：03015
#接口说明：根据《配合集团客户KPI指标统计业务支撑系统改造通知》，该接口上传MAS业务收入信息。
#程序名称: G_S_03015_MONTH.tcl
#功能描述: 生成03015的数据
#运行粒度: 月
#源    表：1.bass2.dw_enterprise_sub_YYYYMM
#          2.bass2.DW_ACCT_SHOULDITEM_YYYYMM
#          3.bass2.DW_ENTERPRISE_MEMBER_MID_YYYYMM
#          4.bass2.dw_newbusi_ismg_YYYYMM
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：夏华学
#编写时间：2007-11-29
#问题记录：1.
#修改历史: 1.2008-06-27  因MAS业务的更改而调整程序
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day
        #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_S_03015_MONTH where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
#  #item_id 80000172,80000173,80000174,  ADC  
#  #plan_id 90300001,90400001,90600001,  ADC 
#  set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.G_S_03015_MONTH
#                  select $op_month,
#                         aa.ENTERPRISE_ID,
#                         char(bb.feature_value),
#                         char(int(sum(fact_fee*100))),'0'
#                   from ( select c.enterprise_id as enterprise_id,
#                                 d.PROD_ID as plan_id,
#                                 b.fact_fee as fact_fee
#                            from bass2.DW_ACCT_SHOULDITEM_$op_month b,
#                                 bass2.DW_ENTERPRISE_MEMBER_MID_$op_month c,
#                  							 bass2.dw_enterprise_sub_$op_month d
#                           where c.user_id=b.user_id and b.item_id in (80000178,80000179) and b.acct_id = d.Acct_id and
#                                 c.enterprise_id = d.enterprise_id and d.PROD_ID in ('90700001') and d.rec_status=1
#                       union all
#                          select c.enterprise_id as enterprise_id,char(plan_ID) as plan_id,b.BASE_FEE+b.INFO_FEE+b.MONTH_FEE+b.FUNC_FEE as fact_fee 
#                            from bass2.DW_ENTERPRISE_MEMBER_MID_$op_month c,
#                                 bass2.dw_newbusi_ismg_$op_month b
#                           where c.user_id=b.user_id and b.plan_ID in (90700001) and 
#                                 c.enterprise_id in (select ENTERPRISE_ID from bass2.dw_enterprise_sub_$op_month
#                                                      where plan_id in (90700001) and rec_status=1)
#                        ) aa,
#                     bass2.DWD_GROUP_ORDER_FEATUR_$last_month_day bb
#                     where aa.ENTERPRISE_ID = bb.GROUP_ID and bb.REC_STATUS=1  and bb.feature_ID='90700006'
#                   group by aa.ENTERPRISE_ID,bb.feature_value; "                
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}  
#	aidb_commit $conn
# aidb_close $handle
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_03015_MONTH
                select $op_month,
                         aa.ENTERPRISE_ID,
                         case  when char(ltrim(rtrim(bb.feature_value))) = 'M01XZ0319000001' then 'M01XZ031900001' else char(ltrim(rtrim(bb.feature_value))) end ,
                         char(int(sum(aa.fact_fee*100))),'0'
                from (select      d.enterprise_id as enterprise_id,
                                       
                                     sum(a.FACT_FEE) as fact_fee
                                  from  
                                  bass2.DW_ACCT_SHOULDITEM_$op_month a ,
                                  (select distinct ENTERPRISE_ID,user_id from bass2.DW_ENTERPRISE_SUB_$op_month) d 
                                  where
                                      a.user_id=d.user_id and a.item_id in (80000448,80000178,80000179,80000200)
                                  and d.enterprise_id not in ('891910006274')
                                 
                        group by  d.enterprise_id
                                
                        ) aa,
                      ( select distinct feature_value,group_id 
					      from bass2.DWD_GROUP_ORDER_FEATUR_$last_month_day 
						 where feature_value like '%M01%' AND REC_STATUS=1 AND 
						       char(ltrim(rtrim(feature_value))) <> 'M01XZ1234567' AND
						       ORDER_ID IN (select DISTINCT ORDER_ID 
							                  from bass2.dw_enterprise_sub_$op_month 
											 where service_id in ('142','935','939','941') and rec_status=1)
                      )bb
					 where ltrim(rtrim(aa.ENTERPRISE_ID)) = ltrim(rtrim(bb.GROUP_ID))
                  group by aa.ENTERPRISE_ID,
				           case  when char(ltrim(rtrim(bb.feature_value))) = 'M01XZ0319000001' then 'M01XZ031900001' 
						         else char(ltrim(rtrim(bb.feature_value))) 
						   end " 


						   
						   
						                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}  
	aidb_commit $conn
  aidb_close  $handle
return 0
}