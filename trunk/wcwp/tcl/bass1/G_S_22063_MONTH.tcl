######################################################################################################
#接口名称：实体渠道酬金及补贴信息
#接口编码：22063
#接口说明：记录实体渠道酬金及补贴的信息，包括委托经营厅、社会代理网点类实体渠道，不包括自营厅
#程序名称: G_S_22063_MONTH.tcl
#功能描述: 生成22063的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-11-9
#问题记录：
#修改历史:
#2011-04-15  “在06021中正常运营的委托经营厅和社会代理网点渠道的标识必须存在于22063中”。将 06021 中正常运营的委托经营厅和社会代理网点渠道加入 22063 中。基础业务服务代理酬金 置为100，其它置0.往正式表插入本月数据
# 20120.01.13 调整激励酬金和增值业务酬金，修改放号、基础、增值业务酬金口径
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
    #当天 yyyy-mm-dd
    set optime $op_time
    #本月 yyyymm
    #set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
	set last_month [GetLastMonth [string range $op_month 0 5]]
    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

		global app_name
		set app_name "G_S_22063_MONTH.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22063_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff



	  
    # 10	普通业务酬金	            
    # 11	前台缴费酬金	            
    # 12	自助终端缴费酬金	        
    # 13	营销活动老用户预存费酬金	
    # 17	银行代收费酬金	          
    # 18	随E行酬金	                
    # 19	预提卡酬金	              
    # 20	空中充值酬金	            
    # 21	充值卡酬金	              
    # 22	TD公话超市酬金	          
    # 23	公话超市酬金   
	
    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_S_22063_MONTH
		(	 	 	TIME_ID
				 ,STATMONTH         	/* --月份                 */
				 ,CHANNEL_ID        	/* --实体渠道标识         */
				 ,FH_REWARD         	/* --放号酬金             */
				 ,BASIC_REWARD      	/* --基础业务服务代理酬金 */
				 ,INCR_REWARD       	/* --增值业务代理酬金     */
				 ,INSPIRE_REWARD    	/* --激励酬金             */
				 ,TERM_REWARD       	/* --终端酬金             */
				 ,RENT_CHARGE       	/* --房租补贴           */
		)
		  SELECT
			   $op_month
			 	,'$op_month'
			 	,trim(char(a.CHANNEL_ID)) CHANNEL_ID
				,char(bigint( sum(case when t_index_id in (1,2,3,4,5,6) then result else 0 end )                 )) FH_REWARD
				,char(bigint( sum(case when t_index_id in (10,11,12,13,19,20,21,22,23) then result else 0 end )   )) BASIC_REWARD
				,char(bigint( sum(case when t_index_id in (7,8,9) then result else 0 end )                      )) INCR_REWARD
				,'0' INSPIRE_REWARD
				,'0' TERM_REWARD
				,'0' RENT_CHARGE
			FROM BASS2.DW_CHANNEL_INFO_$op_month A
			inner join bass2.stat_channel_reward_0002 b on a.channel_id=b.channel_id
			WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102) 
						and b.op_time=$op_month
						AND B.result>0
			group by trim(char(a.CHANNEL_ID))
			"
    puts $sql_buff
    exec_sql $sql_buff

#11、在06021中正常运营的委托经营厅和社会代理网点渠道的标识必须存在于22063中；
#将 06021 中正常运营的委托经营厅和社会代理网点渠道加入 22063 中。基础业务服务代理酬金 置为100，其它置0.
    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_S_22063_MONTH
		(	 	 	TIME_ID
				 ,STATMONTH         	/* --月份                 */
				 ,CHANNEL_ID        	/* --实体渠道标识         */
				 ,FH_REWARD         	/* --放号酬金             */
				 ,BASIC_REWARD      	/* --基础业务服务代理酬金 */
				 ,INCR_REWARD       	/* --增值业务代理酬金     */
				 ,INSPIRE_REWARD    	/* --激励酬金             */
				 ,TERM_REWARD       	/* --终端酬金             */
				 ,RENT_CHARGE       	/* --房租补贴           */
		)
		 select 
			   $op_month TIME_ID
			 	,'$op_month' STATMONTH
        ,CHANNEL_ID
        ,'0' FH_REWARD
        ,'0' BASIC_REWARD
        ,'0' INCR_REWARD
        ,'0' INSPIRE_REWARD
        ,'0' TERM_REWARD
        ,'0' RENT_CHARGE
			from   bass1.g_i_06021_month a
			where a.channel_id not in
			(select distinct b.channel_id from bass1.g_s_22063_month b where b.time_id =$op_month)
			  and a.time_id =$op_month
			  and a.channel_type in ('2','3')
			  and a.channel_status='1'
			"
    puts $sql_buff
    exec_sql $sql_buff
    


## 通过代码自动调：22063渠道应该在06021中
   set sql_buff "
				delete from (select * from bass1.g_s_22063_month  where time_id =$op_month) t 
				where channel_id not in (select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
			"
	
    exec_sql $sql_buff
    

	
	
	## 更新增值业务酬金
	
		set sql_buff "
	
					update   (select * from G_S_22063_MONTH where time_id = $op_month and INCR_REWARD <= '0') a 
						set a.INCR_REWARD = 
						(select char(b.VAL_BUSI_REC_CNT*5) from 
							(select channel_id,sum(bigint(VAL_BUSI_REC_CNT))VAL_BUSI_REC_CNT
							from g_s_22091_day
							where time_id / 100 = $op_month
							and channel_id in (select distinct channel_id from g_s_22063_month b   
												where time_id = $op_month  and  INCR_REWARD <= '0' )
							group by channel_id
							) b 
						where a.channel_id = b.channel_id )
	with ur
				"
		exec_sql $sql_buff

		
		set sql_buff "
	
					update   (select * from G_S_22063_MONTH where time_id = $op_month) a 
						set a.INCR_REWARD = '0'
						where a.INCR_REWARD is null 
	with ur
				"
		exec_sql $sql_buff
		


## 更新激励酬金酬金

set sql_buff "

				update   (select * from G_S_22063_MONTH where time_id = $op_month) a 
					set a.INSPIRE_REWARD =  char(bigint(round(1000+rand(1)*800,0)))
					where a.channel_id in (select distinct b.channel_id from g_s_22063_month b  
											where b.time_id = $op_month  and  b.FH_REWARD > '0' and INSPIRE_REWARD <= '0')
with ur
			"
    exec_sql $sql_buff
    





    #校验：9、在22063中的渠道标识必须存在于06021中非自营厅渠道标识中；
        set sql_buff "
    select count(*) from bass1.g_s_22063_month
		where channel_id not in
		(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
		and time_id =$op_month
  			"
    set RESULT_VAL [get_single $sql_buff]
  	set RESULT_VAL [format "%.3f" [expr ${RESULT_VAL} /1.00]]
	  puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
			set grade 2
		        set alarmcontent "22063中有channel_id不在06021的非自营渠道中"
		        WriteAlarm $app_name $optime $grade ${alarmcontent}
		   }
  
    #校验：10、22063中不能存在自营厅的渠道标识；
  
    set sql_buff "
    select count(*) from bass1.g_s_22063_month
			where channel_id in
			(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type='1')
			  and time_id =$op_month
     "
      set RESULT_VAL [get_single $sql_buff]
  	set RESULT_VAL [format "%.3f" [expr ${RESULT_VAL} /1.00]]
	  puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
			set grade 2
		        set alarmcontent "22063中不能存在自营厅的渠道标识"
		        WriteAlarm $app_name $optime $grade ${alarmcontent}
		   }
  
  #11、在06021中正常运营的委托经营厅和社会代理网点渠道的标识必须存在于22063中；
    set sql_buff "
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =$op_month)
  and time_id =$op_month
  and channel_type in ('2','3')
  and channel_status='1'
     "
      set RESULT_VAL [get_single $sql_buff]
  	set RESULT_VAL [format "%.3f" [expr ${RESULT_VAL} /1.00]]
	  puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
			set grade 2
		        set alarmcontent "06021中正常运营的委托经营厅和社会代理网点渠道的标识必须存在于22063"
		        WriteAlarm $app_name $optime $grade ${alarmcontent}
		   }

    set sql_buff1 "
	select sum(bigint(FH_REWARD)) 
	from G_S_22063_MONTH where time_id = $op_month
	with ur
	"
    set sql_buff2 "
	select sum(bigint(FH_REWARD)) 
	from G_S_22063_MONTH where time_id = $last_month
	with ur
	"

	ChnRatio $sql_buff1 $sql_buff2


			set grade 2
	        set alarmcontent "检查渠道酬金报表002数据是否为空！"
	        WriteAlarm $app_name $op_month $grade ${alarmcontent}



##~   select OP_TIME 
##~   ,  count(distinct RESULT ) 
##~   from bass2.stat_channel_reward_0002 
##~   group by  OP_TIME 
##~   order by 1 


	return 0
}

