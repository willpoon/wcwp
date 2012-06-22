######################################################################################################
#接口单元名称：银行代收代缴及酬金信息
#接口单元编码：22068
#接口单元说明：记录银行代收代缴及酬金的相关信息
#程序名称: G_S_22068_MONTH.tcl
#功能描述: 生成22068的数据
#运行粒度: 月
#源    表：
#1.  bass2.dw_acct_payment_dm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
##~   1.7.9 “托收”包括银行托收及银行代扣。（西藏无代扣这种业务）
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      
 	set	  date_optime	 $ThisMonthFirstDay
	set    last_day_month [GetLastDay [GetNextMonth $op_month]01]
  set  dw_channel_rule_rlt_mm                 "bass2.dw_channel_rule_rlt_mm   "
  set  dw_channel_rule_info_mm                "bass2.dw_channel_rule_info_mm  "
  set  dw_channel_rule_mm                     "bass2.dw_channel_rule_mm       "
  set  dwd_channel_dept_yyyymmdd              "bass2.dwd_channel_dept_${last_day_month}"
	set  channel_bank_reward_yyyymm             "bass2.channel_bank_reward_$op_month   "
  set   dw_acct_payment_dm_yyyymm       "bass2.dw_acct_payment_dm_$op_month "
  set   dwd_channel_dept_yyyymmdd       "bass2.dwd_channel_dept_${last_day_month}"

        global app_name
        set app_name "G_S_22068_MONTH.tcl"
          
    #删除本期数据
	set sql_buf "delete from bass1.G_S_22068_MONTH where time_id=$op_month"
	exec_sql $sql_buf
#
#CREATE TABLE BASS1.G_S_22068_MONTH_1
#(channel_id           bigint,
#channel_name         varchar(128),
#rule_type            varchar(4),
#rule_info_id         integer,
#rule_desc            varchar(1000),
#rule_id              bigint,
#cond_expr            varchar(200)
#)DATA CAPTURE NONE
# IN TBS_APP_BASS1
# INDEX IN TBS_INDEX
#  PARTITIONING KEY
#   (channel_id,rule_id) USING HASHING;

#CREATE TABLE BASS1.G_S_22068_MONTH_2
#	               (
#unique_id            integer,
#city_id              varchar(7),
#county_id            varchar(7),
#geography_type       integer,
#channel_id           bigint,
#channel_name         varchar(128),
#phone_id             varchar(40),
#rule_type            varchar(4),
#rule_info_id         integer,
#rule_desc            varchar(1000),
#rule_id              bigint,
#fee                  decimal(12,5)
# )DATA CAPTURE NONE
# IN TBS_APP_BASS1
# INDEX IN TBS_INDEX
# PARTITIONING KEY
# (unique_id,rule_type) USING HASHING;
#

	set sql_buf "ALTER TABLE BASS1.G_S_22068_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	set sql_buf "ALTER TABLE BASS1.G_S_22068_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	set sql_buf "ALTER TABLE BASS1.G_S_22068_MONTH_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	set sql_buf "ALTER TABLE BASS1.G_S_22068_MONTH_4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	   set sql_buf " insert into   G_S_22068_MONTH_1
	                 select   b.channel_id
                           ,e.channel_name
                           ,c.rule_type
                           ,d.rule_info_id
                           ,c.rule_desc
                           ,d.rule_id
                           ,d.cond_expr
                   from
                        $dw_channel_rule_rlt_mm b,
                        $dw_channel_rule_info_mm c,
                        $dw_channel_rule_mm d,
                        $dwd_channel_dept_yyyymmdd e
                   where b.op_time='$date_optime'
                         and c.op_time='$date_optime'
                         and d.op_time='$date_optime'
                         and b.channel_id=e.channel_id
                         and b.rule_info_id=c.rule_info_id
                         and c.data_status=1
                         and c.rule_info_id=d.rule_info_id
                         and d.data_status=1
                         with ur
                         "
	exec_sql $sql_buf

                         
  set sql_buf "insert into G_S_22068_MONTH_3 (
  										object_id
                      ,entity_type
                      ,unique_id
                      ,phone_id
                      ,region_code
                      ,county_code
                      ,geography_type
                      ,channel_type
                      ,channel_type_dtl
                      ,channel_type_dtl3
                      ,pre_fee
                     )
                SELECT  a.channel_id
                       ,1
                       ,row_number() over()
                       ,b.key_num
                       ,a.channel_city
                       ,a.region_type
                       ,a.geography_type
                       ,a.channel_type
                       ,a.dept_type_dtl
                       ,a.dept_type_dtl3
                       ,sum(b.amount)
                from  $dwd_channel_dept_yyyymmdd a,$dw_acct_payment_dm_yyyymm b
                where  a.organize_id = b.staff_org_id
                      and b.opt_code in ('4286','4287','4179','4180','4181','4182','4310','4311','4103','4144','4468')
                      and rollback_mark='0'
                group by a.channel_id
                       ,b.key_num
                       ,a.channel_city
                       ,a.region_type
                       ,a.geography_type
                       ,a.channel_type
                       ,a.dept_type_dtl
                       ,a.dept_type_dtl3
                       "
	 exec_sql ${sql_buf}
 ##===============================================================================================
puts "解析出银行代收的的酬金"
 ##===============================================================================================
 #
 # 把每一行取出来解析
	set sql_buf "
	select count(0) cnt from 
	(
	select  a.rule_id
             ,a.cond_expr
             ,a.compute_expr
       from $dw_channel_rule_mm a
       ,$dw_channel_rule_info_mm  b
       where a.rule_info_id=b.rule_info_id  
       and  b.rule_type='4' 
       and a.data_status=1 
       and b.data_status=1
	and a.op_time='$date_optime'  
	and b.op_time='$date_optime'
	) a
   "   
	set cnt [get_single $sql_buf]
	puts $cnt
puts "取条件值"	
	set sql_buf "
	select  a.rule_id
             ,a.cond_expr
             ,a.compute_expr
       from $dw_channel_rule_mm a
       ,$dw_channel_rule_info_mm  b
       where 
       a.rule_info_id=b.rule_info_id  
       and  b.rule_type='4' 
	and a.data_status=1 
	and b.data_status=1
	and a.op_time='$date_optime'  
	and b.op_time='$date_optime'
	order by 1
	with ur
             "
		puts $sql_buf

				#set p_row [get_rows $sql_buf 0]
				#puts $p_row
				#set p_row [get_rows $sql_buf 1]
				#puts $p_row
				#set p_row [get_rows $sql_buf 2]
				#puts $p_row
    set n 0
    while { $n < $cnt } {
				set res($n) [get_rows $sql_buf $n]
        #puts $res($n)
        incr n
        #puts $n
    }
	   set row_cnt $n
	   puts $n
	   

      for {set n 0  } { $n < $row_cnt } { incr n } {
           set rule_id        [lindex $res($n) 0]
      		 set cond_expr      [lindex $res($n) 1]
      		 set compute_expr   [lindex $res($n) 2]

puts G_S_22068_MONTH_2
	  set sql_buf "	 insert into  G_S_22068_MONTH_2 (
                               unique_id
                               ,city_id
                               ,county_id
                               ,geography_type
                               ,channel_id
                               ,channel_name
                               ,phone_id
                               ,rule_type
                               ,rule_info_id
                               ,rule_desc
                               ,rule_id
                               ,fee
                            )
			   select   
			   		a.unique_id
					  ,char(a.region_code)
			      ,char(a.county_code)
			      ,a.geography_type
			      ,a.object_id
			      ,b.channel_name
			      ,a.phone_id
			      ,b.rule_type
			      ,b.rule_info_id
			      ,b.rule_desc
			      ,b.rule_id
			      ,${compute_expr}
		       from G_S_22068_MONTH_3 a,
			    G_S_22068_MONTH_1 b
		       where a.object_id=b.channel_id
			     and b.rule_type = '4'
			     and b.rule_id=${rule_id}
			     with ur
	"

				exec_sql $sql_buf
			}   


#2011-08-02
		set sql_buf "
		insert into G_S_22068_MONTH_4
		select '$op_month' OP_MONTH 
		, char(int(count(0))) BNK_TS_CNT
		, char(int(sum(AMOUNT))) BNK_TS_FEE
		from bass2.dw_acct_payment_dm_$op_month a 
		where a.opt_code = '4115'
"

exec_sql $sql_buf

		set sql_buf "
		update G_S_22068_MONTH_4
		set BNK_TS_FEE = '0'
		where BNK_TS_FEE is null
"

exec_sql $sql_buf

		set sql_buf "
		select BNK_TS_CNT, BNK_TS_FEE from  G_S_22068_MONTH_4
		 where op_month = '$op_month'
		 with ur
"

exec_sql $sql_buf


   set p_row [get_row $sql_buf]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]

#以下是非网银酬金
#(select  char(int(sum(a.fee)))
#          from G_S_22068_MONTH_2 a
#          left join G_S_22068_MONTH_3 b  on a.phone_id=b.phone_id
#          where a.unique_id=b.unique_id and a.rule_type='4')
#          	 
		set sql_buf "
			INSERT INTO  BASS1.G_S_22068_MONTH
			( 
         TIME_ID
        ,OP_MONTH
        ,COUNTER_REC_CNT
        ,EBANK_REC_CNT
        ,OTH_REC_CNT
        ,COUNTER_REC_FEE
        ,EBANK_REC_FEE
        ,OTH_REC_FEE
	,BNK_TS_CNT
	,BNK_TS_FEE
        ,REWARD_FEE
			)
			 
select 
         $op_month TIME_ID
        ,'$op_month' OP_MONTH
        ,char(count(distinct case when opt_code in ('4103','4144') then payment_id end ))  COUNTER_REC_CNT
        ,char(count(distinct case when opt_code in ('4468') then payment_id end )) EBANK_REC_CNT
        ,char(count(distinct case when opt_code in ('4286','4287','4179','4180','4181','4182','4310','4311') then payment_id end )) OTH_REC_CNT
        ,char(int(sum( case when opt_code in ('4103') then AMOUNT else 0 end )))  COUNTER_REC_FEE
        ,char(int(sum( case when opt_code in ('4468')  then AMOUNT else 0 end ))) EBANK_REC_FEE
        ,char(int(sum( case when opt_code in ('4286','4287','4179','4180','4181','4182','4310','4311') then AMOUNT else 0 end ))) OTH_REC_FEE
	,'$RESULT_VAL1' BNK_TS_CNT
	,'$RESULT_VAL2' BNK_TS_FEE
	,char((select  int(sum(a.fee))
          from G_S_22068_MONTH_2 a
          left join G_S_22068_MONTH_3 b  on a.phone_id=b.phone_id
          where a.unique_id=b.unique_id and a.rule_type='4')
          + int(sum( case when opt_code in ('4468')  then AMOUNT else 0 end )*0.016) 
          )
				 REWARD_FEE
from bass2.dw_acct_payment_dm_$op_month a 
where opt_code in ('4286','4287','4179','4180','4181','4182','4310','4311','4103','4144','4468')
with ur 
	"
	exec_sql $sql_buf


			
  aidb_runstats bass1.G_S_22068_MONTH 3

#checksum 
set sql_buf "
	select bigint(REWARD_FEE) from G_S_22068_MONTH where TIME_ID = $op_month
	"
	
lezeroalarm $sql_buf "REWARD_FEE <= 0"	

	return 0
}
