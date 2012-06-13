######################################################################################################
#程序名称：	INT_CHECK_GRP_INC_MONTH.tcl
#校验接口：	
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2012-06-09 
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
        set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #程序名
        global app_name
        set app_name "INT_CHECK_GRP_INC_MONTH.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in (
						 'R296'
						,'R297'
						,'R302'
						,'R307'
						,'R310'
						,'R313'
						,'R316'
						,'R319'
						,'R322'
						,'R324'
						,'R326'
						,'R328'
						,'R330'
						,'R332'
						,'R335'
						,'R337'
						,'R339'
					)
			"

		exec_sql $sql_buff


##~   R292	月	02_集团客户	数据专线统一付费收入	03017 集团客户统付收入	数据专线统一付费收入月变动率 ≤ 30%	0.05		"Step1.统计当月和上月，03017（集团客户统付收入）中，业务类型编码为1180(数据专线)的“应收金额”之和，得出当月值和上月值；
##~   Step2.若(当月值-上月值)/上月值*100%>30%，则违反规则。"


##~   G_S_03017_MONTH


##~   1180	数据专线

##~   !!!无数据暂不校验！需要修复！！
##~   select sum(int(income)) from   G_S_03017_MONTH
##~   where ent_busi_id = '1180'
##~   and time_id = $op_month



##~   R293	月	02_集团客户	VOIP专线客户数与收入匹配关系	"02054 集团客户业务订购关系
##~   02057 专线业务订购情况
##~   03017 集团客户统付收入
##~   03018 集团业务个人非统付收入"	

##~   当VOIP专线收入大于0，其对应专线客户数也应该大于0	0.05		"Step1.统计周期内，03017（集团客户统付收入）和03018（集团业务个人非统付收入）接口中，业务类型编码为1160(VOIP专线)的收入之和；
##~   Step2.02054（集团客户业务订购关系）和02057（专线业务订购情况）接口中，截止到统计周期末订购状态正常的有效订购数据中，订购业务类型编码为1160(VOIP专线)的集团客户标识个数，去重；
##~   Step3.比较Step1和Step2结果值。"

##~   R293 无数据 ，需要核查修复！





##~   R296	月	02_集团客户	A类集团客户整体收入	"03004 明细账单
##~   02004 用户
##~   02008 用户状态
##~   01004 集团客户
##~   02049 集团用户成员
##~   03017 集团统付收入"	A类集团客户整体收入环比绝对值小于等于30%	0.05		"Step1.通过02004（用户）、02008（用户状态）和03004（明细账单）接口，得出统计月每位个人用户的收入；
##~   Step3.按照Step1和Step2口径统计相邻月份的值，计算得出当月比上月的环比绝对值，若结果大于30%，则违反规则。"

	set sql_buff "ALTER TABLE BASS1.INT_03004_03017_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	
	set sql_buff "
		insert into bass1.INT_03004_03017_1
		select a.user_id , sum(bigint(FEE_RECEIVABLE)) FEE_RECEIVABLE
		from 
		(select * from G_S_03004_MONTH where time_id = $op_month)  a
		,(select user_id from int_02004_02008_month_$op_month where TEST_FLAG = '0' )  b
		where a.USER_ID = b.user_id
		group by a.user_id
		with ur
		"
	exec_sql $sql_buff

	set sql_buff "ALTER TABLE BASS1.INT_03004_03017_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	set sql_buff "
		insert into bass1.INT_03004_03017_2
		select a.user_id , sum(bigint(FEE_RECEIVABLE)) FEE_RECEIVABLE
		from 
		(select * from G_S_03004_MONTH where time_id = $last_month)  a
		,(select user_id from int_02004_02008_month_$last_month where TEST_FLAG = '0' )  b
		where a.USER_ID = b.user_id
		group by a.user_id
		with ur
		"
	exec_sql $sql_buff


##~   Step2.通过02049（集团用户成员）接口关联Step1结果和01004（集团客户）接口中“集团价值分类编码”in(4，5)的A类集团客户，得出每家A类集团客户下所有成员的收入之和，并与03017（集团统付收入）接口中每家集团客户的统付收入汇总，得出A类集团客户的整体收入；
	set sql_buff "
select mem_fee + uniq_fee
from (
select sum(FEE_RECEIVABLE) mem_fee from table (
        select enterprise_id from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $op_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in ('4','5')  
) a,(select * from G_I_02049_MONTH where time_id = $op_month ) b 
,INT_03004_03017_1 c 
where a.enterprise_id = b.enterprise_id
and b.user_id = c.user_id 
) a ,(

select sum(bigint(INCOME)) uniq_fee
from G_S_03017_MONTH 
where TIME_ID = $op_month

) b where 1 = 1

		"
	set RESULT_VAL1 0
	set RESULT_VAL1 [get_single $sql_buff]

	set sql_buff "
		select mem_fee + uniq_fee
		from (
		select sum(FEE_RECEIVABLE) mem_fee from table (
				select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $last_month
					  ) a
					where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in ('4','5')  
		) a,(select * from G_I_02049_MONTH where time_id = $last_month ) b 
		,INT_03004_03017_2 c 
		where a.enterprise_id = b.enterprise_id
		and b.user_id = c.user_id 
		) a ,(

		select sum(bigint(INCOME)) uniq_fee
		from G_S_03017_MONTH 
		where TIME_ID = $last_month

		) b where 1 = 1
		with ur
"
	set RESULT_VAL2 0
	set RESULT_VAL2 [get_single $sql_buff]
	
	set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL1} / ${RESULT_VAL2} - 1) ]]
		if {  ${RESULT_VAL3} > 0.30 } {
		set grade 2
	        set alarmcontent "exception:R296 指标环比异常"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "ChnRatio pass!"	
	}
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R296',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
exec_sql $sql_buff


##~   R297	月	02_集团客户	B类集团客户整体收入	"03004 明细账单
##~   02004 用户
##~   02008 用户状态
##~   01004 集团客户
##~   02049 集团用户成员
##~   03017 集团统付收入"	B类集团客户整体收入环比绝对值小于等于30%	0.05		"Step1.通过02004（用户）、02008（用户状态）和03004（明细账单）接口，得出统计月每位个人用户的收入；
##~   Step2.通过02049（集团用户成员）接口关联Step1结果和01004（集团客户）接口中“集团价值分类编码”in(6，7)的B类集团客户，得出每家B类集团客户下所有成员的收入之和，并与03017（集团统付收入）接口中每家集团客户的统付收入汇总，得出B类集团客户的整体收入；
##~   Step3.按照Step1和Step2口径统计相邻月份的值，计算得出当月比上月的环比绝对值，若结果大于30%，则违反规则。"

	set sql_buff "
select mem_fee + uniq_fee
from (
select sum(FEE_RECEIVABLE) mem_fee from table (
        select enterprise_id from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $op_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in ('6','7')  
) a,(select * from G_I_02049_MONTH where time_id = $op_month ) b 
,INT_03004_03017_1 c 
where a.enterprise_id = b.enterprise_id
and b.user_id = c.user_id 
) a ,(

select sum(bigint(INCOME)) uniq_fee
from G_S_03017_MONTH 
where TIME_ID = $op_month

) b where 1 = 1

		"
	set RESULT_VAL1 0
	set RESULT_VAL1 [get_single $sql_buff]

	set sql_buff "
		select mem_fee + uniq_fee
		from (
		select sum(FEE_RECEIVABLE) mem_fee from table (
				select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $last_month
					  ) a
					where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in  ('6','7')  
		) a,(select * from G_I_02049_MONTH where time_id = $last_month ) b 
		,INT_03004_03017_2 c 
		where a.enterprise_id = b.enterprise_id
		and b.user_id = c.user_id 
		) a ,(

		select sum(bigint(INCOME)) uniq_fee
		from G_S_03017_MONTH 
		where TIME_ID = $last_month

		) b where 1 = 1
		with ur
"
	set RESULT_VAL2 0
	set RESULT_VAL2 [get_single $sql_buff]
	
	set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL1} / ${RESULT_VAL2} - 1) ]]
		if {  ${RESULT_VAL3} > 0.30 } {
		set grade 2
	        set alarmcontent "exception:R297 指标环比异常"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "ChnRatio pass!"	
	}
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R297',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
exec_sql $sql_buff




##~   R298	月	02_集团客户	A类集团客户信息化收入	"01004 集团客户
##~   02049 集团用户成员
##~   03017 集团统付收入
##~   03018 集团个人非统付收入"	A类集团客户信息化收入环比绝对值小于等于30%	0.05
##~   "Step1.分别统计03017（集团统付收入）接口关联01004（集团客户）接口的A类集团客户中，满足当年业务部门下发的信息化收入口径（例如：2012年信息化收入口径满足《2012年集团信息化收入指标统计口径》）的统付信息化收入之和，得出当月值和上月值；
##~   Step2.02049（集团用户成员）关联01004（集团客户）的A类集团客户和03018（集团个人非统付收入）接口按照当年信息化收入统计口径，得出当月和上月的非统付信息化收入；
##~   Step3.将Step1和Step2中的统付信息化收入和非统付信息化收入加总得出信息化收入，从而分别形成当月A类信息化收入当月值和上月A类信息化收入上月值；
##~   Step4.计算当月值与上月值的环比绝对值，若结果大于30%，则违反规则。"



##~   R299	月	02_集团客户	B类集团客户信息化收入	"01004 集团客户
##~   02049 集团用户成员
##~   03017 集团统付收入
##~   03018 集团个人非统付收入"	B类集团客户信息化收入环比绝对值小于等于30%	0.05		"Step1.分别统计03017（集团统付收入）接口关联01004（集团客户）接口的B类集团客户中，满足当年业务部门下发的信息化收入口径（例如：2012年信息化收入口径满足《2012年集团信息化收入指标统计口径》）的统付信息化收入之和，得出当月值和上月值；
##~   Step2.02049（集团用户成员）关联01004（集团客户）的B类集团客户和03018（集团个人非统付收入）接口按照当年信息化收入统计口径，得出当月和上月的非统付信息化收入；
##~   Step3.将Step1和Step2中的统付信息化收入和非统付信息化收入加总得出信息化收入，从而分别形成当月B类信息化收入当月值和上月B类信息化收入上月值；
##~   Step4.计算当月值与上月值的环比绝对值，若结果大于30%，则违反规则。"






##~   R300	月	02_集团客户	IDC当月总收入	"03017 集团统付收入
##~   03018 集团个人非统付收入"	IDC当月总收入环比绝对值小于等于50%	0.05		"Step1.03017（集团统付收入）和03018（集团个人非统付收入）接口中，业务类型编码为1190（IDC）的当月和上月收入之和；
##~   Step2.当月值与上月值比较。"

##~   无业务收入




##~   R302	月	02_集团客户	企信通当月总收入	"03017 集团统付收入
##~   03018 集团个人非统付收入"	企信通当月总收入环比绝对值小于等于50%	0.05		"Step1.03017（集团统付收入）和03018（集团个人非统付收入）接口中，业务类型编码为1330（企信通）的当月和上月收入之和；
##~   Step2.当月值与上月值比较。"


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "
select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,2)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
select sum(bigint(income)) income from   G_S_03017_MONTH
where ent_busi_id = '1330'
and time_id = $op_month
union all
select sum(bigint(income)) income from   G_S_03018_MONTH
where ent_busi_id = '1330'
and time_id = $op_month
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
select sum(bigint(income)) income from   G_S_03017_MONTH
where ent_busi_id = '1330'
and time_id = $last_month
union all
select sum(bigint(income)) income from   G_S_03018_MONTH
where ent_busi_id = '1330'
and time_id = $last_month
   ) b
) bb
on 1=1
with ur
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R302',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R286 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R307	月	02_集团客户	移动OA当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	移动OA当月总收入环比绝对值小于等于50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select
aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1250')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1250')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1250')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1250')
   ) b
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R307',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R307 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R310	月	02_集团客户	移动CRM当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	移动CRM当月总收入环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1280')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1280')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1280')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1280')
   ) b
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R310',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R310 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R313	月	02_集团客户	移动进销存当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	移动进销存当月总收入环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1260')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1260')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1260')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1260')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R313',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R313 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}









##~   --R316	月	02_集团客户	BlackBerry当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	BlackBerry当月总收入环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1230')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1230')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1230')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1230')
   ) b
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R316',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R316 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R319	月	02_集团客户	企业建站当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	企业建站当月总收入环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1340')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1340')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1340')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1340')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R319',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R319 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R322	月	02_集团客户	手机邮箱当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	手机邮箱当月总收入环比绝对值小于等于50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1220')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1220')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1220')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1220')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R322',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R322 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



		  
##~   --R324	月	02_集团客户	集团彩信当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	集团彩信当月总收入环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select
aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1120')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1120')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1120')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1120')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R324',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R324 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R326	月	02_集团客户	移动400当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	移动400当月总收入环比绝对值小于等于50%






   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1520')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1520')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1520')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1520')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R326',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R326 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R328	月	02_集团客户	呼叫中心直联当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	呼叫中心直联当月总收入环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1070')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1070')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1070')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1070')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R328',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R328 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R330	月	02_集团客户	企业邮箱当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	企业邮箱当月总收入环比绝对值小于等于50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "




select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1210')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1210')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1210')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1210')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R330',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R330 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R332	月	02_集团客户	商户管家总收入	03017 集团统付收入	商户管家总收入环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,2)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1560')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1560')
   ) b
) bb
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R332',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R332 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R335	月	02_集团客户	校讯通当月总收入	"03017 集团统付收入 03018 集团个人非统付收入"	校讯通当月总收入环比绝对值小于等于50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1310')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1310')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1310')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1310')
   ) b
) bb
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R335',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R335 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R337	月	02_集团客户	M2M总收入	"03017 集团统付收入  03018 集团个人非统付收入"	M2M总收入环比绝对值小于等于50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,2)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1241','1249')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1241','1249')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1241','1249')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1241','1249')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R337',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R337 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   --R339	月	02_集团客户	集团信息化短信业务量	22303 集团产品业务量月汇总	当月集团信息化短信业务量环比绝对值小于等于50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select  dan_cnts,bef_cnts,
     case when bef_cnts=0 then 1
          else decimal((dan_cnts-bef_cnts)*1.0/bef_cnts,10,2)
     end
  from 
(
select value(sum(bigint(upmessage)+bigint(downmessage)),0) dan_cnts
 from bass1.G_S_22303_MONTH where time_id=$op_month
) a
inner join
(
select value(sum(bigint(upmessage)+bigint(downmessage)),0) bef_cnts
 from bass1.G_S_22303_MONTH where time_id=$op_month
) b
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R339',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R339 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




	return 0
}
