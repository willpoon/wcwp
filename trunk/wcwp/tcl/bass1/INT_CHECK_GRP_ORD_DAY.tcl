######################################################################################################
#程序名称：	INT_CHECK_GRP_ORD_DAY.tcl
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
        ##~   set curr_month 201207
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        ##~   set last_month 201206		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]] op_month改用curr_month
        set last_month [GetLastMonth [string range $curr_month 0 5]]
		
        #程序名
        global app_name
        set app_name "INT_CHECK_GRP_ORD_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$timestamp and rule_code in (
					 'R265'
					,'R286'
					,'R287'
					,'R288'
					,'R289' -- 未校验
					,'R290' -- 未校验
					,'R291'
					,'R301' -- 未校验
					,'R303'
					,'R304'
					,'R305'
					,'R306'
					,'R308'
					,'R309'
					,'R311'
					,'R312'
					,'R314'
					,'R315'
					,'R317'
					,'R318'
					,'R320'
					,'R321'
					,'R323'
					,'R325'
					,'R327'
					,'R329'
					,'R331'
					,'R333'
					,'R334'
					,'R336'
					,'R338'
					)
			"

		exec_sql $sql_buff





##~   --~   R265	月	03_话单日志	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”	"04016 行业网关短信话单
##~   --~   22036 集团客户端口资源使用情况"	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”中	0.05	

##~   "Step1.04016（行业网关短信话单）接口中发送状态为0“成功”的“服务代码”集合；
##~   Step2.22036（集团客户端口资源使用情况）接口中截止到统计周期末业务类型=1“行业网关短信”的“行业应用代码全码”集合；
##~   Step3.集合Step1是否均在集合Step2中。"



set sql_buff "


select count(0)
from table(

select distinct  SERV_CODE from G_S_04016_DAY where time_id / 100 = $curr_month and SEND_STATUS = '0'
except
                        select distinct APP_LENCODE from 
                        (
                                        select t.*
                                        ,row_number()over(partition by EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
                                        from 
                                        G_A_22036_DAY  t
										where  time_id / 100 <= $curr_month
                          ) a
                        where rn = 1    and OPERATE_TYPE = '1'
										And bigint(OPEN_DATE)/100 <= $curr_month
                        
) a
with ur
"

chkzero2 $sql_buff "R265 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R265',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


##~   R286	月	02_集团客户	A类集团客户到达数	01004 集团客户	A类集团客户到达数较上月变动率 ≤ 30%	0.05	

##~   统计当月和上月中，分别统计01004（集团客户）接口，“客户状态类型编码”为20（在网），“集团价值分类编码”in(4，5)的集团客户标识个数，去重。计算当月环比值，若>30%，则违反规则。

##~   统计当月和上月中，分别统计01004（集团客户）接口，“客户状态类型编码”为20（在网），“集团价值分类编码”in(6，7)的集团客户标识个数，去重。计算当月环比值，若>30%，则违反规则。





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

	set sql_buff "
select 
a.curr_cnts
,b.bef_cnts
,case when bef_cnts=0 then 1
	  else decimal((curr_cnts-bef_cnts)*1.0/bef_cnts,10,4)
 end
from table(
select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type,count(0) curr_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('4','5')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end
) a,
table(select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type
			 ,count(0) bef_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $last_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('4','5')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end) b
where a.type = b.type
"

   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R286',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.3 } {
                set grade 2
                set alarmcontent " R286 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   R287	月	02_集团客户	B类集团客户到达数	01004 集团客户	B类集团客户到达数较上月变动率 ≤ 30%	0.05	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

	set sql_buff "
select
 a.curr_cnts
,b.bef_cnts
,case when bef_cnts=0 then 1
	  else decimal((curr_cnts-bef_cnts)*1.0/bef_cnts,10,4)
 end
from table(
select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type,count(0) curr_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in('6','7')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end
) a,
table(select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type
			 ,count(0) bef_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $last_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('6','7')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end) b
where a.type = b.type
"

   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R287',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.3 } {
                set grade 2
                set alarmcontent " R287 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   R288	月	02_集团客户	清单ID与集团客户标识对应关系之间的多对多记录	01007 集团客户与目标市场清单客户对应关系	01007接口中清单客户ID与集团客户标识对应关系不存在多对多记录条数	0.05


##~   统计01007（集团客户与目标市场清单客户对应关系）接口“对应关系状态”为建立的记录中，目标市场清单ID
##~   既在
##~   清单ID与集团客户标识的一对多关系中
##~   又在
##~   清单ID与集团客户标识的多对一关系中
##~   的记录条数，若结果大于0，则违反规则。	

	set sql_buff "
select count(0) from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
and ENTERPRISE_ID in (
select ENTERPRISE_ID from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by ENTERPRISE_ID having count(0) > 1 
) 
and CUST_ID in (
select CUST_ID  from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by CUST_ID having count(0) > 1 
)


"

chkzero2 $sql_buff "R288 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R288',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  

##~   R289	月	02_集团客户	商户管家集团客户与业务方式的一一对应	02065 商户管家业务订购关系	02065接口中，订购商户管家业务的集团客户只能对应一种业务方式，单机版或者网络版	0.05		

##~   02065（商户管家业务订购关系）接口中，统计集团客户标识对应去重的“业务方式”个数，若有1家以上的集团客户对应两种业务方式，则违反规则。



##~   G_A_02065_DAY


##~   --~   select count(0)
##~   --~   from
##~   --~   (
##~   --~   select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
##~   --~   from G_A_02065_DAY a
##~   --~   where time_id / 100 <= $curr_month
##~   --~   ) t where t.rn =1  and CHNL_STATE = '1'


##~   select count(0) from G_A_02065_DAY

##~   无数据，暂不校验！



##~   R290	月	02_集团客户	商户管家订购的终端与其所在集团客户标识的订购一一对应	"02054 集团业务订购关系
##~   02059 集团业务个人用户绑定关系
##~   02065 商户管家业务订购关系"	订购商户管家业务的终端所在集团客户也须有商户管家业务的订购关系记录	0.05		"Step1.统计02059（集团业务个人用户绑定关系）接口中，截止统计月末订购状态正常，订购业务为“商户管家”产品的非空集团客户标识集合；
##~   Step2.统计02054（集团业务订购关系）和02065（商户管家业务订购关系）接口中订购状态正常，订购业务为“商户管家”产品的集团客户标识集合；
##~   Step3.统计Step1集合不在Step2集合的集团客户标识个数，若结果大于0，则违反规则。"	

##~   无数据，暂不校验！




##~   R291	月	02_集团客户	集团客户端口资源使用情况接口的“集团客户标识”均在集团客户接口的“集团客户标识”中	"22036 集团客户端口资源使用情况
##~   01004 集团客户"	集团客户端口资源使用情况接口的“集团客户标识”均在集团客户接口的“集团客户标识”中	0.05		"Step1.统计22036（集团客户端口资源使用情况）接口中，截止到统计周期末客户类型=0“集团客户”的“集团客户标识”集合；
##~   Step2.统计01004（集团客户）接口中，截止统计周期末集团客户标识快照集合；
##~   Step3.统计Step1集合不在Step2集客中的集团客户标识个数，若结果大于0，则违反规则。"



	set sql_buff "
		select count(0) from  table (
			select distinct EC_CODE from 
								(
												select t.*
														,row_number()over(partition by EC_CODE,CUST_TYPE
														order by time_id desc ) rn 
												from 
												G_A_22036_DAY  t
												where 
												TIME_ID/100 <= $curr_month										
								  ) a
								where rn = 1  
								and bigint(OPEN_DATE)/100 <= $curr_month
								and  CUST_TYPE = '0'
		) t where 
			EC_CODE not in (                  
		select enterprise_id
		from  table (
					select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $curr_month
					  ) a
					where rn = 1
			)   t                     
		)
		with ur
"

##~   集团一经代码无此口径：and CUST_STATU_TYP_ID = '20' ，故踢去！

chkzero2 $sql_buff "R291 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R291',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  


##~   R301	月	02_集团客户	IDC使用集团客户到达数	02054 集团客户业务订购关系	IDC使用集团客户到达数环比绝对值小于等于50%	0.05		"Step1.02054（集团客户业务订购关系）接口中，截止统计月订购状态正常的，业务类型编码为1190（IDC）的当月和上月集团客户标识去重个数；
##~   Step2.当月值与上月值比较。"


##~   无业务




##~   R303	月	02_集团客户	企信通使用集团客户到达数	02054 集团客户业务订购关系	企信通使用集团客户到达数环比绝对值小于等于50%	0.05		"Step1.02054（集团客户业务订购关系）接口中，截止统计月订购状态正常的，业务类型编码为1330（企信通）的当月和上月集团客户标识去重个数；
##~   Step2.当月值与上月值比较。"


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
aa.curr_cnts
,bb.bef_cnts
,case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  
		 from bass1.g_a_02054_day 
      where  time_id / 100 <= $curr_month
         ) z
    where row_id=1
      and status_id='1'
	  and enterprise_busi_type in ('1330')
	  and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
      where  time_id / 100 <= $last_month
         ) z
    where row_id=1
      and status_id='1'
	  and enterprise_busi_type in ('1330')
	  and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R303',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R303 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R304	月	02_集团客户	企信通使用集团个人客户到达数	02059 集团业务个人用户绑定关系	企信通使用集团个人客户到达数环比绝对值小于等于50%
   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
cc.curr_cnts
,dd.bef_cnts
,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
         where  time_id / 100 <= $curr_month
          ) z
        where status_id='1'
          and row_id=1
		  and  bigint(order_date)/100<=$curr_month
		  and   enterprise_busi_type in('1330')
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
         where  time_id / 100 <= $last_month
          ) z
        where status_id='1'
          and row_id=1
		  and  bigint(order_date)/100<=$last_month
		  and   enterprise_busi_type in('1330')
       ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R304',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R304 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R305	月	02_集团客户	移动OA使用集团客户到达数	02054 集团客户业务订购关系	移动OA使用集团客户到达数环比绝对值小于等于50%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R305',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R305 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R306	月	02_集团客户	使用移动OA的集团个人客户数	02059 集团业务个人用户绑定关系	使用移动OA的集团个人客户数环比绝对值小于等于50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select
cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R306',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R306 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}






##~   --R308	月	02_集团客户	移动CRM使用集团客户到达数	02054 集团客户业务订购关系	移动CRM使用集团客户到达数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R308',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R308 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R309	月	02_集团客户	使用移动CRM的集团个人客户数	02059 集团业务个人用户绑定关系	使用移动CRM的集团个人客户数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R309',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R309 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   --R311	月	02_集团客户	移动进销存使用集团客户到达数	02054 集团客户业务订购关系	移动进销存使用集团客户到达数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R311',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R311 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R312	月	02_集团客户	使用移动进销存的集团个人客户数	02059 集团业务个人用户绑定关系	使用移动进销存的集团个人客户数环比绝对值小于等于50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R312',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R312 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R314	月	02_集团客户	BlackBerry使用集团客户到达数	02054 集团客户业务订购关系	BlackBerry使用集团客户到达数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R314',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R314 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   --R315	月	02_集团客户	使用BlackBerry的集团个人客户数	"02059 集团业务个人用户绑定关系 02060 BlackBerry（BES）个人用户绑定关系"	使用BlackBerry的集团个人客户数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$curr_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02060_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$curr_month
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$last_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02060_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$last_month
       ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R315',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R315 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}






##~   --R317	月	02_集团客户	企业建站使用集团客户到达数	"02054 集团客户业务订购关系 02055 企业建站业务订购情况"	企业建站使用集团客户到达数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02055_DAY 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02055_DAY 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R317',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R317 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}













##~   --R318	月	02_集团客户	使用企业建站的集团个人客户数	02059 集团业务个人用户绑定关系	使用企业建站的集团个人客户数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R318',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R318 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R320	月	02_集团客户	手机邮箱使用集团客户到达数	02054 集团客户业务订购关系	手机邮箱使用集团客户到达数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R320',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R320 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R321	月	02_集团客户	使用手机邮箱的集团个人客户数	"02059 集团业务个人用户绑定关系 02061 手机邮箱个人用户绑定关系"	使用手机邮箱的集团个人客户数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$curr_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02061_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$last_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02061_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R321',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R321 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R323	月	02_集团客户	集团彩信使用集团客户到达数	02054 集团客户业务订购关系	集团彩信使用集团客户到达数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1120')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1120')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R323',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R323 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R325	月	02_集团客户	移动400使用集团客户到达数	"02054 集团客户业务订购关系 02064 移动400业务订购情况"	移动400使用集团客户到达数环比绝对值小于等于50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1520')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select OPEN_DT,enterprise_id,ORD_STS,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02064_DAY 
		         where time_id/100<=$curr_month
) z
    where ORD_STS='1'
      and row_id=1
	--and enterprise_busi_type in ('1520')
        and bigint(OPEN_DT)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1520')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select OPEN_DT,enterprise_id,ORD_STS,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02064_DAY
		         where time_id/100<=$last_month
) z
    where ORD_STS='1'
      and row_id=1
	--and enterprise_busi_type in ('1520')
        and bigint(OPEN_DT)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R325',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R325 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R327	月	02_集团客户	呼叫中心直联使用集团客户到达数	02054 集团客户业务订购关系	呼叫中心直联使用集团客户到达数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case　when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1070')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1070')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R327',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R327 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R329	月	02_集团客户	企业邮箱使用集团客户到达数	"02054 集团客户业务订购关系 02056 企业邮箱业务订购情况"	企业邮箱使用集团客户到达数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02056_DAY 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02056_DAY 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R329',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R329 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R331	月	02_集团客户	商户管家订购集团客户数	"02054 集团客户业务订购关系 02065 商户管家业务订购关系"	商户管家订购集团客户数环比绝对值小于等于50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select order_dt,enterprise_id,sts_cd,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02065_DAY
		         where time_id/100<=$curr_month
) z
    where STS_CD='1'
      and row_id=1
	--and ent_busi_id in ('1560')
        and bigint(ORDER_DT)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select order_dt,enterprise_id,sts_cd,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02065_DAY 
		         where time_id/100<=$last_month
) z
    where STS_CD='1'
      and row_id=1
	--and ent_busi_id in ('1560')
        and bigint(ORDER_DT)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R331',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R331 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R333	月	02_集团客户	商户管家订购终端数	02059 集团业务个人用户绑定关系	商户管家订购终端数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when  dd.bef_cnts=0 and cc.curr_cnts = 0 then 0 when dd.bef_cnts=0 and cc.curr_cnts <> 0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$curr_month
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$last_month
		) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R333',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R333 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R334	月	02_集团客户	校讯通当月使用集团个人客户数	02059 集团业务个人用户绑定关系	校讯通当月使用集团个人客户数环比绝对值小于等于50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1310')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1310')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R334',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R334 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R336	月	02_集团客户	M2M当月使用集团客户数	02054 集团客户业务订购关系	M2M订购集团客户数环比绝对值小于等于50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R336',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R336 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R338	月	02_集团客户	M2M订购终端数	"02059 集团业务个人用户绑定关系 02062 M2M个人用户绑定关系"	M2M订购终端数环比绝对值小于等于50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in('1241','1249')
        and bigint(order_date)/100<=$curr_month
        union all
        select enterprise_id,user_id 
         from (select create_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02062_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(create_date)/100<=$curr_month
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in('1241','1249')
        and bigint(order_date)/100<=$last_month
        union all
        select enterprise_id,user_id 
         from (select create_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02062_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(create_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R338',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #检查合法性: 0 - 不正常； 大于0 - 正常
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R338 校验不通过"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



	return 0
}
