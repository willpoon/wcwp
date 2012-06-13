######################################################################################################
#程序名称：	INT_CHECK_R262_MONTH.tcl
#校验接口：	21006 21003 21009
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
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #程序名
        global app_name
        set app_name "INT_CHECK_R262_MONTH.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R262')
			"

		exec_sql $sql_buff



##~   --~   R262	月	03_话单日志	语音通话次数	"21003 GSM普通语音业务月使用
##~   --~   21006 智能网语音业务月使用
##~   --~   21009 智能网VPMN业务使用"	语音通话次数≥0	0.05	


set sql_buff "


select (
select count(0) from G_S_21003_MONTH where time_id = $op_month and bigint(CALL_COUNTS) < 0
) + (
select count(0) from G_S_21006_MONTH where time_id = $op_month and bigint(CALL_COUNTS) < 0
) + (
select count(0) from G_S_21009_DAY where time_id/100 = $op_month and bigint(CALL_COUNTS) < 0
)  ALL_CALL_COUNTS from bass2.dual
"

chkzero2 $sql_buff "R262 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R262',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  



##~   --~   R270	月	11_资费订购	在网用户必须选择一款基础资费套餐	"02020 用户选择基础资费套餐
##~   --~   02004 用户
##~   --~   02008 用户状态"	在网用户必须选择一款基础资费套餐	0.05	




set sql_buff "

select count(0)
from                       
(select user_id from int_02004_02008_month_$op_month where USERTYPE_ID  in ('1010') and TEST_FLAG = '0' )  a 
left join (select * from G_I_02020_MONTH where time_id = $op_month and bigint(PROD_VALID_DATE)/100 <= $op_month ) b on a.user_id = b.user_id
where b.BASE_PROD_ID is null
with ur
"

chkzero2 $sql_buff "R270 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R270',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


##~   R271	月	11_资费订购	选择全球通全网统一基础资费套餐的用户数	"02020 用户选择基础资费套餐
##~   02022 用户选择全球通全网统一基础资费套餐"	02020中统计月内选择全球通统一资费套餐的在网用户数＝02022中统计月内选择全球通统一资费套餐的用户数	0.05	

##~   "第一步：取02020中套餐生效日期小于等于月末，且基础套餐标识在【全球通全网统一资费基础套餐标识】维表中的用户，并关联用户和用户状态，剔除月末离网和测试用户；
##~   第二步：取02022月末快照中套餐生效日期小于等于月末的用户，并关联用户表剔除测试用户；
##~   第三步：比较前两步结果是否相等。"


set sql_buff "

select (  
select count(0)
from 
(select * from G_I_02020_MONTH 
			where time_id = $op_month and bigint(PROD_VALID_DATE)/100 <= $op_month
			and  BASE_PROD_ID in (select NEW_PKG_ID from  bass1.DIM_QW_QQT_PKGID )
			) a
,(
select user_id from int_02004_02008_month_$op_month
where USERTYPE_ID not in ('2010','2020','2030','9000') and  TEST_FLAG = '0') c 
where a.USER_ID = c.USER_ID
) -
(
select count(0)
from 
(select * from G_I_02022_DAY where time_id = $this_month_last_day and bigint(VALID_DT) / 100 <= $op_month ) a 
,(
select user_id from int_02004_02008_month_$op_month
where USERTYPE_ID not in ('2010','2020','2030','9000') and  TEST_FLAG = '0') c 
where a.USER_ID = c.USER_ID
) from bass2.dual with ur
"

chkzero2 $sql_buff "R271 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R271',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  





##~   R272	月	11_资费订购	选择全球通全网统一资费专属叠加资费套餐的用户数	"02021 用户选择叠加资费套餐
##~   02023 用户选择全球通专属叠加资费套餐"	02021中统计月内选择全球通全网统一资费专属叠加资费套餐的在网用户数＝02023中统计内月选择全网统一资费专属叠加资费套餐的用户数	0.05	

##~   G_I_02021_MONTH

##~   G_I_02023_DAY

##~   "第一步：取02021中套餐生效日期小于等于月末，且叠加套餐标识在【全球通全网统一资费叠加套餐标识】维表中的用户，并关联用户和用户状态，剔除月末离网和测试用户；
##~   第二步：取02023月末快照中套餐生效日期小于等于月末的用户，并关联用户表剔除测试用户；
##~   第三步：比较前两步结果是否相等。"




set sql_buff "


select (
select count(0) 
from
(select *
from G_I_02021_MONTH where time_id = $op_month and bigint(PROD_VALID_DATE)/100 <= $op_month
and  OVER_PROD_ID in (select NEW_PKG_ID from  bass1.DIM_QW_QQT_PKGID where OLD_PKG_ID like 'QW_QQT_DJ%')
) a ,int_02004_02008_month_$op_month b 
where a.user_id = b.user_id
and USERTYPE_ID not in ('2010','2020','2030','9000')
and b.TEST_FLAG = '0'
) -
(
select count(0) 
from (
select * from G_I_02023_DAY where time_id = $this_month_last_day
) a  ,int_02004_02008_month_$op_month b 
where a.user_id = b.user_id
and b.USERTYPE_ID not in ('2010','2020','2030','9000')
and b.TEST_FLAG = '0'
) from bass2.dual with ur


"

chkzero2 $sql_buff "R272 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R272',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  
  
  
  
  
  
  
  
  
##~   R278	月	09_渠道运营	实体渠道基础信息日月关系	"06021 实体渠道基础信息
##~   06035 实体渠道基础信息（日增量）"	06021中的实体渠道个数应等于06035月末快照中的实体渠道个数	0.05	

##~   "第一步：取06021中生效日期小于等于统计月末，失效日期大于统计月末，且渠道状态为正常运营的实体渠道标识个数；
##~   第二步：从06035月末快照中，取渠道状态为正常运营的实体渠道标识个数；
##~   第三步：比较第一步和第二步的结果。"





set sql_buff "

select (  
select count(0) from G_I_06021_MONTH where time_id = $op_month
and CHANNEL_STATUS = '1'
) -
(
select count(0)
from
(
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id / 100 <= $op_month
) t where t.rn =1  and CHNL_STATE = '1'
) from bass2.dual with ur


"

chkzero2 $sql_buff "R278 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R278',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  
  
  


##~   R279	月	09_渠道运营	实体渠道购建或租赁信息日月关系	"06022 实体渠道购建或租赁信息
##~   06036 实体渠道购建或租赁信息（日增量）"	06022中的实体渠道个数应等于06036月末快照中的实体渠道个数	0.05	

##~   "第一步：取06022中生效日期小于等于统计月末，失效日期大于统计月末，的实体渠道标识个数；
##~   第二步：取06036月末快照中的实体渠道标识个数；
##~   第三步：比较第一步和第二步的结果。"


##~   G_I_06022_MONTH
##~   G_A_06036_DAY



set sql_buff "

select (
select count(0)
from 
(				
select * from G_I_06022_MONTH where time_id = $op_month ) a,
(select * from G_I_06021_MONTH where time_id = $op_month
and CHANNEL_STATUS = '1')  b
where a.channel_id = b.channel_id 
) -
(
select count(0)
from 
(
	select *
	from
	(
	select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
	from G_A_06036_DAY a
	where time_id / 100 <= $op_month
	) t where t.rn =1  
) a,(select * from G_I_06021_MONTH where time_id = $op_month and CHANNEL_STATUS = '1')  b
where a.channel_id = b.channel_id
) from bass2.dual with ur



"

chkzero2 $sql_buff "R279 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R279',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  




##~   R280	月	09_渠道运营	实体渠道资源配置信息日月关系	"06023 实体渠道资源配置信息
##~   06037 实体渠道资源配置信息（日增量）"	06023中的实体渠道个数应等于06037月末快照中的实体渠道个数	0.05	



##~   G_I_06023_MONTH
##~   G_A_06037_DAY


##~   "第一步：取06023中生效日期小于等于统计月末，失效日期大于统计月末，的实体渠道标识个数；
##~   第二步：取06037月末快照中的实体渠道标识个数；
##~   第三步：比较第一步和第二步的结果。"




set sql_buff "


select (
select count(0)
from 
(select * from G_I_06023_MONTH a  where time_id = $op_month ) a
,(select * from G_I_06021_MONTH a where time_id = $op_month and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) - (
select count(0) from 
(
		select *
		from
		(
		select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		from G_A_06037_DAY a
		where time_id / 100 <= $op_month
		) t where t.rn =1  
) a ,(select * from G_I_06021_MONTH a where time_id = $op_month and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 
) from bass2.dual with ur


"

chkzero2 $sql_buff "R280 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R280',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  


##~   --~   R282	月	10_积分计划	非全球通品牌的客户不应上传品牌奖励积分	02006 用户积分情况	非全球通品牌的客户不应上传品牌奖励积分	0.05	

##~   从02006月末快照中，取品牌奖励积分>0的用户，判断是否存在于用户表中客户品牌≠全球通的非测试用户中，若存在，则违反该规则




set sql_buff "


select sum(points) points
from (
select brand_id,sum(bigint(MONTH_QQT_POINTS)) points
from ( select user_id,brand_id  from bass1.int_02004_02008_month_$op_month ) a 
, (select * from G_I_02006_MONTH where TIME_ID  = $op_month ) b 
where a.user_id = b.user_id 
group by brand_id
) t where brand_id in ('2','3')



"

chkzero2 $sql_buff "R282 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R282',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  

##~   --~   R283	月	10_积分计划	当前可兑换积分不能为负值	02006 用户积分情况	当前可兑换积分不能为负值	0.05	




set sql_buff "


select count(0)
 from G_I_02006_MONTH 
where bigint(CONVERTIBLE_POINTS) < 0                
and time_id = $op_month
with ur

"

chkzero2 $sql_buff "R283 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R283',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  
  
	return 0
}
