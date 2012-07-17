######################################################################################################
#接口名称：用户选择基础资费套餐
#接口编码：02020
#接口说明：用户选择的基础套餐/必选包，上报符合以下条件的套餐订购记录，每用户最多一条记录：
#1.	用户当月曾经在网，不考虑其月末是否离网。不包括历史离网用户。
#2.	每个用户当月只能选择一款基础套餐。当月该用户的相关通信行为曾经依据该档套餐进行计费处理。
#3.	套餐生效日期，指用户通信行为开始依据该档基础套餐进行计费的起始日期。生效日期中的月份应该早于或等于当月。对于次月生效的套餐放在下月数据中上传，不在本月数据中上传。
#程序名称: G_I_02020_MONTH.tcl
#功能描述: 生成02020的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2011-02-18
#问题记录：1.
#修改历史: 1. 1.7.1 规范
#修改历史: 1. 1.7.2 规范 : 针对全球通全网统一资费套餐做转换
#修改历史: 2011-05-24 18:03:24 优化代码的效率与可读性
#修改历史: 2011-05-24 18:03:24 从02022抓全网用户套餐ID
#修改历史: 2011-05-24 18:03:24 核对02022和02020结果表中的user_id一致性。
#修改历史: 2011-06-03 21:53:33 对于套餐变更，取月内生效的最后一次变更
#修改历史: 2011-06-24 18:16:35 原口径不含月内离网，加入之。
#修改历史: 2011-06-24 18:16:35 关联全网套餐从02022月内用户改成02022最后一天用户。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


      #本月 yyyymm
      #set op_time 2011-01-01
      #set optime_month 2011-01
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      #本月最后一天 yyyymmdd
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
      puts $op_time
      puts $op_month

    global app_name
		set app_name "G_I_02020_MONTH.tcl"        


  #删除本期数据
	set sql_buff "delete from bass1.g_i_02020_month where time_id=$op_month"
	exec_sql $sql_buff

##3.	套餐生效日期，指用户通信行为开始依据该档基础套餐进行计费的起始日期。生效日期中的月份应该早于或等于当月。对于次月生效的套餐放在下月数据中上传，不在本月数据中上传。
#	set sql_buff "
#	insert into bass1.g_i_02020_month
#		  (
#		   time_id
#	    ,user_id
#			,base_prod_id
#			,prod_valid_date
#		  )
#		select 
#		     $op_month,
#		     char(a.product_instance_id),
#		     value(d.BASE_PKG_ID,char(a.offer_id)),
#		     value(d.VALID_DT,replace(char(date(a.create_date)),'-',''))
#		from bass2.dw_product_ins_prod_$op_month a
#	  left join 
#				(select user_id , BASE_PKG_ID ,VALID_DT
#					from
#					(select a.*,row_number()over(partition by user_id order by VALID_DT desc ) rn
#					from bass1.g_i_02022_day  a
#					where time_id  = $this_month_last_day
#					) t where  t.rn = 1 
#				) d on a.product_instance_id = d.user_id
#		where  a.state in ('1','4','6','8','M','7','C','9')
#		  and a.user_type_id =1
#		  and a.valid_type = 1
#		  and a.bill_id not in ('D15289014474','D15289014454')
#		  and not exists 
#				  (select 1 from  bass2.dw_product_test_phone_$op_month b 
#				  			where a.product_instance_id	= b.user_id and  b.sts=1
#		  		)
#  "
#
#	exec_sql $sql_buff
#
	set sql_buff "ALTER TABLE BASS1.g_i_02020_month_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
insert into bass1.g_i_02020_month_1 (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=$this_month_last_day ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=$this_month_last_day ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
	"
	exec_sql $sql_buff

##关联g_i_02020_month_1主要是保持和集团一致，尤其是统计离网。
	set sql_buff "
insert into 	 bass1.g_i_02020_month
		  (
			time_id
			,user_id
			,base_prod_id
			,prod_valid_date
		  )
		select  $op_month
		,b.user_id
		,value(value(e.new_pkg_id,d.BASE_PKG_ID),char(a.offer_id)) base_prod_id
		,value(d.VALID_DT,replace(char(date(a.create_date)),'-','')) VALID_DT
		from bass2.dw_product_ins_prod_$op_month  a
		join bass2.dw_product_$op_month b on a.product_instance_id = b.user_id
		join BASS1.g_i_02020_month_1 c on  a.product_instance_id = c.USER_ID
		left join (select user_id , BASE_PKG_ID ,VALID_DT
				from
				(select a.*,row_number()over(partition by user_id order by VALID_DT desc ) rn
				from bass1.g_i_02022_day  a
				where time_id  = $this_month_last_day
				) t where  t.rn = 1 
			  ) d on a.product_instance_id = d.user_id
		left join bass1.DIM_QW_QQT_PKGID e on  d.BASE_PKG_ID = e.old_pkg_id		
		where   a.valid_type = 1 
		and (b.userstatus_id  in (1,2,3,6,8) or b.MONTH_OFF_MARK = 1)
		and not exists 
			(select 1 from  bass2.dw_product_test_phone_$op_month e 
				where a.product_instance_id     = e.user_id and  e.sts=1
			)
	"
exec_sql $sql_buff


  #进行结果数据检查
  
	#检查  02022和02020结果表中的user_id一致性。
	#1.	用户当月曾经在网，不考虑其月末是否离网。不包括历史离网用户。
	#2011-06-23 17:00:06 
	#对于02022 ，考察月数据时，只考察月末最后一天。
	#对于02020 ， 已经是月全量。
	set sql_buff "
    select 
        (select count(distinct user_id) cnt from bass1.g_i_02022_day a where time_id = $this_month_last_day )
         - (select count(0) cnt from bass1.g_i_02020_month a where time_id = $op_month and  BASE_PROD_ID like '9999142110%')
         from bass2.dual
	with ur 
	"

	chkzero 	$sql_buff 1


	#1.	检查用户生效日期
	set sql_buff "
    select count(0) from g_i_02020_month where time_id = $op_month 
    and int(prod_valid_date)/100 > $op_month        
	with ur 
	"
	exec_sql $sql_buff

	chkzero2 	$sql_buff  "prod_valid_date invalid! "


	set sql_buff "
select count(*) from 
                    (
                 select distinct  BASE_PROD_ID from bass1.g_i_02020_month
                      where time_id =$op_month
               except
                 select  BASE_PROD_ID from bass1.g_i_02018_month 
                  		where time_id =$op_month            
                    ) as a
                    with ur
               "
	chkzero 	$sql_buff 2

#02020 可以包含离网的       
2012-07-03 由于上月经常发生删除号码的情况，所以多出27个用户。
  #检查在网用户是否在用户表里头
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id from bass1.g_i_02020_month
	              where time_id =$op_month
               except
		 select user_id from bass2.dw_product_$op_month
		 where usertype_id in (1,2,9) 
		   and test_mark<>1               
	            ) as a
	            "

	 puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02020接口用户选择基础资费套餐有用户不在用户表里头"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }



  #检查用户选择的基础套餐是否在套餐表里头
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select base_prod_id from bass1.g_i_02020_month
	              where time_id =$op_month
               except
							 select base_prod_id from bass1.g_i_02018_month 
							  where time_id =$op_month            
	            ) as a
	            "

	 puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02020接口用户选择基础套餐不在套餐信息表里头"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }

#2011-06-27 16:33:11 核查校验：
#  3、月接口的数据大于日接口中的数据（剔除月接口当月离网影响）
	set sql_buff "
    select 
        (select count(0)
	from    bass1.g_i_02022_day
			where time_id = $this_month_last_day  
				)
         - (select count(0)
	         from    bass1.g_i_02020_month 
		where time_id = $op_month
		and base_prod_id like '9999142110%')
         from bass2.dual
	with ur 
	"


	chkzero 	$sql_buff 3


 aidb_runstats bass1.G_I_02020_MONTH 3


	return 0
}

