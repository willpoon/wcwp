######################################################################################################
#接口名称：用户选择叠加资费套餐
#接口编码：02021
#接口单元说明：用户选择的叠加套餐/可选包，上报符合以下条件的套餐订购记录：
#1.	用户当月曾经在网，不考虑其月末是否离网。不包括历史离网用户。
#2.	每个用户可以选择叠加套餐，也可以不选择。可以选择一款叠加套餐，也可以同时选择几款叠加套餐。当月该用户的相关通信行为曾经依据该档套餐进行计费处理。
#3.	套餐生效日期，指用户通信行为开始依据该档叠加套餐进行计费的起始日期。生效日期中的月份应该早于或等于当月 。
#程序名称: G_I_02021_MONTH.tcl
#功能描述: 生成02021的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2011-02-21
#问题记录：1.
#修改历史: 1. 1.7.1 规范
#						2011-05-03 16:32:56 优化代码，使用子查询。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


      #本月 yyyymm
      #set op_time 2011-01-01
      #set optime_month 2011-01
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      #set op_month 201102
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      #本月最后一天 yyyymmdd
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
      #set this_month_last_day 20110231
      #本月的第一天 yyyymmdd
      set one  "01"
      set this_month_one_day ${op_month}${one}
      
      puts $op_time
      puts $op_month
      puts $this_month_last_day
      puts $this_month_one_day
		global app_name
		set app_name "G_I_02021_MONTH.tcl"        

  #删除本期数据
	set sql_buff "delete from bass1.g_i_02021_month where time_id=$op_month"
	exec_sql $sql_buff

  #清空临时表
	set sql_buff "alter table bass1.g_i_02021_month_temp1 activate not logged initially with empty table"
	exec_sql $sql_buff


  #第一步，抓取在网用户
	set sql_buff "
	insert into bass1.g_i_02021_month_temp1
		  (
       user_id
		  )
   select product_instance_id user_id from bass2.dw_product_ins_prod_$op_month
    where state in ('1','4','6','8','M','7','C','9')
      and user_type_id =1
      and valid_type = 1
      and bill_id not in ('D15289014474','D15289014454')
    except
    select user_id from bass2.dw_product_test_phone_$op_month
    where sts=1
    with ur
   "

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02021_month_temp1 3
	

  #清空临时表
	set sql_buff "alter table bass1.g_i_02021_month_temp2 activate not logged initially with empty table"
	exec_sql $sql_buff


  #第二步：提取所有用户订购了叠加套餐的信息
	set sql_buff "
	insert into bass1.g_i_02021_month_temp2
		  (
       user_id
      ,offer_id
      ,create_date
		  )
		select 
		     a.product_instance_id,
		     a.offer_id,
		     replace(char(date(min(a.create_date))),'-','')
		from (select a.offer_id,a.product_instance_id,a.create_date 
					from  bass2.dw_product_ins_off_ins_prod_$op_month a
					where 
					replace(char(date(a.create_date)),'-','')<='$this_month_last_day'
		  		and replace(char(date(a.expire_date)),'-','')>='$this_month_one_day'
		  		and a.state=1
					and a.valid_type = 1
					) a,
		     bass1.g_i_02019_month_4 b 
		where 
				a.offer_id=b.base_prod_id 
	group by a.product_instance_id,a.offer_id
	with ur
   "

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02021_month_temp2 3


#有别于02020 , 02021user_id不唯一
  #插入目标表
	set sql_buff "
	insert into bass1.g_i_02021_month
		  (
		   time_id
	    ,user_id
			,over_prod_id
			,prod_valid_date
		  )
		select 
		     $op_month,
		     a.user_id,
		     value(value(e.new_pkg_id,c.ADD_PKG_ID),char(a.offer_id)),
		     value(c.VALID_DT,a.create_date) VALID_DT
		from bass1.g_i_02021_month_temp2 a
		inner join bass1.g_i_02021_month_temp1 b	on  a.user_id=b.user_id
		left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
				from  BASS1.ALL_DIM_LKP 
				where BASS1_TBID = 'BASS_STD1_0115'
				and bass1_value like 'QW_QQT_DJ%'
				) d on char(a.offer_id) = d.offer_id
		left join  (select user_id , ADD_PKG_ID,VALID_DT
				from
				(select a.*,row_number()over(partition by user_id,ADD_PKG_ID order by VALID_DT desc ) rn
				from bass1.g_i_02023_day  a
				where time_id  = $this_month_last_day
				) t where  t.rn = 1 
				) c on  a.user_id=c.user_id and d.bass1_offer_id = c.ADD_PKG_ID
		left join bass1.DIM_QW_QQT_PKGID e on  d.ADD_PKG_ID = e.old_pkg_id		
with ur
"

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02021_month 3

  #进行结果数据检查
  
  	#检查  02023和02021结果表中的user_id一致性。
	set sql_buff "
    select 
        (select  count(distinct user_id||ADD_PKG_ID) cnt from bass1.g_i_02023_day a where time_id = $this_month_last_day )                     
         - (select  count(distinct user_id||OVER_PROD_ID) cnt from bass1.g_i_02021_month a where time_id = $op_month and  OVER_PROD_ID like '99991222%')
         from bass2.dual
	with ur 
	"
	exec_sql $sql_buff

	chkzero 	$sql_buff 1
  
  #检查其用户是否在用户表里头
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id from bass1.g_i_02021_month
	              where time_id =$op_month
               except
			 select user_id from bass2.dw_product_$op_month
			 where usertype_id in (1,2,9) 
			   and userstatus_id in (1,2,3,6,8)
			   and test_mark<>1               
	            ) as a
	            with ur
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
	        set alarmcontent "02021月接口用户选择叠加资费套餐有用户不在用户表里头"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }



  #检查用户选择叠加资费套餐是否在套餐表里头
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select over_prod_id from bass1.g_i_02021_month
	              where time_id =$op_month
               except
							 select over_prod_id from bass1.g_i_02019_month 
							  where time_id =$op_month            
	            ) as a
	            with ur
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
	        set alarmcontent "02021月接口用户选择叠加资费套餐不在套餐信息表里头"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


#进行主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id,over_prod_id,count(*) cnt from bass1.g_i_02021_month
	              where time_id =$op_month
	             group by user_id,over_prod_id
	             having count(*)>1
	            ) as a
	            with ur
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
	        set alarmcontent "02021月接口用户选择叠加资费套餐主键唯一性校验未通过"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }





	return 0
}

