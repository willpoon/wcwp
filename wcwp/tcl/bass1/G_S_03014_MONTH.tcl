######################################################################################################
#接口名称：集团客户非统付收入
#接口编码：03014
#接口说明：记录由集团客户统一付费的各项收入，即该帐务周期内集团客户账单收入与一次性费项收入之和，
#          需要各省级经分系统将二者进行合并后上传。
#程序名称: G_S_03014_MONTH.tcl
#功能描述: 生成03014的数据
#运行粒度: 月
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        puts $timestamp
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set objecttable "G_S_03014_MONTH"
        #set db_user $env(DB_USER)

        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#01	用户标识	参见接口单元【用户】用户标识。	char(20)
#02	集团个人非非统付产品分类编码	参见维度指标说明中的BASS_STD1_0105。	char(4)
#03	个人非非统付帐目科目编码		char(1)
#04	应收金额	优惠后的应收金额，单位:分 	number(12)
#05	优惠金额	所有的金额填负值, 单位:分	number(12)


        #建立G_S_03014_MONTH的session表，表名session.tmp03014
	set sql_buff "declare global temporary table session.tmp03014
	        (
	                user_id                char(20)   not null ,
                  standard_product       char(4)    not null ,
                  acct_type              char(1)    not null ,
                  income                 bigint,
                  fav_income             bigint
                )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
  puts $sql_buff
  exec_sql $sql_buff
	puts "建立G_S_03014_MONTH的session表，表名session.tmp03014"






  #生成集团产品非统付收入
	set sql_buff "insert into session.tmp03014
               	select  b.user_id as user_id
                       ,service_id 
                       ,'1'
               	       ,sum(bigint(non_unipay_fee))  as non_unipay_fee
               	       ,0
                  from bass2.dw_enterprise_unipay_$op_month a,
                       (select acct_id,min(user_id) as user_id from  bass2.dw_product_$op_month group by acct_id) b 
                 where a.acct_id = b.acct_id and a.test_mark = 0 
                 group by b.user_id,service_id"
  puts $sql_buff
  exec_sql $sql_buff
	puts "生成商信通非统付费用(个人)"

  
 #生成财信通收入
 set sql_buff "insert into session.tmp03014
               	select  a.user_id as user_id
                       ,'0450' 
                       ,'1'
               	       ,sum(fee)
               	       ,0
                  from bass2.dw_enterprise_industry_apply  a
                 where a.apptype_id = 4 and a.op_time = '$this_month_firstday' 
                 group by a.user_id;"

  puts $sql_buff
  exec_sql $sql_buff




	#============向正式表中插入数据============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_03014_MONTH
                  select
                    ${op_month},
                    aa.user_id,
                    aa.ent_prod_id,
                    aa.acct_type,
                    char(aa.income),
                    char(aa.fav_income)
                  from
                  (
                   select
                     user_id as user_id ,
                     coalesce(bass1.fn_get_all_dim('BASS_STD1_0105',char(standard_product)),'0000') as ent_prod_id,
                     acct_type as acct_type,
                     sum(income*100) as income,
                     sum(fav_income*100) as fav_income
                  from
                      session.tmp03014
                  where coalesce(bass1.fn_get_all_dim('BASS_STD1_0105',char(standard_product)),'0000')  <> '0000'
                  group by
                      user_id ,
                      standard_product,
                      acct_type
                )aa"

          puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "向正式表中插入数据"
	
	
	
	
	
#H041	校讯通	校讯通当月总收入            	  元	
#H056	银信通	本地银信通银信通当月总收入	    元	
#H081	农信通	农信通当月总收入	              元	
#H011 财信通	财信通当月总收入    	          元
#财信通(2034)、校讯通(2035)、银信通(2037)、农信通(2036) 警务通目前放入行业应用卡里面
    set sql_buff "insert into bass1.G_S_03014_MONTH
                   select
                    ${op_month},
                    b.user_id, 
                    case b.apptype_id
                              when 1 then '0120'
                              when 2 then '0130'
                              when 3 then '0140'
                              when 4 then '0450'   
                          end, 
                    '1' ,
                    char(sum(bigint(b.fee*100))) as income,
                    '0'
              from bass2.dw_enterprise_industry_apply b 
             where b.op_time = '$this_month_firstday' and b.apptype_id in (1,2,3,4) and b.user_id is not null 
             group by b.user_id,
                    case b.apptype_id
                              when 1 then '0120'
                              when 2 then '0130'
                              when 3 then '0140'
                              when 4 then '0450'
                          end" 
                 
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "行业应用总收入 插入完毕"
	
##财信通单独临时统计	
#    set sql_buff "insert into bass1.G_S_03014_MONTH
#                   select
#                    ${op_month},
#                    a.user_id, 
#                    '0450', 
#                    '1' ,
#                    char(sum(bigint(fee*100))) as income,
#                    '0'
#              from (select a.user_id as user_id ,a.city_id,a.product_no,4,
#		                       sum(coalesce(b.fee,0)) as fee
#		                  from bass2.dw_product_$op_month a,
#		                       (select user_id,sum(base_fee+info_fee+month_fee+func_fee) as fee
#                                                    from bass2.dw_newbusi_ismg_$op_month
#                                                    where sp_code in ('901848','400002','900139')
#                                                    group by user_id) b
#                     where a.user_id=b.user_id
#               group by a.user_id,a.city_id,a.product_no) a
#			   group by a.user_id" 
#                 
#        puts $sql_buff                                    
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	puts "行业应用总收入 插入完毕"
	
	
	aidb_close $handle

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
