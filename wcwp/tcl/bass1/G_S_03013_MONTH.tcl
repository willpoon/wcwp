######################################################################################################
#接口名称：集团客户统付收入
#接口编码：03013
#接口说明：记录由集团客户统一付费的各项收入，即该帐务周期内集团客户账单收入与一次性费项收入之和，
#          需要各省级经分系统将二者进行合并后上传。
#程序名称: G_S_03013_MONTH.tcl
#功能描述: 生成03013的数据
#运行粒度: 月
#源    表：
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.vpmn收入跟统付收入有重复 2.过滤了一些不合理数据 standard_product <> '0000' and enterprise_id is not null and  enterprise_id <> ''
#修改历史: 1.把原来 standard_product='0000' 改为 standard_product='3001'     zhanght 20090610
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        set ThisMonthFirstDay [string range $optime_month 0 6][string range $optime_month 4 4]01
        puts $ThisMonthFirstDay

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set objecttable "G_S_03013_MONTH"
        #set db_user $env(DB_USER)
       
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

        #建立G_S_03013_MONTH的session表，表名session.tmp03013
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.tmp03013
	        (
	          enterprise_id          char(20)   not null , 
                  standard_product       char(4)    not null , 
                  standard_project       char(3)    not null , 
                  account_item           varchar(2) not null,
                  income                 bigint 
                )
               partitioning key (ENTERPRISE_ID) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
         puts $sql_buff                       
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "建立G_S_03013_MONTH的session表，表名session.tmp03013"	

	#普通VPMN当月统一付费收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                  enterprise_id, 
                  standard_product, 
                  standard_project,
                  account_item,
                  income
                  from
                  (
                       select
                          c.enterprise_id
                          ,'1000' as standard_product
                          ,'999'  as standard_project
                          ,'02' as account_item
                          , sum(a.fact_fee)*100 as income
                       from 
                          bass2.dw_acct_shoulditem_${op_month} a,
                          bass2.dw_enterprise_account_${op_month} b,
                          bass2.dw_enterprise_member_mid_${op_month} c
                       where 
                          c.free_mark =0
                          and c.test_mark = 0
                          and a.user_id = c.user_id
                          and a.acct_id = b.acct_id
                          and b.rec_status = 1
                          and a.item_id not in (80000027,80000032,80000033,80000101)
                       group by 
                          c.enterprise_id
                   )aa"        
        puts $sql_buff                                           
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "普通VPMN当月统一付费收入"	
#c.vpmn_mark = 1
#                          and 
#取消vpmn口径                          
                          
                          	
	#一次性服务收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                    enterprise_id, 
                    standard_product, 
                    '999',
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id
                     ,'3000' as standard_product
                     ,'09' as account_item
                     ,sum(b.pay_fee)*100 as income
                   from
                     bass2.dw_enterprise_msg_${op_month} a, 
                     bass2.dw_enterprise_oneoff_pay_${op_month} b
                   where 
                     a.enterprise_id = b.enterprise_id
                   group by 
                     a.enterprise_id
                  )t"        
        puts $sql_buff                                          
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "一次性服务收入"	
	
	
	#集团统付收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                    enterprise_id, 
                    ent_prod_id, 
                    '999',
                    account_item,
                    income
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'3001') as ent_prod_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from 
                     bass2.dw_enterprise_unipay_${op_month} a
                   where 
                     a.test_mark = 0 and service_id not in ('936')
                   group by 
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'3001')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') 
                  )t"              
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "集团统付收入"	
	
	
#商信通单独统计
	#集团统付收入
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                  select
                    case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end, 
                    '2033', 
                    '999',
                    '06',
                    sum(a.unipay_fee*100) as fact_fee
            from bass2.DW_ENTERPRISE_UNIPAY_$op_month a
							   left outer join 
                 bass2.DW_ENTERPRISE_MSG_$op_month b on a.ENTERPRISE_ID=b.ENTERPRISE_ID
							   left join 
							   bass2.dw_enterprise_account_his_$op_month c on a.acct_id=c.acct_id 
           where a.enterprise_id not in ('891910006274','891950005002') and 
                 a.service_id in ('936') and a.TEST_MARK = 0 and 
                 case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end is not null
        group by case when b.enterprise_id is null then c.enterprise_id else b.enterprise_id end; "              
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "集团统付收入"	


#警务通统付收入
	
	set handle [aidb_open $conn]
	set sql_buff "insert into session.tmp03013
                    select
                    c.enterprise_id, 
                    '3001', 
                    '100',
                    '01',
                    sum(bigint(b.fee*100)) as income
               from bass2.dw_enterprise_industry_apply b left outer join
			              (select distinct enterprise_id,user_id from bass2.dw_enterprise_membersub_$op_month ) c
			              on b.user_id = c.user_id
              where  b.op_time = '$ThisMonthFirstDay' and b.apptype_id in (5) and b.user_id is not null and b.enterprise_id is not null 
               group by c.enterprise_id "
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "集团统付收入"	



                      
                          	

#    set sal_buf00 "insert into session.stat_enterprise_0028_tmp(city_id,s_index_id,result)
#                   select case
#                                       when b.level_def_mode = 1 then 888
#                                       else value(int(b.ent_city_id),891)
#                                  end as city_id,
#                          case a.apptype_id
#                              when 1 then 'H041'
#                              when 2 then 'H056'
#                              when 3 then 'H081'
#                              when 4 then 'H011'
#                              when 5 then 'H026'
#                          end,
#                          sum(a.fee)
#                   from $dw_enterprise_industry_apply a left outer join DW_ENTERPRISE_MEMBER_MID_${year}${month} b
#                   on  a.user_id=b.user_id
#                   where a.op_time = '$year-$month-$day'
#                     and a.apptype_id in (1,2,3,4,5)
#                   group by case
#                                       when b.level_def_mode = 1 then 888
#                                       else value(int(b.ent_city_id),891)
#                                  end,
#                          case a.apptype_id
#                              when 1 then 'H041'
#                              when 2 then 'H056'
#                              when 3 then 'H081'
#                              when 4 then 'H011'
#                              when 5 then 'H026'
#                          end"
#
#    puts $sal_buf00
#    if [catch {aidb_sql $handle $sal_buf00} errmsg] {
#      trace_sql $errmsg 1300
#      puts "errmsg:$errmsg"
#      return -1
#    }
#    aidb_close $handle
#    if [catch {set handle [aidb_open $conn]} errmsg] {
#        trace_sql $errmsg 1302
#        return -1
#      }
#    aidb_commit $conn
#    
#    
#    	
##集团行业应用解决方案包
##集团行业应用
##H041	校讯通	校讯通当月总收入            	  元	
##H056	银信通	本地银信通银信通当月总收入	    元	
##H081	农信通	农信通当月总收入	              元	
##H011 财信通	财信通当月总收入    	          元
##H026 警务通	警务通当月总收入	              元	
#100-警务通
#110-校信通
#120-银信通
#130-气象通
#140-农信通
#150-城管通
#160-商贸通
#170-医疗通
#180-物流通
#190-电力通
#200-安防通
#210-财信通
#
#
#	set handle [aidb_open $conn]
#	set sql_buff "insert into session.tmp03013
#                  select
#                    enterprise_id, 
#                    ent_prod_id, 
#                    '999',
#                    account_item,
#                    income
#                  from
#                  (
#                   select
#                     a.enterprise_id as enterprise_id
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'0000') as ent_prod_id
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') as account_item
#                     ,sum(a.unipay_fee)*100 as income
#                   from 
#                     bass2.dw_enterprise_unipay_${op_month} a
#                   where 
#                     a.test_mark = 0
#                   group by 
#                     a.enterprise_id
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0103',char(service_id)),'0000')
#                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0104',char(service_id)),'10') 
#                  )t"              
#        puts $sql_buff                                    
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	puts "集团统付收入"	




	#============向正式表中插入数据============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.$objecttable 
                  select
                    ${op_month},
                    '${op_month}',
                    enterprise_id,
                    standard_product, 
                    standard_project, 
                    account_item,
                    char(income      )
                  from
                  (
                   select
                     enterprise_id ,
                     standard_product, 
                     standard_project, 
                     account_item,
                     sum(income) as income              
                  from 
                      session.tmp03013
                  where enterprise_id is not null and  enterprise_id <> '' 
                  group by
                      enterprise_id , 
                      standard_product,
                      standard_project,
                      account_item
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
	
	aidb_close $handle

	return 0
}	