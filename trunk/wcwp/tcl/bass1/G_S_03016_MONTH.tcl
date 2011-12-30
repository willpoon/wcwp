######################################################################################################
#接口名称：ADC业务收入情况
#接口编码：03016
#接口说明：根据《配合集团客户KPI指标统计业务支撑系统改造通知》，该接口上传MAS业务收入信息。
#程序名称: G_S_03016_MONTH.tcl
#功能描述: 生成03016的数据
#运行粒度: 月
#源    表：1.bass2.dw_enterprise_sub_YYYYMM
#          2.BASS2.DIM_BS_ENTERPRISE_CUSTOMER_CODE
#          3.bass2.dw_enterprise_unipay_YYYYMM
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：夏华学
#编写时间：2008-01-08
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $ThisMonthFirstDay 

        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #删除本期数据
	set sql_buff "delete from BASS1.G_S_03016_MONTH where time_id=$op_month"
  puts $sql_buff
  exec_sql $sql_buff
  
     
  #01:创建临时表1(装载集团业务代码也服务代码)
	set sql_buff "declare global temporary table session.g_a_03016_day_tmp1
	              (
                   GROUP_ID   CHARACTER(20),
                   SERV_CODE  CHARACTER(21),
                   BUSI_CODE  CHARACTER(10),
                   Flag       CHARACTER(4)
	              )
	              partitioning key
	              (GROUP_ID)
	              using hashing	              
	              with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
  exec_sql $sql_buff

  #插入企信通服务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         case when b.feature_id = '804201009' then b.feature_value end,
                         '',
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
					               (
						             select a.expire_date,a.order_id as order_id,a.group_id as group_id,a.feature_id as feature_id,a.feature_value feature_value
						              from (select expire_date,order_id,group_id,feature_id,feature_value from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day) a,
						                   (select group_id,feature_id,max(expire_date) as expire_date from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day 
						                     group by group_id,feature_id) b
						             where a.group_id = b.group_id and a.feature_id = b.feature_id and a.expire_date = b.expire_date 
						              ) b
	                 where a.service_id='910' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                   and b.feature_id in ('804201009') ;  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入企信通业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         '',
                         case when b.feature_id = '804201008' then b.feature_value end,
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
                         bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day b
	                 where a.service_id='910' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                     and b.feature_id in ('804201008');  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入企信通业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select a.GROUP_ID,
                         a.SERV_CODE,
                         b.BUSI_CODE,
                         '910'
                    from session.g_a_03016_day_tmp1 a,
                         session.g_a_03016_day_tmp1 b
	                 where a.GROUP_ID = b.GROUP_ID and a.flag = '0' and b.flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  
	set sql_buff   "delete from  session.g_a_03016_day_tmp1
                  where Flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  






  #插入商信通服务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         case when b.feature_id like '10070%03' then b.feature_value end,
                         '',
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
					               (
						             select a.expire_date,a.order_id as order_id,a.group_id as group_id,a.feature_id as feature_id,a.feature_value feature_value
						              from (select expire_date,order_id,group_id,feature_id,feature_value from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day) a,
						                   (select group_id,feature_id,max(expire_date) as expire_date from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day 
						                     group by group_id,feature_id) b
						             where a.group_id = b.group_id and a.feature_id = b.feature_id and a.expire_date = b.expire_date 
						              ) b
	                 where a.service_id='936' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                   and b.feature_id  like '10070%03' ;  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入商信通业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         '',
                         case when b.feature_id like '10070%02' then b.feature_value end,
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
                         bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day b
	                 where a.service_id='936' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                     and b.feature_id  like '10070%02';  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入商信通业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select a.GROUP_ID,
                         a.SERV_CODE,
                         b.BUSI_CODE,
                         '936'
                    from session.g_a_03016_day_tmp1 a,
                         session.g_a_03016_day_tmp1 b
	                 where a.GROUP_ID = b.GROUP_ID and a.flag = '0' and b.flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  
	set sql_buff   "delete from  session.g_a_03016_day_tmp1
                  where Flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  
	





  #插入移动OA（ADC）服务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         case when b.feature_id = '90300008' then b.feature_value end,
                         '',
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
					               (
						             select a.expire_date,a.order_id as order_id,a.group_id as group_id,a.feature_id as feature_id,a.feature_value feature_value
						              from (select expire_date,order_id,group_id,feature_id,feature_value from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day) a,
						                   (select group_id,feature_id,max(expire_date) as expire_date from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day 
						                     group by group_id,feature_id) b
						             where a.group_id = b.group_id and a.feature_id = b.feature_id and a.expire_date = b.expire_date 
						              ) b
	                 where a.service_id='903' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                   and b.feature_id in ('90300008') ;  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动OA（ADC）业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         '',
                         case when b.feature_id = '90300006' then b.feature_value end,
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
                         bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day b
	                 where a.service_id='903'  and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                     and b.feature_id in ('90300006');  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动OA（ADC）业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select a.GROUP_ID,
                         a.SERV_CODE,
                         b.BUSI_CODE,
                         '903'
                    from session.g_a_03016_day_tmp1 a,
                         session.g_a_03016_day_tmp1 b
	                 where a.GROUP_ID = b.GROUP_ID and a.flag = '0' and b.flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动OA（ADC）业务代码
	set sql_buff   "delete from  session.g_a_03016_day_tmp1
                  where Flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff




  #插入移动进销存（ADC））服务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         case when b.feature_id = '90400008' then b.feature_value end,
                         '',
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
					               (
						             select a.expire_date,a.order_id as order_id,a.group_id as group_id,a.feature_id as feature_id,a.feature_value feature_value
						              from (select expire_date,order_id,group_id,feature_id,feature_value from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day) a,
						                   (select group_id,feature_id,max(expire_date) as expire_date from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day 
						                     group by group_id,feature_id) b
						             where a.group_id = b.group_id and a.feature_id = b.feature_id and a.expire_date = b.expire_date 
						              ) b
	                 where a.service_id='904' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                   and b.feature_id in ('90400008') ;  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动进销存（ADC）业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         '',
                         case when b.feature_id = '90400006' then b.feature_value end,
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
                         bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day b
	                 where a.service_id='904'  and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                     and b.feature_id in ('90400006');  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动进销存（ADC）业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select a.GROUP_ID,
                         a.SERV_CODE,
                         b.BUSI_CODE,
                         '904'
                    from session.g_a_03016_day_tmp1 a,
                         session.g_a_03016_day_tmp1 b
	                 where a.GROUP_ID = b.GROUP_ID and a.flag = '0' and b.flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  
	set sql_buff   "delete from  session.g_a_03016_day_tmp1
                  where Flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff





  #插入移动CRM（ADC）服务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         case when b.feature_id = '90600008' then b.feature_value end,
                         '',
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
					               (
						             select a.expire_date,a.order_id as order_id,a.group_id as group_id,a.feature_id as feature_id,a.feature_value feature_value
						              from (select expire_date,order_id,group_id,feature_id,feature_value from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day) a,
						                   (select group_id,feature_id,max(expire_date) as expire_date from bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day 
						                     group by group_id,feature_id) b
						             where a.group_id = b.group_id and a.feature_id = b.feature_id and a.expire_date = b.expire_date 
						              ) b
	                 where a.service_id='906' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                   and b.feature_id in ('90600008') ;  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动CRM（ADC）业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select distinct a.enterprise_id,
                         '',
                         case when b.feature_id = '90600006' then b.feature_value end,
                         '0'
                    from bass2.dw_enterprise_sub_$op_month a,
                         bass2.DWd_GROUP_ORDER_FEATUR_$this_month_last_day b
	                 where a.service_id='906' and a.expire_date >= date('$ThisMonthFirstDay') and a.order_id = b.order_id 
		                     and b.feature_id in ('90600006');  "                
	puts $sql_buff
  exec_sql $sql_buff
  
  #插入移动CRM（ADC）业务代码
	set sql_buff   "insert into session.g_a_03016_day_tmp1
                  select a.GROUP_ID,
                         a.SERV_CODE,
                         b.BUSI_CODE,
                         '906'
                    from session.g_a_03016_day_tmp1 a,
                         session.g_a_03016_day_tmp1 b
	                 where a.GROUP_ID = b.GROUP_ID and a.flag = '0' and b.flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff
  
	set sql_buff   "delete from  session.g_a_03016_day_tmp1
                  where Flag = '0'  "                
	puts $sql_buff
  exec_sql $sql_buff



	set sql_buff   "select count(*) from  session.g_a_03016_day_tmp1 where flag = '903' and group_id in ('891896000552','891891000056')
	                       and rtrim(SERV_CODE) <> '' and rtrim(BUSI_CODE) <> ''  "                
	puts $sql_buff
	set count903 [get_single $sql_buff]
  puts $count903
  
  


	#企信通
	set sql_buff   "insert into bass1.G_S_03016_MONTH
                  select $op_month,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
                         char(bigint((sum(a.unipay_fee))*100)) as fee,'0' 
                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
                         session.g_a_03016_day_tmp1 b
	                    on a.enterprise_id = b.GROUP_ID
                   where a.service_id in ('910') and b.flag = '910' and 
                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
                group by a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
	puts $sql_buff
  exec_sql $sql_buff
  
  
  #('910','903','904','906','936')
  #商信通
	set sql_buff   "insert into bass1.G_S_03016_MONTH
                  select $op_month,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
                         char(bigint((sum(a.unipay_fee+a.non_unipay_fee))*100)) as fee,'0' 
                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
                         session.g_a_03016_day_tmp1 b
	                    on a.enterprise_id = b.GROUP_ID
                   where a.service_id in ('936') and b.flag = '936' and 
                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
                group by a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
	puts $sql_buff
  exec_sql $sql_buff

	
  #移动OA（ADC）
	set sql_buff   "insert into bass1.G_S_03016_MONTH
                  select $op_month,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
                         char(bigint((sum(a.unipay_fee+a.non_unipay_fee))*100)) as fee,'0' 
                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
                         session.g_a_03016_day_tmp1 b
	                    on a.enterprise_id = b.GROUP_ID
                   where a.service_id in ('903') and b.flag = '903' and 
                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
                group by a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
	puts $sql_buff
  exec_sql $sql_buff


  #移动进销存（ADC）
	set sql_buff   "insert into bass1.G_S_03016_MONTH
                  select $op_month,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
                         char(bigint((sum(a.unipay_fee+a.non_unipay_fee))*100)) as fee,'0' 
                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
                         session.g_a_03016_day_tmp1 b
	                    on a.enterprise_id = b.GROUP_ID
                   where a.service_id in ('904') and b.flag = '904' and 
                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
                group by a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
	puts $sql_buff
  exec_sql $sql_buff


  #移动CRM（ADC）
	set sql_buff   "insert into bass1.G_S_03016_MONTH
                  select $op_month,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
                         char(bigint((sum(a.unipay_fee+a.non_unipay_fee))*100)) as fee,'0' 
                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
                         session.g_a_03016_day_tmp1 b
	                    on a.enterprise_id = b.GROUP_ID
                   where a.service_id in ('906') and b.flag = '906' and 
                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
                group by a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
	puts $sql_buff
  exec_sql $sql_buff
  
  
  set sql_buff "delete from  bass1.G_S_03016_MONTH where  time_id = 888888;"	
	puts $sql_buff
  exec_sql $sql_buff		
  
  set sql_buff "update bass1.G_S_03016_MONTH set time_id = 888888 where time_id = $op_month;"
	puts $sql_buff
  exec_sql $sql_buff		

  set sql_buff "insert into bass1.G_S_03016_MONTH
                           select
                             $op_month
                             ,GROUP_ID
                             ,EC_ID
                             ,SERV_CODE
                             ,BUSI_CODE
                             ,char(sum(bigint(FEE)))
                             ,char(sum(bigint(FEE_FAV)))
                           from 
                             G_S_03016_MONTH
                           where time_id = 888888
                           group by 
                             GROUP_ID
                             ,EC_ID
                             ,SERV_CODE
                             ,BUSI_CODE;"
	puts $sql_buff
  exec_sql $sql_buff		
							
  set sql_buff "delete from  bass1.G_S_03016_MONTH where  time_id = 888888;"	
	puts $sql_buff
  exec_sql $sql_buff		















#	set sql_buff   "delete from bass1.G_S_03016_MONTH_LS where time_id = $op_month;"
#	puts $sql_buff
#  exec_sql $sql_buff
#
#
#
#
#	#企信通
#	set sql_buff   "insert into bass1.G_S_03016_MONTH_LS
#                  select $op_month,b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
#                         char(bigint((sum(a.unipay_fee))*100)) as fee,'0' 
#                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
#                         session.g_a_03016_day_tmp1 b
#	                    on a.enterprise_id = b.GROUP_ID
#                   where a.service_id in ('910') and b.flag = '910' and 
#                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
#                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
#                group by b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
#	puts $sql_buff
#  exec_sql $sql_buff
#  
#  
#  #('910','903','904','906','936')
#  #商信通
#	set sql_buff   "insert into bass1.G_S_03016_MONTH_LS
#                  select $op_month,b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
#                         char(bigint((sum(a.unipay_fee))*100)) as fee,'0' 
#                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
#                         session.g_a_03016_day_tmp1 b
#	                    on a.enterprise_id = b.GROUP_ID
#                   where a.service_id in ('936') and b.flag = '936' and 
#                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
#                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
#                group by b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
#	puts $sql_buff
#  exec_sql $sql_buff
#
#	
#  #移动OA（ADC）
#	set sql_buff   "insert into bass1.G_S_03016_MONTH_LS
#                  select $op_month,b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
#                         char(bigint((sum(a.unipay_fee))*100)) as fee,'0' 
#                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
#                         session.g_a_03016_day_tmp1 b
#	                    on a.enterprise_id = b.GROUP_ID
#                   where a.service_id in ('903') and b.flag = '903' and 
#                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
#                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
#                group by b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
#	puts $sql_buff
#  exec_sql $sql_buff
#
#
#  #移动进销存（ADC）
#	set sql_buff   "insert into bass1.G_S_03016_MONTH_LS
#                  select $op_month,b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
#                         char(bigint((sum(a.unipay_fee))*100)) as fee,'0' 
#                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
#                         session.g_a_03016_day_tmp1 b
#	                    on a.enterprise_id = b.GROUP_ID
#                   where a.service_id in ('904') and b.flag = '904' and 
#                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
#                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
#                group by b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
#	puts $sql_buff
#  exec_sql $sql_buff
#
#
#  #移动CRM（ADC）
#	set sql_buff   "insert into bass1.G_S_03016_MONTH_LS
#                  select $op_month,b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE,
#                         char(bigint((sum(a.unipay_fee))*100)) as fee,'0' 
#                   from  bass2.dw_enterprise_unipay_$op_month a left outer join 
#                         session.g_a_03016_day_tmp1 b
#	                    on a.enterprise_id = b.GROUP_ID
#                   where a.service_id in ('906') and b.flag = '906' and 
#                         a.enterprise_id not in ('891910006274') and a.enterprise_id is not null and
#                         a.enterprise_id <> '' and rtrim(b.SERV_CODE) <> '' and rtrim(b.BUSI_CODE) <> ''
#                group by b.flag,a.enterprise_id,a.enterprise_id,b.SERV_CODE,b.BUSI_CODE;"  
#	puts $sql_buff
#  exec_sql $sql_buff





 
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


