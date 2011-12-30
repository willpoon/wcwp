######################################################################################################
#接口名称：M2M终端用户
#接口编码：01001
#接口说明：根据使用的专用APN、数据SIM卡或者资费套餐方式，或根据其它方式统计,各省有新增、减少、变更M2M业务时，进行数据上报。
#程序名称: G_A_02051_DAY.tcl
#功能描述: 生成02051的数据
#运行粒度: 日
#源    表：1.bass2.DW_ACCT_SHOULDITEM_yyyymmdd
#          2.bass2.dwd_enterprise_sub_yyyymmdd
#	         3.bass2.dwd_enterprise_msg_yyyymmdd
#	         4.bass2.dw_product_yyyymmdd
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#         20081208 增加全网车务通 夏华学
#  20100120 修改在网用户口径userstatus_id in (1,2,3,6,8)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_a_02051_day where time_id=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
#select a.product_no,c.Enterprise_id,c.Group_name,'4','1','银信通',replace(char(d.create_date),'-',''),'1'
#from bass2.DW_ACCT_SHOULDITEM_200805 a,
#     bass2.dwd_enterprise_sub_20080610 b,
#	 bass2.dwd_enterprise_msg_20080610 c,
#	 bass2.dw_product_20080610 d
#where a.acct_id in ('1001316278') and b.service_id = '944'   and 
#	  a.acct_id = b.Acct_id and b.group_id = c.Enterprise_id and
#	  a.user_id = d.user_id
#with ur;

         
          
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_02051_day
                     select
                        $timestamp
                        ,a.product_no,c.Group_name,c.Enterprise_id,
                        case when b.service_id = '944' then '4'
                             when b.service_id = '945' then '9'
                        end
                        ,'1','无线POS',replace(char(d.create_date),'-',''),
                        case when d.userstatus_id in (1,2,3,6,8) then '1'
                             else '2'
                        end
                      from bass2.DW_ACCT_SHOULDITEM_$timestamp a,
                           bass2.dwd_enterprise_sub_$timestamp b,
	                         bass2.dwd_enterprise_msg_$timestamp c,
	                         bass2.dw_product_$timestamp d
                     where b.service_id in ('944')   and 
	                         a.acct_id = b.Acct_id and b.group_id = c.Enterprise_id and
	                         a.user_id = d.user_id and d.usertype_id in (1,2,9)

                      except
                      select
                        $timestamp
                        ,MSISDN
                        ,ENTERPRISE_NAME
                        ,ENTERPRISE_ID
                        ,CALLING_ID
                        ,GPRS_TYPE
                        ,DATA_SOURCE
                        ,OPENCARD_DATE
                        ,STATE
                      from
                        bass1.g_a_02051_day
                      where
                        time_id<$timestamp 
                       "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle





#气象卡
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_02051_day
                select  $timestamp
                        ,b.product_no,c.Group_name,c.Enterprise_id,
                        '8'
                        ,'1','气象卡',replace(char(b.create_date),'-',''),
                        case when d.userstatus_id in (1,2,3,6,8) then '1'
                             else '2'
                        end
                      from BASS2.DW_ENTERPRISE_EXTSUB_RELA_DS b,
	                         bass2.dwd_enterprise_msg_$timestamp c,
	                         bass2.dw_product_$timestamp d
                     where b.user_id = d.user_id     and
					                 b.service_id in ('947')   and 
	                         b.Enterprise_id = c.Enterprise_id and
	                         d.usertype_id in (1,2,9)          and
	                         b.rec_status=1                    and
						     b.enterprise_id not in ('891880005002','891910006274') 

                      except
                      select
                        $timestamp
                        ,MSISDN
                        ,ENTERPRISE_NAME
                        ,ENTERPRISE_ID
                        ,CALLING_ID
                        ,GPRS_TYPE
                        ,DATA_SOURCE
                        ,OPENCARD_DATE
                        ,STATE
                      from
                        bass1.g_a_02051_day
                      where
                        time_id<$timestamp 
                       "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle






#车载数据卡
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_02051_day
                select  $timestamp
                        ,b.product_no,c.Group_name,c.Enterprise_id,
                        '2'
                        ,'1','车载数据卡',replace(char(b.create_date),'-',''),
                        case when d.userstatus_id in (1,2,3,6,8) then '1'
                             else '2'
                        end
                      from BASS2.DW_ENTERPRISE_EXTSUB_RELA_DS b,
	                         bass2.dwd_enterprise_msg_$timestamp c,
	                         bass2.dw_product_$timestamp d
                     where b.user_id = d.user_id     and
					                 b.service_id in ('946')   and 
	                         b.Enterprise_id = c.Enterprise_id and
	                         d.usertype_id in (1,2,9)          and
	                         b.rec_status=1                    and
						            	 b.enterprise_id not in ('891880005002','891910006274') 

                      except
                      select
                        $timestamp
                        ,MSISDN
                        ,ENTERPRISE_NAME
                        ,ENTERPRISE_ID
                        ,CALLING_ID
                        ,GPRS_TYPE
                        ,DATA_SOURCE
                        ,OPENCARD_DATE
                        ,STATE
                      from
                        bass1.g_a_02051_day
                      where
                        time_id<$timestamp 
                       "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle
	


#全网车务通
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_02051_day
                select  $timestamp
                        ,b.product_no,c.Group_name,c.Enterprise_id,
                        '2'
                        ,'1','全网车务通',replace(char(b.create_date),'-',''),
                        case when d.userstatus_id in (1,2,3,6,8) then '1'
                             else '2'
                        end
                      from BASS2.DW_ENTERPRISE_EXTSUB_RELA_DS b,
	                         bass2.dwd_enterprise_msg_$timestamp c,
	                         bass2.dw_product_$timestamp d
                     where b.user_id = d.user_id     and
					                 b.service_id in ('942')   and 
	                         b.Enterprise_id = c.Enterprise_id and
	                         d.usertype_id in (1,2,9)          and
	                         b.rec_status=1                    and
						     b.enterprise_id not in ('891920005130','891910006274') 

                      except
                      select
                        $timestamp
                        ,MSISDN
                        ,ENTERPRISE_NAME
                        ,ENTERPRISE_ID
                        ,CALLING_ID
                        ,GPRS_TYPE
                        ,DATA_SOURCE
                        ,OPENCARD_DATE
                        ,STATE
                      from
                        bass1.g_a_02051_day
                      where
                        time_id<$timestamp 
                       "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle
	



#电力数据卡
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_02051_day
                select  $timestamp
                        ,b.product_no,c.Group_name,c.Enterprise_id,
                        '1'
                        ,'1','电力数据卡',replace(char(b.create_date),'-',''),
                        case when d.userstatus_id in (1,2,3,6,8) then '1'
                             else '2'
                        end
                      from BASS2.DW_ENTERPRISE_EXTSUB_RELA_DS b,
	                         bass2.dwd_enterprise_msg_$timestamp c,
	                         bass2.dw_product_$timestamp d
                     where b.user_id = d.user_id     and
					                 b.service_id in ('949')   and 
	                         b.Enterprise_id = c.Enterprise_id and
	                         d.usertype_id in (1,2,9)          and
	                         b.rec_status=1                    and
						            	 b.enterprise_id not in ('891880005002','891910006274') 

                      except
                      select
                        $timestamp
                        ,MSISDN
                        ,ENTERPRISE_NAME
                        ,ENTERPRISE_ID
                        ,CALLING_ID
                        ,GPRS_TYPE
                        ,DATA_SOURCE
                        ,OPENCARD_DATE
                        ,STATE
                      from
                        bass1.g_a_02051_day
                      where
                        time_id<$timestamp 
                       "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle
	
	
		
#  set handle [aidb_open $conn]
#  set sql_buff "insert into bass1.g_a_02051_day
#                     select
#                        $timestamp
#                        ,d.product_no,c.Group_name,c.Enterprise_id
#                        ,'9'
#                        ,'1','无线POS',replace(char(a.ent_create_date),'-',''),
#                        case when a.rec_status = 1 then '1'
#                             else '2'
#                        end
#                      from (select enterprise_id,user_id,ent_create_date,max(rec_status)  as rec_status
#                              from (select rec_status,user_id,enterprise_id,ent_create_date,max(create_date)  
#                                      from bass2.dw_enterprise_extsub_rela_ds 
#                                     where service_id='945' and enterprise_id not in ('891880005002') 
#                                     group by rec_status,user_id,enterprise_id,ent_create_date
#                                     order by user_id ) t
#                              group by user_id,enterprise_id,ent_create_date )a,
#	                         bass2.dwd_enterprise_msg_$timestamp c,
#	                         bass2.dw_product_$timestamp d
#                     where a.Enterprise_id = c.Enterprise_id and
#	                         a.user_id = d.user_id and d.usertype_id in (1,2,9)
#                      except
#                      select
#                        $timestamp
#                        ,MSISDN
#                        ,ENTERPRISE_NAME
#                        ,ENTERPRISE_ID
#                        ,CALLING_ID
#                        ,GPRS_TYPE
#                        ,DATA_SOURCE
#                        ,OPENCARD_DATE
#                        ,STATE
#                      from
#                        bass1.g_a_02051_day
#                      where
#                        time_id<$timestamp 
#                       "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}      
#	aidb_commit $conn
#	aidb_close $handle

	return 0
}