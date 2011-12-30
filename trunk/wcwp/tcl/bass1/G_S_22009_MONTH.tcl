######################################################################################################
#接口名称：客户SP注册情况
#接口编码：22009
#接口说明：记录中国移动公司客户SP注册情况信息。
#程序名称: G_S_22009_MONTH.tcl
#功能描述: 生成22009的数据
#运行粒度: 月
#源    表：1.bass2.dim_newbusi_spinfo(SP维表)
#          2.bass2.dwd_product_regsp_yyyymm(用户注册SP信息)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.SP业务类型编码的对应关系需确认
#          2.用户注册SP信息是不是真正的全量，只是现在有效的客户注册SP的记录。
#修改历史: 1.由于 DWD_PRODUCT_REGSP_YYYYMM接口由月接口改成日接口了，
#          bass2.DWD_PRODUCT_REGSP_$op_month    改成 bass2.DWD_PRODUCT_REGSP_$this_month_last_day
#          bass2.DWD_PRODUCT_REGSP_$last_month  改成 bass2.DWD_PRODUCT_REGSP_$last_month_last_day
#          xiahuaxue  2007-09-11 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6] 
        
        
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
       
        #上月最后一天 yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]         
                
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22009_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

#建立临时表
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22009_month_tmp
	              (
	                service_code     character(21),
                        sp_code          character(12),
                        sp_operator_type character(2),
                        add_count        bigint,
                        addup_count      bigint,
                        leave_count       bigint
	              )with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn

	aidb_close $handle
	
#新增用户数
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22009_month_tmp
                                      select
                                         substr(char(a.serv_code) ,1,21)
                                         ,char(b.sp_code)
                                         ,case 
                                           when b.busi_type in ('105','106') and b.serv_code<>'11000001' then '03'
                                           when b.busi_type='109' then '04'
                                           when b.busi_type='108' then '02'
                                           when b.busi_type='114' then '06'
                                           when b.busi_type='112' then '05'
                                           when b.busi_type='107' then '01'
                                           when b.busi_type='115' then '09'
                                           when b.busi_type='130' then '14'
                                           when b.busi_type='119' then '15'
                                           when b.busi_type='140' then '12'
                                           when b.busi_type='104' then '16'
                                           when b.serv_code='11000001' then '11'
                                           else '99'
                                          end
                                         ,count(distinct b.user_id)
                                         ,0
                                         ,0
                                      from  bass2.dim_newbusi_spinfo a,
                                            bass2.dwd_product_regsp_$this_month_last_day b
                                       where bigint(b.sp_code)>0 
                                           and bigint(substr(replace(char(date(b.valid_date)),'-',''),1,6))=$op_month                               
                                           and bigint(a.sp_code)=bigint(b.sp_code)   
                                      group by 
                                          substr(char(a.serv_code) ,1,21)
                                         ,char(b.sp_code)
                                         ,case 
                                           when b.busi_type in ('105','106') and b.serv_code<>'11000001' then '03'
                                           when b.busi_type='109' then '04'
                                           when b.busi_type='108' then '02'
                                           when b.busi_type='114' then '06'
                                           when b.busi_type='112' then '05'
                                           when b.busi_type='107' then '01'
                                           when b.busi_type='115' then '09'
                                           when b.busi_type='130' then '14'
                                           when b.busi_type='119' then '15'
                                           when b.busi_type='140' then '12'
                                           when b.busi_type='104' then '16'
                                           when b.serv_code='11000001' then '11'
                                           else '99'
                                          end "
        puts $sql_buff                                          
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn

	aidb_close $handle

#到达用户数
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22009_month_tmp
                                      select
                                         substr(char(a.serv_code) ,1,21)
                                         ,char(b.sp_code)
                                         ,case 
                                           when b.busi_type in ('105','106') and b.serv_code<>'11000001' then '03'
                                           when b.busi_type='109' then '04'
                                           when b.busi_type='108' then '02'
                                           when b.busi_type='114' then '06'
                                           when b.busi_type='112' then '05'
                                           when b.busi_type='107' then '01'
                                           when b.busi_type='115' then '09'
                                           when b.busi_type='130' then '14'
                                           when b.busi_type='119' then '15'
                                           when b.busi_type='140' then '12'
                                           when b.busi_type='104' then '16'
                                           when b.serv_code='11000001' then '11'
                                           else '99'
                                          end
                                         ,0
                                         ,count(distinct b.user_id)
                                         ,0
                                      from  bass2.dim_newbusi_spinfo a,
                                            bass2.dwd_product_regsp_$this_month_last_day b
                                       where bigint(b.sp_code)>0 
                                           and replace(char(b.expire_date),'-','')>='${last_month_last_day}'
                                           and bigint(a.sp_code)=bigint(b.sp_code)                             
                                      group by 
                                          substr(char(a.serv_code) ,1,21)
                                         ,char(b.sp_code)
                                         ,case 
                                           when b.busi_type in ('105','106') and b.serv_code<>'11000001' then '03'
                                           when b.busi_type='109' then '04'
                                           when b.busi_type='108' then '02'
                                           when b.busi_type='114' then '06'
                                           when b.busi_type='112' then '05'
                                           when b.busi_type='107' then '01'
                                           when b.busi_type='115' then '09'
                                           when b.busi_type='130' then '14'
                                           when b.busi_type='119' then '15'
                                           when b.busi_type='140' then '12'
                                           when b.busi_type='104' then '16'
                                           when b.serv_code='11000001' then '11'
                                           else '99'
                                          end "
        puts $sql_buff                                                              
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
#当月流失用户数
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22009_month_tmp
                                      select
                                         substr(char(a.serv_code) ,1,21)
                                         ,char(b.sp_code)
                                         ,case 
                                           when char(b.busi_type) in ('105','106') and b.serv_code<>'11000001' then '03'
                                           when char(b.busi_type)='109' then '04'
                                           when char(b.busi_type)='108' then '02'
                                           when char(b.busi_type)='114' then '06'
                                           when char(b.busi_type)='112' then '05'
                                           when char(b.busi_type)='107' then '01'
                                           when char(b.busi_type)='115' then '09'
                                           when char(b.busi_type)='130' then '14'
                                           when char(b.busi_type)='119' then '15'
                                           when char(b.busi_type)='140' then '12'
                                           when char(b.busi_type)='104' then '16'
                                           when b.serv_code='11000001' then '11'
                                           else '99'
                                          end
                                         ,0
                                         ,0
                                         ,count(distinct b.user_id)
                                      from  bass2.dim_newbusi_spinfo a,
                                            (
                                             select a.user_id,a.sp_code,a.busi_type,a.serv_code from  bass2.dw_product_regsp_$last_month_last_day  a
									     where (user_id,sp_code) in 
				                                   (select user_id,sp_code
				                                    from  bass2.dw_product_regsp_$last_month_last_day 
				                                    where int(sp_code)>0
				                                    except  
				                                    select user_id,sp_code 
				                                    from  bass2.dwd_product_regsp_$this_month_last_day
				                                    where int(sp_code)>0
				                                    )
			                                ) b
                                       where  a.sp_code=b.sp_code

                                      group by 
                                          substr(char(a.serv_code) ,1,21)
                                         ,char(b.sp_code)
                                         ,case 
                                           when char(b.busi_type) in ('105','106') and b.serv_code<>'11000001' then '03'
                                           when char(b.busi_type)='109' then '04'
                                           when char(b.busi_type)='108' then '02'
                                           when char(b.busi_type)='114' then '06'
                                           when char(b.busi_type)='112' then '05'
                                           when char(b.busi_type)='107' then '01'
                                           when char(b.busi_type)='115' then '09'
                                           when char(b.busi_type)='130' then '14'
                                           when char(b.busi_type)='119' then '15'
                                           when char(b.busi_type)='140' then '12'
                                           when char(b.busi_type)='104' then '16'
                                           when b.serv_code='11000001' then '11'
                                           else '99'
                                          end "
        puts $sql_buff                                                              
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
#汇总到结果表	
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22009_month
                                      select
                                         $op_month
                                         ,'$op_month'
                                         ,value(service_code,' ')
                                         ,sp_code
                                         ,sp_operator_type
                                         ,char(sum(add_count))
                                         ,char(sum(addup_count))
                                         ,char(sum(leave_count))
                                      from  session.g_s_22009_month_tmp
                                      group by 
                                          service_code
                                         ,sp_code
                                         ,sp_operator_type"
        puts $sql_buff                                                                                                
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	return 0
}
