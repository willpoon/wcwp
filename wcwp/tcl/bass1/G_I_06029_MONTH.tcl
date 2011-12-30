######################################################################################################
#接口名称：SP业务代码
#接口编码：06029
#接口说明：业务代码表示业务类别，由内容/应用服务提供商自己制定。
#          各省只上报提供本地服务（本地/全网业务本地接入）的sp局数据。
#程序名称: G_I_06029_MONTH.tcl
#功能描述: 生成06029的数据
#运行粒度: 月
#源    表：1.bass2.dim_newbusi_spinfo(SP维表)
#          2.bass2.dw_newbusi_ismg_yyyymm(梦网清单)
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：tym
#编写时间：2007-03-22
#问题记录：1.目前SP维表的sp_type只有7个值，不能满足BASS_STD1_0004的需要,所以用CASE语句来完成映射关系。
#          2.因为订购一个SP服务,一般首先是免费的,所以存在一个SP服务有几种计费类型的记录。
#修改历史: 1.2007年7月份数据，出现sp_code=931043的生效日期为2007524的数据，所有用case语句屏蔽这些数据
#          2.在程序中增加SP企业代码，SP业务代码做联合主键的控制
#          3.8月份数据发现sp_fee有6位的,故修改程序,用substr来截取该字段的值@20070903 by tym
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06029_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #----建立临时表-----#
        set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.g_i_06029_month_tmp
                     (
                      sp_code             varchar(12 ),
                      operator_code       varchar(10 ),
                      operator_name       varchar(50 ),
                      sp_bill_id          varchar( 1 ),
                      sp_fee              varchar( 10 ),
                      effect_date         varchar( 8 ),
                      invalid_date        varchar( 8 )
                      )
                      partitioning key
                      (sp_code)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_06029_month_tmp
                         select
                           substr(a.sp_code,1,12)
                           ,value(substr(a.oper_code,1,10),' ')
                           ,substr(b.sp_name,1,50)
                            ,case
                               when a.bill_flag=3 then '0'
                               when a.bill_flag=2 then '1'
                               when a.bill_flag=1 then '2'
                              end
                            ,case
                               when a.bill_flag in (1,3) then '0'
                               else rtrim(char(int(a.charge4*100)))
                             end
                            ,case
                               when length(rtrim(b.valid_date))<8 then '20070101'
                               else b.valid_date
                             end
                            ,case
                               when length(rtrim(b.expire_date))<8 then '20101231'
                               else b.expire_date
                             end
                         from 
                           bass2.dw_newbusi_ismg_$op_month a ,
                           (select a.* 
                             from bass2.dim_newbusi_spinfo a,
                                 (select sp_code,max(valid_date) as valid_date 
                                  from bass2.dim_newbusi_spinfo
			                            group by sp_code
			                           )b
                              where a.sp_code=b.sp_code and a.valid_date=b.valid_date
                              and (a.sp_region <> 1 or a.sp_name like '%西藏移动%')
                           ) b
                         where a.bill_flag in (1,2,3)
                             and a.sp_code=b.sp_code
                         group by
                            substr(a.sp_code,1,12)
                           ,substr(a.oper_code,1,10)
                           ,substr(b.sp_name,1,50)
                            ,case
                               when a.bill_flag=3 then '0'
                               when a.bill_flag=2 then '1'
                               when a.bill_flag=1 then '2'
                              end
                            ,case
                               when a.bill_flag in (1,3) then '0'
                               else  rtrim(char(int(a.charge4*100)))
                             end
                             ,case
                               when length(rtrim(b.valid_date))<8 then '20070101'
                               else b.valid_date
                             end
                            ,case
                               when length(rtrim(b.expire_date))<8 then '20101231'
                               else b.expire_date
                             end      "
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
	set sql_buff "insert into bass1.g_i_06029_month
                 select  a.time_id
                       ,a.sp_code
                       ,a.operator_code
                       ,a.operator_name
                       ,a.sp_bill_id
                       ,a.sp_fee
                       ,a.effect_date
                       ,a.invalid_date
                 from
                 (
                  select
                           $op_month time_id
                           ,sp_code
                           ,operator_code
                           ,operator_name
                           ,sp_bill_id
                           ,substr(rtrim(char(sum(bigint(sp_fee)))),1,5)  sp_fee
                           ,char(max(bigint(effect_date)))  effect_date
                           ,char(max(bigint(invalid_date)))  invalid_date
                           ,row_number()over(partition by sp_code,operator_code order by $op_month desc) row_id
                         from
                           session.g_i_06029_month_tmp
                         group by
                           sp_code
                           ,operator_code
                           ,operator_name
                           ,sp_bill_id 
                 ) a
                 where a.row_id=1     "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06029_month where time_id=$op_month and length(ltrim(rtrim(sp_code))) <> 6;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2030
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06029_month where time_id=$op_month and substr(ltrim(sp_code),1,1) not in ('4','7','9')"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2040
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   
    
	return 0
}