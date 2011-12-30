######################################################################################################
#接口名称：动力100业务订购情况
#接口编码：02058
#接口说明：
#程序名称: G_A_02058_DAY.tcl
#功能描述: 生成02058的数据
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhaolp2
#编写时间：2009-06-25
#问题记录：1.
#修改历史: 1.业务逻辑上变为 bigint(e.porder_id)=bigint(a.order_id) 关联
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #当天 yyyy-mm-dd
        set optime $op_time

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02058_DAY  where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


#2011-05-31 19:28:05 一次性修复		
#set sql_buff "
#insert into G_A_02058_DAY select * from   G_A_02058_DAY_T
#"
#exec_sql $sql_buff
#

	#通过商品包订购关系中的pack_id来限制动力100业务以及其porder_id等于产品订购ID来关联，抓取动力100的订购业务
	set sql_buff "insert into BASS1.G_A_02058_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,SUB_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				D.BASS1_VALUE as SUB_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
                else '2'					
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a
				inner join (select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b on A.SERVICE_ID=B.SERVICE_ID
				inner join (select * from bass1.ALL_DIM_LKP_163 where bass1_tbid='BASS_STD1_0108') c on A.SERVICE_ID=C.XZBAS_VALUE
				inner join (select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') d on A.SERVICE_ID=D.XZBAS_VALUE
				inner join 
				(select porder_id,porder_name from bass2.DW_PACK_ORDER_INFO_$timestamp where pack_id in('10086','10087') and rec_status=1) e
				  on bigint(e.porder_id)=bigint(a.order_id)
			where DATE(A.done_date)='${op_time}'
			  and a.enterprise_id not in ('891910006274','891880005002')"
			
##		DATE(A.CREATE_DATE)='${op_time}'
##

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
  #保证主键唯一性，删除同一天订购又退订的数据
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02058_DAY a where exists
								(
								select * from 
								(
								select time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date,count(*)
								from bass1.G_A_02058_DAY
								where time_id = $timestamp
								group by time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date
								having count(*)>1
								) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.enterprise_busi_type = b.enterprise_busi_type
								  and a.manage_mode = b.manage_mode
								  and a.order_date = b.order_date
								  and a.time_id=$timestamp
								)"
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