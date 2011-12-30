######################################################################################################
#接口名称：手机邮箱个人用户绑定关系
#接口编码：02061
#接口说明：
#程序名称: G_A_02061_DAY.tcl
#功能描述: 生成02061的数据
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhaolp2
#编写时间：2009-07-06
#问题记录：1.
#修改历史: 1.2011-03-17 15:50:30 由于手机邮箱全部都是测试数据，无实际订购，所以后续暂不上报,先手工生成修复数据，然后屏蔽手机邮箱个人用户绑定关系的抽取。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

##        #当天 yyyymmdd
##        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
##
##        #当天 yyyy-mm-dd
##        set optime $op_time
##


##--##2011-03-17 15:55:08 手工生成修复数据
##--/**
##--		CREATE TABLE BASS1.G_A_02061_DAY_0317repair
##--		 (TIME_ID               INTEGER,
##--		  ENTERPRISE_ID         CHARACTER(20),
##--		  USER_ID               CHARACTER(20),
##--		  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
##--		  MANAGE_MODE           CHARACTER(1),
##--		  EXPIRE_DATE           CHARACTER(8),
##--		  PAY_TYPE              CHARACTER(1),
##--		  ORDER_DATE            CHARACTER(8),
##--		  STATUS_ID             CHARACTER(1)
##--		 )
##--		  DATA CAPTURE NONE
##--		 IN TBS_APP_BASS1
##--		 INDEX IN TBS_INDEX
##--		  PARTITIONING KEY
##--		   (USER_ID,
##--		    TIME_ID
##--		   ) USING HASHING;
##--		
##--			ALTER TABLE BASS1.G_A_02061_DAY_0317repair
##--			  LOCKSIZE ROW
##--			  APPEND OFF
##--			  NOT VOLATILE;
##--			delete from   bass1.G_A_02061_DAY_0317repair;
##--			insert into   bass1.G_A_02061_DAY_0317repair
##--			select 
##--			         20110317
##--			        ,ENTERPRISE_ID
##--			        ,USER_ID
##--			        ,ENTERPRISE_BUSI_TYPE
##--			        ,MANAGE_MODE
##--			        ,'20110317' EXPIRE_DATE
##--			        ,PAY_TYPE
##--			        ,ORDER_DATE
##--			        ,'2' STATUS_ID
##--			from 
##--			(
##--			select t.*,row_number()over(partition by user_id order by time_id ) rn 
##--			from 
##--			(
##--			select * from G_A_02061_DAY
##--			where ENTERPRISE_BUSI_TYPE = '1220'
##--			and  MANAGE_MODE = '2'
##--			and length(trim(user_id)) = 14
##--			) t
##--			) t2 
##--			where rn = 1 
##--			and STATUS_ID ='1'
##--			--
##--			--上传修复数据
##--			insert into BASS1.G_A_02061_DAY
##--			select * from bass1.G_A_02061_DAY_0317repair
##--			;
##--			
##--**/


##        #删除本期数据
##        set handle [aidb_open $conn]
##	set sql_buff "delete from BASS1.G_A_02061_DAY where TIME_ID=$timestamp"
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##
##
##	#1220	手机邮箱
##
##  #92602002	手机邮箱（短信）_3元/月
##  #92602003	手机邮箱（彩信）_8元/月
##  #92602004	手机邮箱（WAP）_15元/月
##  #92602005	手机邮箱（客户端）_28元/月
##  #
##
##	#资费类型未确认口径，填其他
##	set sql_buff "insert into BASS1.G_A_02061_DAY
##			(
##				TIME_ID
##				,ENTERPRISE_ID
##				,USER_ID
##				,ENTERPRISE_BUSI_TYPE
##				,MANAGE_MODE
##				,EXPIRE_DATE
##				,PAY_TYPE
##				,ORDER_DATE
##				,STATUS_ID
##			)
##			select distinct
##				${timestamp},
##				A.ENTERPRISE_ID,
##				D.USER_ID,
##				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
##				case
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end as MANAGE_MODE,
##				replace(char(date(A.EXPIRE_DATE)),'-','') as EXPIRE_DATE,
##			  case when e.sprom_id=92602005 then '5'
##				else '5' end  as PAY_TYPE,
##				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
##				case
##					when a.REC_STATUS=1 then '1'
##					when a.REC_STATUS=0 then '2'
##				end as STATUS_ID
##			from
##				bass2.DW_ENTERPRISE_SUB_DS a
##				inner join (select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b on A.SERVICE_ID=B.SERVICE_ID
##				inner join (select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c on A.SERVICE_ID=C.XZBAS_VALUE
##        left join bass2.DW_ENTERPRISE_MEMBERSUB_DS d on A.ORDER_ID=D.ORDER_ID
##        inner join bass2.dw_product_sprom_ds e on d.user_id =e.user_id
##			where DATE(A.done_date)='${op_time}'
##			  and C.BASS1_VALUE='1220'"
##
##
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
##
##
##  #保证主键唯一性，删除同一天订购又退订的数据
##  set handle [aidb_open $conn]
##	set sql_buff "delete  from bass1.G_A_02061_DAY a where exists
##								(
##								select * from
##									(
##									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date,count(*)
##									from bass1.G_A_02061_DAY
##									where time_id = $timestamp
##									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date
##									having count(*)>1
##									) as b
##								where a.time_id = b.time_id
##								  and a.enterprise_id = b.enterprise_id
##								  and a.user_id = b.user_id
##								  and a.enterprise_busi_type = b.enterprise_busi_type
##								  and a.manage_mode = b.manage_mode
##								  and a.order_date = b.order_date
##								  and a.time_id=$timestamp
##								 )"
##	puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2020
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle		
##
##
	return 0
}