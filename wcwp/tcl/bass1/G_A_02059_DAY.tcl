######################################################################################################
#接口名称：集团业务个人用户绑定关系
#接口编码：02059
#接口说明：
#程序名称: G_A_02059_DAY.tcl
#功能描述: 生成02059的数据
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhaolp2
#编写时间：2009-06-25 
#问题记录：1.
#修改历史: 1.20100415 1.6.5规范修改加入动力100的具体子产品 但对于打包在“动力100”中的子业务，除了在“动力100业务订购情况”接口中上报外（02058），也需在此接口上报
#          20100626 业务逻辑上变为 bigint(e.porder_id)=bigint(a.order_id) 关联
#          20100818 加上V网成员的绑定关系，只上传增量，其历史全量并没有上传
#          20101027 liuzhilong 1240 M2M（行业应用卡） 改成 1241M2M（车务通）、 1249 M2M（其它）
#          20110315 panzhiwei 1040无线商务订购关系修复
#          20110321 panzhiwei 1340	企业建站(ADC)部分 由于企业建站业务只要企业客户订购企业建站后，集团个人客户默认可以使用该功能，无需个人客户单独申请办理该服务，故无企业建站这个集团业务与个人客户绑定关系。此处修复是把所有订购的集团下属成员加进去作为个人订购，以防止：交叉验证规则：企业建站（ADC）使用集团客户到达数 > 使用企业建站（ADC）的集团个人客户数（异常）之异常！
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #set timestamp 20090712
        #当天 yyyy-mm-dd
        set optime $op_time
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02059_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#1220	手机邮箱
	#1230	BLACK BERRY
	#1240	M2M（行业应用卡）
	#1320   农信通
	#2011-03-15 10:55:09 1040无线商务电话	单独处理
	#商信通/农信通需要特殊处理
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				A.USER_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.CREATE_DATE)),'-','') as ORDER_DATE,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_EXTSUB_RELA_DS a,
				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where DATE(A.CREATE_DATE)='$op_time'
				and A.SERVICE_ID=B.SERVICE_ID
				and A.SERVICE_ID=C.XZBAS_VALUE
				and C.BASS1_VALUE not in ('1230','1241','1249','1220','1320','1040')
		"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
#####对企业建站ADC进行一个全量数据修复，修复后屏蔽此代码
##	set sql_buff "insert into BASS1.G_A_02059_DAY
##			(
##				TIME_ID
##				,ENTERPRISE_ID
##				,USER_ID
##				,ENTERPRISE_BUSI_TYPE
##				,MANAGE_MODE
##				,ORDER_DATE
##				,STATUS_ID
##			)
##			select distinct
##				${timestamp},
##				A.ENTERPRISE_ID,
##				A.USER_ID,
##				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
##				case
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end as MANAGE_MODE,
##				replace(char(date(A.CREATE_DATE)),'-','') as ORDER_DATE,
##				case
##					when REC_STATUS=1 then '1'
##					when REC_STATUS=0 then '2'
##				else '2'
##				end as STATUS_ID
##			from
##				bass2.DW_ENTERPRISE_EXTSUB_RELA_DS a,
##				(select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
##				(select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
##			where A.REC_STATUS=1
##				and A.SERVICE_ID=B.SERVICE_ID
##				and A.SERVICE_ID=C.XZBAS_VALUE
##				and C.BASS1_VALUE in ('1340')
##		"
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

##1040	无线商务电话部分
##此处仅跑一次,全量修复历史数据，第二天屏蔽代码
##2011-03-16 8:53:34 屏蔽代码
##	set sql_buff "insert into BASS1.G_A_02059_DAY select * from G_A_02059_DAY_0315modify
##							 "
##
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
	
#无线商务订购关系为正常的数据增量,和02049集团成员接口口径一致
	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1040'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02054_day a,
								      bass2.dw_enterprise_member_mid_${timestamp} b,
								      bass2.dw_enterprise_msg_${timestamp} c,
								      bass2.dw_product_${timestamp} d
								where a.enterprise_id=b.enterprise_id
								  and a.enterprise_id=c.enterprise_id
								  and b.user_id=d.user_id
								  and d.usertype_id in (1,2,3,6)
								  and a.enterprise_busi_type='1040'
								  and a.time_id=${timestamp}
								  and a.status_id='1'
"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#无线商务订购关系为退订的数据增量
	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1040'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02054_day a,
								      (select distinct enterprise_id,user_id 
								        from BASS1.G_A_02059_DAY 
								       where enterprise_busi_type='1040' 
								         and time_id<${timestamp}) b
								where a.enterprise_id=b.enterprise_id
								  and a.time_id=${timestamp}
								  and a.enterprise_busi_type='1040'
								  and a.status_id='2'
"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

##2011-03-21 12:19:50 panzhiwei 此处仅跑一次,全量修复历史数据，第二天屏蔽代码
##1340	企业建站(ADC)部分 由于企业建站业务只要企业客户订购企业建站后，集团个人客户默认可以使用该功能，无需个人客户单独申请办理该服务，故无企业建站这个集团业务与个人客户绑定关系。
##此处修复是把所有订购的集团下属成员加进去作为个人订购，以防止：
##交叉验证规则：企业建站（ADC）使用集团客户到达数 > 使用企业建站（ADC）的集团个人客户数（异常）
##之异常！

#2011-03-22 8:38:06注释掉一次性修复代码
##	set sql_buff "insert into BASS1.G_A_02059_DAY select * from G_A_02059_DAY_20110321fix1340
##							 "
##
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
	
#企业建站(ADC)订购关系为正常的数据增量,和02049集团成员接口口径一致

	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1340'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02055_day a,
								      bass2.dw_enterprise_member_mid_${timestamp} b,
								      bass2.dw_enterprise_msg_${timestamp} c,
								      bass2.dw_product_${timestamp} d
								where a.enterprise_id=b.enterprise_id
								  and a.enterprise_id=c.enterprise_id
								  and b.user_id=d.user_id
								  and d.usertype_id in (1,2,3,6)
								  and a.enterprise_busi_type='1340'
								  and a.time_id=${timestamp}
								  and a.status_id='1'
								  and d.test_mark = 0								  
"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#企业建站(ADC)订购关系为退订的数据增量
	set sql_buff "insert into BASS1.G_A_02059_DAY
								select ${timestamp}
											 ,a.enterprise_id
											 ,b.user_id
											 ,'1340'
											 ,'3'
											 ,'${timestamp}'
											 ,a.status_id
								from 	bass1.g_a_02055_day a,
								      (select distinct enterprise_id,user_id 
								        from BASS1.G_A_02059_DAY 
								       where enterprise_busi_type='1340' 
								         and time_id<${timestamp}) b
								where a.enterprise_id=b.enterprise_id
								  and a.time_id=${timestamp}
								  and a.enterprise_busi_type='1340'
								  and a.status_id='2'
					"

  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	
	#商信通个人订购部分	
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				'' as ENTERPRISE_ID,
				A.USER_ID,
				'1290' as ENTERPRISE_BUSI_TYPE,
				'2' as MANAGE_MODE,
				replace(char(date(A.CREATE_DATE)),'-','') as ORDER_DATE,
				case
					when STS=1 then '1'
					else '2'
				end as STATUS_ID
			from
				BASS2.DW_PRODUCT_REGSP_DS A
			where DATE(A.STS_DATE)='$op_time'
				and sp_code in ('600000','901870','801160','900141')"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#农信通个人订购部分(第一次送全量，第二天增量)
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				'' as ENTERPRISE_ID,
				A.USER_ID,
				'1320' as ENTERPRISE_BUSI_TYPE,
				'2' as MANAGE_MODE,
				replace(char(date(min(A.CREATE_DATE))),'-','') as ORDER_DATE,
				case
					when STS=1 then '1'
					else '2'
				end as STATUS_ID
			from
				BASS2.DW_PRODUCT_REGSP_DS A
			where DATE(A.STS_DATE)='$op_time'
				and a.busi_type='133'
		   group by A.USER_ID,
		         case
					when STS=1 then '1'
					else '2'
				 end
			"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #加入动力100的具体子产品订购关系
    set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02059_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,USER_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				A.USER_ID,
				D.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(A.done_date)),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
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
			  and a.enterprise_id not in ('891910006274','891880005002')
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


###    ###1000	VPMN 
###    20100818 V网成员的绑定关系增量数据抓取，定义为1000

    set handle [aidb_open $conn]
		set sql_buff "insert into BASS1.G_A_02059_DAY
				(
					TIME_ID
					,ENTERPRISE_ID
					,USER_ID
					,ENTERPRISE_BUSI_TYPE
					,MANAGE_MODE
					,ORDER_DATE
					,STATUS_ID
				)
				select distinct 
					${timestamp},
			    a.enterprise_id, 
			    b.user_id,
			    '1000' as enterprise_busi_type,
			    '3'    as manage_mode,
			    replace(char(date(min(b.create_date))),'-','') as order_date,
			    case 
			      when b.sts=0 then '1'
			      else '2'
			    end as status_id
			from bass2.dwd_enterprise_vpmn_rela_$timestamp a,
			     bass2.dw_vpmn_member_ds b
			where a.vpmn_id=b.vpmn_id
			  and DATE(b.sts_date)='${op_time}'
			group by a.enterprise_id,b.user_id,
			case 
			  when b.sts=0 then '1'
			  else '2'
			end
			"


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

#2011-05-31 15:58:36
#修复02059，对于usertype_id = 8 的用户置失效，以通过R177校验。
#一次性修复。
#set sql_buff "
#insert into G_A_02059_DAY select * from   G_A_02059_DAY_T
#"
#exec_sql $sql_buff


  #保证主键唯一性，删除同一天订购又退订的数据
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.G_A_02059_DAY a where exists
								(
									select * from
									(
									select time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date,count(*)
									from bass1.G_A_02059_DAY
									where time_id = $timestamp
									group by time_id,enterprise_id,user_id,enterprise_busi_type,manage_mode,order_date
									having count(*)>1
									) as b
								where a.time_id = b.time_id
								  and a.enterprise_id = b.enterprise_id
								  and a.user_id = b.user_id
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