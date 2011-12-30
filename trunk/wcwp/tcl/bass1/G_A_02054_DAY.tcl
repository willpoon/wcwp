######################################################################################################
#接口名称：集团客户业务订购关系
#接口编码：02054
#接口说明：
#1、本接口为日增量接口，首次上报订购状态为正常的全量订购关系。
#2、原则上，没有集团订购关系的业务，则不在本接口上报，如目前的农信通、校讯通、务工易、农信宝、买卖易等，
#   但请各省及时跟踪各业务的实际发展情况，如实进行判断和上报。
#3、企业建站、企业邮箱、专线业务、动力100等已单独开接口上报订购关系的业务，
#   在此接口不再上报。但对于可能打包在“动力100”中的子业务，如其直接与集客签订服务协议，仍需在此接口上报。
#程序名称: G_A_02054_DAY.tcl
#功能描述: 生成02054的数据
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhaolp2
#编写时间：2009-06-25  
#问题记录：1.
#修改历史: 1. 20100325 liuqf 增加V网集团订购清单 20100415 1.6.5规范修改加入动力100的具体子产品 但对于打包在“动力100”中的子业务，除了在“动力100业务订购情况”接口中上报外（02058），也需在此接口上报
#             20110215 liuqf 修复黑莓的测试数据
#							20110317 panzw 修复1220手机邮箱测试数据
#							20110630 panzw 修复R216所依赖的1520 移动400数据
#							20110805 panzw 剔除测试集团
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  #当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	
  #当天 yyyy-mm-dd
  set optime $op_time

  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from BASS1.G_A_02054_DAY where TIME_ID=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

  #需要剔除的业务----------------------
	#1310	校讯通
	#1320	农信通
	#1430	务工易
	#1440	农信宝
	#1450	买卖易
	#1340	企业建站
	#1210	企业邮箱
	#1170	互联网专线
	#3000	动力100

  #检查，现存在业务关系的-----------
  #1030 无线PBX
  #1140 集团彩铃
  #1170 互联网专线==
  #1210 企业邮箱==
  #1220 手机邮箱             
  #1240 M2M（行业应用卡）
  #1250 移动OA           
  #1260 移动进销存       
  #1280 移动CRM
  #1290 商信通 
  #1310 校讯通==
  #1320 农信通==
  #1330 企信通
  #1340 企业建站==
  #1360 警务通
  
#2011-06-30 11:30:44  
##	set sql_buff "insert into BASS1.G_A_02054_DAY
##			(
##				TIME_ID
##				,ENTERPRISE_ID
##				,ENTERPRISE_BUSI_TYPE
##				,MANAGE_MODE
##				,ORDER_DATE
##				,STATUS_ID
##			)
##			select distinct
##				${timestamp},
##				A.ENTERPRISE_ID,
##				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
##				case
##					when C.BASS1_VALUE='1290' then '2'
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end as MANAGE_MODE,
##				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
##				case
##					when a.REC_STATUS=1 then '1'
##					when a.REC_STATUS=0 then '2'
##		        else '2'
##				end as STATUS_ID
##			from
##				bass2.DW_ENTERPRISE_SUB_DS a,
##				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
##				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
##			where DATE(A.done_date)='$op_time'
##			  and A.SERVICE_ID = B.SERVICE_ID
##			  and a.SERVICE_ID = c.XZBAS_VALUE
##				and c.BASS1_VALUE not in ('1310','1320','1430','1440','1450','1340','1210','1170','3000','1230','1220')
##		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
##		 		case
##					when C.BASS1_VALUE='1290' then '2'
##					when upper(B.CONFIG_VALUE)='MAS' then '1'
##					when upper(B.CONFIG_VALUE)='ADC' then '2'
##					else '3'
##				end,
##				case
##					when REC_STATUS=1 then '1'
##					when REC_STATUS=0 then '2'
##				else '2'
##				end "
##
#2011-06-30 11:30:50
#更改1520的管理模式为3
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when C.BASS1_VALUE='1290' then '2'
					when C.BASS1_VALUE='1520' then '3'					
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
		        else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a,
				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where DATE(A.done_date)='$op_time'
			  and A.SERVICE_ID = B.SERVICE_ID
			  and a.SERVICE_ID = c.XZBAS_VALUE
				and c.BASS1_VALUE not in ('1310','1320','1430','1440','1450','1340','1210','1170','3000','1230','1220')
		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
		 		case
					when C.BASS1_VALUE='1290' then '2'
					when C.BASS1_VALUE='1520' then '3'						
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end "


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


###2011-03-17 12:39:15 1220	手机邮箱修复
###此处仅跑一次,全量修复历史数据，第二天屏蔽代码
##  set handle [aidb_open $conn]
##	set sql_buff "insert into BASS1.G_A_02054_DAY 
##		select * from bass1.G_A_02054_DAY_0317_1220repair
##	"
##							 
##  puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace $errmsg 2006
##		puts $errmsg
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##  aidb_close $handle
	
#1230 BLACK BERRY  单独进行处理，剔除测试数据
  set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
				case
					when C.BASS1_VALUE='1290' then '2'
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end as MANAGE_MODE,
				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
				case
					when a.REC_STATUS=1 then '1'
					when a.REC_STATUS=0 then '2'
		        else '2'
				end as STATUS_ID
			from
				bass2.DW_ENTERPRISE_SUB_DS a,
				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
			where DATE(A.done_date)='$op_time'
			  and A.SERVICE_ID = B.SERVICE_ID
			  and a.SERVICE_ID = c.XZBAS_VALUE
				and c.BASS1_VALUE ='1230'
				and a.enterprise_id not in ('89100000000683','89103000041929','89103000161144','89103000498290')
		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
		 		case
					when C.BASS1_VALUE='1290' then '2'
					when upper(B.CONFIG_VALUE)='MAS' then '1'
					when upper(B.CONFIG_VALUE)='ADC' then '2'
					else '3'
				end,
				case
					when REC_STATUS=1 then '1'
					when REC_STATUS=0 then '2'
				else '2'
				end "


  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2006
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



####1230 BLACK BERRY  修复送给集团的测试数据,20110215送完数据后将屏蔽这段代码
###  set handle [aidb_open $conn]
###	set sql_buff "insert into BASS1.G_A_02054_DAY select * from G_A_02054_DAY_black"
###
###
###  puts $sql_buff
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2006
###		puts $errmsg
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###



###临时处理：20101011 liuqf 全量处理ICT客户数这个数据，只抓现有订购关系有效的，订购时间不变
###
###  set handle [aidb_open $conn]
###	set sql_buff "insert into BASS1.G_A_02054_DAY
###			(
###				TIME_ID
###				,ENTERPRISE_ID
###				,ENTERPRISE_BUSI_TYPE
###				,MANAGE_MODE
###				,ORDER_DATE
###				,STATUS_ID
###			)
###			select distinct
###				${timestamp},
###				A.ENTERPRISE_ID,
###				C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
###				case
###					when C.BASS1_VALUE='1290' then '2'
###					when upper(B.CONFIG_VALUE)='MAS' then '1'
###					when upper(B.CONFIG_VALUE)='ADC' then '2'
###					else '3'
###				end as MANAGE_MODE,
###				replace(char(date(min(A.done_date))),'-','') as ORDER_DATE,
###				case
###					when a.REC_STATUS=1 then '1'
###					when a.REC_STATUS=0 then '2'
###		        else '2'
###				end as STATUS_ID
###			from
###				bass2.DW_ENTERPRISE_SUB_DS a,
###				(select * from BASS2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b,
###				(select * from BASS1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') c
###			where A.SERVICE_ID = B.SERVICE_ID
###			  and a.SERVICE_ID = c.XZBAS_VALUE
###				and c.BASS1_VALUE in ('1550')
###		 group by A.ENTERPRISE_ID,C.BASS1_VALUE,
###		 		case
###					when C.BASS1_VALUE='1290' then '2'
###					when upper(B.CONFIG_VALUE)='MAS' then '1'
###					when upper(B.CONFIG_VALUE)='ADC' then '2'
###					else '3'
###				end,
###				case
###					when REC_STATUS=1 then '1'
###					when REC_STATUS=0 then '2'
###				else '2'
###				end "
###
###
###  puts $sql_buff
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2006
###		puts $errmsg
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###
###
###
###    ###1000	VPMN 长
###    ###新增代码段，插入V网集团清单，第一次全量为状态在网的，第二天上传增量数据
    set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct 
				${timestamp},
				a.enterprise_id, 
				'1000' as enterprise_busi_type,
				'3'    as manage_mode,
				replace(char(date(min(b.create_date))),'-','') as order_date,
				case 
				  when b.sts=0 then '1'
				  else '2'
				end as status_id
			from bass2.dwd_enterprise_vpmn_rela_$timestamp a,
			     bass2.dwd_vpmn_enterprise_$timestamp b
			where a.vpmn_id=b.vpmn_id
			  and DATE(b.CREATE_DATE)='$op_time'
			group by a.enterprise_id,
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



###临时处理：20101022 liuqf 全量处理无线商务电话客户数这个数据，只抓现有订购关系有效的，订购时间不变

  set handle [aidb_open $conn]
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				cust_party_role_id as ENTERPRISE_ID,
				'1040' as ENTERPRISE_BUSI_TYPE,
				'3' as MANAGE_MODE,
				replace(char(date(done_date)),'-','') as ORDER_DATE,
				case
					when STATE in ('1','4','6','8','M') then '1'
		      else '2'
				end as STATUS_ID
			from bass2.ODS_PRODUCT_INS_PROD_${timestamp}
		 where offer_id = 111389150019 and user_type_id = 2
       and cust_party_role_id not in ('89103000498290')
       and DATE(done_date)='$op_time'
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
	set sql_buff "insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
				${timestamp},
				A.ENTERPRISE_ID,
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


	

  #保证主键唯一性，删除通一天订购又退订的数据
  set handle [aidb_open $conn]
	set sql_buff "delete  from bass1.g_a_02054_day a where exists 
								 (
								select * from 
									(
									select time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date ,count(*)
									from bass1.g_a_02054_day
									where time_id = $timestamp
									group by time_id,enterprise_id,enterprise_busi_type,manage_mode,order_date
									having count(*)>1
									) as b
									where a.enterprise_id = b.enterprise_id
							      and a.order_date = b.order_date
							      and a.time_id = b.time_id
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
	
#2011-05-31 18:09:04 一次性修复 for R186		
#set sql_buff "
#insert into G_A_02054_DAY select * from   G_A_02054_DAY_T
#"
#exec_sql $sql_buff


#2011-06-30 11:28:55
#一次性修复
##移动400
#
#       set sql_buff "
#       insert into G_A_02054_DAY select * from G_A_02054_DAY_FIX1520
#       "
#       exec_sql $sql_buff
#
#
# 2011-08-12 
#剔除 1520 测试集团
#
#     set sql_buff "
#     insert into G_A_02054_DAY select * from G_A_02054_DAY_FIX20110812
#     "
#     exec_sql $sql_buff
#

#2011-10-20 企信通数据修复
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Deal_fixqixintong $op_time $optime_month


  #1.检查chkpkunique
set tabname "g_a_02054_day"
set pk "ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE"
chkpkunique ${tabname} ${pk} ${timestamp}

	return 0
}
