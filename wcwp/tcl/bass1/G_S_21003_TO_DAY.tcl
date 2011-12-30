######################################################################################################
#接口名称：
#接口编码：
#接口说明：生成21003的中间表数据
#程序名称: G_S_21003_TO_DAY.tcl
#功能描述: 
#运行粒度: 日
#源    表：1.bass1.int_210012916_yyyymm
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: liuzhilong 20090911 去除where 条件  and DRTYPE_ID  not in (1700,9901,9902,9903)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
  
  #set op_time 2008-12-02
	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_S_21003_TO_DAY where TIME_ID=${Timestamp}"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_21003_TO_DAY
                      (
                      TIME_ID
                      ,PRODUCT_NO
                      ,BRAND_ID
                      ,SVC_TYPE_ID
                      ,TOLL_TYPE_ID
                      ,IP_TYPE_ID
                      ,ADVERSARY_ID
                      ,ROAM_TYPE_ID
                      ,CALL_TYPE_ID
                      ,CALL_COUNTS
                      ,BASE_BILL_DURATION
                      ,TOLL_BILL_DURATION
                      ,CALL_DURATION
                      ,BASE_CALL_FEE
                      ,TOLL_CALL_FEE
                      ,CALLFW_TOLL_FEE
                      ,CALL_FEE
                      ,FAVOURED_BASECALL_FEE
                      ,FAVOURED_TOLLCALL_FEE
                      ,FAVOURED_CALLFW_TOLLFEE
                      ,FAVOURED_CALL_FEE
                      ,FREE_DURATION
                      ,FAVOUR_DURATION
                      ,MNS_TYPE
                      ,OPP_PROPERTY
                      )
          select
           ${Timestamp}
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end  ip_type_id
				/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_${op_month} a
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id					
          WHERE a.op_time=$Timestamp
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,adversary_id
					,roam_type_id
					,call_type_id
					with ur
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

	return 0
}
#####################################
#and svcitem_id not in ('1700','9901','9902','9903') 
#--屏蔽1700:娱音在线、音信互动业务 others:VPMN数据--
####################################