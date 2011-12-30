######################################################################################################
#接口名称：国际漫游来访通话情况日使用
#接口编码：21017
#接口说明：记录国际漫游来访通话情况日使用相关信息
#程序名称: G_S_21017_DAY.tcl
#功能描述: 生成2100的数据
#运行粒度: 日
#源    表：1.bass2.cdr_call_roamin_dtl_yyyymmdd
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21017_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21017_day
                (
                TIME_ID
                ,BILL_DATE
                ,ADVERSARY_ID
                ,CALL_TYPE_ID
                ,CALL_COUNTS
                ,CALL_DURATION
                ,CALL_DURATION_M
                ,BASECALL_FEE
                ,TOLL_FEE
                ,CALL_FEE  
                )

              SELECT
		$timestamp 
		,'$timestamp' as BILL_DATE
	        ,case   
	           when OPPOSITE_ID in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78) THEN '010000'
		   when OPPOSITE_ID in (13,14) then '020000'
		   when OPPOSITE_ID=2 then '023000'
		   when OPPOSITE_ID=1 then '032000'
		   when OPPOSITE_ID=3 then '050000'
		   when OPPOSITE_ID=115 then '040000'
		   when OPPOSITE_ID=8 then '080000'
		   when OPPOSITE_ID IN (5,6,7) then '081000'
	           else '990000' 
	         end			AS adversary_id
                ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0011',char(calltype_id)),'01') as CALLTYPE_ID
                ,char(count(*)                                )
                ,char(int(sum(CALL_DURATION)                 ))                   
                ,char(int(sum(CALL_DURATION_M)               ))
                ,char(int(sum(BASECALL_FEE)                  ))
                ,char(int(sum(TOLL_FEE)                      ))
                ,char(int(sum(BASECALL_FEE+TOLL_FEE+INFO_FEE)))
              FROM
                bass2.cdr_call_roamin_dtl_${timestamp}
              WHERE 
                roamtype_id=3
              GROUP BY
	        case   
	           when OPPOSITE_ID in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78) THEN '010000'
		   when OPPOSITE_ID in (13,14) then '020000'
		   when OPPOSITE_ID=2 then '023000'
		   when OPPOSITE_ID=1 then '032000'
		   when OPPOSITE_ID=3 then '050000'
		   when OPPOSITE_ID=115 then '040000'
		   when OPPOSITE_ID=8 then '080000'
		   when OPPOSITE_ID IN (5,6,7) then '081000'
	           else '990000' 
	         end	
                ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0011',char(calltype_id)),'01')"
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