######################################################################################################
#接口名称：梦网短信话单
#接口编码：04005
#接口说明：梦网短信话单。
#程序名称: G_S_04005_DAY.tcl
#功能描述: 生成04005的数据
#运行粒度: 日
#源    表：1.bass2.cdr_ismg_dtl_yyyymmdd(梦网短信话单)      
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#    1.修改原因  input_time 有空值情况，导致程序运行异常
#    2.修改时间  20070803
#    3.修改人    夏华学
#    4.处理方式 
#      	  ,substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2)
#         ,substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2)
#      修改为
#      	  ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),$timestamp)
#         ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
#     20090901 1.6.2规范去掉imei字段
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04005_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04005_DAY
                     (
                     time_id
                     ,product_no
                     ,imsi
                     ,city_id
                     ,opposite_no
                     ,calltype_id
                     ,usertype_id
                     ,sp_code
                     ,oper_code
                     ,serv_code
                     ,paytype_id
                     ,sms_status
                     ,sms_basefee
                     ,info_fee
                     ,month_fee
                     ,info_len
                     ,smsc_id
                     ,ismg_id
                     ,forward_ismg_id
                     ,info_type
                     ,input_date
                     ,input_time
                     ,process_date
                     ,process_time
                     ,start_date
                     ,start_time
                     )
              select
		                 $timestamp
                     ,product_no
                     ,coalesce(imsi,' ')
                     ,value(char(city_id),'891')
                     ,value(opp_number,'0')
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(record_type)),'')
		                 ,'0'
                     ,substr(sp_code,1,12)
                     ,value(substr(oper_code,1,10),'0')
                     ,substr(ser_code,1,21)
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                     ,case when send_state='0' then '0' 
                           when send_state is null then '0'
                           else '1' end
                     ,coalesce(char(int(base_fee*100)),'0') 
                     ,case
                        when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 2 then char(int(charge4*100))
                        when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                        when record_type in(1,2,3,11,12,13) and bill_flag = 5 then char(int(charge4*100))
                        else '0' end
                     ,case
      	                when bill_flag = 0 then '0'
      	                when bill_flag = 1 then '0'
      	                when bill_flag = 2 then '0'
      	                when bill_flag = 3 then char(int(charge4*100))
      	                when bill_flag = 4 then char(int(charge4*100))
      	                when bill_flag = 5 then '0'
      	                else '0' end
      	             ,substr((char(message_length)),1,3)
      	             ,value(smsc_id,'0')
      	             ,value(ismg_id,'0')
      	             ,case when forward_ismg_id is not null then substr(forward_ismg_id,1,6) else '0' end
      	             ,case when service_id = 50006 then '01'
      	                   when service_id = 305040 then '02'
      	                   when service_id = 305041 then '03'
      	                   else '99'
      	                   end
      	             ,value(substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2),'$timestamp')
                     ,value(substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2),'002030')
                     ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                     ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                     ,substr(char(process_time),1,4)||substr(char(process_time),6,2)||substr(char(process_time),9,2)
                     ,substr(char(process_time),12,2)||substr(char(process_time),15,2)||substr(char(process_time),18,2)
                   from 
                     bass2.cdr_ismg_dtl_$timestamp 
                   where (drtype_id not in (5103,1222,61102) and process_time is not null and substr(sp_code,1,12) is not null
				and svcitem_id in (300001,300002,300003,300004)
			  ) 
			 or substr(ser_code,1,5) in ('12590') 		    
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