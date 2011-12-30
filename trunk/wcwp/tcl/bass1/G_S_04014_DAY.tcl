######################################################################################################
#接口名称：语音杂志短信话单
#接口编码：04014
#接口说明：语音杂志短信话单。
#程序名称: G_S_04014_DAY.tcl
#功能描述: 生成04014的数据
#运行粒度: 日
#源    表：1.bass2.cdr_ismg_dtl_yyyymmdd
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 20090901 1.6.2规范去掉imei字段
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04014_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       






        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04014_DAY
                           (
                             time_id
                            ,product_no
                            ,imsi
                            ,city_id
                            ,other_msisdn
                            ,sms_bill_type
                            ,cdr_user_type
                            ,serv_code
                            ,sms_fee_type
                            ,sms_send_state
                            ,basecall_fee
                            ,info_fee
                            ,month_fee
                            ,info_len
                            ,sms_center_code
                            ,sms_gate_code
                            ,frnt_gate_code
                            ,apply_date
                            ,apply_time
                            ,gate_deal_date
                            ,gate_deal_time
                            ,bill_deal_date
                            ,bill_deal_time
                           )
                         select
                           ${timestamp}
                           ,product_no
                           ,imsi
                           ,char(city_id)
                           ,value(opp_number,'0')
                           ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0009',char(record_type)),'')
                           ,'0'
                           ,ser_code
                           ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0047',char(bill_flag)),'')
                           ,case 
                              when send_state is not null then coalesce(bass1.fn_get_all_dim('BASS_STD2_0008',char(send_state)),'') 
                              else '0' 
                            end
                           ,case 
                              when record_type in (0,10) then char(int(base_fee*100)) 
                              else '0' 
                            end
                           ,case
                              when record_type in(1,2,3,11,12,13) and bill_flag = 0 then '0'
                              when record_type in(1,2,3,11,12,13) and bill_flag = 1 then '0'
                              when record_type in(1,2,3,11,12,13) and bill_flag = 2 then char(int(charge4*100))
                              when record_type in(1,2,3,11,12,13) and bill_flag = 3 then '0'
                              when record_type in(1,2,3,11,12,13) and bill_flag = 4 then '0'
                              when record_type in(1,2,3,11,12,13) and bill_flag = 5 then char(int(charge4*100))
                              else '0' 
                             end
                           ,case
      	                      when bill_flag = 0 then '0'
      	                      when bill_flag = 1 then '0'
      	                      when bill_flag = 2 then '0'
      	                      when bill_flag = 3 then char(int(charge4*100))
      	                      when bill_flag = 4 then '0'
      	                      when bill_flag = 5 then '0'
      	                      else '0' 
      	                    end
      	                   ,substr((char(message_length)),1,3)
      	                   ,value(smsc_id,'0')
      	                   ,ismg_id
      	                   ,case when forward_ismg_id is not null then forward_ismg_id else '0' end
      	                   ,substr(char(input_time),1,4)||substr(char(input_time),6,2)||substr(char(input_time),9,2)
                           ,substr(char(input_time),12,2)||substr(char(input_time),15,2)||substr(char(input_time),18,2)
                           ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                           ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                           ,substr(char(process_time),1,4)||substr(char(process_time),6,2)||substr(char(process_time),9,2)
                           ,substr(char(process_time),12,2)||substr(char(process_time),15,2)||substr(char(process_time),18,2)
                        from 
                          bass2.cdr_ismg_dtl_${timestamp} 
                        where 
                          substr(ser_code,1,5) in ('12590')"
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