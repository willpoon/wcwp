######################################################################################################
#接口名称：通用下载话单
#接口编码：04007
#接口说明：通用下载话单。
#程序名称: G_S_04007_DAY.tcl
#功能描述: 生成04007的数据
#运行粒度: 日
#源    表：1.bass2.cdr_kj_yyyymmdd(百宝箱Kjava清单)
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: LIUQF20090802-9:32 针对KJAVA中dev_code超出6位的业务，直接cut substr(dev_code,1,6) 
#          20090901 1.6.2规范去掉imei字段
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04007_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
      
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04007_DAY
                (
                time_id                            
                ,product_no                         
                ,usertype_id                        
                ,app_type                           
                ,src_type                           
                ,ota_code                           
                ,sp_code                            
                ,oper_code                          
                ,gate_ip                            
                ,visit_area                         
                ,start_date                         
                ,start_time                         
                ,end_date                           
                ,end_time                           
                ,download_dur                       
                ,valid_use_times                    
                ,valid_use_time                     
                ,flux                               
                ,billing_type                       
                ,info_fee                           
                ,disc_rate                          
                ,bearer_type                        
                ,imsi
                )
              select
                ${timestamp}
                ,product_no
                ,case when brand_id=7 then '2' else '1' end
                ,rtrim(char(app_type))
                ,value(case when src_type=1 then '01'
                     when src_type=10 then '10'
                     when src_type=11 then '11'
                     end,'')
                ,substr(dev_code,1,6)   
                ,sp_code
                ,substr(oper_code,1,10)
                ,'0'
                ,value(char(province_id),'891')
                ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                ,substr(char(stop_time),1,4)||substr(char(stop_time),6,2)||substr(char(stop_time),9,2)
                ,substr(char(stop_time),12,2)||substr(char(stop_time),15,2)||substr(char(stop_time),18,2)
                ,char(dnload_duration)
                ,char(valid_times)
                ,char(online_duration)
                ,value(char(data_size),'')
                ,'0'||char(charge_type)
                ,value(case when charge_type in (1,2,4,6,8) then char(charge4/10) else '0' end,'')
                ,value(char(discount),'')
                ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0004',char(carry_type)),'')
                ,value(imsi,'')
              from 
                bass2.cdr_kj_${timestamp}
              where rtrim(char(app_type)) in ('1','2') and sp_code like '7%' and imsi is not null"
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