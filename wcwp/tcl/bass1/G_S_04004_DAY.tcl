######################################################################################################
#接口名称：彩信话单
#接口编码：04004
#接口说明：
#程序名称: G_S_04004_DAY.tcl
#功能描述: 生成04004的数据
#运行粒度: 日
#源    表：1.bass2.cdr_mms_yyyymmdd(彩信话单)  
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#  20070902  夏华学
#            将char(usertype_id)置为常值 '0' --普通用户
#            将send_status in (0,1,2,3) AND RTRIM(char(usertype_id)) <> '201'  改为  send_status in (0,1,2,3) 
#  20090901 1.6.2规范去掉imei字段
#  20120509 根据集团核查HQ_业务相关数据 ，修改彩信业务类型，使记录的分类趋于合理
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

	set sql_buff "values ( int(replace(char(current date - 118 days),'-','')) )"
   exec_sql $sql_buff
   set DeletedDate [get_single $sql_buff]
   puts $DeletedDate
	set sql_buff "delete from  bass1.G_S_04004_DAY_STORE2  where time_id= int(replace(char(current date - 118 days),'-',''))"
   exec_sql $sql_buff

	set sql_buff "insert into  bass1.G_S_04004_DAY_STORE2  select * from  bass1.G_S_04004_DAY where time_id= int(replace(char(current date - 118 days),'-','')) "
   exec_sql $sql_buff

	set sql_buff "delete from bass1.g_s_04004_day where time_id= int(replace(char(current date - 118 days),'-','')) "
   exec_sql $sql_buff

	set sql_buff "delete from bass1.g_s_04004_day where time_id=$timestamp" 
	exec_sql $sql_buff            
	
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04004_DAY
                           (
                             time_id
                            ,product_no
                            ,roam_type_id
                            ,user_type
                            ,mm_bill_type
                            ,code_of_prov_prefecture_users
                            ,send_s_addr
                            ,receiver_s_addr
                            ,fwd_product_no
                            ,send_date
                            ,send_time
                            ,info_type
                            ,applcn_type
                            ,fwd_copy_type
                            ,billing_type
                            ,bearer_mode
                            ,call_fee
                            ,info_fee
                            ,mm_len
                            ,mm_send_status
                            ,mmsc_id_of_orig_prty
                            ,mmsc_id_of_receiver
                            ,mm_content_type
                            ,sp_ent_code
                            ,svc_code
                            ,bus_code
                            ,mm_kind
                            ,bus_srv_id
                            ,imsi
                             )
                          select
                            ${timestamp}
                            ,product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0012',char(roam_type)),'500') 
                            ,'0'
                            
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0038',char(mm_type)),'')
                            ,value(char(province_id),'891')
                            ,value(char(send_address),' ')
                            ,value(char(receive_address),' ')
                            ,''
                            ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)			
                            ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0041',CHAR(INFO_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0042',CHAR(APP_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0044',CHAR(TRANSMIT_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0039',CHAR(CHARGE_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0036',CHAR(CARRY_TYPE)),'')
                            ,value(char((charge1 + charge2 + charge3)/10),'0')
                            ,char(charge4/10)
                            ,char(mm_length)
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0037',char(send_status)),'')
                            ,send_mmsc_id
                            ,receive_mmsc_id
                            ,content_type
                            ,value(sp_code,'0')
                            ,value(ser_code,'0')
                            ,value(oper_code,'0')
                            ,char(mm_class)
                            ,case   when drtype_id = 203 then '4' 
									when app_type in (1,2) then '3' 
									when app_type in (3,4) then '2' 
									else '1'
							 end 
                           ,value(imsi,' ')
                         from  
                           bass2.cdr_mms_${timestamp} 
                         where 
                           send_status in (0,1,2,3) and send_mmsc_id is not null"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle


							 
							 
aidb_runstats bass1.g_s_04004_day 3
	return 0
}


 ##~   (0,'终端到终端');
 ##~   (1,'Email(MM3接口的外接应用)到终端');
 ##~   (2,'终端到Email(MM3接口的外接应用)');
 ##~   (3,'(VAS应用到终端');
 ##~   (4,'终端到VAS应用');

##~   (203,'国际彩信话单');
##~   (103,'彩信话单');
##~   (303,'联通的互联彩信话单');

##~   彩信业务类型：
##~   1 国内点对点彩信
##~   2 梦网彩信
##~   3 邮箱彩信
##~   4 国际点对点彩信

##~   select length(SEND_ADDRESS),length(RECEIVE_ADDRESS),
##~   case when drtype_id = 203 then '4' --国际点对点彩信
##~   when app_type in (1,2) then '3' --邮箱彩信
##~   when app_type in (3,4) then '2' --邮箱彩信
##~   else '1' end --国内点对点彩信
##~   ,count(0)
##~   from  bass2.cdr_mms_20120508 
##~   where send_status in (0,1,2,3) and send_mmsc_id is not null
##~   group by length(SEND_ADDRESS),length(RECEIVE_ADDRESS),case when drtype_id = 203 then '4' --国际点对点彩信
##~   when app_type in (1,2) then '3' --邮箱彩信
##~   when app_type in (3,4) then '2' --邮箱彩信
##~   else '1' end order by 3,1,2

##~   1           2           3 4          
##~   ----------- ----------- - -----------
         ##~   11          11 1      349109
          ##~   6          11 2      247931
         ##~   11           6 2         162
         ##~   11          13 3           1
         ##~   11          15 3           1
         ##~   11          16 3           3
         ##~   11          17 3           1
         ##~   11          19 3           1
         ##~   12          12 4           1
         ##~   13          13 4           2
         ##~   14          14 4          16
         ##~   15          12 4           1
         ##~   15          13 4          17
         ##~   15          14 4           4
         ##~   15          15 4           2

##~   select BUS_SRV_ID,count(0)
 ##~   from BASS1.G_S_04004_DAY
                ##~   where time_id= 20120508
##~   group by 	BUS_SRV_ID			
				
				
                            ##~   ,case 
                              ##~   when drtype_id=103 and app_type=0        then '1'
                              ##~   when drtype_id=103 and app_type in (1,2) then '3'
                              ##~   when drtype_id=103 and app_type in (3,4) then '2'
                              ##~   else '4'
							  
							  ##~   svcitem_id

                      ##~   case when drtype_id=103 and sp_code='801234' then 400006
                           ##~   when drtype_id=103 and app_type=0 then 400001
                           ##~   when drtype_id=103 and app_type in (1,2) then 400003
                           ##~   when drtype_id=103 and app_type in (3,4) then 400004
                           ##~   when drtype_id=303                       then 400002
                           ##~   when drtype_id=203                       then 400005
                           ##~   else 400000 end as svcitem_id,
						   
						   ##~   and a.svcitem_id in (400001,400002,400005) 


##~   insert into dim_svc_item (svcitem_id,svcitem_name,svcitem_id2,svcitem_name2,svcitem_id3,svcitem_name3) values (400001,'网内点对点彩信',4,'彩信业务',2,'数据业务');
##~   insert into dim_svc_item (svcitem_id,svcitem_name,svcitem_id2,svcitem_name2,svcitem_id3,svcitem_name3) values (400002,'网外点对点彩信',4,'彩信业务',2,'数据业务');
##~   insert into dim_svc_item (svcitem_id,svcitem_name,svcitem_id2,svcitem_name2,svcitem_id3,svcitem_name3) values (400003,'邮箱彩信',4,'彩信业务',2,'数据业务');
##~   insert into dim_svc_item (svcitem_id,svcitem_name,svcitem_id2,svcitem_name2,svcitem_id3,svcitem_name3) values (400004,'梦网彩信',4,'彩信业务',2,'数据业务');
##~   insert into dim_svc_item (svcitem_id,svcitem_name,svcitem_id2,svcitem_name2,svcitem_id3,svcitem_name3) values (400005,'国际彩信',4,'彩信业务',2,'数据业务');
##~   insert into dim_svc_item (svcitem_id,svcitem_name,svcitem_id2,svcitem_name2,svcitem_id3,svcitem_name3) values (400006,'手机报(梦网彩信)',4,'彩信业务',2,'数据业务');



                            ##~   ,case   when drtype_id = 203 then '4' --国际点对点彩信
									##~   when app_type in (1,2) then '3' --邮箱彩信
									##~   when  ( drtype_id=103 and app_type=0 ) or  (  drtype_id=303   ) then '1' --国内点对点彩信
									##~   else '2' --梦网彩信
							 ##~   end 

##~   select 
##~   case   when drtype_id = 203 then '4' --国际点对点彩信
									##~   when app_type in (1,2) then '3' --邮箱彩信
									##~   when  ( drtype_id=103 and app_type=0 ) or  (  drtype_id=303   ) then '1' --国内点对点彩信
									##~   else '2' --梦网彩信
							 ##~   end 
##~   ,count(0)
##~   from  bass2.cdr_mms_20120508 
##~   where send_status in (0,1,2,3) and send_mmsc_id is not null
##~   group by
##~   case   when drtype_id = 203 then '4' --国际点对点彩信
									##~   when app_type in (1,2) then '3' --邮箱彩信
									##~   when  ( drtype_id=103 and app_type=0 ) or  (  drtype_id=303   ) then '1' --国内点对点彩信
									##~   else '2' --梦网彩信
							 ##~   end 
