######################################################################################################
#接口名称：
#接口编码：
#接口说明：
#程序名称: INT_21007_YYYYMM.tcl
#功能描述: 生成21007的中间表数据
#运行粒度: 日
#源    表：1.bass2.cdr_sms_dtl_yyyymmdd
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 增加移动秘书，语音短信等梦网短信 20080202  
#修改历史: liuzhilong 20090806 修改  cdr_type_id 取值维度
#          2010-01-24 修改客户到达数据、在网客户数的口径 userstatus_id in (1,2,3,6,8)    
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.int_21007_$op_month where op_time=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_21007_$op_month
                (
                OP_TIME
                ,PRODUCT_NO
                ,BRAND_ID
                ,SVC_TYPE_ID
                ,CDR_TYPE_ID
                ,END_STATUS
                ,ADVERSARY_ID
                ,SMS_COUNTS
                ,SMS_BASE_FEE
                ,SMS_INFO_FEE
                ,SMS_MONTH_FEE
                )
           SELECT
		  $timestamp		               as op_time
		  ,product_no      		       as product_no
		  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id
      ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0010',char(svcitem_id)),'70') as svc_type_id
		  ,case 
		     when svcitem_id=200003 and calltype_id=0 then '21'
		     when svcitem_id=200003 and calltype_id<>0 then '28'
		     when svcitem_id not in (200003) and calltype_id=0 then '00'
		     when svcitem_id not in (200003) and calltype_id<>0 then '01'
		     else '01' 
		   end	               as cdr_type_id
		  ,'0'		               as end_status
		  ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
          when opp_number like '%156%'  then  '021100' 
          when opposite_id=13 and opp_number not like '%156%'   then   '021900' 
          when opposite_id=2 then   '022000'            
          when opp_number like '%133%'  then  '031100'   
          when opposite_id=17  then  '031200'
          when opposite_id =14 and opp_number not like '%133%' then  '031900'     
          when opposite_id=1 then   '032000'
          when opposite_id in (4,116)  then   '033000'   
          when opposite_id=15 then   '034000'   
          when opposite_id=3  then  '051000'  
          when opposite_id=8   then '080000'
      else '990000'
            end			as adversary_id
		  ,count(*)	                as sms_counts
		  ,sum(base_fee*100)	as sms_base_fee
		  ,0		as sms_info_fee
		  ,0		as sms_month_fee
		from 
	          bass2.cdr_sms_dtl_$timestamp
	  where svcitem_id <> 200006
		group by
		  product_no
		  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') 
		  ,svcitem_id
		  ,case 
		     when svcitem_id=200003 and calltype_id=0 then '21'
		     when svcitem_id=200003 and calltype_id<>0 then '28'
		     when svcitem_id not in (200003) and calltype_id=0 then '00'
		     when svcitem_id not in (200003) and calltype_id<>0 then '01'
		     else '01' 
		   end	 	             
		  ,final_state
		  		  ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
          when opp_number like '%156%'  then  '021100' 
          when opposite_id=13 and opp_number not like '%156%'   then   '021900' 
          when opposite_id=2 then   '022000'            
          when opp_number like '%133%'  then  '031100'   
          when opposite_id=17  then  '031200'
          when opposite_id =14 and opp_number not like '%133%' then  '031900'     
          when opposite_id=1 then   '032000'
          when opposite_id in (4,116)  then   '033000'   
          when opposite_id=15 then   '034000'   
          when opposite_id=3  then  '051000'  
          when opposite_id=8   then '080000'
      else '990000'
            end	"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}     
	aidb_commit $conn
	aidb_close $handle


##100003,'移动秘书(语音)(12580)' 
##100021,'全球呼自动秘书(1259,12591)'
##100025,'呼转短信'
##100027,'语音短信(语音清单)'
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_21007_$op_month
                (
                OP_TIME
                ,PRODUCT_NO
                ,BRAND_ID
                ,SVC_TYPE_ID
                ,CDR_TYPE_ID
                ,END_STATUS
                ,ADVERSARY_ID
                ,SMS_COUNTS
                ,SMS_BASE_FEE
                ,SMS_INFO_FEE
                ,SMS_MONTH_FEE
                )
           SELECT
		  $timestamp		             as op_time
		  ,a.product_no      		       as product_no
		  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') as brand_id
      ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0010',char(a.svcitem_id)),'70') as svc_type_id
		  ,case 
		     when svcitem_id=200003 and calltype_id=0 then '21'
		     when svcitem_id=200003 and calltype_id<>0 then '28'
		     when svcitem_id not in (200003) and calltype_id=0 then '00'
		     when svcitem_id not in (200003) and calltype_id<>0 then '01'
		     else '01' 
		   end	        as cdr_type_id
		  ,'0'		               as end_status  
		  ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
          when opposite_id=2 then   '022000'            
          when opposite_id=17  then  '031200'
          when opposite_id=1 then   '032000'
          when opposite_id in (4,116) then   '033000'   
          when opposite_id=15 then   '034000'   
          when opposite_id=3  then  '051000'  
          when opposite_id=8   then '080000'
      else '990000'
            end			as adversary_id
		  ,count(*)	                as sms_counts
		  ,sum(a.base_fee*100)	as sms_base_fee
		  ,0		as sms_info_fee
		  ,0		as sms_month_fee
		from 
	          bass2.dw_newbusi_call_$timestamp a, bass2.dw_product_$timestamp b 
                 where b.usertype_id in (1,2,9) and b.USERSTATUS_ID in (1,2,3,6,8)
                 and a.user_id=b.user_id and a.SVCITEM_ID in (100003,100021,100025,100027)  
		group by
		  a.product_no
		  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') 
		  ,svcitem_id
		  ,case 
		     when svcitem_id=200003 and calltype_id=0 then '21'
		     when svcitem_id=200003 and calltype_id<>0 then '28'
		     when svcitem_id not in (200003) and calltype_id=0 then '00'
		     when svcitem_id not in (200003) and calltype_id<>0 then '01'
		     else '01' 
		   end	              
		  ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
          when opposite_id=2 then   '022000'            
          when opposite_id=17  then  '031200'
          when opposite_id=1 then   '032000'
          when opposite_id in (4,116)  then   '033000'   
          when opposite_id=15 then   '034000'   
          when opposite_id=3  then  '051000'  
          when opposite_id=8   then '080000'
      else '990000'
            end"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}     
	aidb_commit $conn
	aidb_close $handle
	
#集团客户短信(梦网下行话单)	
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_21007_$op_month
                (
                OP_TIME
                ,PRODUCT_NO
                ,BRAND_ID
                ,SVC_TYPE_ID
                ,CDR_TYPE_ID
                ,END_STATUS
                ,ADVERSARY_ID
                ,SMS_COUNTS
                ,SMS_BASE_FEE
                ,SMS_INFO_FEE
                ,SMS_MONTH_FEE
                )
           SELECT
		  $timestamp		             as op_time
		  ,a.product_no      		       as product_no
		  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') as brand_id
      ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0010',char(a.svcitem_id)),'70') as svc_type_id
		  ,case 
		     when svcitem_id=200003 and calltype_id=0 then '21'
		     when svcitem_id=200003 and calltype_id<>0 then '28'
		     when svcitem_id not in (200003) and calltype_id=0 then '00'
		     when svcitem_id not in (200003) and calltype_id<>0 then '01'
		     else '01' 
		   end	           as cdr_type_id
		  ,'0'		               as end_status
		  ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
          when opp_number like '%156%'  then  '021100' 
          when opposite_id=13 and opp_number not like '%156%'   then   '021900' 
          when opposite_id=2 then   '022000'            
          when opp_number like '%133%'  then  '031100'   
          when opposite_id=17  then  '031200'
          when opposite_id =14 and opp_number not like '%133%' then  '031900'     
          when opposite_id=1 then   '032000'
          when opposite_id in (4,116)  then   '033000'   
          when opposite_id=15 then   '034000'   
          when opposite_id=3  then  '051000'  
          when opposite_id=8   then '080000'
      else '990000'
            end			as adversary_id
		  ,count(*)	                as sms_counts
		  ,sum(a.base_fee*100)	as sms_base_fee
		  ,0		as sms_info_fee
		  ,0		as sms_month_fee
		from 
	          bass2.dw_newbusi_ismg_$timestamp a, bass2.dw_product_$timestamp b 
                 where b.USERSTATUS_ID in (1,2,3,6,8) and a.user_id=b.user_id and a.SVCITEM_ID in (300009)  
		group by
		  a.product_no
		  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') 
		  ,svcitem_id
		  ,case 
		     when svcitem_id=200003 and calltype_id=0 then '21'
		     when svcitem_id=200003 and calltype_id<>0 then '28'
		     when svcitem_id not in (200003) and calltype_id=0 then '00'
		     when svcitem_id not in (200003) and calltype_id<>0 then '01'
		     else '01' 
		   end	               
		   ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
          when opp_number like '%156%'  then  '021100' 
          when opposite_id=13 and opp_number not like '%156%'   then   '021900' 
          when opposite_id=2 then   '022000'            
          when opp_number like '%133%'  then  '031100'   
          when opposite_id=17  then  '031200'
          when opposite_id =14 and opp_number not like '%133%' then  '031900'     
          when opposite_id=1 then   '032000'
          when opposite_id in (4,116)  then   '033000'   
          when opposite_id=15 then   '034000'   
          when opposite_id=3  then  '051000'  
          when opposite_id=8   then '080000'
      else '990000'
            end"
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