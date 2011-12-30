######################################################################################################
#接口名称：
#接口编码：
#接口说明：
#程序名称: INT_210012916_YYYYMM.tcl
#功能描述: 生成21001,21002,21009,21016的中间表的数据(同时21003_to_day的数据也走这个表)
#运行粒度: 日
#源    表：1.bass2.cdr_call_dtl_yyyymmdd
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 20090422  优惠前呼转第二段长途费 和  优惠后呼转第二段长途费  增加漫游限制  夏华学
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.int_210012916_${op_month} where op_time=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_210012916_${op_month}
		(
			op_time
			,user_id
			,product_no
			,brand_id
			,svc_type_id
			,toll_type_id
			,ip_type_id
			,adversary_id
			,roam_type_id
			,call_type_id
			,call_counts
			,base_bill_duration
			,toll_bill_duration
			,call_duration
			,base_call_fee
			,toll_call_fee
			,callfw_toll_fee
			,call_fee
			,favoured_basecall_fee
			,favoured_tollcall_fee
			,favoured_callfw_tollfee
			,favoured_call_fee
			,free_duration
			,favour_duration
			,CALLMOMENT_ID
			,svcitem_id
			,MNS_TYPE
      ,OPP_PROPERTY
		)
         select
           $timestamp
	   ,user_id		  as user_id
	   ,product_no		  as product_no
	   ,brand_id                      as brand_id
           ,case
              when substr(opp_number,1,5)='12590' or substr(opp_number,1,5)='12596' then '003'
	      when substr(opp_number,1,5)='12586' then  '004'
	      when substr(opp_number,1,5)='12597' then  '005'
	      when substr(opp_number,1,5)='12598' then  '006'
	      when substr(opp_number,1,5)='12580' then  '013'
	      when substr(opp_number,1,5)='12530' then  '014'
	      when substr(opp_number,1,5)='17266' then '018'
	      else '009'
	    end  as svc_type_id	   
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0001',char(tolltype_id)),'010') as toll_type_id
           ,case
              when substr(opp_number,1,5)='17950' then '2101'
              when substr(opp_number,1,5)='17951' then '2102'
              when substr(opp_number,1,5)='17200' then '2201'
              when substr(opp_number,1,5)='17201' then '2202'
              when substr(opp_number,1,5)='17202' then '2203'
              when substr(opp_number,1,5)='17255' then '2204'
              when substr(opp_number,1,5)='17910' then '3101'
              when substr(opp_number,1,5)='17911' then '3102'
              when substr(opp_number,1,5)='19300' then '3104'
              when substr(opp_number,1,5)='17900' then '4101'
              when substr(opp_number,1,5)='17901' then '4102'
              when substr(opp_number,1,5)='17908' then '4103'
              when substr(opp_number,1,5)='17909' then '4104'
              when substr(opp_number,1,5)='17968' then '4108'
              when substr(opp_number,1,5)='17969' then '4109'
              when substr(opp_number,1,5)='19730' then '5101'
              when substr(opp_number,1,5)='17931' then '5102'
              when substr(opp_number,1,5)='17920' then '5103'
              when substr(opp_number,1,5)='17921' then '5104'
              when substr(opp_number,1,5)='17960' then '5105'
              when substr(opp_number,1,5)='17961' then '5106'
              when substr(opp_number,1,5)='17990' then '6101'
              when substr(opp_number,1,5)='17991' then '6102'
              when substr(opp_number,1,5)='68300' then '6104'
              when substr(opp_number,1,5)='96168' then '6105'
              when substr(opp_number,1,5)='17970' then '7101'
              when substr(opp_number,1,5)='17971' then '7102'
	      else '1000'
	    end  as ip_type_id
	   
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
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0012',char(roamtype_id)),'500') as roam_type_id
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01') as call_type_id
	   ,count(*)	as call_counts		   
	   ,sum(call_duration_m)		as base_bill_duration
	   ,sum(call_duration_s)*60	as toll_bill_duration
	   ,sum(call_duration)		as call_duration
	   ,sum(basecall_fee+charge1_disc)*100              as base_call_fee
	   ,sum(toll_fee+charge2_disc)*100               as toll_call_fee
	   ,sum(case 
	        when calltype_id in (2,3) and roamtype_id <> 0 then toll_fee+charge2_disc
		else 0
	      end)*100			as callfw_toll_fee
	   ,sum(basecall_fee + toll_fee+other_fee+info_fee+charge1_disc+charge2_disc+charge3_disc+charge4_disc)*100  as call_fee
	   ,sum(basecall_fee)*100	 as favoured_basecall_fee
	   ,sum(toll_fee)*100	                as favoured_tollcall_fee
	   ,sum(case
	   	when calltype_id in (2,3) and roamtype_id <> 0 then toll_fee
	   	else 0
	     end)*100			as favoured_callfw_tollfee
	   ,sum(basecall_fee + toll_fee+other_fee+info_fee)*100	as favoured_call_fee
	   ,sum(case
	   	when bill_mark =0 then call_duration
	   	else 0
	     end)			as free_duration
	   ,0			as favour_duration
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'201')	as callmoment_id
	   ,char(drtype_id) as svcitem_id
	   ,MNS_TYPE
     ,OPP_PROPERTY
        from 
          bass2.cdr_call_dtl_$timestamp
        group by  
	   user_id
	   ,MNS_TYPE
     ,OPP_PROPERTY
	   ,product_no
	   ,brand_id
           ,case
              when substr(opp_number,1,5)='12590' or substr(opp_number,1,5)='12596' then '003'
	      when substr(opp_number,1,5)='12586' then  '004'
	      when substr(opp_number,1,5)='12597' then  '005'
	      when substr(opp_number,1,5)='12598' then  '006'
	      when substr(opp_number,1,5)='12580' then  '013'
	      when substr(opp_number,1,5)='12530' then  '014'
	      when substr(opp_number,1,5)='17266' then '018'
	      else '009'
	    end 
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0001',char(tolltype_id)),'010')
           ,case
              when substr(opp_number,1,5)='17950' then '2101'
              when substr(opp_number,1,5)='17951' then '2102'
              when substr(opp_number,1,5)='17200' then '2201'
              when substr(opp_number,1,5)='17201' then '2202'
              when substr(opp_number,1,5)='17202' then '2203'
              when substr(opp_number,1,5)='17255' then '2204'
              when substr(opp_number,1,5)='17910' then '3101'
              when substr(opp_number,1,5)='17911' then '3102'
              when substr(opp_number,1,5)='19300' then '3104'
              when substr(opp_number,1,5)='17900' then '4101'
              when substr(opp_number,1,5)='17901' then '4102'
              when substr(opp_number,1,5)='17908' then '4103'
              when substr(opp_number,1,5)='17909' then '4104'
              when substr(opp_number,1,5)='17968' then '4108'
              when substr(opp_number,1,5)='17969' then '4109'
              when substr(opp_number,1,5)='19730' then '5101'
              when substr(opp_number,1,5)='17931' then '5102'
              when substr(opp_number,1,5)='17920' then '5103'
              when substr(opp_number,1,5)='17921' then '5104'
              when substr(opp_number,1,5)='17960' then '5105'
              when substr(opp_number,1,5)='17961' then '5106'
              when substr(opp_number,1,5)='17990' then '6101'
              when substr(opp_number,1,5)='17991' then '6102'
              when substr(opp_number,1,5)='68300' then '6104'
              when substr(opp_number,1,5)='96168' then '6105'
              when substr(opp_number,1,5)='17970' then '7101'
              when substr(opp_number,1,5)='17971' then '7102'
	      else '1000'
	    end  
	   ,case when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then     '010000'    
           when opp_number like '%156%'  then  '021100' 
           when opposite_id=13 and opp_number not like '%156%'   then   '021900' 
           when opposite_id=2 then   '022000'            
           when opp_number like '%133%'  then  '031100'   
           when opposite_id=17  then  '031200'
           when opposite_id =14 and opp_number not like '%133%' then  '031900'     
           when opposite_id=1 then   '032000'
           when opposite_id in(4,116)  then   '033000'   
           when opposite_id=15 then   '034000'   
           when opposite_id=3  then  '051000'  
           when opposite_id=8   then '080000'
      else '990000' end
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0012',char(roamtype_id)),'500')
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
	   ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'201')
	   ,char(drtype_id) "
         puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  exec db2 connect to bassdb user bass1 using bass1
  exec db2 runstats on table bass1.int_210012916_${op_month} with distribution and detailed indexes all
  exec db2 terminate

	return 0
}
################################参考
#漫入话单在bass2.cdr_call_roamin_yyyymmdd 表中，bass2.cdr_call_dtl_yyyymmdd都是非来访的话单
#0	本地
#1	省内漫游
#2	省际漫入
#3	国际漫入
#4	省际漫出
#5	港澳台国际漫出
#6	省内边界漫游
#7	省际边界漫入
#8	省际边界漫出
#9	非港澳台国际漫出
#100	国际漫出(国际点对点短信)
#101	国际漫入(国际点对点短信)
#        where 
#          roamtype_id in (0,1,4,5,6,7,8)
#select distinct roamtype_id  from bass2.cdr_call_dtl_20070602
#with ur;
#9
#1
#0
#4
#DROP TABLE BASS1.INT_210012916_200703;
#CREATE TABLE BASS1.INT_210012916_200703
# (OP_TIME                  INTEGER,
#  USER_ID                  VARCHAR(13),
#  PRODUCT_NO               VARCHAR(13),
#  BRAND_ID                 SMALLINT,
#  SVC_TYPE_ID              VARCHAR(5),
#  TOLL_TYPE_ID             CHARACTER(3),
#  IP_TYPE_ID               VARCHAR(16),
#  ADVERSARY_ID             VARCHAR(6),
#  ROAM_TYPE_ID             CHARACTER(3),
#  CALL_TYPE_ID             CHARACTER(2),
#  CALL_COUNTS              BIGINT,
#  BASE_BILL_DURATION       BIGINT,
#  TOLL_BILL_DURATION       BIGINT,
#  CALL_DURATION            BIGINT,
#  BASE_CALL_FEE            BIGINT,
#  TOLL_CALL_FEE            BIGINT,
#  CALLFW_TOLL_FEE          BIGINT,
#  CALL_FEE                 BIGINT,
#  FAVOURED_BASECALL_FEE    BIGINT,
#  FAVOURED_TOLLCALL_FEE    BIGINT,
#  FAVOURED_CALLFW_TOLLFEE  BIGINT,
#  FAVOURED_CALL_FEE        BIGINT,
#  FREE_DURATION            BIGINT,
#  FAVOUR_DURATION          BIGINT,
#  CALLMOMENT_ID            CHARACTER(3),
#  SVCITEM_ID               VARCHAR(16)
# )
#  DATA CAPTURE NONE
#  IN TBS_APP_BASS1
#  INDEX IN TBS_INDEX
#  PARTITIONING KEY
#   (OP_TIME,
#    USER_ID,
#    PRODUCT_NO
#   )USING HASHING
#   NOT LOGGED INITIALLY;
################################
#	   ,case   
#	      when opposite_id in (0,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  then '010000'
#	      when opposite_id=13 then '021000'
#	      when opposite_id=14 then '022000'
#	      when opposite_id=2 then '023000'
#	      when opposite_id in(4,116) then '031000'
#	      when opposite_id=1 then '032000'
#	      when opposite_id=3 then '051000'
#	      when opposite_id=15 then '061000'
#	      when opposite_id=115 then '042000'
#	      when opposite_id in (5,6,7,8) then '080000'
#	      when opposite_id=121 then '039000'
#	      else '990000' 
#            end			as adversary_id
#########################