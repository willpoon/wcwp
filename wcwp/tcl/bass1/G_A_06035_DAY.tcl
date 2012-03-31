
######################################################################################################		
#�ӿ�����: ʵ������������Ϣ����������                                                               
#�ӿڱ��룺06035                                                                                          
#�ӿ�˵������¼��Ӫ����ί�о�Ӫ���������������24Сʱ����Ӫҵ����ʵ�������Ļ�����Ϣ
#��������: G_A_06035_DAY.tcl                                                                            
#��������: ����06035������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 
##~   1. panzw 20110922	1.7.5 newly added
##~   2. panzw 20111125	�Բ���channel_info�����ʧЧ ('3')
##~   3.panzw 20120331�޸Ľӿ�06035��ʵ������������Ϣ��������������1��	�����ֶΣ���������ϵ�绰�������Ƿ�֧�ֵ���ȡ���������Ƿ��ṩVIP���񡱡����Ƿ�֧���ն����ۡ������Ƿ��ṩ�������񡱣�

#######################################################################################################   



proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-06
   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
    set last_day [GetLastDay [string range $timestamp 0 7]]
    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
		set last_month [GetLastMonth [string range $op_month 0 5]]
    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_A_06035_DAY where time_id=$timestamp"
    puts $sql_buff
    exec_sql $sql_buff



	set sql_buff "ALTER TABLE BASS1.G_A_06035_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	set sql_buff "ALTER TABLE BASS1.G_A_06035_DAY_NEWEST ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	

    set sql_buff "
    insert into BASS1.G_A_06035_DAY_1
		(	 	
			CHANNEL_ID                      /**ʵ��������ʶ:���� **/
			,CHANNEL_TYPE                    /**ʵ����������:������Ϊ�� **/
			,SELF_24_IND                     /**24Сʱ����Ӫҵ���Ͻ�Ӫҵ��������ʶ **/
			,CMCC_ID                         /**����CMCC��Ӫ��˾��ʶ:������Ϊ�� **/
			,COUNTY                          /**��������:������Ϊ�� **/
			,REGION                          /**����/Ƭ������:������Ϊ�� **/
			,CHANNEL_NAME                    /**��������:������Ϊ�� **/
			,CHANNEL_ADDR                    /**������ַ:������Ϊ�� **/
			,CHNL_MANAGER_NAME               /**������������:������Ϊ�� **/
			,CHNL_MANAGER_PHONE              /**����������ϵ�绰:������Ϊ�� **/
			,CHNL_PHONE			              /**������ϵ�绰 1.7.9 **/
			,GEO_ID                          /**����λ������:������Ϊ�� **/
			,AREA_TYPE                       /**������̬:������Ϊ�� **/
			,CHANNEL_BASE_TYPE               /**������������:������Ϊ�� **/
			,IF_EX                           /**�Ƿ�����:������Ϊ�� **/
			,IF_MOB_SALEHALL                 /**�Ƿ�Ϊ�ֻ�����:������Ϊ�� **/
			,IF_PICKUP			              /**�Ƿ��ṩ����ȡ�� 1.7.9 **/
			,IF_SRV_VIP			              /**�Ƿ��ṩVIP���� 1.7.9 **/
			,IF_SALE_TERM			          /**�Ƿ�֧���ն����� 1.7.9 **/
			,IF_SRV_ACROSS			           /**�Ƿ��ṩ�������� 1.7.9 **/			
			,CHNL_STAR                       /**�����Ǽ�:       **/
			,CHNL_STATE                      /**����״̬:������Ϊ�� **/
			,OPEN_TIME                       /**Ӫҵ��ʼʱ��:������Ϊ�� **/
			,CLOSE_TIME                      /**Ӫҵ����ʱ��:������Ϊ�� **/
			,CONTRACT_EFF_DT                 /**Э��ǩ����Ч����:������Ϊ�� **/
			,CONTRACT_END_DT                 /**Э���ֹ����:������Ϊ�� **/
			,CO_OP_DUR                       /**�Ѻ�������:     **/
			,LONGTITUDE                      /**����:���ǵ�С����λ���ӿ��и��ֶγ���Ϊ10�� **/
			,LATITUDE                        /**γ��:���ǵ�С����λ���ӿ��и��ֶγ���Ϊ10�� **/
			,ZX_INVEST_FEE                   /**װ���ۼ�Ͷ���ܶ�: **/
			,SB_INVEST_FEE                   /**�豸�ۼ�Ͷ���ܶ�: **/
			,OFFICE_INVEST_FEE               /**�칫��Ӫҵ�Ҿ��ۼ�Ͷ���ܶ�: **/
			,SUBSIDY_FEE                     /**һ������ͷ����: **/
		  )
	SELECT
		trim(char(a.channel_id)) CHANNEL_ID
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101') CMCC_ID
		,value(b.county_name,'δ֪') COUNTY
		,value(c.thorpe_name,'δ֪') REGION
		,value(a.channel_name,'δ֪') CHANNEL_NAME
		,value(a.channel_address,'δ֪') CHANNEL_ADDR
		,'δ֪' CHNL_MANAGER_NAME 
		,value(a.TEL_NUMBER,'δ֪') CHNL_MANAGER_PHONE
		,value(substr(replace(a.TEL_NUMBER,'-',''),1,11),'δ֪') CHNL_PHONE  /**������ϵ�绰 1.7.9 **/
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  GEO_ID
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end AREA_TYPE
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end CHANNEL_BASE_TYPE
		,case when a.is_exclude=1 then '1' else '0' end IF_EX
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '0' else '' end 	IF_PICKUP
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '0' else '' end	IF_SRV_VIP
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '0' else '' end	IF_SALE_TERM
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '0' else '' end	IF_SRV_ACROSS
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then ''
			    else case when value(trim(char(channel_level)),'6') > '6' then '6' else value(trim(char(channel_level)),'6')
		 end end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,value (a1.longitude,
			case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  
       ) longitude
		,value (a1.latitude,
		case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  
			) latitude
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dim_channel_info a
	left join (select channel_id,LONGITUDE,LATITUDE
			from 
			(			
			select channel_id
			,LONGTITUDE LONGITUDE
			,LATITUDE
			,row_number()over(partition by channel_id order by time_id desc ) rn 
			from g_a_06035_day where time_id <= $last_day
			)  t where rn = 1
		   ) a1 on trim(char(a.channel_id)) = a1.channel_id	
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
 with ur
 "


  exec_sql $sql_buff



    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_A_06035_DAY
select $timestamp time_id 
,t.* from table(
       SELECT  
	a.*
	from bass1.G_A_06035_DAY_1 a
		except 
      select 
	CHANNEL_ID
        ,CHANNEL_TYPE
        ,SELF_24_IND
        ,CMCC_ID
        ,COUNTY
        ,REGION
        ,CHANNEL_NAME
        ,CHANNEL_ADDR
        ,CHNL_MANAGER_NAME
        ,CHNL_MANAGER_PHONE
		,CHNL_PHONE
        ,GEO_ID
        ,AREA_TYPE
        ,CHANNEL_BASE_TYPE
        ,IF_EX
        ,IF_MOB_SALEHALL
		,IF_PICKUP
		,IF_SRV_VIP
		,IF_SALE_TERM
		,IF_SRV_ACROSS		
        ,CHNL_STAR
        ,CHNL_STATE
        ,OPEN_TIME
        ,CLOSE_TIME
        ,CONTRACT_EFF_DT
        ,CONTRACT_END_DT
        ,CO_OP_DUR
        ,LONGTITUDE
        ,LATITUDE
        ,ZX_INVEST_FEE
        ,SB_INVEST_FEE
        ,OFFICE_INVEST_FEE
        ,SUBSIDY_FEE
	from 
	( select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
	from G_A_06035_DAY a 
	) o where o.rn = 1 
) t
 with ur
 "

  exec_sql $sql_buff
  
 #2011.11.25  ����ʧЧ������ʧЧ  (��channel_info �����Ҳ�������ΪʧЧ)
 
 
     set sql_buff "
insert into G_A_06035_DAY_NEWEST
select distinct
         TIME_ID
        ,CHANNEL_ID
        ,CHANNEL_TYPE
        ,SELF_24_IND
        ,CMCC_ID
        ,COUNTY
        ,REGION
        ,CHANNEL_NAME
        ,CHANNEL_ADDR
        ,CHNL_MANAGER_NAME
        ,CHNL_MANAGER_PHONE
		,CHNL_PHONE		
        ,GEO_ID
        ,AREA_TYPE
        ,CHANNEL_BASE_TYPE
        ,IF_EX
        ,IF_MOB_SALEHALL
		,IF_PICKUP
		,IF_SRV_VIP
		,IF_SALE_TERM
		,IF_SRV_ACROSS				
        ,CHNL_STAR
        ,CHNL_STATE
        ,OPEN_TIME
        ,CLOSE_TIME
        ,CONTRACT_EFF_DT
        ,CONTRACT_END_DT
        ,CO_OP_DUR
        ,LONGTITUDE
        ,LATITUDE
        ,ZX_INVEST_FEE
        ,SB_INVEST_FEE
        ,OFFICE_INVEST_FEE
        ,SUBSIDY_FEE
from (
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id  <=  $timestamp
) t where t.rn =1 and CHNL_STATE = '1'
 with ur
 "
 
 exec_sql $sql_buff

     set sql_buff "
insert into  G_A_06035_DAY
select distinct 
        $timestamp TIME_ID
        ,CHANNEL_ID
        ,CHANNEL_TYPE
        ,SELF_24_IND
        ,CMCC_ID
        ,COUNTY
        ,REGION
        ,CHANNEL_NAME
        ,CHANNEL_ADDR
        ,CHNL_MANAGER_NAME
        ,CHNL_MANAGER_PHONE
		,CHNL_PHONE
        ,GEO_ID
        ,AREA_TYPE
        ,CHANNEL_BASE_TYPE
        ,IF_EX
        ,IF_MOB_SALEHALL
		,IF_PICKUP
		,IF_SRV_VIP
		,IF_SALE_TERM
		,IF_SRV_ACROSS				
        ,CHNL_STAR
        ,'3' CHNL_STATE
        ,OPEN_TIME
        ,CLOSE_TIME
        ,CONTRACT_EFF_DT
        ,CONTRACT_END_DT
        ,CO_OP_DUR
        ,LONGTITUDE
        ,LATITUDE
        ,ZX_INVEST_FEE
        ,SB_INVEST_FEE
        ,OFFICE_INVEST_FEE
        ,SUBSIDY_FEE
from G_A_06035_DAY_NEWEST a
where char(a.channel_id) not in 
(
select char(a.channel_id)
from bass2.dim_channel_info a
where a.channel_type_class in (90105,90102)
)
with ur     
 "
 
 exec_sql $sql_buff
 
##
##if { $timestamp == 20111001  } {
##
##  #1.���chkpkunique
##	set tabname "G_A_06035_DAY"
##	set pk 			"CHANNEL_ID"
##	chkpkunique ${tabname} ${pk} ${timestamp}
##
##	set sql_buff "
##    update G_A_06035_DAY
##    set time_id = 20111001
##    where time_id < 20111001
## "
##  exec_sql $sql_buff
##  
##  
##}
  #���н�����ݼ��

#1.���chkpkunique
	set tabname "G_A_06035_DAY"
	set pk 			"CHANNEL_ID"
	chkpkunique ${tabname} ${pk} ${timestamp}
  

	return 0
}






##~   rename bass1.G_A_06035_DAY_1 to G_A_06035_DAY_1_old20120331
##~   CREATE TABLE "BASS1   "."G_A_06035_DAY_1"  (
                  ##~   "CHANNEL_ID" CHAR(40) , 
                  ##~   "CHANNEL_TYPE" CHAR(1) , 
                  ##~   "SELF_24_IND" CHAR(25) , 
                  ##~   "CMCC_ID" CHAR(5) , 
                  ##~   "COUNTY" CHAR(30) , 
                  ##~   "REGION" CHAR(50) , 
                  ##~   "CHANNEL_NAME" CHAR(100) , 
                  ##~   "CHANNEL_ADDR" CHAR(100) , 
                  ##~   "CHNL_MANAGER_NAME" CHAR(40) , 
                  ##~   "CHNL_MANAGER_PHONE" CHAR(40) , 
                  ##~   "CHNL_PHONE" CHAR(15) , --������ϵ�绰                  
                  ##~   "GEO_ID" CHAR(1) , 
                  ##~   "AREA_TYPE" CHAR(1) , 
                  ##~   "CHANNEL_BASE_TYPE" CHAR(1) , 
                  ##~   "IF_EX" CHAR(1) , 
                  ##~   "IF_MOB_SALEHALL" CHAR(1) , 
                  ##~   "IF_PICKUP" CHAR(1) , --�Ƿ��ṩ����ȡ��
                  ##~   "IF_SRV_VIP" CHAR(1) , --�Ƿ��ṩVIP����
                  ##~   "IF_SALE_TERM" CHAR(1) , --�Ƿ�֧���ն�����
                  ##~   "IF_SRV_ACROSS" CHAR(1) , --�Ƿ��ṩ��������                  
                  ##~   "CHNL_STAR" CHAR(1) , 
                  ##~   "CHNL_STATE" CHAR(1) , 
                  ##~   "OPEN_TIME" CHAR(4) , 
                  ##~   "CLOSE_TIME" CHAR(4) , 
                  ##~   "CONTRACT_EFF_DT" CHAR(8) , 
                  ##~   "CONTRACT_END_DT" CHAR(8) , 
                  ##~   "CO_OP_DUR" CHAR(4) , 
                  ##~   "LONGTITUDE" CHAR(10) , 
                  ##~   "LATITUDE" CHAR(10) , 
                  ##~   "ZX_INVEST_FEE" CHAR(10) , 
                  ##~   "SB_INVEST_FEE" CHAR(10) , 
                  ##~   "OFFICE_INVEST_FEE" CHAR(10) , 
                  ##~   "SUBSIDY_FEE" CHAR(10) )   
                 ##~   DISTRIBUTE BY HASH("CHANNEL_ID")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



##~   rename bass1.G_A_06035_DAY_NEWEST to G_A_06035_DAY_NEWEST_old20120331
##~   CREATE TABLE "BASS1   "."G_A_06035_DAY_NEWEST"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "CHANNEL_ID" CHAR(40) , 
                  ##~   "CHANNEL_TYPE" CHAR(1) , 
                  ##~   "SELF_24_IND" CHAR(25) , 
                  ##~   "CMCC_ID" CHAR(5) , 
                  ##~   "COUNTY" CHAR(30) , 
                  ##~   "REGION" CHAR(50) , 
                  ##~   "CHANNEL_NAME" CHAR(100) , 
                  ##~   "CHANNEL_ADDR" CHAR(100) , 
                  ##~   "CHNL_MANAGER_NAME" CHAR(40) , 
                  ##~   "CHNL_MANAGER_PHONE" CHAR(40) , 
                  ##~   "CHNL_PHONE" CHAR(15) , --������ϵ�绰        				  
                  ##~   "GEO_ID" CHAR(1) , 
                  ##~   "AREA_TYPE" CHAR(1) , 
                  ##~   "CHANNEL_BASE_TYPE" CHAR(1) , 
                  ##~   "IF_EX" CHAR(1) , 
                  ##~   "IF_MOB_SALEHALL" CHAR(1) , 
                  ##~   "IF_PICKUP" CHAR(1) , --�Ƿ��ṩ����ȡ��
                  ##~   "IF_SRV_VIP" CHAR(1) , --�Ƿ��ṩVIP����
                  ##~   "IF_SALE_TERM" CHAR(1) , --�Ƿ�֧���ն�����
                  ##~   "IF_SRV_ACROSS" CHAR(1) , --�Ƿ��ṩ��������    				  
                  ##~   "CHNL_STAR" CHAR(1) , 
                  ##~   "CHNL_STATE" CHAR(1) , 
                  ##~   "OPEN_TIME" CHAR(4) , 
                  ##~   "CLOSE_TIME" CHAR(4) , 
                  ##~   "CONTRACT_EFF_DT" CHAR(8) , 
                  ##~   "CONTRACT_END_DT" CHAR(8) , 
                  ##~   "CO_OP_DUR" CHAR(4) , 
                  ##~   "LONGTITUDE" CHAR(10) , 
                  ##~   "LATITUDE" CHAR(10) , 
                  ##~   "ZX_INVEST_FEE" CHAR(10) , 
                  ##~   "SB_INVEST_FEE" CHAR(10) , 
                  ##~   "OFFICE_INVEST_FEE" CHAR(10) , 
                  ##~   "SUBSIDY_FEE" CHAR(10) )   
                 ##~   DISTRIBUTE BY HASH("CHANNEL_ID")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



##~   RENAME "BASS1   "."G_A_06035_DAY" TO G_A_06035_DAY_OLD20120331
##~   CREATE TABLE "BASS1   "."G_A_06035_DAY"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "CHANNEL_ID" CHAR(40) , 
                  ##~   "CHANNEL_TYPE" CHAR(1) , 
                  ##~   "SELF_24_IND" CHAR(25) , 
                  ##~   "CMCC_ID" CHAR(5) , 
                  ##~   "COUNTY" CHAR(30) , 
                  ##~   "REGION" CHAR(50) , 
                  ##~   "CHANNEL_NAME" CHAR(100) , 
                  ##~   "CHANNEL_ADDR" CHAR(100) , 
                  ##~   "CHNL_MANAGER_NAME" CHAR(40) , 
                  ##~   "CHNL_MANAGER_PHONE" CHAR(40) , 
                  ##~   "CHNL_PHONE" CHAR(15) , --������ϵ�绰
                  ##~   "GEO_ID" CHAR(1) , 
                  ##~   "AREA_TYPE" CHAR(1) , 
                  ##~   "CHANNEL_BASE_TYPE" CHAR(1) , 
                  ##~   "IF_EX" CHAR(1) , 
                  ##~   "IF_MOB_SALEHALL" CHAR(1) , 
                  ##~   "IF_PICKUP" CHAR(1) , --�Ƿ��ṩ����ȡ��
                  ##~   "IF_SRV_VIP" CHAR(1) , --�Ƿ��ṩVIP����
                  ##~   "IF_SALE_TERM" CHAR(1) , --�Ƿ�֧���ն�����
                  ##~   "IF_SRV_ACROSS" CHAR(1) , --�Ƿ��ṩ��������
                  ##~   "CHNL_STAR" CHAR(1) , 
                  ##~   "CHNL_STATE" CHAR(1) , 
                  ##~   "OPEN_TIME" CHAR(4) , 
                  ##~   "CLOSE_TIME" CHAR(4) , 
                  ##~   "CONTRACT_EFF_DT" CHAR(8) , 
                  ##~   "CONTRACT_END_DT" CHAR(8) , 
                  ##~   "CO_OP_DUR" CHAR(4) , 
                  ##~   "LONGTITUDE" CHAR(10) , 
                  ##~   "LATITUDE" CHAR(10) , 
                  ##~   "ZX_INVEST_FEE" CHAR(10) , 
                  ##~   "SB_INVEST_FEE" CHAR(10) , 
                  ##~   "OFFICE_INVEST_FEE" CHAR(10) , 
                  ##~   "SUBSIDY_FEE" CHAR(10) )   
                 ##~   DISTRIBUTE BY HASH("CHANNEL_ID")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

##~   insert into G_A_06035_DAY
##~   (
		##~   TIME_ID
        ##~   ,CHANNEL_ID
        ##~   ,CHANNEL_TYPE
        ##~   ,SELF_24_IND
        ##~   ,CMCC_ID
        ##~   ,COUNTY
        ##~   ,REGION
        ##~   ,CHANNEL_NAME
        ##~   ,CHANNEL_ADDR
        ##~   ,CHNL_MANAGER_NAME
        ##~   ,CHNL_MANAGER_PHONE
		##~   ,	 CHNL_PHONE
        ##~   ,GEO_ID
        ##~   ,AREA_TYPE
        ##~   ,CHANNEL_BASE_TYPE
        ##~   ,IF_EX
        ##~   ,IF_MOB_SALEHALL
		##~   ,	 IF_PICKUP
		##~   ,	 IF_SRV_VIP
		##~   ,	 IF_SALE_TERM
		##~   ,	 IF_SRV_ACROSS
        ##~   ,CHNL_STAR
        ##~   ,CHNL_STATE
        ##~   ,OPEN_TIME
        ##~   ,CLOSE_TIME
        ##~   ,CONTRACT_EFF_DT
        ##~   ,CONTRACT_END_DT
        ##~   ,CO_OP_DUR
        ##~   ,LONGTITUDE
        ##~   ,LATITUDE
        ##~   ,ZX_INVEST_FEE
        ##~   ,SB_INVEST_FEE
        ##~   ,OFFICE_INVEST_FEE
        ##~   ,SUBSIDY_FEE
##~   )
##~   select 
		##~   TIME_ID
        ##~   ,CHANNEL_ID
        ##~   ,CHANNEL_TYPE
        ##~   ,SELF_24_IND
        ##~   ,CMCC_ID
        ##~   ,COUNTY
        ##~   ,REGION
        ##~   ,CHANNEL_NAME
        ##~   ,CHANNEL_ADDR
        ##~   ,CHNL_MANAGER_NAME
        ##~   ,CHNL_MANAGER_PHONE
		##~   ,'' CHNL_PHONE		
        ##~   ,GEO_ID
        ##~   ,AREA_TYPE
        ##~   ,CHANNEL_BASE_TYPE
        ##~   ,IF_EX
        ##~   ,IF_MOB_SALEHALL
		##~   ,'0' IF_PICKUP
		##~   ,'0' IF_SRV_VIP
		##~   ,'0' IF_SALE_TERM
		##~   ,'0' IF_SRV_ACROSS		
        ##~   ,CHNL_STAR
        ##~   ,CHNL_STATE
        ##~   ,OPEN_TIME
        ##~   ,CLOSE_TIME
        ##~   ,CONTRACT_EFF_DT
        ##~   ,CONTRACT_END_DT
        ##~   ,CO_OP_DUR
        ##~   ,LONGTITUDE
        ##~   ,LATITUDE
        ##~   ,ZX_INVEST_FEE
        ##~   ,SB_INVEST_FEE
        ##~   ,OFFICE_INVEST_FEE
        ##~   ,SUBSIDY_FEE
##~   from 	G_A_06035_DAY_OLD20120331

