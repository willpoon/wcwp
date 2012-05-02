######################################################################################################
#�ӿ����ƣ�
#�ӿڱ��룺
#�ӿ�˵����
#��������: INT_0400810_YYYYMM.tcl
#��������: ����04008,04010���м��
#��������: ��
#Դ    ��1.bass2.cdr_call_dtl_yyyymmdd
#          2.bass1.g_user_lst
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1 �Զ��������� 2��������
#�޸���ʷ: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyy-mm-dd
	set optime $op_time
	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.int_0400810_$op_month where op_time=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle






        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_0400810_$op_month
                   (
                        OP_TIME
                       ,BRAND_ID
                       ,DRTYPE_ID
                       ,PRODUCT_NO
                       ,IMSI
                       ,MSRN
                       ,IMEI
                       ,OPPOSITE_NO
                       ,THIRD_NO
                       ,CITY_ID
                       ,ROAM_LOCN
                       ,OPP_CITY_ID
                       ,OPP_ROAM_LOCN
                       ,ADVERSARY_ID
                       ,ADVERSARY_NET_TYPE
                       ,MSC_CODE
                       ,CELL_ID
                       ,LAC_ID
                       ,OPP_CELL_ID
                       ,OPP_LAC_ID
                       ,OUTGO_TRNK
                       ,INCOMING_TRNK
                       ,START_DATE
                       ,START_TIME
                       ,CALL_DURATION
                       ,BASE_BILL_DURATION
                       ,TOLL_BILL_DURATION
                       ,BILLING_ID
                       ,BASE_CALL_FEE
                       ,TOLL_CALL_FEE
                       ,CALLFW_TOLL_FEE
                       ,INFO_FEE
                       ,BALANCE_FEE
                       ,FAV_BASE_CALL_FEE
                       ,FAV_TOLL_CALL_FEE
                       ,FAV_CALLFW_TOLL_FEE
                       ,FAV_INFO_FEE
                       ,ROAM_TYPE_ID
                       ,TOLL_TYPE_ID
                       ,SVCITEM_ID
                       ,CALL_TYPE_ID
                       ,SERVICE_CODE
                       ,USER_TYPE
                       ,FEE_TYPE
                       ,INNER_ID
                       ,OPER_INNER_ID
                       ,VPMN_GRP_ID
                       ,SCP_ID
                       ,END_CALL_TYPE
                       ,VIDEO_TYPE 
                       ,MNS_TYPE 

               )
                      select
                         $timestamp
                        ,BRAND_ID
                        ,DRTYPE_ID
                        ,PRODUCT_NO
                        ,IMSI
                        ,value(substr(MSRN,1,11),'')
                        ,value(IMEI,' ')
                        ,value(OPP_NUMBER,' ') as OPPOSITE_NO
                        ,value(A_NUMBER,'') as THIRD_NO
                        ,char(int(COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0008',char(CITY_ID)),'891')))
                        ,char(int(COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0008',char(ROAM_CITY_ID)),'891')))
                        ,char(int(COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0008',char(OPP_CITY_ID)),'891')))
                        ,char(int(COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0008',char(OPP_ROAM_CITY_ID)),'891')))
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
                        ,case   when OPPOSITE_ID in (0,13,32,12536,95130,95177,259005,259088,72,12,60,61,62,63,64,65,83,91,101,102,103,104,105,107,108,109,122,81,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,106,78)  THEN '02'
                      		        when OPPOSITE_ID IN (1,2,3,115) then '01'
                      		        when OPPOSITE_ID=14 then '03'
                      		        when OPPOSITE_ID=121  then '05'
                      		        when OPPOSITE_ID in(4,116) then '11'
                      			when OPPOSITE_ID=15    then '99'
                      			else '04'
                      			end as ADVERSARY_NET_TYPE
                        ,value(MSC_ID,' ') as  MSC_CODE
                        ,value(CELL_ID,' ')
                        ,value(LAC_ID,' ')
                        ,'0' as OPP_CELL_ID
                        ,'0' as OPP_LAC_ID
                        ,value(OUT_TRUNKID,' ') as OUTGO_TRNK
                        ,value(IN_TRUNKID,' ') as  INCOMING_TRNK
                        ,substr(char(START_TIME),1,4)||substr(char(START_TIME),6,2)||substr(char(START_TIME),9,2)
                        ,substr(char(START_TIME),12,2)||substr(char(START_TIME),15,2)||substr(char(START_TIME),18,2)
                        ,char(int(call_duration)) as CALL_DURATION
                        ,char(int(call_duration_m)) as BASE_BILL_DURATION
                        ,char(int((call_duration_s)*60)) as TOLL_BILL_DURATION
                        ,char(BILL_MARK) as BILLING_ID
                        ,char(int(basecall_fee)*100) as BASE_CALL_FEE
                        ,char(int(toll_fee)*100) as TOLL_CALL_FEE
                        ,CASE WHEN calltype_id IN (2,3)
                      			THEN char(int((toll_fee+charge2_disc)*100))
                      			ELSE '0'
                      		     END as CALLFW_TOLL_FEE
                        ,char(int((INFO_FEE)*100)) as INFO_FEE
                        ,'0' as BALANCE_FEE
                        ,char(int((charge1_disc)*100)) as FAV_BASE_CALL_FEE
                        ,char(int((charge2_disc)*100)) as FAV_TOLL_CALL_FEE
                        ,CASE WHEN calltype_id IN (2,3)
                      			THEN char(int(charge2_disc*100))
                      			ELSE '0'
                      		     END   as FAV_CALLFW_TOLL_FEE
                        ,char(int((charge4_disc)*100)) as FAV_INFO_FEE
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500') AS ROAM_TYPE_ID
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',char(tolltype_id)),'010') AS TOLL_TYPE_ID
                        ,case
                          when substr(a.opp_number,1,5)='12590' or substr(a.opp_number,1,5)='12596' then '003'
	                  when substr(a.opp_number,1,5)='12586' then  '004'
	                  when substr(a.opp_number,1,5)='12597' then  '005'
	                  when substr(a.opp_number,1,5)='12598' then  '006'
	                  when substr(a.opp_number,1,5)='12580' then  '013'
	                  when substr(a.opp_number,1,5)='12530' then  '014'
	                  when substr(a.opp_number,1,5)='17266' then '018'
	                  else '009'
	                end  AS SVCITEM_ID
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0011',char(calltype_id)),'01') AS CALL_TYPE_ID
                        ,'0021' as SERVICE_CODE
                        ,'0' as USER_TYPE
                        ,'0' as FEE_TYPE
                        ,value(MOC_ID,' ') as INNER_ID
                        ,value(MTC_ID,' ') as  OPER_INNER_ID
                        ,value(ENTERPRISE_ID,' ') as  VPMN_GRP_ID
                        ,value(SCP_ID,' ') as SCP_ID
                        ,case
                          when substr(char(value(STOP_CAUSE,0)),1,1)='0' then '0'
                          else '1'
                         end
                       ,char(a.VIDEO_TYPE) 
                       ,char(a.MNS_TYPE)    
                      from 
                        bass2.cdr_call_dtl_${timestamp} a,
                        bass1.g_user_lst b 
                      where
                        b.time_id=${op_month}
                        and a.user_id=b.user_id and length(OPP_NUMBER) <= 24
						and brand_id is not null"
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
#################################################################
# �ο���
#  TIME_ID
# ,BRAND_ID                 #�û�Ʒ��
# ,DRTYPE_ID                #�������ͣ������޳��������ߡ����Ż���ҵ�񻰵�
# ,PRODUCT_NO               #�Ʒ��ֻ�����
# ,IMSI                     #�ƷѺ����IMSI��
# ,MSRN                     #��̬���κ���
# ,IMEI                     #�ƷѺ�����ƶ��ն��豸��
# ,OPPOSITE_NO              #�Զ˺���
# ,THIRD_NO                 #����������
# ,CITY_ID                  #������
# ,ROAM_LOCN                #���õ�
# ,OPP_CITY_ID              #�Զ˹�����
# ,OPP_ROAM_LOCN            #�Զ˵��õ�
# ,ADVERSARY_ID             #�Զ˵�����Ӫ�̱�ʶ
# ,ADVERSARY_NET_TYPE       #�Զ��������ʹ���
# ,MSC_CODE                 #����������
# ,CELL_ID                  #С������
# ,LAC_ID                   #LAC������
# ,OPP_CELL_ID              #�Զ�С������
# ,OPP_LAC_ID               #�Զ�LAC������
# ,OUTGO_TRNK               #���м̱�ʶ
# ,INCOMING_TRNK            #���м̱�ʶ
# ,START_DATE               #ͨ����ʼ����
# ,START_TIME               #ͨ����ʼʱ��
# ,CALL_DURATION            #ͨ��ʱ��
# ,BASE_BILL_DURATION       #�����ѼƷ�ʱ��
# ,TOLL_BILL_DURATION       #��;�ѼƷ�ʱ��
# ,BILLING_ID               #�Ʒѱ�־
# ,BASE_CALL_FEE            #����ͨ����,Ϊ�Żݺ����
# ,TOLL_CALL_FEE            #��;ͨ����,Ϊ�Żݺ����
# ,CALLFW_TOLL_FEE          #��ת�ڶ��γ�;��
# ,INFO_FEE                 #��Ϣ��(��ҵ������Ϊ"�ƶ�ɳ��"��"������־"ʱ����Ϣ��)
# ,BALANCE_FEE              #���Ѻϼ�
# ,FAV_BASE_CALL_FEE        #����ͨ�����Ż�
# ,FAV_TOLL_CALL_FEE        #��;ͨ�����Ż�
# ,FAV_CALLFW_TOLL_FEE      #��ת�ڶ��γ�;���Ż�
# ,FAV_INFO_FEE             #��Ϣ���Ż�
# ,ROAM_TYPE_ID             #�������ͱ���BASS_STD2_0012
# ,TOLL_TYPE_ID             #��;���ͱ���BASS_STD2_0001
# ,SVCITEM_ID               #ҵ�����ͱ���BASS_STD2_0018
# ,CALL_TYPE_ID             #�������ͱ���BASS_STD2_0011
# ,SERVICE_CODE             #ҵ��������BASS_STD2_0051
# ,USER_TYPE                #�����û����ͱ���BASS_STD2_0049
# ,FEE_TYPE                 #�������ͱ���BASS_STD2_0048
# ,INNER_ID                 #�����ڲ���ʶ
# ,OPER_INNER_ID            #�Զ��ڲ���ʶ
# ,VPMN_GRP_ID              #VPMN�û�Ⱥ��ʶ
# ,SCP_ID                   #SCP��ʶ
# ,END_CALL_TYPE            #ͨ���������ͱ���BASS_STD2_0050
#################################################################