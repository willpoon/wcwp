######################################################################################################
#�ӿ����ƣ�GSM��ͨ��������
#�ӿڱ��룺04008
#�ӿ�˵����
#��������: G_S_04008_DAY.tcl
#��������: ����04008������
#��������: ��
#Դ    ��1.bass1.int_0400810_yyyymm
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: liuzhilong 20090911 ȥ��where ����  and DRTYPE_ID  not in (1700,9901,9902,9903)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyy-mm-dd
  #set op_time 2008-05-31
	set optime $op_time
	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04008_day where time_id=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
           
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04008_DAY
                      ( 
                      time_id
                      ,product_no
                      ,imsi
                      ,msrn
                      ,imei
                      ,opposite_no
                      ,third_no
                      ,city_id
                      ,roam_locn
                      ,opp_city_id
                      ,opp_roam_locn
                      ,adversary_id
                      ,adversary_net_type
                      ,msc_code
                      ,cell_id
                      ,lac_id
                      ,opp_cell_id
                      ,opp_lac_id
                      ,outgo_trnk
                      ,incoming_trnk
                      ,start_date
                      ,start_time
                      ,call_duration
                      ,base_bill_duration
                      ,toll_bill_duration
                      ,billing_id
                      ,base_call_fee
                      ,toll_call_fee
                      ,callfw_toll_fee
                      ,info_fee
                      ,fav_base_call_fee
                      ,fav_toll_call_fee
                      ,fav_callfw_toll_fee
                      ,fav_info_fee
                      ,roam_type_id
                      ,toll_type_id
                      ,svcitem_id
                      ,call_type_id
                      ,service_code
                      ,user_type
                      ,fee_type
                      ,end_call_type
                      ,mns_type
                      ,VIDEO_TYPE
                      )
                     select
                         op_time
                        ,product_no
                        ,imsi
                        ,msrn
                        ,imei
                        ,opposite_no
                        ,third_no
                        ,city_id
                        ,roam_locn
                        ,opp_city_id
                        ,opp_roam_locn
                        ,case when adversary_id= '010000'  then  '010000'  
                             when adversary_id like '02%' then  '020000'
                             when adversary_id like '03%' then  '030000'
                             when adversary_id like '05%' then  '050000'
                         else '990000' end as adversary_id
                        ,adversary_net_type
                        ,msc_code
                        ,cell_id
                        ,lac_id
                        ,opp_cell_id
                        ,opp_lac_id
                        ,outgo_trnk
                        ,incoming_trnk
                        ,start_date
                        ,start_time
                        ,call_duration
                        ,base_bill_duration
                        ,toll_bill_duration
                        ,billing_id
                        ,base_call_fee
                        ,toll_call_fee
                        ,callfw_toll_fee
                        ,info_fee
                        ,fav_base_call_fee
                        ,fav_toll_call_fee
                        ,fav_callfw_toll_fee
                        ,fav_info_fee
                        ,roam_type_id
                        ,toll_type_id
                        ,svcitem_id
                        ,call_type_id
                        ,service_code
                        ,user_type
                        ,fee_type
                        ,end_call_type
                        ,value(MNS_TYPE,'0')
                        ,value(VIDEO_TYPE,'0')
                     from 
                       bass1.int_0400810_$op_month
                     where 
                       op_time=$timestamp

                       "
#  ȥ��where ����  and DRTYPE_ID  not in (1700,9901,9902,9903)

        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}    
	aidb_commit $conn
	aidb_close $handle

## ÿ�����R107���꣬ͨ���˽ű���ÿ�����һ���Զ�����
##��������ھ��б䣬��ô�����δ˶δ��룬�ֹ�����
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
ADJ_04008_R107 $op_time $optime_month
ADJ_04008_R108 $op_time $optime_month

	return 0
}