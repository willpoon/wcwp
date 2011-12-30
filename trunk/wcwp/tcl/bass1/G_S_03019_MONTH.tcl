
######################################################################################################		
#�ӿ�����: �ص�����ҵ������                                                               
#�ӿڱ��룺03019                                                                                          
#�ӿ�˵�����ص�����ҵ���������й��ƶ����û���ʹ���й��ƶ�����ҵ��������ָ�����������ڡ������ķ���ϸ����Ϣ���Ҿ����������Żݴ���
#��������: G_S_03019_MONTH.tcl                                                                            
#��������: ����03019������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]      
        #���� YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
            
 
        #ɾ����������
	set sql_buff "delete from bass1.G_S_03019_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
  
	set sql_buff "
	insert into bass1.G_S_03019_MONTH
                        select
                          $op_month
                          ,a.user_id
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0117',char(a.item_id)),'0901')
                          ,'$op_month'
                          ,char(bigint(sum(a.fact_fee)*100))
                          ,substr(char(bigint(sum(case when a.fav_fee >0 then a.fav_fee else 0 end)*-100)),1,8) 
                        from   bass2.dw_acct_shoulditem_$op_month a
                        inner join 
	                        (
	                         select user_id from bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9)
	                         except
	                         select user_id from bass2.dw_product_${last_month} where userstatus_id in (0,4,5,7,9)
	                         ) b on a.user_id=b.user_id
                        where
                        a.item_id  in (select  bigint(XZBAS_VALUE) from BASS1.ALL_DIM_LKP   where  bass1_tbid = 'BASS_STD1_0117')
                        group by
                          a.user_id                       
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0117',char(a.item_id)),'0901') 
                          with ur
			  "
  exec_sql $sql_buff

	return 0
}
