######################################################################################################
#�ӿ����ƣ� ������GPRS����
#�ӿڱ��룺04018
#�ӿ�˵�����������û���GPRS���� ÿ�ճ�ȡ��������
#��������: G_S_04018_DAY.tcl
#��������:
#��������: ��
#Դ    ��1.
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺20090828
#�����¼��1.
#�޸���ʷ: 1.20091123 and a.apn_ni in ('CMTDS') �ھ��޸�Ϊ��a.drtype_id in (8307) 
#          2.20091201 ���ݼ���Ҫ����� a.apn_ni<>'CMLAP'
#          1.6.5�淶�޸ģ������ֶ�  20100520 �˺���13989088567 ӳ��Ϊ�գ������޳�ext_holds5 is not null
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


	     #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   
        
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
			 #����  yyyymm
			 set last_month [GetLastMonth [string range $op_month 0 5]]
			 #������ yyyymm
			 set last_last_month [GetLastMonth [string range $last_month 0 5]]


  #ɾ����������
	set sql_buff "delete from bass1.G_S_04018_DAY where time_id=$timestamp "
	puts $sql_buff
	exec_sql $sql_buff


	#============����ʽ���в��� ����============================
	#   
	set sql_buff "
	 insert into bass1.G_S_04018_DAY (
					 TIME_ID
					,INTRA_PRODUCT_NO
					,PRODUCT_NO
					,APNNI
					,UP_FLOWS
					,DOWN_FLOWS
					,START_TIME
					,DURATION
					,MNS_TYPE
					,IMEI
					)
          select $timestamp
    			 ,b.ext_holds5
    			 ,a.product_no
    			 ,a.apn_ni
    			 ,char(sum(a.upflow))
    			 ,char(sum(a.downflow))
    			 ,replace(char(date(a.start_time)),'-','')||replace(char(time(a.start_time)),'.','')
    			 ,char(sum(a.duration))
    			 ,value(char(a.mns_type),'0')
    			 ,value(a.imei,'0')
		    from bass2.CDR_GPRS_LOCAL_$timestamp a ,
		         (select ext_holds5
		            	,id_value
		            	,valid_date
		            	,sts  
		            	,row_number() over(partition by ext_holds5 order by valid_date desc ) row_id
					from bass2.dwd_product_regsp_$timestamp
				   where busi_type='737' and ext_holds5 is not null ) b 
		    where a.product_no=b.id_value
		      and a.drtype_id in (8307)
		      and a.apn_ni<>'CMLAP'
		      and b.row_id=1     
		    group by b.ext_holds5
		    		,a.product_no
		    		,a.apn_ni
		    		,a.start_time
		    		,value(char(a.mns_type),'0')
		    		,value(a.imei,'0')
		"
   puts $sql_buff
   exec_sql $sql_buff
aidb_runstats bass1.G_S_04018_DAY 3
	return 0
}


