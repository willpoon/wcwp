######################################################################################################
#�ӿ����ƣ�GPRS����
#�ӿڱ��룺04002
#�ӿ�˵����GPRS������
#��������: G_S_04002_DAY.tcl
#��������: ����04002������
#��������: ��
#Դ    ��1.bass2.cdr_gprs_local_yyyymmdd(GPRS�嵥(���أ����ݼƷ�) )   
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22 
#�����¼��
#�޸���ʷ: liuzhilong 20090630 1.6.0�淶 ȥ��������GPRS����
#          20090901 1.6.2�淶 ����imei�ֶ�char(17)
####       20091123 �޸�ȥ�������������Ŀھ� apn_ni not in ('CMTDS') ��Ϊdrtype_id not in (8307)
####       1.6.5�淶ȥ�����ﹲģ�ն��嵥 bigint(product_no) not between 14734500000 and 14734999999
####            ����ҵ����� service_code �Լ�ɾ��һЩ�ֶ�,����¹淶
####       1.6.7�淶 �޳����ź˼����� service_code not in (1010000001,1010000002,2000000000)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

        global app_name
        set app_name "G_S_04002_DAY.tcl"


		set logFile_prevDay "/bassapp/bass1/tcl/log/G_S_04002_DAY.runstat"
		##~   set logFile_prevDay "/bassapp/bass1/tcl/log/G_S_04002_DAY.tcl.out.r20120430"
		set ModLogTime [clock format [file mtime ${logFile_prevDay}] -format "%Y%m%d"]

        exec /bassapp/bass1/tcl/parse04002log
        exec cat /bassapp/bass1/tcl/log/G_S_04002_DAY.runstat
		
        set gDataDateTime 0

		
        set f [open /bassapp/bass1/tcl/log/G_S_04002_DAY.runstat]
        while { [gets $f DataDateTime] >= 0 } {
        set gDataDateTime $DataDateTime
        }
        close $f
	
		if {$gDataDateTime < $timestamp} {
			set redo_flag 0
		} else {
			set redo_flag 1
		}

##~   --
##~   ���ϴγɹ�������������� С�� ����Ҫ������������ڣ����� ��־�޸�ʱ�� ���� �������� ��rename
##~   ����Ƕ��δ��� ��  ��־�ļ����޸Ĺ�����rename
##~   --
		puts $gDataDateTime
		puts $ModLogTime
		puts "�ж��Ƿ���Ҫ����..."
		if {  $timestamp > $gDataDateTime   } {
			if { $ModLogTime == $timestamp } {
				puts "��������״�ִ��..."
				set sql_buff "rename   BASS1.G_S_04002_DAY_PREV to G_S_04002_DAY_SWAP "
				exec_sql $sql_buff
				set sql_buff "rename   BASS1.G_S_04002_DAY_THIS to G_S_04002_DAY_PREV "
				exec_sql $sql_buff
				set sql_buff "rename   BASS1.G_S_04002_DAY_SWAP to G_S_04002_DAY_THIS "
				exec_sql $sql_buff	
			} else {
				puts "runstat��־�ļ����챻�޸ģ�������շ��״�ִ�У������˴˳���"
			}
		} else {
			puts "�Ѿ�д��G_S_04002_DAY.log��־��������״����У������˴˳���"
		}


	##~   if { $redo_flag == 0 } {

		set sql_buff "alter table bass1.G_S_04002_DAY_THIS activate not logged initially with empty table"
		exec_sql $sql_buff	

		set sql_buff "insert into bass1.G_S_04002_DAY_THIS
						(
						 time_id
						,product_no
						,roam_locn
						,roam_type_id
						,apnni
						,start_time
						,call_duration
						,up_flows
						,down_flows
						,all_fee
						,mns_type
						,imei
						,service_code
						 )
				 select
					  $timestamp
					  ,product_no
					  ,COALESCE(char(roam_city_id),'891') as roam_locn
					  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500') as roam_type_id
					  ,apn_ni
					  ,replace(char(date(start_time)),'-','')||replace(char(time(start_time)),'.','') as start_time
					  ,char(sum(duration))        as call_duration
					  ,char(sum(data_flow_up1+data_flow_up2))   as up_flows
					  ,char(sum(data_flow_down1+data_flow_down2)) as down_flows
					  ,char(sum(charge1+charge2+charge3+charge4)/10) as all_fee
					  ,value(char(mns_type),'0')  as mns_type
					  ,value(char(imei),'0')      as imei
					  ,case when upper(apn_ni)<>'CMWAP' then ''
							when upper(apn_ni)='CMWAP' and service_code in ('999998','999999') then ''
					   else service_code end      as service_code
				from bass2.CDR_GPRS_LOCAL_$timestamp
				where drtype_id not in (8307)
				  and bigint(product_no) not between 14734500000 
				  and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
				  and service_code not in ('1010000001','1010000002','2000000000')
				group by product_no
					  ,COALESCE(char(roam_city_id),'891')
					  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500')
					  ,apn_ni
					  ,start_time
					  ,value(char(mns_type),'0')
					  ,value(char(imei),'0')
					  ,case when upper(apn_ni)<>'CMWAP' then ''
							when upper(apn_ni)='CMWAP' and service_code in ('999998','999999') then ''
					   else service_code end
					with ur
					 "
		exec_sql $sql_buff

set sql_buff "insert into bass1.G_S_04002_DAY_THIS
						(
						 time_id
						,product_no
						,roam_locn
						,roam_type_id
						,apnni
						,start_time
						,call_duration
						,up_flows
						,down_flows
						,all_fee
						,mns_type
						,imei
						,service_code
						 )
				 select
					  $timestamp
					  ,product_no
					  ,COALESCE(char(substr(VPLMN1,3)),'852') as roam_locn
					  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAM_TYPE)),'500') as roam_type_id
					  ,apn_ni
					  ,replace(char(date(start_time)),'-','')||replace(char(time(start_time)),'.','') as start_time
					  ,char(sum(duration))        as call_duration
					  ,char(sum(data_flow_up1+data_flow_up2))   as up_flows
					  ,char(sum(data_flow_down1+data_flow_down2)) as down_flows
					  ,char(sum(charge1+charge2+charge3+charge4)/10) as all_fee
					  ,'0' as mns_type
					  ,' '     as imei
					  ,case when upper(apn_ni)<>'CMWAP' then ''
					   else '4000000001' end      as service_code
				from bass2.cdr_gprs_l_$timestamp
				where drtype_id not in (8307)
				  and bigint(product_no) not between 14734500000 
				  and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
				group by product_no
					  ,COALESCE(char(substr(VPLMN1,3)),'852') 
					  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAM_TYPE)),'500') 
					  ,apn_ni
					  ,start_time
					  ,case when upper(apn_ni)<>'CMWAP' then ''
					   else '4000000001' end 
					with ur
					 "
		exec_sql $sql_buff
					 
	##~   } else {
		##~   puts "G_S_04002_DAY_THIS �Ѿ��ܹ����������ܣ�"
			##~   set grade 2
	        ##~   set alarmcontent "��������04002,���ֹ�����"
	        ##~   WriteAlarm $app_name $timestamp $grade ${alarmcontent}
		
	##~   }



    #ɾ����������
	##~   ɾ������̫��ʱ������������£�����delete
	if { $redo_flag == 0 } {
		set sql_buff "values 1"
	} else {
		set sql_buff "delete from bass1.g_s_04002_day where time_id=$timestamp"
	}
	exec_nolog bass1.g_s_04002_day  $sql_buff


    #���뱾������
	set sql_buff "insert into  bass1.g_s_04002_day
	(
		TIME_ID
        ,PRODUCT_NO
        ,ROAM_LOCN
        ,ROAM_TYPE_ID
        ,APNNI
        ,START_TIME
        ,CALL_DURATION
        ,UP_FLOWS
        ,DOWN_FLOWS
        ,ALL_FEE
        ,MNS_TYPE
        ,IMEI
        ,SERVICE_CODE
	)
	select 
		TIME_ID
        ,PRODUCT_NO
        ,ROAM_LOCN
        ,ROAM_TYPE_ID
        ,APNNI
        ,START_TIME
        ,CALL_DURATION
        ,UP_FLOWS
        ,DOWN_FLOWS
        ,ALL_FEE
        ,MNS_TYPE
        ,IMEI
        ,SERVICE_CODE
		from bass1.G_S_04002_DAY_THIS
	with ur
"
	exec_sql $sql_buff

aidb_runstats bass1.G_S_04002_DAY_THIS 3
aidb_runstats bass1.g_s_04002_day 3

	return 0
}


##~   04002�ֹ�����!

##~   --s1:
##~   delete from  bass1.G_S_04002_DAY_THIS

##~   --s2:
##~   --�滻���ָ�ʽ�����ڣ�

##~   insert into bass1.G_S_04002_DAY_THIS
	##~   (
	 ##~   time_id
	##~   ,product_no
	##~   ,roam_locn
	##~   ,roam_type_id
	##~   ,apnni
	##~   ,start_time
	##~   ,call_duration
	##~   ,up_flows
	##~   ,down_flows
	##~   ,all_fee
	##~   ,mns_type
	##~   ,imei
	##~   ,service_code
	 ##~   )
##~   select
##~   20121003
##~   ,product_no
##~   ,COALESCE(char(roam_city_id),'891') as roam_locn
##~   ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500') as roam_type_id
##~   ,apn_ni
##~   ,replace(replace(char(date(start_time)),'-','')||replace(char(time(start_time)),'.',''),':','') as start_time
##~   ,char(sum(duration))        as call_duration
##~   ,char(sum(data_flow_up1+data_flow_up2))   as up_flows
##~   ,char(sum(data_flow_down1+data_flow_down2)) as down_flows
##~   ,char(sum(charge1+charge2+charge3+charge4)/10) as all_fee
##~   ,value(char(mns_type),'0')  as mns_type
##~   ,value(char(imei),'0')      as imei
##~   ,case when upper(apn_ni)<>'CMWAP' then ''
			##~   when upper(apn_ni)='CMWAP' and service_code in ('999998','999999') then ''
##~   else service_code end      as service_code
##~   from bass2.CDR_GPRS_LOCAL_20121003
##~   where drtype_id not in (8307)
##~   and bigint(product_no) not between 14734500000 
##~   and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
##~   and service_code not in ('1010000001','1010000002','2000000000')
##~   and  date(START_TIME) = '2012-10-03' and date(PROCESS_TIME) = '2012-10-03'
##~   group by product_no
##~   ,COALESCE(char(roam_city_id),'891')
##~   ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500')
##~   ,apn_ni
##~   ,start_time
##~   ,value(char(mns_type),'0')
##~   ,value(char(imei),'0')
##~   ,case when upper(apn_ni)<>'CMWAP' then ''
			##~   when upper(apn_ni)='CMWAP' and service_code in ('999998','999999') then ''
##~   else service_code end
##~   with ur


##~   --s3
   
   
   ##~   insert into bass1.G_S_04002_DAY_THIS
		##~   (
		 ##~   time_id
		##~   ,product_no
		##~   ,roam_locn
		##~   ,roam_type_id
		##~   ,apnni
		##~   ,start_time
		##~   ,call_duration
		##~   ,up_flows
		##~   ,down_flows
		##~   ,all_fee
		##~   ,mns_type
		##~   ,imei
		##~   ,service_code
		 ##~   )
##~   select
  ##~   20121003
  ##~   ,product_no
  ##~   ,COALESCE(char(substr(VPLMN1,3)),'852') as roam_locn
  ##~   ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAM_TYPE)),'500') as roam_type_id
  ##~   ,apn_ni
##~   ,replace(replace(char(date(start_time)),'-','')||replace(char(time(start_time)),'.',''),':','') as start_time
  ##~   ,char(sum(duration))        as call_duration
  ##~   ,char(sum(data_flow_up1+data_flow_up2))   as up_flows
  ##~   ,char(sum(data_flow_down1+data_flow_down2)) as down_flows
  ##~   ,char(sum(charge1+charge2+charge3+charge4)/10) as all_fee
  ##~   ,'0' as mns_type
  ##~   ,' '     as imei
  ##~   ,case when upper(apn_ni)<>'CMWAP' then ''
   ##~   else '4000000001' end      as service_code
##~   from bass2.cdr_gprs_l_20121003
##~   where drtype_id not in (8307)
##~   and bigint(product_no) not between 14734500000 
##~   and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
##~   group by product_no
  ##~   ,COALESCE(char(substr(VPLMN1,3)),'852') 
  ##~   ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAM_TYPE)),'500') 
  ##~   ,apn_ni
  ##~   ,start_time
  ##~   ,case when upper(apn_ni)<>'CMWAP' then ''
   ##~   else '4000000001' end 
##~   with ur
   

##~   --s4:

##~   delete from bass1.g_s_04002_day where time_id = 20121003
##~   insert into  bass1.g_s_04002_day
	##~   (
		##~   TIME_ID
        ##~   ,PRODUCT_NO
        ##~   ,ROAM_LOCN
        ##~   ,ROAM_TYPE_ID
        ##~   ,APNNI
        ##~   ,START_TIME
        ##~   ,CALL_DURATION
        ##~   ,UP_FLOWS
        ##~   ,DOWN_FLOWS
        ##~   ,ALL_FEE
        ##~   ,MNS_TYPE
        ##~   ,IMEI
        ##~   ,SERVICE_CODE
	##~   )
	##~   select 
		##~   TIME_ID
        ##~   ,PRODUCT_NO
        ##~   ,ROAM_LOCN
        ##~   ,ROAM_TYPE_ID
        ##~   ,APNNI
        ##~   ,START_TIME
        ##~   ,CALL_DURATION
        ##~   ,UP_FLOWS
        ##~   ,DOWN_FLOWS
        ##~   ,ALL_FEE
        ##~   ,MNS_TYPE
        ##~   ,IMEI
        ##~   ,SERVICE_CODE
		##~   from bass1.G_S_04002_DAY_THIS
	##~   with ur

##~   --done
