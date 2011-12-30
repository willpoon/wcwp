#################################################################
#��������: INT_CHECK_DATARULE_DAY.tcl
#��������: 
#�����ţ�DR01,DR02,DR03,DR04,DR05,DR11,DR12,DR13,DR14,DR15,DR16,DR17,DR18,DR19,DR21,DR22,DR31,DR32
#�������ԣ����ݹ����Լ�ȡֵ��ΧУ��
#�������ͣ���

#ָ��ժҪ��
#          DR01	�û�ҵ������ȡֵ                          
#          DR02	�ͻ�Ʒ��ȡֵ                              
#          DR03	�û���������ȡֵ                          
#          DR04	����SIM����־ȡֵ                         
#          DR05	δ�����Ź�˾��Ч����û�״̬�������⴫9000
#          DR11	�������Ż�����SP��ҵ����                  
#          DR12	�������Ż�����SP��ҵ����                  
#          DR13	�������Ż����ļƷ�����                    
#          DR14	�������Ż����а����Ʒѻ�������Ϣ��        
#          DR15	�������Ż����а��¼Ʒѻ�������Ϣ��        
#          DR16	�������Ż����а��»����ķ���              
#          DR17	�������Ż����а����Ʒѻ����ķ���          
#          DR18	����WAP�����а����Ʒѻ�������Ϣ��         
#          DR19	����WAP�����а��»����İ��·�             
#          DR21	ͨ�����ػ����е�SP��ҵ����                
#          DR22	ͨ�����ػ����еļƷѻ�����Ϣ��            
#          DR31	������־������SP�������                  
#          DR32	������־�����а����Ʒѻ����ķ���   
#                
#add by zhanght 2009.05.18        

#����������
#          DR01	�û�ҵ�����͡�(1,2)
#          DR02	�����û��Ŀͻ�Ʒ�ơ�(1,2,3)
#          DR03	�����û����û��������͡�(1,2)
#          DR04	�����û�������SIM����־�ʣ�0,1��
#          DR05	δ�����Ź�˾��Ч����û�״̬�������⴫9000
#          DR11	�������Ż�����SP��ҵ����Ϊ�յı���С��3��
#          DR12	�������Ż�����SP��ҵ����Ϊ�յı���С��3��
#          DR13	�������Ż����ļƷ����Ͳ�Ϊ��
#          DR14	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0
#          DR15	�������Ż����а��¼Ʒѻ�������Ϣ�ѡ�0
#          DR16	�������Ż����а��¼Ʒѻ�������Ϣ��=0���Ұ��·ѡ�0
#          DR17	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0
#          DR18	����WAP�����У��ǰٷְ��ۿ۵İ����Ʒѻ�������Ϣ�ѡ�0
#          DR19	����WAP�����У��ǰٷְ��ۿ۵İ��»����İ��·ѡ�0
#          DR21	ͨ�����ػ����е�SP��ҵ���붼��'7'��ͷ
#          DR22	ͨ�����ػ����и��ּƷѻ�����Ϣ�ѡ�0
#          DR31	������־SP�������Ҫ�ԡ�12590','12559'��'12596'��ͷ
#          DR32	������־�����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0

#У�����
#        02004 �û�
#        02008 �û�״̬
#        02008 �û�״̬
#        04004 ���Ż���
#        04005 �������Ż���
#        04006 ����WAP����
#        04007 ͨ�����ػ���
#        04014 ������־����
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ��Ļ�ѧ
#��дʱ�䣺2008-05-16
#�����¼��1.
#�޸���ʷ: 1.
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        #set op_time 2008-06-30
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #��������
        set app_name "INT_CHECK_DATARULE_DAY.tcl"

        #--ɾ����������
        set handle [aidb_open $conn]
#####	set sql_buff "\
#####		delete from bass1.g_rule_check where time_id=$timestamp
#####		and rule_code in('DR01','DR02','DR03','DR04','DR05','DR11','DR12','DR13','DR14','DR15','DR16','DR17','DR18','DR19','DR21','DR22','DR31','DR32') "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####
#####
########################################DR01	�û�ҵ������ȡֵ#############################
#####        #--DR01	�û�ҵ������ȡֵ
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and USER_BUS_TYP_ID not in ('01','02')"
#####
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "�û�ҵ�����ͷǡ�(1,2)������ " 
#####	
#####	   #--��У��ֵ����У������
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR01',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--�ж�
#####        #--�쳣
#####        #--1���û�ҵ�����͡�(1,2)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "�۷���ָ��DR01	�û�ҵ������ȡֵ�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR01 �û�ҵ������ȡֵУ�����"
#####         
#####         
#####
#####
########################################DR02	�ͻ�Ʒ��ȡֵ #############################
#####        #--DR02	�ͻ�Ʒ��ȡֵ 
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and brand_id not in ('1','2','3') "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "�ͻ�Ʒ��ȡֵ�ǡ�(1,2,3)������ " 
#####	
#####	   #--��У��ֵ����У������
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR02',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--�ж�
#####        #--�쳣
#####        #--1���ͻ�Ʒ��ȡֵ��(1,2,3)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "�۷���ָ��DR02	�ͻ�Ʒ��ȡֵ�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR02 �ͻ�Ʒ��ȡֵУ�����"
#####
#####
########################################DR03	�û���������ȡֵ #############################
#####        #--DR03	�û���������ȡֵ 
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and prompt_type not in ('1','2') "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "�û���������ȡֵ�ǡ�(1,2)������ " 
#####	
#####	   #--��У��ֵ����У������
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR03',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--�ж�
#####        #--�쳣
#####        #--1���ͻ�Ʒ��ȡֵ��(1,2,3)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "�۷���ָ��DR03	�û���������ȡֵ�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR03	�û���������ȡֵУ�����"
#####
#####
#####
########################################DR04	����SIM����־ȡֵ #############################
#####        #--DR04	����SIM����־ȡֵ 
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02004_DAY where time_id = $timestamp and prompt_type not in ('1','2') "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "����SIM����־ȡֵ�ǡ�(1,2)������ " 
#####	
#####	   #--��У��ֵ����У������
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR04',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--�ж�
#####        #--�쳣
#####        #--1������SIM����־ȡֵ��(1,2)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "�۷���ָ��DR04	����SIM����־ȡֵ�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR04	����SIM����־ȡֵУ�����"
#####
#####
#####
#####
########################################DR05	δ�����Ź�˾��Ч����û�״̬�������⴫9000 #############################
#####        #--DR05	δ�����Ź�˾��Ч����û�״̬�������⴫9000
#####  set handle [aidb_open $conn]
#####  set sqlbuf " SELECT COUNT(*) FROM G_A_02008_DAY where time_id = $timestamp and usertype_id = '9000' "
#####      if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####	   	WriteTrace $errmsg 1001
#####	   	return -1
#####	 }      
#####	 if [catch {set INVOLID [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####	   	WriteTrace $errmsg 1002
#####	   	return -1
#####	 }        
#####	aidb_commit $conn
#####	puts "δ�����Ź�˾��Ч����û�״̬�������⴫9000�ǡ�(9000)������ " 
#####	
#####	   #--��У��ֵ����У������
#####	   set handle [aidb_open $conn]
#####	   set sql_buff "\
#####	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR05',$INVOLID,$INVOLID,0,0) "
#####	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####	   	WriteTrace "$errmsg" 2005
#####	   	aidb_close $handle
#####	   	return -1
#####	   }
#####	   aidb_commit $conn
#####
#####	      #--�ж�
#####        #--�쳣
#####        #--1���ͻ�Ʒ��ȡֵ��(9000)
#####         if {$INVOLID>0 } {
#####	  	set grade 2
#####	        set alarmcontent "�۷���ָ��DR05	δ�����Ź�˾��Ч����û�״̬�������⴫9000�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#####	 }
#####   puts "DR05	δ�����Ź�˾��Ч����û�״̬�������⴫9000"
#####
#####   aidb_close $handle
#####
#####
#####
#####
########################################DR11	�������Ż�����SP��ҵ���� #############################
#####        #--DR11	�������Ż�����SP��ҵ����  �������Ż�����SP��ҵ����Ϊ�յı���С��3��
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id = $timestamp and sp_ent_code is null with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id = $timestamp  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####        
#####   set DEC_TARGET_VAL1 [format "%.4f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
#####        
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR11',$RESULT_VAL1,$RESULT_VAL2,$DEC_TARGET_VAL1,0.03) "        
#####   exec_sql $sqlbuf
#####
#####	if {$DEC_TARGET_VAL1>=0.03 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR11	�������Ż�����SP��ҵ���볬�����ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} elseif {${DEC_TARGET_VAL1}>=0.025 } {
#####		set grade 3
#####	        set alarmcontent "�۷���ָ��DR11	�������Ż�����SP��ҵ����ӽ����ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####		}
#####	puts "DR11	�������Ż�����SP��ҵ����У�����"
#####
#####
#####
########################################DR12	�������Ż�����SP��ҵ���� #############################
#####        #--DR12	�������Ż�����SP��ҵ����  �������Ż�����SP��ҵ����Ϊ�յı���С��3��
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id = $timestamp and sp_code is null with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id = $timestamp  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####        
#####   set DEC_TARGET_VAL1 [format "%.4f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
#####        
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR12',$RESULT_VAL1,$RESULT_VAL2,$DEC_TARGET_VAL1,0.03) "        
#####   exec_sql $sqlbuf
#####
#####	if {$DEC_TARGET_VAL1>=0.03 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR12	�������Ż�����SP��ҵ���볬�����ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} elseif {${DEC_TARGET_VAL1}>=0.025 } {
#####		set grade 3
#####	        set alarmcontent "�۷���ָ��DR12	�������Ż�����SP��ҵ����ӽ����ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####		}
#####	puts "DR12	�������Ż�����SP��ҵ����У�����"
#####
#####
########################################DR13	�������Ż����ļƷ����Ͳ�Ϊ�� #############################
#####   #--DR13	�������Ż����ļƷ����Ͳ�Ϊ��
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id = $timestamp and (PayType_id is null or rtrim(PayType_id) = '') with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR13',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR13	�������Ż����ļƷ����Ͳ�Ϊ�ճ������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR13	�������Ż����ļƷ����Ͳ�Ϊ��У�����"
#####
#####
########################################DR14	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0 #############################
#####   #--DR14	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id =  $timestamp and billing_type = '2' and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR14',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR14	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR14	�������Ż����а����Ʒѻ�������Ϣ��У�����"
#####
#####
#####
########################################DR15	�������Ż����а��¼Ʒѻ�������Ϣ�ѡ�0 #############################
#####   #--DR15	�������Ż����а��¼Ʒѻ�������Ϣ�ѡ�0
#####   set sqlbuf " select count(*)  from G_S_04004_day where time_id =  $timestamp and billing_type = '3'  and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR15',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR15	�������Ż����а��¼Ʒѻ�������Ϣ�ѡ�0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR15	�������Ż����а��¼Ʒѻ�������Ϣ�ѡ�0У�����"
#####
#####
#####
########################################DR16	�������Ż����а��¼Ʒѻ�������Ϣ��=0���Ұ��·ѡ�0#############################
#####   #--DR16	�������Ż����а��¼Ʒѻ�������Ϣ��=0���Ұ��·ѡ�0
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '03' and bigint(info_fee) <> 0  with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####   
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '03' and bigint(month_fee) < 0  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR16',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
#####   exec_sql $sqlbuf
#####
#####  puts $RESULT_VAL1
#####  puts $RESULT_VAL2
#####	if {$RESULT_VAL1>0 || $RESULT_VAL2 > 0 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR16	�������Ż����а��¼Ʒѻ�������Ϣ��=0���Ұ��·ѡ�0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR16	�������Ż����а��¼Ʒѻ�������Ϣ��=0���Ұ��·ѡ�0У�����"
#####	
#####	
########################################DR17	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0#############################
#####   #--DR17	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '02'  and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####   
#####   set sqlbuf " select count(*)  from G_S_04005_day where time_id =  $timestamp and paytype_id = '02' and bigint(month_fee) <> 0 with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR17',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0 || $RESULT_VAL2 > 0 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR17	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR17	�������Ż����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0У�����"
#####	
#####	
########################################DR18	����WAP�����У��ǰٷְ��ۿ۵İ����Ʒѻ�������Ϣ�ѡ�0#############################
#####   #--DR18	����WAP�����У��ǰٷְ��ۿ۵İ����Ʒѻ�������Ϣ�ѡ�0
#####   set sqlbuf " select count(*)  from G_S_04006_day where time_id =  $timestamp and disc_rate <> '100' and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR18',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0} {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR18	����WAP�����У��ǰٷְ��ۿ۵İ����Ʒѻ�������Ϣ�ѡ�0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR18	����WAP�����У��ǰٷְ��ۿ۵İ����Ʒѻ�������Ϣ�ѡ�0У�����"
#####	
#####	
#####	
########################################DR19	����WAP�����У��ǰٷְ��ۿ۵İ��»����İ��·ѡ�0#############################
#####   #--DR19	����WAP�����У��ǰٷְ��ۿ۵İ��»����İ��·ѡ�0
#####   set sqlbuf " select count(*)  from G_S_04006_day where time_id =  $timestamp and disc_rate <> '100' and bigint(month_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR19',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR19	����WAP�����У��ǰٷְ��ۿ۵İ��»����İ��·ѡ�0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR19	����WAP�����У��ǰٷְ��ۿ۵İ��»����İ��·ѡ�0У�����"
#####	
#####
#####
#####
#####
#####
########################################DR21	ͨ�����ػ����е�SP��ҵ���붼��'7'��ͷ#############################
#####   #--DR21	ͨ�����ػ����е�SP��ҵ���붼��'7'��ͷ
#####   set sqlbuf " select count(*)  from G_S_04007_day where time_id =  $timestamp and substr(sp_code,1,1) <> '7' with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR21',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR21	ͨ�����ػ����е�SP��ҵ���붼��'7'��ͷ�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR21	ͨ�����ػ����е�SP��ҵ���붼��'7'��ͷУ�����"
#####
#####
#####
########################################DR22	ͨ�����ػ����и��ּƷѻ�����Ϣ�ѡ�0#############################
#####   #--DR22	ͨ�����ػ����и��ּƷѻ�����Ϣ�ѡ�0
#####   set sqlbuf " select count(*)  from G_S_04007_day where time_id =  $timestamp and bigint(info_fee) < 0 with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR22',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR22	ͨ�����ػ����и��ּƷѻ�����Ϣ�ѡ�0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR22	ͨ�����ػ����и��ּƷѻ�����Ϣ�ѡ�0У�����"
#####
#####
#####
#####
########################################DR31	������־SP�������Ҫ�ԡ�12590','12559'��'12596'��ͷ#############################
#####   #--DR31	������־SP�������Ҫ�ԡ�12590','12559'��'12596'��ͷ
#####   set sqlbuf " select count(*)  from G_S_04014_day where time_id =  $timestamp and substr(serv_code,1,5) not in ('12590','12559','12596') "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR31',$RESULT_VAL1,$RESULT_VAL1,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1 > 0} {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR31	������־SP�������Ҫ��12590,12559��12596��ͷ�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR31	������־SP�������Ҫ�ԡ�12590','12559'��'12596'��ͷУ�����"
#####
#####
#####
#####
#####
########################################DR32	������־�����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0#############################
#####   #--DR32	������־�����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0
#####   set sqlbuf " select count(*)  from G_S_04014_day where time_id =  $timestamp and sms_fee_type = '02' and bigint(info_fee) < 0   with ur "
#####   set RESULT_VAL1 [get_single $sqlbuf]
#####   
#####   set sqlbuf " select count(*)  from G_S_04014_day where time_id =  $timestamp and sms_fee_type = '02' and bigint(month_fee) <> 0  with ur "
#####   set RESULT_VAL2 [get_single $sqlbuf]
#####  
#####   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'DR32',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
#####   exec_sql $sqlbuf
#####
#####	if {$RESULT_VAL1>0 || $RESULT_VAL2 > 0 } {
#####		set grade 2
#####	        set alarmcontent "�۷���ָ��DR32	������־�����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0�������ſ��˷�Χ"
#####	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#####	} 
#####	puts "DR32	������־�����а����Ʒѻ�������Ϣ�ѡ�0���Ұ��·�=0У�����"
#####

	return 0
}




	
#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------
#   	set sql_buff "INSERT INTO bass1.G_REPORT_CHECK(TIME_ID,RULE_ID,FLAG,RET_VAL) VALUES
#		(                                             
#			$op_month,
#			'B10',
#			1,
#			'$RESULT_VAL')"
#exec_sql $sql_buff
  
 

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------
#set RESULT_VAL [get_single $sql_buff]
#puts "10:�������żƷ���  $RESULT_VAL"
