#################################################################
#��������: INT_CHECK_CUSTSRV_MONTH.tcl
#��������: 
#�����ţ�X4,X5,X6,X7,X8,X9,Y0,Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Z0,Z1,Z2
#�������ԣ�ƽ���ϵ
#�������ͣ���
#ָ��ժҪ��X4: ������Ͷ�ߴ���
#          X5: ������Ͷ�ߴ���
#          X6: ����ҵ��Ͷ�ߴ���
#          X7: ��������Ͷ�ߴ���
#          X8: ���Ż���Ͷ�ߴ���
#          X9: ��������Ͷ�ߴ���
#          Y0: WAPͶ�ߴ���
#          Y1: �ٱ���Ͷ�ߴ���
#          Y2: SP�ͷ�����
#          Y3: ����ҵ�񻰷���Ͷ�ߴ���
#          Y4: �������Ż�����Ͷ�ߴ���
#          Y5: ���Ż���������Ͷ�ߴ���
#          Y6: �������Ż�����Ͷ�ߴ���
#          Y7: WAPҵ�񻰷���Ͷ�ߴ���
#          Y8: �ٱ��仰����Ͷ�ߴ���
#          Y9: ����ҵ��������Ͷ�ߴ���
#          Z0: ��������������Ͷ�ߴ���
#          Z1: ���Ż���������Ͷ�ߴ���
#          Z2: ��������������Ͷ�ߴ���
#����������X4: 22029�ӿ���"������Ͷ��"�ϼ� �� 22034�ӿ���"������Ͷ��"������֮��
#          X5: 22029�ӿ���"������Ͷ��"�ϼ� �� 22034�ӿ���"������Ͷ��"������֮��
#          X6: 22029�ӿ���"����ҵ��"��Ͷ�ߴ����ϼƣ�22028�ӿ��в���ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
#          X7: 22029�ӿ���"��������"��Ͷ�ߴ����ϼƣ�22028�ӿ�����������ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
#          X8: 22029�ӿ���"���Ż���"��Ͷ�ߴ����ϼƣ�22028�ӿ������Ż���ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
#          X9: 22029�ӿ���"��������"��Ͷ�ߴ����ϼƣ�22028�ӿ�����������ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
#          Y0: 22029�ӿ���"WAP"��Ͷ�ߴ����ϼƣ�22028�ӿ���WAP��"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
#          Y1: 22029�ӿ���"�ٱ���"��Ͷ�ߴ����ϼƣ�22028�ӿ��аٱ����"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
#          Y2: 22029�ӿ���"SP�ͷ�����"��Ͷ�ߴ����ϼƣ�22028�ӿ���"SP�ͷ�����"��Ͷ�ߴ����ϼ�
#          Y3: 22034�ӿ���"����ҵ�񻰷���"Ͷ�ߴ�����22028�ӿ���"����ҵ�񻰷���"��Ͷ�ߴ����ϼ�
#          Y4: 22034�ӿ���"�������Ż�����"Ͷ�ߴ�����22028�ӿ���"�������Ż�����"��Ͷ�ߴ����ϼ�
#          Y5: 22034�ӿ���"���Ż���������"Ͷ�ߴ�����22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼ�
#          Y6: 22034�ӿ���"�������Ż�����"Ͷ�ߴ�����22028�ӿ���"�������Ż�����"��Ͷ�ߴ����ϼ�
#          Y7: 22034�ӿ���"WAP������"Ͷ�ߴ�����22028�ӿ���"WAP������"��Ͷ�ߴ����ϼ�
#          Y8: 22034�ӿ���"�ٱ��仰����"Ͷ�ߴ�����22028�ӿ���"�ٱ��仰����"��Ͷ�ߴ����ϼ�
#          Y9: 22034�ӿ���"����ҵ��������"Ͷ�ߴ�����22028�ӿ���"����ҵ��������"��Ͷ�ߴ����ϼ�
#          Z0: 22034�ӿ���"��������������"Ͷ�ߴ�����22028�ӿ���"��������������"��Ͷ�ߴ����ϼ�
#          Z1: 22034�ӿ���"���Ż���������"Ͷ�ߴ�����22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼ�
#          Z2: 22034�ӿ���"���Ż���������"Ͷ�ߴ�����22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼ�
#У�����1.BASS1.G_S_22028_MONTH
#          2.BASS1.G_S_22029_MONTH
#          3.BASS1.G_S_22034_MONTH
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#***************************************************/
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #���� yyyy-mm
        set opmonth $optime_month
        #������      
        set app_name "INT_CHECK_CUSTSRV_MONTH.tcl"

####        set handle [aidb_open $conn]
####	set sql_buff "\
####		delete from bass1.g_rule_check where time_id=$op_month
####        and rule_code in ('X4','X5','X6','X7','X8','X9','Y0','Y1','Y2','Y3','Y4','Y5','Y6','Y7','Y8','Y9','Z0','Z1','Z2')"
####
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
###############################################
####       #--X4: ������Ͷ�ߴ���
####       #--22029�ӿ���"������Ͷ��"�ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "\
####               SELECT 
####                 COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####               FROM  
####                 BASS1.G_S_22029_MONTH
####               WHERE 
####                  TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='020000'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22034�ӿ���"������Ͷ��"������֮��
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####	     AND CMPLT_TYPE_ID IN ('020101','020102','020105','020106','020111','020103','020112','020199',
####				'020402','020401','020499','020500','020601','020602','020603','020606','020604',
####				'020605','020607','020608','020609','020610','020611','020612','020613','020614')"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--1��22029�ӿ���"������Ͷ��"�ϼ� �� 22034�ӿ���"������Ͷ��"������֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}<${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��X4�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####       puts "x4 end"  	
####
####        #--X5: ������Ͷ�ߴ���
####        #--22029�ӿ���"������Ͷ��"�ϼ� 
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='040000';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#------22034�ӿ���"������Ͷ��"������֮��
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####	     AND CMPLT_TYPE_ID IN ('040101','040102','040103','040701','040702','040703','040704','040705',
####				'040901','040902','040903','040904','040999','040601','040602','040603','040699',
####				'041101','041102','041104','041103','041199','042011','042012','042015','042013',
####				'042014','042020','042060','042070','042030','042040','042050','042999','043001',
####				'043002','043003','043101','043102','043103','043199','049900')"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--�ж�
####       #--�쳣
####       #--1��22029�ӿ���"������Ͷ��"�ϼ� �� 22034�ӿ���"������Ͷ��"������֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}<${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��X5�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####         puts "x5 end"         
####
####        #--X6: ����ҵ��Ͷ�ߴ���
####      #--22029�ӿ���"����ҵ��"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070202';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ��в���ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='370'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####        #--�ж�
####        #--�쳣
####        #--22029�ӿ���"����ҵ��"��Ͷ�ߴ����ϼƣ�22028�ӿ��в���ҵ���"ҵ��ʹ����.�ն�ʹ������+
####        #--ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��X6�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####        puts "x6 end"     
####
####      #--X7: ��������Ͷ�ߴ���
####      #--22029�ӿ���"��������"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070702';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ�����������ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='252'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--�ж�
####       #--�쳣
####       #--22029�ӿ���"��������"��Ͷ�ߴ����ϼƣ�22028�ӿ�����������ҵ���"ҵ��ʹ����.�ն�ʹ������+
####       #--ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��X7�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####         puts "x7 end"  
####        
####        #--X8: ���Ż���Ͷ�ߴ���
####      #--22029�ӿ���"���Ż���"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070706';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ������Ż���ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='340'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X8',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--1��22029�ӿ���"���Ż���"��Ͷ�ߴ����ϼƣ�22028�ӿ������Ż���ҵ���"ҵ��ʹ����.�ն�ʹ������+
####    #--ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��X8�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####         puts "x8 end"  
####         
####        #--X9: ��������Ͷ�ߴ���
####      #--22029�ӿ���"��������"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070703';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ�����������ҵ���"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='283'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'X9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--1��22029�ӿ���"��������"��Ͷ�ߴ����ϼƣ�22028�ӿ�����������ҵ���"ҵ��ʹ����.�ն�ʹ������+
####    #--ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��X9�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####        } 
####      puts "x9 end"                
####
####       #--Y0: WAPͶ�ߴ���
####      #--22029�ӿ���"WAP"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070704';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���WAP��"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='310'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--�ж�
####       #--�쳣
####       #--1��1��22029�ӿ���"WAP"��Ͷ�ߴ����ϼƣ�22028�ӿ���WAP��"ҵ��ʹ����.�ն�ʹ������+
####       #--ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y0�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y0 end" 
####         
####         #----Y1: �ٱ���Ͷ�ߴ���
####         #--22029�ӿ���"�ٱ���"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='070705';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ��аٱ����"ҵ��ʹ����.�ն�ʹ������+ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮��
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('070801','070802')
####				AND BUS_FUNC_ID='410'"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y1',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--122029�ӿ���"�ٱ���"��Ͷ�ߴ����ϼƣ�22028�ӿ��аٱ����"ҵ��ʹ����.�ն�ʹ������+
####    #--ҵ��ʹ����.ҵ��ʹ����"Ͷ�ߴ���֮�ͳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y1�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y1 end" 
####         
####          #----Y2: SP�ͷ�����
####      #--22029�ӿ���"SP�ͷ�����"��Ͷ�ߴ����ϼ�
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22029_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  ACCEPT_FROM IN ('01','08','11','99') 
####                  AND CMPLT_TYPE_ID='010800';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"SP�ͷ�����"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='010800'
####				AND BUS_FUNC_ID IN ('310','410','283','370','252','340');"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####       #--�ж�
####       #--�쳣
####       #--22029�ӿ���"SP�ͷ�����"��Ͷ�ߴ����ϼƣ�22028�ӿ���"SP�ͷ�����"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y2�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y2 end" 
####         
####         
####         #------Y3: ����ҵ�񻰷���Ͷ�ߴ���
####      #--22034�ӿ���"����ҵ�񻰷���"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020402';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"����ҵ�񻰷���"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='370';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y3',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"����ҵ�񻰷���"Ͷ�ߴ�����22028�ӿ���"����ҵ�񻰷���"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y3�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y3 end" 
####         
####         #--Y4: �������Ż�����Ͷ�ߴ���
####      #--22034�ӿ���"�������Ż�����"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020613';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"�������Ż�����"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='252';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"�������Ż�����"Ͷ�ߴ�����22028�ӿ���"�������Ż�����"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y4�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y4 end" 
####         
####         #--Y5: ���Ż���������Ͷ�ߴ���
####        # --22034�ӿ���"���Ż���������"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020611';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='340';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"���Ż���������"Ͷ�ߴ�����22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y5�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y5 end" 
####         
####         #--Y6: �������Ż�����Ͷ�ߴ���
####      #--22034�ӿ���"�������Ż�����"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020611';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"�������Ż�����"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='283';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"�������Ż�����"Ͷ�ߴ�����22028�ӿ���"�������Ż�����"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y6�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y6 end" 
####         
####          #--Y7: WAPҵ�񻰷���Ͷ�ߴ���
####      #--22034�ӿ���"WAP������"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020605';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"WAP������"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='310';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"WAP������"Ͷ�ߴ�����22028�ӿ���"WAP������"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y7�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y7 end" 
####         
####       #--Y8: �ٱ��仰����Ͷ�ߴ���
####       #--22034�ӿ���"�ٱ��仰����"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='020612';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#----22028�ӿ���"�ٱ��仰����"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID IN ('020301','020302','020303','020304','020305')
####				AND BUS_FUNC_ID='410';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y8',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"�ٱ��仰����"Ͷ�ߴ�����22028�ӿ���"�ٱ��仰����"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y8�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y8 end" 
####         
####           #--Y9: ����ҵ��������Ͷ�ߴ���
####      #--22034�ӿ���"����ҵ��������"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='041104';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ���"����ҵ��������"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='370';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Y9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"����ҵ��������"Ͷ�ߴ�����22028�ӿ���"����ҵ��������"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Y9�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Y9 end" 
####
####       #----Z0: ��������������Ͷ�ߴ���
####       #--22034�ӿ���"��������������"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='042015';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ���"��������������"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='252';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"��������������"Ͷ�ߴ�����22028�ӿ���"��������������"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Z0�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####	}
####         puts "Z0 end"          
####
####         #----Z1: ���Ż���������Ͷ�ߴ���
####      #--22034�ӿ���"���Ż���������"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='042070';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='340';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z1',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"���Ż���������"Ͷ�ߴ�����22028�ӿ���"���Ż���������"��Ͷ�ߴ����ϼƳ���
####	set handle [aidb_open $conn]
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Z1�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
####         puts "Z1 end" 
####         
####         #----Z2: ��������������Ͷ�ߴ���
####      #--22034�ӿ���"��������������"Ͷ�ߴ���
####       
####       set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22034_MONTH
####             WHERE TIME_ID=$op_month
####                  AND  CMPLT_TYPE_ID='042060';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL1"
####	
####	#--22028�ӿ���"��������������"��Ͷ�ߴ����ϼ�
####
####	set handle [aidb_open $conn]
####        set sqlbuf "SELECT 
####             	COALESCE(SUM(BIGINT(CMPLT_NUM)),0)
####             FROM  BASS1.G_S_22028_MONTH
####             WHERE TIME_ID=$op_month
####				AND CMPLT_TYPE_ID='041000'
####				AND BUS_FUNC_ID='283';"
####
####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	puts "$DEC_RESULT_VAL2"
####	
####	#--��У��ֵ����У������
####	set handle [aidb_open $conn]
####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####	#set DEC_RESULT_VAL2 1
####	
####    #--�ж�
####    #--�쳣
####    #--22034�ӿ���"��������������"Ͷ�ߴ�����22028�ӿ���"��������������"��Ͷ�ߴ����ϼƳ���
####	if {${DEC_RESULT_VAL1}!=${DEC_RESULT_VAL2} } {
####	     	set grade 2
####	        set alarmcontent "׼ȷ��ָ��Z2�������ſ��˷�Χ"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
####         } 
         puts "Z2 end"       
####################################
        return 0
}