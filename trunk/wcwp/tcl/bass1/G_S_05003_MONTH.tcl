######################################################################################################
#�ӿ����ƣ�SP����
#�ӿڱ��룺05003
#�ӿ�˵������¼�й��ƶ����й��ƶ������ṩ�̵Ľ�����Ϣ��
#��������: G_S_05003_MONTH.tcl
#��������: ����05003������
#��������: ��
#Դ    ��1.bass2.dw_newbusi_ismg_yyyymm(�ƶ�����)
#          2.bass2.dim_js_rule(�������)
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰֻ�ܴ��ƶ���������ܹ�����������
#          2.SPҵ�����ͱ����޷�ϸ�������㼯�Ź�˾��Ҫ
#�޸���ʷ: 1.20090420zhanght�������ҵ���޳���������û�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
  
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        set thismonthdays [string range $optime_month 7 8]
        puts $thismonthdays


        #ɾ����������
	set sql_buff "delete from bass1.g_s_05003_month where time_id=$op_month"
  puts $sql_buff
  exec_sql $sql_buff
       
             
	#--�������Ž���
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,case when a.svcitem_id=300010 then '09' 
                              when a.svcitem_id=300011 then '08' 
                              when a.svcitem_id=300012 then '13' 
                              when a.svcitem_id=300013 then '17' 
                      	      when a.svcitem_id=300016 then '05' 
                      	      when a.svcitem_id=300017 then '14' 
                      	      when a.svcitem_id=400006 then '15'
                      	      when a.svcitem_id in (300001,300002,300003,300004) then '01' 
                      	   end
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,char((
                          sum(case when b.bjh_flag='1' and a.calltype_id=1 then 1 else 0 end )-
                          sum(case when b.bjh_flag='1' and a.calltype_id=0 then 1 else 0 end ))*50) 
                        ,value(char(bigint(sum((a.info_fee + a.month_fee)*(100-b.rate)*10))),'0') 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_ismg_$op_month a,
                        (select distinct sp_code,rate,bjh_flag from bass2.dim_js_rule) b
                      where 
                        a.sp_code=b.sp_code and a.bill_mark=1 and a.svcitem_id in (300001,300002,300003,300004,300010,300011,300012,300013,300016,300017)
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,case when a.svcitem_id=300010 then '09' 
                              when a.svcitem_id=300011 then '08' 
                              when a.svcitem_id=300012 then '13' 
                              when a.svcitem_id=300013 then '17' 
                      	      when a.svcitem_id=300016 then '05' 
                      	      when a.svcitem_id=300017 then '14' 
                      	      when a.svcitem_id=400006 then '15'
                      	      when a.svcitem_id in (300001,300002,300003,300004) then '01' 
                      	   end
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff



	#--16	����   �н�������
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,'16'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,char((
                          sum(case when b.bjh_flag='1' and a.calltype_id=1 then 1 else 0 end )-
                          sum(case when b.bjh_flag='1' and a.calltype_id=0 then 1 else 0 end ))*50) 
                        ,value(char(bigint(sum((a.info_fee + a.month_fee)*(100-b.rate)*10))) ,'0')
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_ismg_$op_month a,
                        (select distinct sp_code,rate,bjh_flag from bass2.dim_js_rule) b,
                        bass2.dw_product_$op_month c
                      where 
                        a.sp_code=b.sp_code and a.bill_mark=1 and a.svcitem_id in (300007)
                        and a.user_id=c.user_id
                        and c.FREE_MARK=0
                        and c.test_mark=0
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff
  


	#--16	����   û�н�������
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,'16'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0' 
                        ,value(char(bigint(sum((a.info_fee + a.month_fee)*850))) ,'0')
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_ismg_$op_month a,
                        bass2.dw_product_$op_month b
                      where 
                        a.sp_code not in (select distinct sp_code from bass2.dim_js_rule)
                        and a.bill_mark=1 and a.svcitem_id in (300007)
                        and a.user_id=b.user_id
                        and b.FREE_MARK=0
                        and b.test_mark=0
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff





	#--�ٱ������
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,case when a.sp_code = '600902' then '16' else '04' end
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0'
                        ,char(bigint(sum((a.info_fee + a.month_fee)*15*10))) 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_kj_$op_month a
                      where 
                        a.svcitem_id in (800001,800002)
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff
	

	#--WAP����
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,'03'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0'
                        ,char(bigint(sum((a.info_fee + a.month_fee)*15*10))) 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_wap_$op_month a
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff
	


	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,value(a.ser_code,'')
                        ,value(a.sp_code,'')
                        ,'02'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0'
                        ,char(bigint(sum((a.info_fee + a.month_fee)*15*10))) 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_mms_$op_month a
                      where
                      	sp_code is not null
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"

  puts $sql_buff
  exec_sql $sql_buff
					      				
		return 0
}
########reference
#(300001,'ȫ��ͨ��������(���л���)',3,'��������',2,'����ҵ��');  
#(300002,'ȫ��ͨ��������(���л���)',3,'��������',2,'����ҵ��');  
#(300003,'���������ʺ����',3,'��������',2,'����ҵ��');          
#(300004,'�����·�m�ļ�',3,'��������',2,'����ҵ��');             
#(300005,'���������ҵ��',3,'��������',2,'����ҵ��');            
#(300006,'����USSDҵ����Ϣ��',3,'��������',2,'����ҵ��');        
#(300007,'����������������',3,'��������',2,'����ҵ��');          
#(300008,'������־(������Ϣ��)',3,'��������',2,'����ҵ��');      
#(300009,'���ſͻ�����(�������л���)',3,'��������',2,'����ҵ��');
#(300010,'����(�����·�m�ļ�)',3,'��������',2,'����ҵ��');       
#(300011,'�ƶ�����վ(�������л���)',3,'��������',2,'����ҵ��');  
#(300012,'�����ܼ�',3,'��������',2,'����ҵ��');                  
#(300013,'��������(�����������л���)',3,'��������',2,'����ҵ��');
#(300014,'����USSDҵ��ͨ�ŷ�',3,'��������',2,'����ҵ��');        
#(300015,'ȫ��USSDҵ�񻰵�',3,'��������',2,'����ҵ��');          
#(300016,'PDAҵ��MISC�·��Ļ���',3,'��������',2,'����ҵ��');     
#(300017,'�ֻ�����',3,'��������',2,'����ҵ ��');   


#06	������־
#07	�ƶ�ɳ��
#11	�ƶ�֤ȯ
#12	�ֻ�Ǯ��
#15	���ػ�������



#01	��������     1
#02	����
#03	WAP
#04	�ٱ���       1
#05	PDA         1
#06	������־
#07	�ƶ�ɳ��
#08	�ƶ�����վ   1
#09	161�ƶ�����  1
#11	�ƶ�֤ȯ
#12	�ֻ�Ǯ��
#13	�Ų��ܼ�     1
#14	��������     1
#15	���ػ�������
#16	����         1
#17	��������     1
#99	����

#SP����ӿ��޸ġ���֤�ٱ��䡢���š��������š�������־����������
##################################################


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


