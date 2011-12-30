######################################################################################################
#�ӿ����ƣ�
#�ӿڱ��룺
#�ӿ�˵����
#��������: INT_02004_02008_YYYYMM.tcl
#��������: ����02004,02008���������ݵ�user_id
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymmdd
#          2.bass1.g_a_02004_day
#          3.bass1.G_A_02008_DAY
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: ȥ�����䶳�ڡ����޸ġ�ͣ�����š��ھ�Ϊ userstatus_id=9 ���޸�ʱ�䣺2009-05-31 �޸��ˣ�zhanght
###20091126 �� dw_product_bass1_ �滻ԭ�����û���
#  20100125 �޸������û��ھ�userstatus_id in (1,2,3,6,8)
#  20100128 �޸Ŀھ� when userstatus_id=9 then '1033'Ϊ when userstatus_id=9 then '2030' ��Ϊ�䶳��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.INT_02004_02008_${op_month} where op_time=$Timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
     
        #----������ʱ��-----#
        set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.int_02004_02008
                     (
                      user_id     varchar(20) not null , 
                      brand_id    varchar(1) not null ,
                      usertype_id varchar(4) not null, 
                      brand_flag int not null,
                      usertype_flag int not null				
                      )
                      partitioning key 
                      (user_id)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
        puts "����ʱ�����"



        #--����02004����������
        set handle [aidb_open $conn]

	set sql_buff "Insert into session.int_02004_02008
                   select
                     user_id
                     ,brand_id
                     ,'0000'
                     ,1
                     ,0
                   from
                     (
                       select
                         user_id
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id
                         ,case
                            when test_mark=1 then '3'
                            when free_mark=1 then '2'
                            else '1'
                          end
                       from 
                         bass2.dw_product_bass1_${Timestamp} 
                       where 
                         userstatus_id in (1,2,3,6,8)
                         and usertype_id in (1,2,9)
                       except
                       select a.user_id,a.brand_id,usertype_id from $db_user.g_a_02004_day a,
                           (
                             select max(time_id) as time_id,user_id from $db_user.g_a_02004_day 
                             where time_id<$Timestamp 
                             group by user_id
                            )b
                            where a.user_id = b.user_id and a.time_id=b.time_id
                     )aa"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	puts "����02004����"
	
	#---����02008����������
	set handle [aidb_open $conn]        
	set sql_buff "Insert into session.int_02004_02008
                    select
                     user_id
                     ,'0'
                     ,usertype_id
                     ,0
                     ,1
                     from
                     (
                       select
                         user_id
                         ,case 
                            when userstatus_id=1 and stopstatus_id=0 then '1010'
                            when userstatus_id=1 and stopstatus_id between 11 and 16 then '1031'
                            when userstatus_id=1 and stopstatus_id in (28,29) then '1032'
                            when userstatus_id=9 then '2030'
                            when userstatus_id=1 and stopstatus_id not in (0,11,12,13,14,15,16,28,29) then '1039' 
                            when userstatus_id in (3,6) then '1022'
                            when userstatus_id in (2)   then '1021'
                            when userstatus_id=4 then '2010'
                            when userstatus_id=8 then '1040'
                            when userstatus_id in (5,7) then '2020' 
                            else '1039' 
                     	   end as usertype_id
                         from 
                           bass2.dw_product_bass1_${Timestamp} 
                         where 
                           userstatus_id<>0
                           and usertype_id in (1,2,9)
                       except
                        select a.user_id,a.usertype_id from $db_user.G_A_02008_DAY a,
                           (select max(time_id) as time_id,user_id from $db_user.G_A_02008_DAY 
                            where time_id<$Timestamp 
                            group by user_id
                            )b
                           where a.user_id = b.user_id and a.time_id=b.time_id
                       )aa"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	puts "����02008����"
	
	#---��������----#
	set handle [aidb_open $conn]

	set sql_buff "Insert into $db_user.INT_02004_02008_${op_month}
                      select
                        $Timestamp 
                        ,user_id 
                        ,max(brand_id ) 
                        ,max(usertype_id)  
                        ,max(brand_flag)
                        ,max(usertype_flag) 
                      from 
                        session.int_02004_02008
                      group by user_id"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	
	puts "���ܽ���"

		
	aidb_close $handle

	return 0
}
###########�ο�##################
#0	��ʷ�û�
#1	�����û�
#2	ӪҵԤ����
#3	����Ԥ����
#4	Ӫҵ����
#5	��������
#6	����Ԥ����
#7	��������
#8	������
#9	������
#not in ('2010','2020','2030','1040','1021','9000')
#(1,2,3,6)
# ,case 
#    when userstatus_id=1 and stopstatus_id=0 then '1010' --����--
#    when userstatus_id=1 and stopstatus_id in (28,29) then '1032' --�߶�ͣ��--
#    when userstatus_id=1 and stopstatus_id in (2,3) then '1033'   --ͣ������--
#    when userstatus_id=1 and stopstatus_id not in (0,2,3,28,29) then '1031' --Ƿͣ--
#    when userstatus_id in (3,6) then '1022' --Ƿ��Ԥ����--
#    when userstatus_id=4 then '2010' --��������--
#    when userstatus_id=8 then '1040' --������--
#    when userstatus_id=9 then '2030' --�䶳��
#    when userstatus_id in (5,7) then '2020' --��������--
#    else '1039' --����ԭ��ͣ��--
#   end as usertype_id
#0	δͣ��״̬                       -1	[Null]	δ֪�û�״̬
#1	����ͣ��                         0	1	�����û�    
#2	��ʧͣ��                         1	0	��ʷ�û�    
#3	Ӫҵ��ͣ                         2	2	ӪҵԤ����  
#11	����������ͣ                     3	5	��������    
#12	��������ͣ                       4	7	��������    
#13	������ͣ(Ԥ����ͣ)               40	8	������      
#14	������ͣ(Ԥ��ͣ)                 41	9	������      
#15	Ƿ�ѵ�ͣ                         100	3	����Ԥ����  
#16	Ƿ��ͣ                           101	4	Ӫҵ����    
#26	����ͣ(�û�Υ��ͣ)               102	6	����Ԥ����  
#27	����(�û�Υ��ͣ) ����ͣ
#28	�߶�ͣ
#29	�߶�����ͣ
#31	IPBUS�ʻ�����״̬(ҵ������)
#32	IPBUS�ʻ�����״̬(�������)
#33	IPBUS�ʻ�����״̬(������)
#
#(0,'��ʷ�û�');  
#(1,'�����û�');  
#(2,'ӪҵԤ����');
#(3,'����Ԥ����');
#(4,'Ӫҵ����');  
#(5,'��������');  
#(6,'����Ԥ����');
#(7,'��������');  
#(8,'������');    
#(9,'������');   
#################################