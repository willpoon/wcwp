######################################################################################################
#�ӿ����ƣ��û�
#�ӿڱ��룺02004
#�ӿ�˵����������û���ָ����������������񣬱�������ΨһMSISDN�����塣
#          ������û�������"�������û�"��"ȫ��ͨ��ط�Ʒ����ͨ�ֻ��û�"��
#          "�ƶ������û�"��ʹ��"�û�ҵ������"���֡��û�������ֻ���������ֻ��û�����ͨ/����/���⣩
#          ������SIM���û������������Ų�Ʒ����û���IPֱͨ���û�����
#��������: G_A_02004_DAY.tcl
#��������: ����02004������
#��������: ��
#Դ    ��1.bass2.dw_product_bass1_yyyymmdd
#          3.bass1.INT_02004_02008_YYYYMM
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.���������־�ֶ�@20070801 By tym
#         2.��case 
#                         when a.crm_brand_id2 in (30,190) then '02'
#                         else '01' 
#                        end  
#           �޸�Ϊ '01'       ԭ�����ڲ������������û�    �Ļ�ѧ
#�޸���ʷ:  20091126 �� dw_product_bass1_ �滻ԭ�����û���
#           20100823 �������������ݣ�����ͷ���� liuqf
# 2011.11.30 v.1.7.7 panzw 2���޸Ľӿ�02004���û���	
#�޸����ԡ�����������ʶ����������������BASS1_UM(ͨ��ʵ�塢���ӡ�ֱ���������������޷��鲢����Ӧ��ʵ�塢���ӡ�ֱ������)��BASS1_UA(ͨ��ʵ�塢���ӡ�ֱ���������������);
#�޸���д˵�����������������ֶ���ʱ�ϱ�BASS1_UM/BASS1_UA�ļ�¼��ʡ���辡��ȷ�������������������������ʽ���£�һ��������ͳ��ʱ�Ե������һ��Ϊ��ֹ���ڣ�
#���޷�ʶ����������������ݲ��޸�
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
	#����  yyyymm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#������ yyyymm
	set last_last_month [GetLastMonth [string range $last_month 0 5]]
	#��������ڣ���ʽdd(��������20070411 ����11,����20070708������8)
	set today_dd [format "%.0f" [string range $Timestamp 6 7]]
	
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

	set sql_buff "\
		DELETE FROM $db_user.G_A_02004_DAY where time_id=$Timestamp"
 exec_sql $sql_buff




	set sql_buff "insert into $db_user.G_A_02004_DAY
                       (
                       time_id
                       ,user_id
                       ,cust_id
                       ,usertype_id
                       ,create_date
                       ,user_bus_typ_id
                       ,product_no
                       ,imsi
                       ,cmcc_id
                       ,channel_id
                       ,mcv_typ_id
                       ,prompt_type
                       ,subs_style_id
                       ,brand_id
                       ,sim_code
                       )
                     select
                       $Timestamp
                       ,a.user_id
                       ,a.cust_id
                       ,case 
                         when a.test_mark=1 then '3' 
                         when a.free_mark=1 then '2'
                         else '1' 
                        end
                       ,replace(char(create_date),'-','')
                       ,'01'
                       ,a.product_no
                       ,value(a.imsi,'0')
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101')
                       ,char(a.channel_id)
                       ,'1'
				       ,case
				         when a.accttype_id=0 then '1'
				         else '2'
				        end
                       ,'04'
                       ,b.brand_id
                       ,case 
                         when a.crm_brand_id2=70 then '1' 
                         else '0' 
                        end
                     from 
                       bass2.dw_product_bass1_$Timestamp a,
                       (
                        select 
                          * 
                        from 
                          $db_user.INT_02004_02008_${op_month} 
                        where
                          op_time=$Timestamp
                          and brand_flag=1
                       )b              
                     where 
                       a.user_id = b.user_id "
 exec_sql $sql_buff
	
	set sql_buff "delete  from BASS1.g_a_02004_day a
                     
                     where a.time_id=$Timestamp and user_id = '1110117651'	"
 exec_sql $sql_buff

#ÿ�յ����û���Ϣ,���02004�ӿں�02008�ӿ��û�����һ����� --201008023 ------------------------��1��----------------------

#20100823 ����֮ǰ�����´�����6��27�յ���ʷ�������û�����
#---����
###insert into   BASS1.G_A_02008_DAY_20100823bak
###select * from BASS1.G_A_02008_DAY;
###commit;
###
###insert into   BASS1.G_A_02004_DAY_20100823bak
###select * from BASS1.G_A_02004_DAY;
###commit;
###
###
###--�����������
###delete from BASS1.G_A_02004_DAY
###where time_id=20100627;
###commit;
###
###delete from BASS1.G_A_02008_DAY
###where time_id=20100627;
###commit;
###
###
###--�����͸����ŵ�����
###insert into BASS1.G_A_02004_DAY
###select * from G_A_02004_DAY_take
###where time_id=20100627;
###commit;
###
###
###insert into BASS1.G_A_02008_DAY
###select * from G_A_02008_DAY_take
###where time_id=20100627;
###commit;

	#�����ʱ��3
	set sql_buff "delete from BASS1.temp_check_02004 where time_id=$Timestamp"
 exec_sql $sql_buff


	#�����û���
	set sql_buff "
			insert into BASS1.temp_check_02004 
			select $Timestamp,user_id from 
				(
				select distinct user_id from G_A_02008_DAY 
				except
				select distinct user_id from G_A_02004_DAY 
				)a
			"
 exec_sql $sql_buff

	#02004�ӿ���������
	set sql_buff "
			insert into G_A_02004_DAY
			   (
			    time_id
			   ,user_id
			   ,cust_id
			   ,usertype_id
			   ,create_date
			   ,user_bus_typ_id
			   ,product_no
			   ,imsi
			   ,cmcc_id
			   ,channel_id
			   ,mcv_typ_id
			   ,prompt_type
			   ,subs_style_id
			   ,brand_id
			   ,sim_code
			   )
			 select
			   $Timestamp
			   ,a.user_id
			   ,a.cust_id
			   ,case 
			     when a.test_mark=1 then '3' 
			     when a.free_mark=1 then '2'
			     else '1' 
			    end
			   ,replace(char(create_date),'-','')
			   ,'01'
			   ,a.product_no
			   ,value(a.imsi,'0')
			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101')
			   ,char(a.channel_id)
			   ,'1'
				,case
				when a.accttype_id=0 then '1'
				else '2'
				end
			   ,'04'
			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') as brand_id
			   ,case 
			     when a.crm_brand_id2=70 then '1' 
			     else '0' 
			    end
			 from bass2.dw_product_bass1_$Timestamp a,
			      BASS1.temp_check_02004 b             
			 where a.user_id = b.user_id
			   and b.time_id = $Timestamp
	"
 exec_sql $sql_buff


#/**
#458	�漰����������������������������ͨ������������ֱ�����������޶���
#1��(02004)�û��ӿڡ�����������ʶ����ͨ������������ֱ����������
#�����¹�����д�ض������ʶ��
#��վ�����ߡ����š�wap�������ն˵���������ֱ�������ֱ��Ӧ��д
#'BASS1_WB', 'BASS1_HL','BASS1_SM','BASS1_WP','BASS1_ST', 'BASS1_DS' 
#(�ַ����ִ�Сд)�� ʡ��˾����ռ������6�����������á�
#2�� (01002)���˿ͻ���(01004)���ſͻ���(02013) IPֱͨ���̶��û���
#������������ʶ����������������ʶ���μ�(02004)�û��ӿ��ϱ���	
#1.7.3	2011-5-17	����������20110601����Ч
#**/
#add     

#2011-06-01 21:57:54 trans channel
#add

set sql_buff "alter table bass1.G_A_02004_DAY_CHNL_MID activate not logged initially with empty table"
exec_sql $sql_buff


set sql_buff "
insert into G_A_02004_DAY_CHNL_MID
select 
         a.TIME_ID
        ,a.USER_ID
        ,a.CUST_ID
        ,a.USERTYPE_ID
        ,a.CREATE_DATE
        ,a.USER_BUS_TYP_ID
        ,a.PRODUCT_NO
        ,a.IMSI
        ,a.CMCC_ID
        ,case when b.CHANNEL_ID is null then 'BASS1_DS' 
        		else a.channel_id end CHANNEL_ID
        ,a.MCV_TYP_ID
        ,a.PROMPT_TYPE
        ,a.SUBS_STYLE_ID
        ,a.BRAND_ID
        ,a.SIM_CODE
from G_A_02004_DAY a
left join (select distinct CHANNEL_ID  from G_I_06021_MONTH )  b on a.CHANNEL_ID = b.CHANNEL_ID
where a.time_id = $Timestamp
"
exec_sql $sql_buff

	set sql_buff "\
		DELETE FROM $db_user.G_A_02004_DAY where time_id=$Timestamp
		"
  exec_sql $sql_buff

set sql_buff "
	insert into  $db_user.G_A_02004_DAY 
	select * from G_A_02004_DAY_CHNL_MID
	"
exec_sql $sql_buff



#����Ʒ�Ʊ䶯�����û�����ʱ��䶯����� 20100823----------------------------------��2��---------------------------------
#
#
#	#�����ʱ��5
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.temp_check_02004_2 where time_id=$Timestamp"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#	#�����û���
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into BASS1.temp_check_02004_2 
#			select distinct $Timestamp,user_id,create_date
#			  from bass1.G_A_02004_DAY
#			 where create_date='20100701'
#			   and time_id=20100701
#			"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#	#�����ʱ��6
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.G_A_02004_DAY_create "
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#
#	#02004�ӿ���������,�Ȳ���G_A_02004_DAY_create����
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into G_A_02004_DAY_create
#			   (
#			    time_id
#			   ,user_id
#			   ,cust_id
#			   ,usertype_id
#			   ,create_date
#			   ,user_bus_typ_id
#			   ,product_no
#			   ,imsi
#			   ,cmcc_id
#			   ,channel_id
#			   ,mcv_typ_id
#			   ,prompt_type
#			   ,subs_style_id
#			   ,brand_id
#			   ,sim_code
#			   )
#			 select
#			   $Timestamp
#			   ,a.user_id
#			   ,a.cust_id
#			   ,case 
#			     when a.test_mark=1 then '3' 
#			     when a.free_mark=1 then '2'
#			     else '1' 
#			    end
#			   ,replace(char(a.create_date),'-','')
#			   ,'01'
#			   ,a.product_no
#			   ,'0' as imsi
#			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13100')
#			   ,char(a.channel_id)
#			   ,'1'
#				,case
#				when a.accttype_id=0 then '1'
#				else '2'
#				end
#			   ,'04'
#			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') as brand_id
#			   ,case 
#			     when a.crm_brand_id2=70 then '1' 
#			     else '0' 
#			    end
#			 from bass2.dw_product_bass1_$Timestamp a,
#			      BASS1.temp_check_02004_2 b             
#			 where a.user_id = b.user_id
#			   and b.time_id = $Timestamp
#	"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#
#	#�����ʱ��7,��ȡ������ڵļ�¼�û��嵥
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.G_A_02004_DAY_create_2 "
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#
#	#02004�ӿ���������,�Ȳ���G_A_02004_DAY_create_2����
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into G_A_02004_DAY_create_2
#			   (
#			    time_id
#			   ,user_id
#			   )
#			 select
#			    a.time_id
#			   ,a.user_id
#			 from BASS1.G_A_02004_DAY a,
#			      BASS1.G_A_02004_DAY_create b             
#			 where a.user_id = b.user_id
#			   and b.time_id = $Timestamp
#			   and a.time_id = $Timestamp
#	"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#
#	#ɾ��02004�ӿڱ��е�����ڵļ�¼�û��嵥
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.G_A_02004_DAY where user_id in (select user_id from bass1.G_A_02004_DAY_create_2 where time_id = $Timestamp ) and time_id = $Timestamp"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#
#	#02004�ӿ������������һ�����������޸����û��嵥����
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into G_A_02004_DAY
#			   (
#			    time_id
#			   ,user_id
#			   ,cust_id
#			   ,usertype_id
#			   ,create_date
#			   ,user_bus_typ_id
#			   ,product_no
#			   ,imsi
#			   ,cmcc_id
#			   ,channel_id
#			   ,mcv_typ_id
#			   ,prompt_type
#			   ,subs_style_id
#			   ,brand_id
#			   ,sim_code
#			   )
#			 select
#			    time_id
#			   ,user_id
#			   ,cust_id
#			   ,usertype_id
#			   ,create_date
#			   ,user_bus_typ_id
#			   ,product_no
#			   ,imsi
#			   ,cmcc_id
#			   ,channel_id
#			   ,mcv_typ_id
#			   ,prompt_type
#			   ,subs_style_id
#			   ,brand_id
#			   ,sim_code
#			 from BASS1.G_A_02004_DAY_create
#			 where time_id = $Timestamp
#	"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#


#	
#       #���������־�ֶ�@20070801 By tym
#       if {$today_dd > 12} {
#           set RegDatFrmMis "bass2.stat_zd_village_users_$last_month" 
#       } else {
#	   set RegDatFrmMis "bass2.stat_zd_village_users_$last_last_month" 
#       }    
#              
#        set handle [aidb_open $conn]
#	set sql_buff "update BASS1.g_a_02004_day a
#                     set a.region_flag=value(( select 
#                                                 char(LOCNTYPE_ID)  
#                                               from 
#                                                 $RegDatFrmMis b 
#                                               where 
#                                                 a.time_id=$Timestamp 
#                                                 and a.user_id=b.user_id
#                                               ),'2')
#                     where a.time_id=$Timestamp	"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	       
#2011-07-31 16:16:18
source /bassapp/bass1/tcl/INT_02004_02062_SIMCODE.tcl
Deal_imp $op_time $optime_month

#20111021 �޸��������
#source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
#Deal_fiximsi $op_time $optime_month

return 0

}
################################�ο�#################
# when crm_brand_id2 in (30,190) then '02'  --�����׼�����к��±�׼������--
#####################################################