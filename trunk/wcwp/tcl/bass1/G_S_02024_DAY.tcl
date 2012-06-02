
######################################################################################################		
#�ӿ�����: "ȫ��ͨ�����ʷ��ײ��û��ɹ�������"                                                               
#�ӿڱ��룺02024                                                                                          
#�ӿ�˵����"�ϱ���ǰ�����гɹ������ȫ��ͨ�����ʷ��ײ͵Ķ�����ϵ"
#��������: G_S_02024_DAY.tcl                                                                            
#��������: ����02024������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110727
#�����¼��
#�޸���ʷ: 1. panzw 20110727	1.7.4 newly added
#	2011-12-31 ����ͳһ�ʷѹ������
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #�ϸ��� yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #������
        global app_name
        set app_name "G_S_02024_DAY.tcl"
	
  #ɾ����������
	set sql_buff "delete from bass1.G_S_02024_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	set sql_buff "ALTER TABLE BASS1.G_S_02024_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	
  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
	set sql_buff "
	
insert into bass1.G_S_02024_DAY_1
select distinct 
	$timestamp time_id
	,a.PRODUCT_INSTANCE_ID USER_ID
	,char(a.offer_id) BASE_PKG_ID
	,char(a.ORG_ID) CHANNEL_ID
	,replace(char(date(a.CREATE_DATE)),'-','') REC_DT
	,replace(char(date(a.VALID_DATE) ),'-','') VALID_DT
from  bass2.Dw_product_ins_off_ins_prod_ds a 
	, bass2.dw_product_$timestamp b 
where  a.state =1 
	and a.PRODUCT_INSTANCE_ID=b.user_id 
	and b.usertype_id in (1,2,9) 
	and b.userstatus_id in (1,2,3,6,8)
	and a.valid_type in (1,2)
	and date(a.CREATE_DATE) = '$op_time'
	and date(a.VALID_DATE)>= '$op_time'
	and date(a.expire_date) >= '$op_time'
	and not exists ( 
			 select 1 from bass2.dwd_product_test_phone_$timestamp b 
			 where a.product_instance_id = b.USER_ID  and b.sts = 1
			)   
	and char(a.offer_id) in (
		SELECT char(offer_id) FROM BASS2.dim_prod_up_offer 
		WHERE OFFER_TYPE='OFFER_PLAN'
		and TRADEMARK = 161000000001
	union 
		select char(xzbas_value)  as offer_id 
		from  BASS1.ALL_DIM_LKP 
		where BASS1_TBID = 'BASS_STD1_0114'
		and bass1_value like 'QW_QQT_JC%'
		and XZBAS_COLNAME not like '�ײͼ���%'
	)
with ur

  "
	exec_sql $sql_buff

## ����ת��
##ȫ���ײ�IDת��


## ����ײͲ���ֱ��ȥ�أ��п�����������Ч

	set sql_buff "	
insert into bass1.G_S_02024_DAY
select t.time_id
,t.user_id
,value(b.new_pkg_id ,t.BASE_PKG_ID) BASE_PKG_ID
,t.CHANNEL_ID
,t.REC_DT
,t.VALID_DT
from (
select time_id
	,a.user_id
	, value(bass1.fn_get_all_dim_ex('BASS_STD1_0114',a.BASE_PKG_ID),a.BASE_PKG_ID) BASE_PKG_ID
	, value(char(b.CHANNEL_ID),'BASS1_DS') CHANNEL_ID
	, a.REC_DT
	, a.VALID_DT
from bass1.G_S_02024_DAY_1 a 
left join (select distinct channel_ID,organize_id from  bass2.dim_channel_info a where a.channel_type_class in (90105,90102) ) b on a.CHANNEL_ID = char(b.organize_id )
where a.base_pkg_id not in (SELECT 
			distinct XZBAS_VALUE
		FROM BASS1.ALL_DIM_LKP 
		WHERE BASS1_TBID = 'BASS_STD1_0114'
and BASS1_VALUE  not like '%QW%')
) t 
left join bass1.DIM_QW_QQT_PKGID  b on t.BASE_PKG_ID = b.old_pkg_id
with ur
"

	exec_sql $sql_buff
	
  #���н�����ݼ��
  #1.���chkpkunique
	set tabname "G_S_02024_DAY"
	set pk 			"user_id||BASE_PKG_ID"
	chkpkunique ${tabname} ${pk} ${timestamp}
  
  #2.��������û��Ƿ����û�����ͷ
	set sql_buff "select count(*) from 
	            (
		     select user_id from bass1.G_S_02024_DAY
		      where time_id =$timestamp
		       except
		  select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1
	            ) as a
	            "
	chkzero2 $sql_buff "���û������û�����ͷ"

##�ײ�ID ��� 02018
	set sql_buff "select count(*) from 
	            (
		     select distinct BASE_PKG_ID from bass1.G_S_02024_DAY
		      where time_id =$timestamp
		       except
		      select distinct value(b.new_pkg_id,BASE_PROD_ID) 
			from G_I_02018_MONTH a 
			left join bass1.DIM_QW_QQT_PKGID  b on a.BASE_PROD_ID = b.old_pkg_id
			where time_id = $last_month
	            ) as a
	            "
	chkzero2 $sql_buff "���ײͲ��� 02018 ��"

#### ���� 1���û����������У��
#
#	set tabname "G_S_02024_DAY"
#	set pk 			"user_id"
#	chkpkunique ${tabname} ${pk} ${timestamp}
# 

##channel_ID ��� 
#�ݲ�У��
#
#	set sql_buff "select count(*) from 
#	            (
#		     select distinct CHANNEL_ID from bass1.G_S_02024_DAY
#		      where time_id =$timestamp and CHANNEL_ID not like  'BASS1%'
#		       except
#		     select distinct CHANNEL_ID from G_I_06021_MONTH where time_id = $last_month
#	            ) as a
#	            "
#	chkzero2 $sql_buff "�зǷ�����"
	return 0
}

