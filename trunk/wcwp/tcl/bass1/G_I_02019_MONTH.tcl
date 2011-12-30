######################################################################################################
#�ӿ����ƣ������ʷ��ײ�
#�ӿڱ��룺02019
#�ӿ�˵�����ʷ��ײ����Դ���Ż�Ϊ��Ҫ��ʽ�����Ŀ���û�����ʹ��ͨ��ҵ����Ƶģ�������ͨ�������š�����������ͨ��
#��Ʒ��ҵ����Ϻ���ʷѽṹ���ʷ��ײ���Ҫ��Ϊ�������ʷ��ײ�/��ѡ�����͡������ʷ��ײ�/��ѡ�������࣬���¼�ơ������ײ͡��͡������ײ͡���
#1.	�����ײ�/��ѡ���������û�Ϊ������������Ҫ���ڻ����ײ�֮��ѡȡ�ĵ����ʷѡ�
#2.	ÿ���ϴ���ֹͳ����ĩ���е����ײͣ��������ۺ�ͣ��״̬���ײ͡�
#��������: G_I_02019_MONTH.tcl
#��������: ����02019������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2011-02-18
#�����¼��1.
#�޸���ʷ: 1. 1.7.1 �淶
#�޸���ʷ: 1. 1.7.2 �淶
#2.
#3.	ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײ�Ҳ�ڱ��ӿ��ϱ����ϱ���ʽ˵����
#1)	�䡰�����ײͱ�ʶ�����밴����һ��BASS_STD1_0115��ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ���涨�����
#2.2)	��Ӧ�������ײ����÷�Χ���С�1����
#2011-05-03 16:51:19 ����sessionΪʵ����Ա��02021���á�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


      #���� yyyymm
      #set op_time 2011-01-01
      #set optime_month 2011-01
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      #�������һ�� yyyymmdd
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
      puts $op_time
      puts $op_month
		global app_name
		set app_name "G_I_02019_MONTH.tcl"        

  #ɾ����������
	set sql_buff "delete from bass1.g_i_02019_month where time_id=$op_month"
	exec_sql $sql_buff

  #�����м���ʱ��(һ����)
	##drop table BASS1.g_i_02019_month_1;
	##CREATE TABLE BASS1.g_i_02019_month_1
	## (
	##	  base_prod_id        bigint,
	##		trademark           bigint
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (base_prod_id
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.g_i_02019_month_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;

	
	set sql_buff "ALTER TABLE BASS1.g_i_02019_month_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

  #������ȡ�����ײ���Ϣ
	set sql_buff "
	insert into BASS1.g_i_02019_month_1
		  (
	     base_prod_id
			,trademark
		  )
		select 
		    a.product_item_id                         base_prod_id,
		    b.trademark
		from bass2.dim_prod_up_product_item a,
		  bass2.Dim_prod_up_offer b
		where a.item_type = 'OFFER_PLAN' 
		and b.offer_type in ('OFFER_PLAN')
		and a.product_item_id = b.offer_id
		and a.platform_id in (1,2)
		and replace(char(date(a.create_date)),'-','')<='$this_month_last_day'
		and replace(char(date(b.create_date)),'-','')<='$this_month_last_day'
   "

	exec_sql $sql_buff

	  aidb_runstats bass1.g_i_02019_month_1 3


  #�����м���ʱ��2

  #�����м���ʱ��(һ����)
	##drop table BASS1.g_i_02019_month_2;
	##CREATE TABLE BASS1.g_i_02019_month_2
	## (
	##	  base_prod_id        bigint,
	##		base_prod_name      character(200),
	##		prod_status         character(1),
	##		prod_begin_time     character(8),
	##		prod_end_time       character(8),
	##		platform_id         int,
	##		trademark           int
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (base_prod_id
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.g_i_02019_month_2
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
  

##161000000004 	���еش�
##161000000001 	ȫ��ͨ
##161000000005 	������
##161000000031 	���߿��
##161000000033 	С��ͥ���
##161000000032 	���߿��
##161000000101 	Ԥ��������ͷ���
##161000000102 	���ѷ���������[Ԥ��]
##161000000103 	Ԥ�������������

###1 ȫ��ͨ/4 ���еش�/9 ������/20 ����
###1�����ɵ�����ȫ��ͨ�����ײ��ϵĵ����ײ� =1
###2�����ɵ����������л����ײ���           =9
###3�����ɵ����ڶ��еش������ײ���         =4
###4���ȿɵ�����ȫ��ͨ�����ײͣ�Ҳ�ɵ����������л����ײ��ϵĵ����ײ� =10
###5���ȿɵ����ڶ��еش������ײͣ�Ҳ�ɵ����������л����ײ���         =13
###6���ȿɵ�����ȫ��ͨ�����ײͣ�Ҳ�ɵ����ڶ��еش��ײ���             =5
###7��������ȫ��ͨ�����еش��������еĵ����ײ�                       =14
###11�����������������ſͻ��ĵ����ײ͡��������ײͣ�     

	set sql_buff "ALTER TABLE BASS1.g_i_02019_month_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

  #��ȡ�ͻ����ײʹ��ڹ�ϵ�ĵ����ײ���Ϣ
	set sql_buff "
	insert into BASS1.g_i_02019_month_2
		  (
	     base_prod_id
	    ,base_prod_name
	    ,prod_status
	    ,prod_begin_time
	    ,prod_end_time
	    ,platform_id
			,trademark
		  )
		select 
		    aa.product_item_id  base_prod_id,
		    aa.name             base_prod_name,
		    case when aa.del_flag='1' then '1'
		    else '2' end prod_status,
		    replace(char(date(aa.create_date)),'-','') prod_begin_time,
		    replace(char(date(aa.exp_date)),'-','')    prod_end_time,
		    aa.platform_id,
		    case when bb.trademark=161000000001 then 1 
		         when bb.trademark=161000000005 then 4
		         when bb.trademark=161000000004 then 9
		    else 20 end trademark
		from bass2.dim_prod_up_product_item aa,
		(
		select distinct b.relat_product_item_id,a.trademark
		from bass1.g_i_02019_month_1 a,
		     bass2.dim_prod_up_plan_plan_rel b
		where a.base_prod_id = b.product_item_id
		  and (b.extend_attr_g <> '0' or b.extend_attr_g is null)
		) bb
		where aa.product_item_id=bb.relat_product_item_id
   "

	exec_sql $sql_buff

	  aidb_runstats bass1.g_i_02019_month_2 3


#####********������ȡ����Ʒ�Ƶ��ײͱ�ʶ*********######

	set sql_buff "
	declare global temporary table session.g_i_02019_month_tmp3
		(   
		  base_prod_id        bigint,
		  cnt                 int
		)
	partitioning key (base_prod_id) using hashing
	with replace on commit preserve rows not logged in tbs_user_temp"
	exec_sql $sql_buff


	set sql_buff "
	insert into session.g_i_02019_month_tmp3
		  (
	     base_prod_id
			,cnt
		  )
   select base_prod_id,sum(trademark) from bass1.g_i_02019_month_2 group by base_prod_id
   "

	exec_sql $sql_buff

	#2011-05-01 20:21:29
	#1)	�䡰�����ײͱ�ʶ�����밴����һ��BASS_STD1_0115��ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ���涨�����
	#2.2)	��Ӧ�������ײ����÷�Χ���С�1����
	#

  #����Ŀ���,״̬ID����ʧЧʱ������ٴ��ж�
	set sql_buff "
	insert into bass1.g_i_02019_month
		  (
				time_id
				,over_prod_id
				,over_prod_name
				,over_prod_area
				,vac_prod_flag
				,city_prod_flag
				,prod_status
				,prod_begin_time
				,prod_end_time
		  )
		select 
		   $op_month
		  ,value(d.bass1_offer_id,char(a.base_prod_id))   over_prod_id
		  ,a.base_prod_name over_prod_name
		  ,case when b.cnt=1 then '1'
		        when b.cnt=9 then '2'
		        when b.cnt=4 then '3'
		        when b.cnt=10 then '4'
		        when b.cnt=13 then '5'
		        when b.cnt=5 then '6'
		        when b.cnt=14 then '7'
		   else '11' end over_prod_area
		   ,case when a.base_prod_name like '%����%' then '1' else '0' end vac_prod_flag
		   ,case when a.base_prod_name like '%����%' then '1' else '0' end city_prod_flag
		   ,case when a.prod_end_time<='$this_month_last_day' or a.prod_status='2' then '2' else '1' end prod_status
		   ,a.prod_begin_time
		   ,a.prod_end_time
		 from 
		    (
		    select distinct
		       base_prod_id
		      ,base_prod_name
		      ,prod_status
		      ,prod_begin_time
		      ,prod_end_time
		    from bass1.g_i_02019_month_2
		    ) a
		left join session.g_i_02019_month_tmp3 b on a.base_prod_id = b.base_prod_id
	left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
							from  BASS1.ALL_DIM_LKP 
							where BASS1_TBID = 'BASS_STD1_0115'
					      and bass1_value like 'QW_QQT_DJ%'
				      ) d on char(a.base_prod_id) = d.offer_id
  "

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02019_month 3


#for 02021 :���02021 ���ı�
  #�����м���ʱ��(һ����)
	##drop table BASS1.g_i_02019_month_4;
	##CREATE TABLE BASS1.g_i_02019_month_4
	## (
	##	  base_prod_id        bigint
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (base_prod_id
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.g_i_02019_month_4
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
	

	set sql_buff "ALTER TABLE BASS1.g_i_02019_month_4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
			set sql_buff "
		insert into bass1.g_i_02019_month_4
		select distinct base_prod_id 
		from  bass1.g_i_02019_month_2
		"
	exec_sql $sql_buff
	
	  aidb_runstats bass1.g_i_02019_month_4 3


	
#��������Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select over_prod_id,count(*) cnt from bass1.g_i_02019_month
	              where time_id =$op_month
	             group by over_prod_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02019�½ӿڵ����ʷ��ײ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }



	return 0
}
