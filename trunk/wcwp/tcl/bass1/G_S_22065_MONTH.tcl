######################################################################################################
#�ӿ����ƣ������������
#�ӿڱ��룺22065
#�ӿ�˵������¼���е��������������
#��������: G_S_22065_MONTH.tcl
#��������: ����22065������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2010-12-30
#�����¼��1.7.0 �淶�����ӿ�
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22065_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff


    # ����ʱ��1
    set sql_buff "
			DECLARE GLOBAL TEMPORARY TABLE SESSION.G_S_22065_MONTH_TMP
          (
						CMCC_ID        CHARACTER(5)    NOT NULL,
						ZZHI_CNT       BIGINT,
						TERM_CNT       BIGINT,
						CARD_CNT       BIGINT,
						PAYMENT_CNT    BIGINT,
						OTHER_CNT      BIGINT,
						E_PAY_AMOUNT   BIGINT,
						O_TERM_AMOUNT  BIGINT,
						ZHI_CUST_CNT   BIGINT,
						E_CUST_CNT     BIGINT,
						TX_CUST_CNT    BIGINT
           )
      PARTITIONING KEY (CMCC_ID) USING HASHING
      WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED IN TBS_USER_TEMP
			"
    puts $sql_buff
    exec_sql $sql_buff

########Y �ƶ�QQ����ҵ��
########W PIM-�Ų��ܼ�
########S �ͻ���
########Q 12580IVR
########L WEB
########J SP
########I ����Ӫҵ��(MMS)
########H ��������ƽ̨
########e �ͷ�
########D 12530.IVR
########c CMMB�ͻ���
########b Ӫҵǰ̨������
########B ����Ӫҵ��
########9 �����ն˽ɷ�
########6 WAPӪҵ��
########5 USSD
########4 ����Ӫҵ��

    #1��ץȡ��ֵҵ�������������ҵ�������
    #����ʱ����뱾������
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_TMP
					  (
							cmcc_id
							,zzhi_cnt
							,term_cnt
							,card_cnt
							,payment_cnt
							,other_cnt
							,e_pay_amount
							,o_term_amount
							,zhi_cust_cnt
							,e_cust_cnt
							,tx_cust_cnt
						)
				select
				 COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(h.REGION_CODE,2,3)),'13101'),
				sum(case when b.offer_type=2 then 1 else 0 end),
				0,0,0,
				sum(case when b.offer_type<>2 then 1 else 0 end),
				0,0,0,0,0
				  from bass2.dw_product_ord_cust_$op_month a,
				       bass2.dw_product_ord_offer_dm_$op_month b,
				       bass2.dim_prod_up_product_item d,
				       bass2.dim_pub_channel e,
				       bass2.dim_sys_org_role_type f,
				       bass2.dim_cfg_static_data g,
				       bass2.dw_channel_info_$op_month h
				 where a.customer_order_id = b.customer_order_id
				   and b.offer_id = d.product_item_id
				   and a.org_id = e.channel_id
				   and a.channel_type = g.code_value
				   and g.code_type = '911000'
				   and e.channeltype_id = f.org_role_type_id
				   and a.org_id = h.channel_id
				   and h.channel_type_class in (90105,90102)
				   and a.channel_type in ('Q','L','I','e','D','B','6','4')
				 group by  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(h.REGION_CODE,2,3)),'13101')     
	  "

    puts $sql_buff
    exec_sql $sql_buff

###4179	���п�ʵʱԤ��ɷ�
###4180	���п�ʵʱԤ��ɷѳ���
###4181	���п��¸��ѽɷ�
###4182	���п��¸��ѽɷѳ���
###4310	���п������ɷ�
###4311	���п������ɷѳ���
###4468	���Ͻɷ�
###4469	���Ͻɷѳ���

    #2�����ӳ�ֵ�ɷѱ���/���ӽ��ѽ��/���������ն˽��ѽ��
    #����ʱ����뱾������
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_TMP
					  (
							cmcc_id
							,zzhi_cnt
							,term_cnt
							,card_cnt
							,payment_cnt
							,other_cnt
							,e_pay_amount
							,o_term_amount
							,zhi_cust_cnt
							,e_cust_cnt
							,tx_cust_cnt
						)
				select
					  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',b.region_id),'13101'),
						0,0,0,
						count(*),
						0,
						sum(amount),
						sum(case when (c.paytype_name like '%�����ն�%' or c.paytype_name like '%���������ն�%') then amount else 0 end),
						0,0,0
				  from BASS2.dw_acct_payment_dm_$op_month b,BASS2.Dim_acct_paytype c
				where b.opt_code=c.paytype_id
				   and
				   (  c.paytype_name like '%�����ն�%' 
				   or c.paytype_name like '%���������ն�%'
				   or c.paytype_name like '%�ֻ�֧��%'
				   or c.paytype_name like '%WAP%'
				   or c.paytype_id in ('4179','4180','4181','4182','4310','4311','4468','4469')
				   )
				   and b.state='0'
				   and c.sts=1
				group by COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',b.region_id),'13101')
	    "

    puts $sql_buff
    exec_sql $sql_buff


    #3��ͨ�ſͻ���
    #����ʱ����뱾������
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_TMP
					  (
							cmcc_id
							,zzhi_cnt
							,term_cnt
							,card_cnt
							,payment_cnt
							,other_cnt
							,e_pay_amount
							,o_term_amount
							,zhi_cust_cnt
							,e_cust_cnt
							,tx_cust_cnt
						)
				select
				      COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',b.city_id),'13101'),
				        0,0,0,0,0,0,0,0,0,
				        count (distinct b.user_id)
				  from bass2.dw_product_$op_month b,
				  (select distinct user_id from bass1.int_22038_$op_month) c
				where b.user_id=c.user_id
				group by COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',b.city_id),'13101')
	    "

    puts $sql_buff
    exec_sql $sql_buff


    # ����ʱ��2
    #TYPE ���з���
    set sql_buff "
			DECLARE GLOBAL TEMPORARY TABLE SESSION.G_S_22065_MONTH_PRODUCT1
          (
						CMCC_ID        CHARACTER(5)    NOT NULL,
						E_PRODUCT_NO   CHARACTER(15),
						TYPE           CHARACTER(1)
           )
      PARTITIONING KEY (CMCC_ID) USING HASHING
      WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED IN TBS_USER_TEMP
			"
    puts $sql_buff
    exec_sql $sql_buff

    #����������½�ͻ��嵥-------------
    #��վ
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_PRODUCT1
					  (
							cmcc_id
							,e_product_no
							,type
						)
				select distinct 
				  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',b.city_id),'13101'),
				  c.login_product_no,
				  '1'
				from bass2.dw_product_$op_month b,
				     bass2.dw_wcc_user_action_ds c
				where c.login_product_no=b.product_no
				  and c.login_product_no is not null 
				  and c.is_success=1
				  and replace(char(date(c.login_time)),'-','')<='$this_month_first_day'
				  and replace(char(date(c.login_time)),'-','')<='$this_month_last_day'
	    "

    puts $sql_buff
    exec_sql $sql_buff    

    #����
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_PRODUCT1
					  (
							cmcc_id
							,e_product_no
							,type
						)
					select distinct 
					  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',a.city_id),'13101'),
					  b.phone_id,
					  '2'
					from bass2.dw_product_$op_month a, 
					     bass2.dw_kf_sms_cmd_receive_dm_$op_month b, 
					     bass2.dw_kf_cmd_hint_def_dm_$op_month c
					where a.product_no = b.phone_id 
					  and b.cmd_id = c.cmd_id 
					  and b.deal_result = c.cmd_result
					  and c.result_jieg in (0)
	    "

    puts $sql_buff
    exec_sql $sql_buff       

##    #���ţ��޳��˿��г�ֵ
##    set sql_buff "
##	    insert into SESSION.G_S_22065_MONTH_PRODUCT1
##					  (
##							cmcc_id
##							,e_product_no
##							,type
##						)
##					select distinct 
##					  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',a.city_id),'13101'),
##					  b.phone_id,
##					  '3'
##					from bass2.dw_product_$op_month a, 
##					     bass2.dw_kf_sms_cmd_receive_dm_$op_month b, 
##					     bass2.dw_kf_cmd_hint_def_dm_$op_month c
##					where a.product_no = b.phone_id 
##					  and b.cmd_id = c.cmd_id 
##					  and b.deal_result = c.cmd_result
##					  and c.result_jieg in (0)
##					  and c.remarks not like '%����%'
##	    "
##
##    puts $sql_buff
##    exec_sql $sql_buff  

    
    #�����˹�
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_PRODUCT1
					  (
							cmcc_id
							,e_product_no
							,type
						)
					select distinct 
					  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',a.city_id),'13101'),
					  b.caller,
					  '3'
					from bass2.dw_product_$op_month a, 
					     bass2.dw_custsvc_agent_tele_dm_$op_month b
					where a.product_no = b.caller
	    "

    puts $sql_buff
    exec_sql $sql_buff   


    #�����ն�
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_PRODUCT1
					  (
							cmcc_id
							,e_product_no
							,type
						)
					select distinct 
					  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',b.opt_region_id),'13101'),
					  b.key_num,
					  '4'
					from BASS2.dw_acct_payment_dm_$op_month b,
					     BASS2.Dim_acct_paytype c
					where b.opt_code=c.paytype_id
				   and
				   (  c.paytype_name like '%�����ն�%' 
				   or c.paytype_name like '%���������ն�%'
				   )
				   and b.state='0'
				   and c.sts=1
	    "

    puts $sql_buff
    exec_sql $sql_buff 


    #���г�ֵ�û�
    set sql_buff "
	    insert into SESSION.G_S_22065_MONTH_PRODUCT1
					  (
							cmcc_id
							,e_product_no
							,type
						)
					select
						  coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(CITY_ID)),'13101') 
						  ,product_no
						  ,'5'
						from bass2.stat_channel_reward_0007 
					WHERE channel_type in (90105,90102)
						  and op_time=$op_month
	    "

    puts $sql_buff
    exec_sql $sql_buff 

    #ͳ�Ƶ���������½�ͻ���
    set sql_buff "
		insert into SESSION.G_S_22065_MONTH_TMP
					  (
							cmcc_id
							,zzhi_cnt
							,term_cnt
							,card_cnt
							,payment_cnt
							,other_cnt
							,e_pay_amount
							,o_term_amount
							,zhi_cust_cnt
							,e_cust_cnt
							,tx_cust_cnt
						)
         select cmcc_id,
                0,0,0,0,0,0,0,0,
                count(distinct e_product_no),
                0
           from SESSION.G_S_22065_MONTH_PRODUCT1
          where type in ('1','2','3','4')
       group by cmcc_id
	    "

    puts $sql_buff
    exec_sql $sql_buff 

    #ͳ�����е���������½�ͻ���
    set sql_buff "
		insert into SESSION.G_S_22065_MONTH_TMP
					  (
							cmcc_id
							,zzhi_cnt
							,term_cnt
							,card_cnt
							,payment_cnt
							,other_cnt
							,e_pay_amount
							,o_term_amount
							,zhi_cust_cnt
							,e_cust_cnt
							,tx_cust_cnt
						)
				select bb.cmcc_id,
                0,0,0,0,0,0,0,
                count(distinct bb.e_product_no),
                0,0
         from
         (
         select cmcc_id,e_product_no
           from SESSION.G_S_22065_MONTH_PRODUCT1
          where type in ('1','2','3','4')
          except
         select cmcc_id,e_product_no
           from SESSION.G_S_22065_MONTH_PRODUCT1
          where type in ('5')
         ) bb
       group by bb.cmcc_id
	    "

    puts $sql_buff
    exec_sql $sql_buff 



    #����Ŀ���
    set sql_buff "
    insert into BASS1.G_S_22065_MONTH
				  (
				    time_id
				    ,statmonth
						,cmcc_id
						,zzhi_cnt
						,term_cnt
						,card_cnt
						,payment_cnt
						,other_cnt
						,e_pay_amount
						,o_term_amount
						,zhi_cust_cnt
						,e_cust_cnt
						,tx_cust_cnt
					)
		select $op_month
		      ,'$op_month'
		      ,cmcc_id
					,char(sum(zzhi_cnt))
					,char(sum(term_cnt))
					,char(sum(card_cnt))
					,char(sum(payment_cnt))
					,char(sum(other_cnt))
					,char(sum(e_pay_amount))
					,char(sum(o_term_amount))
					,char(sum(zhi_cust_cnt))
					,char(sum(e_cust_cnt))
					,char(sum(tx_cust_cnt))
			from SESSION.G_S_22065_MONTH_TMP
	group by cmcc_id
	"
    puts $sql_buff
    exec_sql $sql_buff



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
		#WriteTrace "$errmsg" 2005
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
		#WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		#WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle


	return $result
}
#--------------------------------------------------------------------------------------------------------------




