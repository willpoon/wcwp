
######################################################################################################		
#�ӿ�����: �����ն�����Ӫ���������û��������                                                               
#�ӿڱ��룺02067                                                                                          
#�ӿ�˵�����û��������ն�ʱ��������ն�����Ӫ���������
#��������: G_S_02067_MONTH.tcl                                                                            
#��������: ����02067������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120605
#�����¼��
#�޸���ʷ: 1. panzw 20120605	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

        global app_name
        set app_name "G_S_02067_MONTH.tcl"
          
    #ɾ����������
	set sql_buf "delete from bass1.G_S_02067_MONTH where time_id=$op_month"
	exec_sql $sql_buf
#
	set sql_buf "ALTER TABLE BASS1.G_S_02067_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

#���ֶν�ȡ���·��ࣺ
#01:�Ż���վ
#02:10086�绰Ӫҵ��
#03: ����Ӫҵ��
#04:WAP��վ
#05:�����նˣ��������е������նˣ�������ʵ��������24СʱӪҵ���ڲ��ŵ������նˣ��������̳��ȳ��������ڷŵ������նˡ���
#

    #����������½�ͻ��嵥-------------
    #��վ
#�����ö����Ĵ��룬��������dw_product_ �����ú���ֱ�ӹ��� ������  a.userstatus_id>0 �ǲ����� , ����ĳ� a.userstatus_id in (1,2,3,6,8)

    set sql_buff "
	    insert into G_S_02067_MONTH_1
					  (
							e_product_no
							,type
						)
				select distinct 
				  b.login_product_no,
				  '01'
             from  bass2.dw_product_$op_month a,bass2.dw_wcc_user_action_$op_month b
             where b.login_product_no=a.product_no and b.login_type=1 and b.is_success=1
                   and a.userstatus_id in (1,2,3,6,8)
		   and replace(char(date(b.login_time)),'-','') like '$op_month%'
	    "

    exec_sql $sql_buff    

    set sql_buff "
	    insert into G_S_02067_MONTH
					  (
						 TIME_ID
						,USER_ID
						,IMEI
						,PLAN_DESC
						,EFF_DT
						,PLAN_DUR
						,MIN_CONSUME
						,PREPAY_FEE
						,PHONE_COST
						,GIFT_FEE
						,GIFT_DUR
						)
					select 
						$op_month TIME_ID
						, a.USER_ID
						,value(c.imei,'35470604221430') IMEI
						,a.COND_NAME PLAN_DESC
						, substr(replace(char(date(a.VALID_DATE)),'-',''),1,8) EFF_DT
						,char((year(date(EXPIRE_DATE))-year(date(VALID_DATE)))*12+(month(date(EXPIRE_DATE))-month(date(VALID_DATE))) + 1) PLAN_DUR
						,MIN_CONSUME
						,char(PROM_APPOR) PREPAY_FEE
						,char(RES_FEE) PHONE_COST
						,GIFT_FEE
						,GIFT_DUR
					from bass2.dw_product_user_promo_$op_month a 
					join bass2.ods_up_res_tiem_$op_month b  on  a.cond_id = bigint(b.prom_id)
					left join (					
						select user_id,max(imei) imei from 
						bass2.dw_product_mobilefunc_$op_month a
						where USERSTATUS_ID in (1,2,3,6,8)
						and  usertype_id in (1,2,9) 
						and a.imei is not null
						group by user_id 
					) c on a.user_id = c.user_id
 					where  substr(replace(char(date(a.VALID_DATE)),'-',''),1,6)  = '$op_month'					
					with ur
	    "

    exec_sql $sql_buff 



  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_02067_MONTH"
  set pk   "OP_MONTH||E_CHANNEL_TYPE"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
        
  aidb_runstats bass1.G_S_02067_MONTH 3


	return 0
}
