######################################################################################################
#�ӿ����ƣ��ƶ�400ҵ��������ϸ
#�ӿڱ��룺22307
#�ӿ�˵�����˽ӿ��ϱ��ƶ�400ҵ������4001���뼶��ϸ��
#          ����������03017�����ſͻ�ͳ�����룩�ӿ����ƶ�400ҵ������Ӧ��һ�¡�
#��������: G_S_22307_MONTH.tcl
#��������: ����22307������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-10-27
#�����¼��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        puts $timestamp
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        set ThisMonthFirstDay [string range $optime_month 0 6][string range $optime_month 4 4]01
        puts $ThisMonthFirstDay

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set objecttable "G_S_22307_MONTH"
        #set db_user $env(DB_USER)

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		# WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	# �ƶ�400ҵ��
	set handle [aidb_open $conn]
	set sql_buff "
			insert into bass1.$objecttable
				(    TIME_ID
						,ENTERPRISE_ID
						,NUM4001
						,INCOME    )
			select ${op_month}
						,aa.enterprise_id as enterprise_id
						,bb.feature_value
           	,char(sum(bigint(aa.unipay_fee*100))) 
       from  (select case when b.enterprise_id is not null then b.enterprise_id
											    when c.enterprise_id is not null then c.enterprise_id 
											    when d.enterprise_id is not null then d.enterprise_id
								      	  else '' 
								     end as enterprise_id,
					 					 a.user_id,
										 sum(case when b.acct_id is not null then a.fact_fee else 0 end) as unipay_fee 
							from (select cust_id
													,acct_id
													,user_id
													,sum(fact_fee) as fact_fee
										from bass2.dw_acct_shoulditem_${op_month}
										where item_id in (80000549,80000550,80000517,80000518,80000519,80000540)
										group by cust_id
														,acct_id
														,user_id ) a 
							left join ( select cust_id
																,acct_id
																,enterprise_id 
                          from bass2.dw_enterprise_sub_${op_month}
                          where service_id in '931' and rec_status = 1
					                group by cust_id
					                				,acct_id
					                				,enterprise_id
					                ) b on a.cust_id=b.cust_id and a.acct_id = b.acct_id
					    left join bass2.dw_enterprise_msg_${op_month} c on a.cust_id=c.cust_id
					    left join bass2.dw_enterprise_member_his_${op_month} d on a.cust_id=d.cust_id
					group by case when b.enterprise_id is not null then b.enterprise_id 
										    when c.enterprise_id is not null then c.enterprise_id 
										    when d.enterprise_id is not null then d.enterprise_id
					         			else '' 
					         end,
					   		 a.user_id
					) aa
    , ( select order_id,feature_value 
    		from bass2.Dwd_group_order_featur_$last_month_day where feature_id='4115017001'
			) bb
   where aa.user_id = bb.order_id
         and aa.enterprise_id is not null
         and aa.enterprise_id not in ('89102999670396','89103000041929')
   group by aa.enterprise_id ,
            bb.feature_value
                  "
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		# WriteTrace "$errmsg" 2020
		aidb_close $handle
		puts $errmsg
		return -1
	}
	aidb_commit $conn

	aidb_close $handle

	return 0
}
