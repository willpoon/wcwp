######################################################################################################
#�ӿ����ƣ����ſͻ���ͨ��ҵӦ�����
#�ӿڱ��룺02050
#�ӿ�˵�������ſͻ�����ͨ����ҵӦ��ҵ������ݡ�
#��������: G_I_02050_MONTH.tcl
#��������: ����02050������
#��������: ��
#Դ    ��1.bass2.dw_newbusi_ismg_yyyymm(�ƶ��������嵥)
#          2.bass2.dw_enterprise_member_yyyymm(���ų�Ա�±�)
#          3.bass2.dw_product_yyyymm(�û��±�) 
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��  
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.ֻ�м����û���û��SI 2.����ֻ��У��ͨ������ͨ
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
           
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM bass1.G_I_02050_MONTH where time_id=$op_month  "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_I_02050_MONTH
                      select
                      $op_month,
                      '0',
                      enterprise_id,
                      substr(enterprise_name,1,20),
                      '',
                      sp_code,
                      ser_code,
                      oper_code,
                      '',
                      case when apptype_id=1 then '110'
                           when apptype_id=2 then '120'
                      	 end,
                      '1',
                      case when bill_flag=3 then '1'
                           else char(bill_flag)	
                      	 end 
                      from
                      (
                      select
                        enterprise_id,
                        max(enterprise_name) as enterprise_name,
                        max(sp_code) as sp_code,
                        max(ser_code) as ser_code,
                        max(oper_code) as oper_code,
                        max(apptype_id) as apptype_id,
                        max(bill_flag) as bill_flag
                      from
                      (
                      select 
                        b.enterprise_id,
                        b.enterprise_name,
                        a.user_id,
                        a.sp_code,
                        a.ser_code,
                        oper_code,
                        a.apptype_id,
                        a.bill_flag 
                      from 
                        bass2.dw_newbusi_ismg_${op_month} a,
                        bass2.dw_enterprise_member_${op_month} b,
                      	bass2.dw_product_${op_month} c 
                      where 
                        a.apptype_id in (1,2) 
                        and a.user_id=c.user_id 
                        and b.cust_id=c.cust_id 
                      )aa
                    group by
                      enterprise_id
                    )cc"
        
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn

	aidb_close $handle

	return 0
}
