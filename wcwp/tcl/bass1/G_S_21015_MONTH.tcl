######################################################################################################
#�ӿ����ƣ��������ֺ��ƶ�������ͨ�û������
#�ӿڱ��룺21015
#�ӿ�˵����ָ�������ֺ��ƶ�������ͨ�û��������
#��������: G_S_21015_MONTH.tcl
#��������: ����21015������
#��������: ��
#Դ    ��1. bass2.dw_comp_cust_yyyymm(���������û����ϱ�)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ�����ձ���ҵ���»���ֻ�����ֲ��־������ֵ�Ʒ�ƣ���bass2.dim_comp_brand��
#          2.��û�в��ԣ���Ϊdw_comp_cust_200703��û�м���������������ʧ��־
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21015_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
              
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21015_month
                      select
                        $op_month
                        ,'$op_month'
                        ,case when a.comp_brand_id=3 then '021100'   
                               when a.comp_brand_id=4	then '021900'   
                               when a.comp_brand_id=7	then '022000'   
                               when a.comp_brand_id=10	then '031100' 
                               when a.comp_brand_id=9	then '031200'   
                               when a.comp_brand_id=11	then '031900' 
                               when a.comp_brand_id=2	then '032000'   
                               when a.comp_brand_id in(1,8) then	'033000' 
                               when a.comp_brand_id=6	then '034000'   
                               when a.comp_brand_id=5	then '051000'   
                               when a.comp_brand_id=12	then '080000' 
                               when a.comp_brand_id=13	then '990000'
                          else '990000'
                          end 
                        ,char(sum(a.comp_month_new_mark))
                        ,char(sum(a.comp_month_off_mark))
                      from 
                        bass2.dw_comp_cust_$op_month a
                      group by 
                        case when a.comp_brand_id=3 then '021100'   
                               when a.comp_brand_id=4	then '021900'   
                               when a.comp_brand_id=7	then '022000'   
                               when a.comp_brand_id=10	then '031100' 
                               when a.comp_brand_id=9	then '031200'   
                               when a.comp_brand_id=11	then '031900' 
                               when a.comp_brand_id=2	then '032000'   
                               when a.comp_brand_id in(1,8) then	'033000' 
                               when a.comp_brand_id=6	then '034000'   
                               when a.comp_brand_id=5	then '051000'   
                               when a.comp_brand_id=12	then '080000' 
                               when a.comp_brand_id=13	then '990000'
                          else '990000'
                          end  "
        puts $sql_buff
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