######################################################################################################
#�ӿ����ƣ��������ֺ��ƶ���ҵ���û���ͨ���
#�ӿڱ��룺21014
#�ӿ�˵������¼�й��ƶ��������ֺ��й��ƶ��û�֮�以�����ţ���ͨ���ţ��������
#��������: G_S_21014_MONTH.tcl
#��������: ����21014������
#��������: ��
#Դ    ��1.bass2.dw_comp_all_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ�����ձ���ҵ���»���ֻ�����ֲ��־������ֵ�Ʒ�ƣ���bass2.dim_comp_brand��
#�޸���ʷ: 1.2009-05-26 zhanght ����һ���淶1.5.8���޸ġ�����������Ӫ��Ʒ�����ͱ��롱������������ҵ�����ͱ��롱
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  #���� yyyymm
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
  
  #ɾ����������
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21014_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
      
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21014_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end
                        ,'01' 
                        ,char(count(distinct comp_product_no))
                        ,char(sum(mo_sms_counts + mt_sms_counts))
                      from  bass2.dw_comp_all_$op_month
                      where  sms_mark=1
                      group by 
                        case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end "
        puts $sql_buff    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle





  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21014_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end
                        ,'02' 
                        ,char(count(distinct comp_product_no))
                        ,char(sum(mo_mms_counts + mt_mms_counts))
                      from  bass2.dw_comp_all_$op_month
                      where  mms_mark=1
                      group by 
                        case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end "
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