######################################################################################################
#�ӿ����ƣ��������ֺ��ƶ��û�ͨ�����
#�ӿڱ��룺21013
#�ӿ�˵����ָ���������û����й��ƶ��û���ͨ���������
#��������: G_S_21013_MONTH.tcl
#��������: ����21013������
#��������: ��
#Դ    ��1.bass2.dw_comp_all_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ�����ձ���ҵ���»���ֻ�����ֲ��־������ֵ�Ʒ�ƣ���bass2.dim_comp_brand��
#          2.��Ϊͨ��ʱ���ֵ���ʶֻ�б��ӿ�ʹ�ã����Ҹ�ά��Ƚ�������������CASE��䡣
#�޸���ʷ: 1.2009-05-21 zhanght ����һ���淶1.5.8���޸ġ�����������Ӫ��Ʒ�����ͱ��롱
##         2.2009-09-18 liuzhilong �޸Ĵ���ͳ���߼�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21013_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
              
        set handle [aidb_open $conn]
##	set sql_buff "insert into bass1.g_s_21013_month
##                      select
##                         ${op_month}
##                         ,'${op_month}'
##                         ,case                                                                     
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>0 and (aa.in_call_duration_m + aa.out_call_duration_m)<50 then     '01' 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=50 and ( aa.in_call_duration_m + aa.out_call_duration_m)<100 then  '02'                  
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=100 and (aa.in_call_duration_m + aa.out_call_duration_m)<150 then '03'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=150 and (aa.in_call_duration_m + aa.out_call_duration_m)<200 then '04'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=200 and (aa.in_call_duration_m + aa.out_call_duration_m)<250 then '05'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=250 and (aa.in_call_duration_m + aa.out_call_duration_m)<300 then '06'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=300 and (aa.in_call_duration_m + aa.out_call_duration_m)<350 then '07'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=350 and (aa.in_call_duration_m + aa.out_call_duration_m)<400 then '08'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=400 and (aa.in_call_duration_m + aa.out_call_duration_m)<450 then '09'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=450 and (aa.in_call_duration_m + aa.out_call_duration_m)<500 then '10'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=500 and (aa.in_call_duration_m + aa.out_call_duration_m)<550 then '11'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=550 and (aa.in_call_duration_m + aa.out_call_duration_m)<600 then '12'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=600 and (aa.in_call_duration_m + aa.out_call_duration_m)<650 then '13'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=650 and (aa.in_call_duration_m + aa.out_call_duration_m)<700 then '14'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=700 and (aa.in_call_duration_m + aa.out_call_duration_m)<750 then '15'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=750 and (aa.in_call_duration_m + aa.out_call_duration_m)<800 then '16'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=800 and (aa.in_call_duration_m + aa.out_call_duration_m)<850 then '17'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=850 and (aa.in_call_duration_m + aa.out_call_duration_m)<900 then '18'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=900 and (aa.in_call_duration_m + aa.out_call_duration_m)<950 then '19'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=950 and (aa.in_call_duration_m + aa.out_call_duration_m)<1000 then '20'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1000 and (aa.in_call_duration_m +aa.out_call_duration_m)<1100 then '21'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1100 and (aa.in_call_duration_m +aa.out_call_duration_m)<1200 then '22'                                      
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1200 and (aa.in_call_duration_m +aa.out_call_duration_m)<1300 then '23'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1300 and (aa.in_call_duration_m +aa.out_call_duration_m)<1400 then '24'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1400 and (aa.in_call_duration_m +aa.out_call_duration_m)<1500 then '25'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1500 and (aa.in_call_duration_m +aa.out_call_duration_m)<1600 then '26'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1600 and (aa.in_call_duration_m +aa.out_call_duration_m)<1700 then '27'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1700 and (aa.in_call_duration_m +aa.out_call_duration_m)<1800 then '28'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1800 and (aa.in_call_duration_m +aa.out_call_duration_m)<1900 then '29'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1900 and (aa.in_call_duration_m +aa.out_call_duration_m)<2000 then '30'
##                            when (aa.in_call_duration_m + aa.out_call_duration_m)>=2000 then '31' 
##                          end
##                         ,case when aa.comp_brand_id=3 then '021100'   
##                               when aa.comp_brand_id=4	then '021900'   
##                               when aa.comp_brand_id=7	then '022000'   
##                               when aa.comp_brand_id=10	then '031100' 
##                               when aa.comp_brand_id=9	then '031200'   
##                               when aa.comp_brand_id=11	then '031900' 
##                               when aa.comp_brand_id=2	then '032000'   
##                               when aa.comp_brand_id in(1,8) then	'033000' 
##                               when aa.comp_brand_id=6	then '034000'   
##                               when aa.comp_brand_id=5	then '051000'   
##                               when aa.comp_brand_id=12	then '080000' 
##                               when aa.comp_brand_id=13	then '990000'
##                          else '990000'
##                          end 
##                         ,char(sum(aa.comp_product_no_count))
##                         ,char(sum(aa.in_call_counts + aa.out_call_counts))
##                         ,char(sum(aa.in_call_duration_m + aa.out_call_duration_m))
##                       from (select comp_brand_id,cmcc_product_no,count(distinct comp_product_no) as comp_product_no_count,
##					                          sum(in_call_counts) as in_call_counts,sum(out_call_counts) as out_call_counts,
##					                          sum(in_call_duration_m) as in_call_duration_m,sum(out_call_duration_m) as out_call_duration_m 
##							                from  bass2.dw_comp_all_$op_month
##							                where sms_mark=0 and (in_call_duration_m + out_call_duration_m) > 0
##							             group by comp_brand_id,cmcc_product_no
##						                	) aa
##                       group by 
##                          case                                                                     
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>0 and (aa.in_call_duration_m + aa.out_call_duration_m)<50 then     '01' 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=50 and ( aa.in_call_duration_m + aa.out_call_duration_m)<100 then  '02'                  
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=100 and (aa.in_call_duration_m + aa.out_call_duration_m)<150 then '03'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=150 and (aa.in_call_duration_m + aa.out_call_duration_m)<200 then '04'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=200 and (aa.in_call_duration_m + aa.out_call_duration_m)<250 then '05'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=250 and (aa.in_call_duration_m + aa.out_call_duration_m)<300 then '06'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=300 and (aa.in_call_duration_m + aa.out_call_duration_m)<350 then '07'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=350 and (aa.in_call_duration_m + aa.out_call_duration_m)<400 then '08'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=400 and (aa.in_call_duration_m + aa.out_call_duration_m)<450 then '09'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=450 and (aa.in_call_duration_m + aa.out_call_duration_m)<500 then '10'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=500 and (aa.in_call_duration_m + aa.out_call_duration_m)<550 then '11'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=550 and (aa.in_call_duration_m + aa.out_call_duration_m)<600 then '12'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=600 and (aa.in_call_duration_m + aa.out_call_duration_m)<650 then '13'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=650 and (aa.in_call_duration_m + aa.out_call_duration_m)<700 then '14'                 
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=700 and (aa.in_call_duration_m + aa.out_call_duration_m)<750 then '15'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=750 and (aa.in_call_duration_m + aa.out_call_duration_m)<800 then '16'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=800 and (aa.in_call_duration_m + aa.out_call_duration_m)<850 then '17'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=850 and (aa.in_call_duration_m + aa.out_call_duration_m)<900 then '18'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=900 and (aa.in_call_duration_m + aa.out_call_duration_m)<950 then '19'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=950 and (aa.in_call_duration_m + aa.out_call_duration_m)<1000 then '20'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1000 and (aa.in_call_duration_m +aa.out_call_duration_m)<1100 then '21'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1100 and (aa.in_call_duration_m +aa.out_call_duration_m)<1200 then '22'                                      
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1200 and (aa.in_call_duration_m +aa.out_call_duration_m)<1300 then '23'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1300 and (aa.in_call_duration_m +aa.out_call_duration_m)<1400 then '24'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1400 and (aa.in_call_duration_m +aa.out_call_duration_m)<1500 then '25'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1500 and (aa.in_call_duration_m +aa.out_call_duration_m)<1600 then '26'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1600 and (aa.in_call_duration_m +aa.out_call_duration_m)<1700 then '27'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1700 and (aa.in_call_duration_m +aa.out_call_duration_m)<1800 then '28'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1800 and (aa.in_call_duration_m +aa.out_call_duration_m)<1900 then '29'
##                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1900 and (aa.in_call_duration_m +aa.out_call_duration_m)<2000 then '30'
##                            when (aa.in_call_duration_m + aa.out_call_duration_m)>=2000 then '31' 
##                          end
##                         ,case when aa.comp_brand_id=3 then '021100'   
##                               when aa.comp_brand_id=4	then '021900'   
##                               when aa.comp_brand_id=7	then '022000'   
##                               when aa.comp_brand_id=10	then '031100' 
##                               when aa.comp_brand_id=9	then '031200'   
##                               when aa.comp_brand_id=11	then '031900' 
##                               when aa.comp_brand_id=2	then '032000'   
##                               when aa.comp_brand_id in(1,8) then	'033000' 
##                               when aa.comp_brand_id=6	then '034000'   
##                               when aa.comp_brand_id=5	then '051000'   
##                               when aa.comp_brand_id=12	then '080000' 
##                               when aa.comp_brand_id=13	then '990000'
##                          else '990000'     
##                          end  "
##        puts $sql_buff


	set sql_buff "insert into bass1.g_s_21013_month
                      select
                         ${op_month}
                         ,'${op_month}'
                         ,case                                                                     
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>0 and (aa.in_call_duration_m + aa.out_call_duration_m)<50 then     '01' 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=50 and ( aa.in_call_duration_m + aa.out_call_duration_m)<100 then  '02'                  
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=100 and (aa.in_call_duration_m + aa.out_call_duration_m)<150 then '03'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=150 and (aa.in_call_duration_m + aa.out_call_duration_m)<200 then '04'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=200 and (aa.in_call_duration_m + aa.out_call_duration_m)<250 then '05'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=250 and (aa.in_call_duration_m + aa.out_call_duration_m)<300 then '06'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=300 and (aa.in_call_duration_m + aa.out_call_duration_m)<350 then '07'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=350 and (aa.in_call_duration_m + aa.out_call_duration_m)<400 then '08'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=400 and (aa.in_call_duration_m + aa.out_call_duration_m)<450 then '09'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=450 and (aa.in_call_duration_m + aa.out_call_duration_m)<500 then '10'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=500 and (aa.in_call_duration_m + aa.out_call_duration_m)<550 then '11'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=550 and (aa.in_call_duration_m + aa.out_call_duration_m)<600 then '12'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=600 and (aa.in_call_duration_m + aa.out_call_duration_m)<650 then '13'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=650 and (aa.in_call_duration_m + aa.out_call_duration_m)<700 then '14'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=700 and (aa.in_call_duration_m + aa.out_call_duration_m)<750 then '15'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=750 and (aa.in_call_duration_m + aa.out_call_duration_m)<800 then '16'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=800 and (aa.in_call_duration_m + aa.out_call_duration_m)<850 then '17'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=850 and (aa.in_call_duration_m + aa.out_call_duration_m)<900 then '18'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=900 and (aa.in_call_duration_m + aa.out_call_duration_m)<950 then '19'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=950 and (aa.in_call_duration_m + aa.out_call_duration_m)<1000 then '20'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1000 and (aa.in_call_duration_m +aa.out_call_duration_m)<1100 then '21'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1100 and (aa.in_call_duration_m +aa.out_call_duration_m)<1200 then '22'                                      
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1200 and (aa.in_call_duration_m +aa.out_call_duration_m)<1300 then '23'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1300 and (aa.in_call_duration_m +aa.out_call_duration_m)<1400 then '24'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1400 and (aa.in_call_duration_m +aa.out_call_duration_m)<1500 then '25'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1500 and (aa.in_call_duration_m +aa.out_call_duration_m)<1600 then '26'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1600 and (aa.in_call_duration_m +aa.out_call_duration_m)<1700 then '27'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1700 and (aa.in_call_duration_m +aa.out_call_duration_m)<1800 then '28'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1800 and (aa.in_call_duration_m +aa.out_call_duration_m)<1900 then '29'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1900 and (aa.in_call_duration_m +aa.out_call_duration_m)<2000 then '30'
                            when (aa.in_call_duration_m + aa.out_call_duration_m)>=2000 then '31' 
                          end
                         ,case when aa.comp_brand_id=3 then '021100'   
                               when aa.comp_brand_id=4	then '021900'   
                               when aa.comp_brand_id=7	then '022000'   
                               when aa.comp_brand_id=10	then '031100' 
                               when aa.comp_brand_id=9	then '031200'   
                               when aa.comp_brand_id=11	then '031900' 
                               when aa.comp_brand_id=2	then '032000'   
                               when aa.comp_brand_id in(1,8) then	'033000' 
                               when aa.comp_brand_id=6	then '034000'   
                               when aa.comp_brand_id=5	then '051000'   
                               when aa.comp_brand_id=12	then '080000' 
                               when aa.comp_brand_id=13	then '990000'
                          else '990000'
                          end 
                         ,char(sum(aa.comp_product_no_count))
                         ,char(sum(aa.in_call_counts + aa.out_call_counts))
                         ,char(sum(aa.in_call_duration_m + aa.out_call_duration_m))
                       from (select comp_brand_id,comp_product_no,count(distinct comp_product_no) as comp_product_no_count,
					                          sum(in_call_counts) as in_call_counts,sum(out_call_counts) as out_call_counts,
					                          sum(in_call_duration_m) as in_call_duration_m,sum(out_call_duration_m) as out_call_duration_m 
							                from  bass2.dw_comp_all_$op_month
							                where sms_mark=0 and (in_call_duration_m + out_call_duration_m) > 0
							             group by comp_brand_id,comp_product_no
						                	) aa
                       group by 
                          case                                                                     
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>0 and (aa.in_call_duration_m + aa.out_call_duration_m)<50 then     '01' 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=50 and ( aa.in_call_duration_m + aa.out_call_duration_m)<100 then  '02'                  
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=100 and (aa.in_call_duration_m + aa.out_call_duration_m)<150 then '03'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=150 and (aa.in_call_duration_m + aa.out_call_duration_m)<200 then '04'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=200 and (aa.in_call_duration_m + aa.out_call_duration_m)<250 then '05'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=250 and (aa.in_call_duration_m + aa.out_call_duration_m)<300 then '06'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=300 and (aa.in_call_duration_m + aa.out_call_duration_m)<350 then '07'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=350 and (aa.in_call_duration_m + aa.out_call_duration_m)<400 then '08'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=400 and (aa.in_call_duration_m + aa.out_call_duration_m)<450 then '09'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=450 and (aa.in_call_duration_m + aa.out_call_duration_m)<500 then '10'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=500 and (aa.in_call_duration_m + aa.out_call_duration_m)<550 then '11'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=550 and (aa.in_call_duration_m + aa.out_call_duration_m)<600 then '12'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=600 and (aa.in_call_duration_m + aa.out_call_duration_m)<650 then '13'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=650 and (aa.in_call_duration_m + aa.out_call_duration_m)<700 then '14'                 
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=700 and (aa.in_call_duration_m + aa.out_call_duration_m)<750 then '15'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=750 and (aa.in_call_duration_m + aa.out_call_duration_m)<800 then '16'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=800 and (aa.in_call_duration_m + aa.out_call_duration_m)<850 then '17'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=850 and (aa.in_call_duration_m + aa.out_call_duration_m)<900 then '18'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=900 and (aa.in_call_duration_m + aa.out_call_duration_m)<950 then '19'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=950 and (aa.in_call_duration_m + aa.out_call_duration_m)<1000 then '20'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1000 and (aa.in_call_duration_m +aa.out_call_duration_m)<1100 then '21'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1100 and (aa.in_call_duration_m +aa.out_call_duration_m)<1200 then '22'                                      
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1200 and (aa.in_call_duration_m +aa.out_call_duration_m)<1300 then '23'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1300 and (aa.in_call_duration_m +aa.out_call_duration_m)<1400 then '24'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1400 and (aa.in_call_duration_m +aa.out_call_duration_m)<1500 then '25'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1500 and (aa.in_call_duration_m +aa.out_call_duration_m)<1600 then '26'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1600 and (aa.in_call_duration_m +aa.out_call_duration_m)<1700 then '27'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1700 and (aa.in_call_duration_m +aa.out_call_duration_m)<1800 then '28'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1800 and (aa.in_call_duration_m +aa.out_call_duration_m)<1900 then '29'
                            when  (aa.in_call_duration_m + aa.out_call_duration_m)>=1900 and (aa.in_call_duration_m +aa.out_call_duration_m)<2000 then '30'
                            when (aa.in_call_duration_m + aa.out_call_duration_m)>=2000 then '31' 
                          end
                         ,case when aa.comp_brand_id=3 then '021100'   
                               when aa.comp_brand_id=4	then '021900'   
                               when aa.comp_brand_id=7	then '022000'   
                               when aa.comp_brand_id=10	then '031100' 
                               when aa.comp_brand_id=9	then '031200'   
                               when aa.comp_brand_id=11	then '031900' 
                               when aa.comp_brand_id=2	then '032000'   
                               when aa.comp_brand_id in(1,8) then	'033000' 
                               when aa.comp_brand_id=6	then '034000'   
                               when aa.comp_brand_id=5	then '051000'   
                               when aa.comp_brand_id=12	then '080000' 
                               when aa.comp_brand_id=13	then '990000'
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
