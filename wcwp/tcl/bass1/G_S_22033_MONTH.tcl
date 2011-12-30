######################################################################################################
#�ӿ����ƣ�SPҵ�������
#�ӿڱ��룺22033
#�ӿ�˵������¼�û�ʹ��spҵ���ҵ����������Ϣ����Ҫ������������������spҵ��������ص��굥��
#��������: G_S_22033_MONTH.tcl
#��������: ����22033������
#��������: ��
#Դ    ��1.bass2.dw_newbusi_ismg_yyyymm
#          2.bass2.dw_newbusi_call_yyyymm
#          3.bass2.dw_newbusi_wap_yyyymm
#          4.bass2.dw_newbusi_mms_yyyymm
#          5.bass2.dw_newbusi_kj_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��Ϊbass2.dw_product_func_200702û�����ݣ�������ʧע���û�������ͳ�Ƴ���
#          2.��Ϊbass2.dw_acct_shoulditem_200703û�����ݣ����Թ��ܷ�/���·Ѳ���ͳ�Ƴ���
#          3.Ҫ��BOSS�˶���Ŀ��Ŀ�������Щ��Ŀ��Ŀ���ڰ��·�/���ܷѡ�
#          4.ȷ�Ϻ���Ŀ��Ŀ����������
#          5.��bill_counts�Ʒ��� �޸ĳ� countsҵ����  �Ļ�ѧ 20080523
#�޸���ʷ: 1.
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
        set db_user $env(DB_USER)
        #�½ӿ�ʹ��
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_S_22033_MONTH where time_id=${op_month}"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
          ##---1 �����������-----#       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_22033_MONTH
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,case when svcitem_id=300007 then '16' 
                              when svcitem_id=300010 then '09' 
                              when svcitem_id=300011 then '08' 
                              when svcitem_id=300012 then '13' 
                              when svcitem_id=300013 then '17' 
                      	      when svcitem_id=300016 then '05' 
                      	      when svcitem_id=300017 then '14' 
                      	   else '01' 
                             end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee + month_fee)*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_ismg_${op_month}
                         group by
                         case when svcitem_id=300007 then '16'  
                              when svcitem_id=300010 then '09'  
                              when svcitem_id=300011 then '08'  
                              when svcitem_id=300012 then '13'  
                              when svcitem_id=300013 then '17'  
                      	      when svcitem_id=300016 then '05'  
                      	      when svcitem_id=300017 then '14'  
                      	   else '01' 
                      	   end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
   "
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn

    #-----2 �����ƶ�ɳ����������־-----------#
    set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,case when svcitem_id=100001 then '07' 
                              when svcitem_id=100002 then '06' 
                              when svcitem_id=100022 then '12' 
                              end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + toll_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee )*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_call_${op_month} 
                         where svcitem_id in (100001,100002,100022)
                         group by
                         case when svcitem_id=100001 then '07' 
                              when svcitem_id=100002 then '06' 
                              when svcitem_id=100022 then '12' 
                              end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn
	
	#----3 ����wap----#
	
	set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                     select
                         ${op_month}
                        ,'${op_month}'
                        ,'03' 
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee + month_fee)*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_wap_${op_month}
                         group by
                          COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn	
	
	#-----4 ���ɲ��� -----#
	set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                      select
                         ${op_month}
                         ,'${op_month}'
                         ,'02'
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee + month_fee )*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_mms_${op_month} where svcitem_id in (400003,400004,400006)
                         group by
                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	#-----5 ���ɰٱ��� -----#
	set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                      select
                         ${op_month}
                         ,'${op_month}'
                         ,'05'
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,'0'
                         ,char(sum(counts))
                         ,'0'
                         ,char(sum(int(data_size/1024/1024)))
                         ,char(sum(dnload_duration))
                         ,char(int(sum(info_fee + month_fee )*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_kj_${op_month} 
                         group by
                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
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
########################����
#         ##---1 �����������-----#       
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.G_S_22033_MONTH
#                      select
#                         ${op_month}
#                        ,'${op_month}'
#                        ,case when svcitem_id=300007 then '16'  ---����
#                              when svcitem_id=300010 then '09'  ---161�ƶ�����--
#                              when svcitem_id=300011 then '08'  ---�ƶ�����վ
#                              when svcitem_id=300012 then '13'  ---�Ų��ܼ�
#                              when svcitem_id=300013 then '17'  ---��������
#                      	      when svcitem_id=300016 then '05'  ----PDA
#                      	      when svcitem_id=300017 then '14'  ----��������
#                      	   else '01'  --��������
#                             end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
#                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee + month_fee)*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_ismg_${op_month}
#                         group by
#                         case when svcitem_id=300007 then '16'  ---����
#                              when svcitem_id=300010 then '09'  ---161�ƶ�����--
#                              when svcitem_id=300011 then '08'  ---�ƶ�����վ
#                              when svcitem_id=300012 then '13'  ---�Ų��ܼ�
#                              when svcitem_id=300013 then '17'  ---��������
#                      	      when svcitem_id=300016 then '05'  ----PDA
#                      	      when svcitem_id=300017 then '14'  ----��������
#                      	   else '01'  --��������
#                      	   end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#   "
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn
#
#    #-----2 �����ƶ�ɳ����������־-----------#
#    set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                      select
#                         ${op_month}
#                        ,'${op_month}'
#                        ,case when svcitem_id=100001 then '07'  ---�ƶ�ɳ��
#                              when svcitem_id=100002 then '06'  ---���Ż���--
#                              when svcitem_id=100022 then '12'  ---�ֻ�Ǯ��--
#                              end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + toll_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
#                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee )*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_call_${op_month} 
#                         where svcitem_id in (100001,100002,100022)
#                         group by
#                         case when svcitem_id=100001 then '07'  ---�ƶ�ɳ��
#                              when svcitem_id=100002 then '06'  ---���Ż���--
#                              when svcitem_id=100022 then '12'  ---�ֻ�Ǯ��--
#                              end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn
#	
#	#----3 ����wap----#
#	
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                     select
#                         ${op_month}
#                        ,'${op_month}'
#                        ,'03' ---����wap
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee + month_fee)*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_wap_${op_month}
#                         group by
#                          COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn	
#	
#	#-----4 ���ɲ��� -----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                      select
#                         ${op_month}
#                         ,'${op_month}'
#                         ,'02'
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
#                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee + month_fee )*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_mms_${op_month} where svcitem_id in (400003,400004,400006)
#                         group by
#                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	
#	#-----5 ���ɰٱ��� -----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                      select
#                         ${op_month}
#                         ,'${op_month}'
#                         ,'05'
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,'0'
#                         ,char(sum(counts))
#                         ,'0'
#                         ,char(sum(data_size))
#                         ,char(sum(dnload_duration))
#                         ,char(int(sum(info_fee + month_fee )*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_kj_${op_month} 
#                         group by
#                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
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
#	return 0
#}
###############################