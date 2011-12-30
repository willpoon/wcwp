######################################################################################################
#�ӿ����ƣ���ҵ��ע�����
#�ӿڱ��룺22032
#�ӿ�˵������¼�û�ע����ҵ��Ļ�����Ϣ��ע�Ȿ�ӿڵ�Ԫֻ���ͺͱ�����صĹ��ܱ�ʶ���룬
#          ����Ҫȫ�����ͣ���Ҫ���͵�ҵ���ܱ�ʶΪ�� 010 020 030 040 050 060 070 080 090 100 110 120 130 131 132
#��������: G_S_22032_MONTH.tcl
#��������: ����22032������
#��������: ��
#Դ    ��1.bass2.dw_product_func_yyyymm(�û����ܹ�ϵ�±�)
#          2.bass2.dw_acct_shoulditem_yyyymm(��ϸ�ʵ��±�)
#          3.bass2.dw_product_yyyymm(�û������±�)
#          4.bass2.dw_product_busi_sprom_dm_yyyymm
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-05-25
#�����¼��
#�޸���ʷ: 20100124 �޸������û��ھ� userstatus_id in (1,2,3,6,8)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #��Ȼ�µ�һ�� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
  #ɾ����������
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.G_S_22032_MONTH where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #01������ʱ��session.g_s_22032_month_tmp1
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22032_month_tmp1
	              (
	                        user_id                 varchar(30),
                          bus_func_flag		        varchar(3), 
                          cust_brnd_id			      varchar(1), 
                          register_user_num		    bigint,     
                          add_register_user_num	  bigint,     
                          lost_register_user_num	bigint,     
                          month_fee_usr_num		    bigint,     
                          func_fee			bigint,     
                          month_fee			bigint      
	              )with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

  #ע���û�
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp1
                select
                       distinct a.user_id
                       ,case when a.prod_id=30500401 then '010'
                             when a.prod_id=30500301 then '020'
                             when a.prod_id in (30505101, 30505102) then '030'
                             when a.prod_id=30503001 then '040'
                             when a.prod_id=30501801 then '050'
                             when a.prod_id=30504001 then '060'
                             when a.prod_id=50001010 then '070'
                             when a.prod_id in (30512002, 30512001) then '080'
                             when a.prod_id in (30502401, 30502801, 50002002, 50002001, 50002003) then '100'
                             when a.prod_id=30513001 then '110'
                             when a.prod_id=30501204 then '120'
                             when a.prod_id in (50001405 ,50001402) then '131'
                             when a.prod_id in (50001406, 50001410, 50001407, 50001408, 50001411, 50001412, 50001413, 50001414, 50001416, 50001403) then '132'
                        end   
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                       ,1
                       ,0
                       ,0
                       ,0
                       ,0
                       ,0
                      from 
                        bass2.dw_product_func_$op_month a,
                        bass2.dw_product_$op_month b
                      where
                            b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.user_id=b.user_id 
                        and replace(char(date(a.expire_date)),'-','')>='$timestamp'
                        and a.prod_id in (30500401 ,30500301 , 30505101 ,30505102 ,30503001 ,30501801 ,30504001 ,50001010 ,                                                                              
                                      30512002 ,30512001 , 99001688 ,30502401 ,30502801 ,50002002 ,50002001 ,50002003 ,                                          
                                      30513001 ,30501204 , 50001405 ,50001402 ,50001406 ,50001410 ,50001407 ,50001408 ,
                                      50001411 ,50001412 ,50001413 ,50001414 ,50001416 ,50001403)
                union
                select
                                       distinct a.user_id
                                       ,case when a.sprom_id=99001688 then '090'
                                             when a.sprom_id in (50001405, 50001402) then '131'
                                             else '132'
                                        end      
                                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                                       ,1
                                       ,0
                                       ,0
                                       ,0
                                       ,0
                                       ,0
                                      from 
                                        bass2.dw_product_sprom_$op_month a,
                                        bass2.dw_product_$op_month b
                                      where b.userstatus_id in (1,2,3,6,8) 
                                        and b.usertype_id in (1,2,9)
                                        and a.user_id=b.user_id 
                                        and a.active_mark=1
                						            and a.sprom_id in(50001405,50001402,50001406,50001410,50001407,50001408,
                                                          50001411,50001412,50001413,50001414,50001416,50001403,99001688)
                with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle

        
#360  �ֻ�Ǯ��
#ע���û���
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp1
                      select
                       distinct a.user_id
                       ,'070'
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                       ,1
                       ,0
                       ,0
                       ,0
                       ,0
                       ,0
                      from 
                        bass2.DW_PRODUCT_REGSP_$op_month a,
                        bass2.dw_product_$op_month b
                      where b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.BUSI_TYPE ='140'
                        and b.usertype_id in (1,2,9)
                        and replace(char(date(a.expire_date)),'-','')>='$timestamp'
                        and a.user_id=b.user_id with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle




  #����ע���û�
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp1
                select
                       distinct a.user_id
                       ,case when a.prod_id=30500401 then '010'
                             when a.prod_id=30500301 then '020'
                             when a.prod_id in (30505101, 30505102) then '030'
                             when a.prod_id=30503001 then '040'
                             when a.prod_id=30501801 then '050'
                             when a.prod_id=30504001 then '060'
                             when a.prod_id=50001010 then '070'
                             when a.prod_id in (30512002, 30512001) then '080'
                             when a.prod_id in (30502401, 30502801, 50002002, 50002001, 50002003) then '100'
                             when a.prod_id=30513001 then '110'
                             when a.prod_id=30501204 then '120'
                             when a.prod_id in (50001405 ,50001402) then '131'
                             when a.prod_id in (50001406, 50001410, 50001407, 50001408, 50001411, 50001412, 50001413, 50001414, 50001416, 50001403) then '132'
                        end   
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                       ,0
                       ,1
                       ,0
                       ,0
                       ,0
                       ,0
                      from 
                        bass2.dw_product_func_$op_month a,
                        bass2.dw_product_$op_month b
                      where
                            b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.user_id=b.user_id
                        and replace(char(date(a.valid_date)),'-','') like '$op_month%' 
                        and a.prod_id in (30500401 ,30500301 , 30505101 ,30505102 ,30503001 ,30501801 ,30504001 ,50001010 ,                                                                              
                                      30512002 ,30512001 , 99001688 ,30502401 ,30502801 ,50002002 ,50002001 ,50002003 ,                                          
                                      30513001 ,30501204 , 50001405 ,50001402 ,50001406 ,50001410 ,50001407 ,50001408 ,
                                      50001411 ,50001412 ,50001413 ,50001414 ,50001416 ,50001403)
                union
                select
                                       distinct a.user_id
                                       ,case when a.sprom_id=99001688 then '090'
                                             when a.sprom_id in (50001405, 50001402) then '131'
                                             else '132'
                                        end      
                                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                                       ,0
                                       ,1
                                       ,0
                                       ,0
                                       ,0
                                       ,0
                                      from 
                                        bass2.dw_product_sprom_$op_month a,
                                        bass2.dw_product_$op_month b
                                      where b.userstatus_id in (1,2,3,6,8)
                                        and b.usertype_id in (1,2,9)
                                        and a.user_id=b.user_id
                                        and replace(char(date(a.valid_date)),'-','') like '$op_month%' 
                						            and a.sprom_id in(50001405,50001402,50001406,50001410,50001407,50001408,
                                                          50001411,50001412,50001413,50001414,50001416,50001403,99001688)
                with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle	

       
#360  �ֻ�Ǯ��
#����ע���û���
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp1
                      select
                       distinct a.user_id
                       ,'070'
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                       ,0
                       ,1
                       ,0
                       ,0
                       ,0
                       ,0
                      from 
                        bass2.DW_PRODUCT_REGSP_$op_month a,
                        bass2.dw_product_$op_month b
                      where b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.BUSI_TYPE ='140'
                        and b.usertype_id in (1,2,9)
                        and replace(char(date(a.valid_date)),'-','') like '$op_month%'  
                        and a.user_id=b.user_id with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle






  #02������ʱ��session.g_s_22032_month_tmp2  �����µ����ҵ��ע���û���
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22032_month_tmp2
	              (
	                        user_id                 varchar(20),
                          bus_func_flag		        varchar(3),
                          cust_brnd_id			      varchar(1)
	              )with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #����ע���û�
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp2
                      select
                       distinct a.user_id
                       ,case when a.prod_id=30500401 then '010'
                             when a.prod_id=30500301 then '020'
                             when a.prod_id in (30505101, 30505102) then '030'
                             when a.prod_id=30503001 then '040'
                             when a.prod_id=30501801 then '050'
                             when a.prod_id=30504001 then '060'
                             when a.prod_id=50001010 then '070'
                             when a.prod_id in (30512002, 30512001) then '080'
                             when a.prod_id in (30502401, 30502801, 50002002, 50002001, 50002003) then '100'
                             when a.prod_id=30513001 then '110'
                             when a.prod_id=30501204 then '120'
                             when a.prod_id in (50001405 ,50001402) then '131'
                             when a.prod_id in (50001406, 50001410, 50001407, 50001408, 50001411, 50001412, 50001413, 50001414, 50001416, 50001403) then '132'
                        end   
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                      from 
                        bass2.dw_product_func_$last_month a,
                        bass2.dw_product_$last_month b
                      where
                            b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.user_id=b.user_id
                        and replace(char(date(a.expire_date)),'-','')>='$l_timestamp'
                        and a.prod_id in (30500401 ,30500301 , 30505101 ,30505102 ,30503001 ,30501801 ,30504001 ,50001010 ,                                                                              
                                      30512002 ,30512001 , 99001688 ,30502401 ,30502801 ,50002002 ,50002001 ,50002003 ,                                          
                                      30513001 ,30501204 , 50001405 ,50001402 ,50001406 ,50001410 ,50001407 ,50001408 ,
                                      50001411 ,50001412 ,50001413 ,50001414 ,50001416 ,50001403)
                union
                select
                                       distinct a.user_id
                                       ,case when a.sprom_id=99001688 then '090'
                                             when a.sprom_id in (50001405, 50001402) then '131'
                                             else '132'
                                        end      
                                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                                      from 
                                        bass2.dw_product_sprom_$last_month a,
                                        bass2.dw_product_$last_month b
                                      where b.userstatus_id in (1,2,3,6) 
                                        and b.usertype_id in (1,2,9)
                                        and a.user_id=b.user_id
                                        and a.active_mark=1
                						            and a.sprom_id in(50001405,50001402,50001406,50001410,50001407,50001408,
                                                          50001411,50001412,50001413,50001414,50001416,50001403,99001688)
                                       with ur                   "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle

       
#360  �ֻ�Ǯ��  
#����ע���û���
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp2
                      select
                       distinct a.user_id
                       ,'070'
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                       from  bass2.DW_PRODUCT_REGSP_$last_month a,
                        bass2.dw_product_$last_month b
                      where b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.BUSI_TYPE ='140'
                        and b.usertype_id in (1,2,9)
                        and replace(char(date(a.expire_date)),'-','')>='$l_timestamp'
                        and a.user_id=b.user_id with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle
	



	
	#03������ʱ��session.g_s_22032_month_tmp3  �ű��µ����ҵ��ע���û���
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22032_month_tmp3
	              (
	                        user_id                 varchar(20),
                          bus_func_flag		        varchar(3), 
                          cust_brnd_id			      varchar(1) 
	              )with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #����ע���û�
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp3
                      select
                       distinct a.user_id
                       ,case when a.prod_id=30500401 then '010'
                             when a.prod_id=30500301 then '020'
                             when a.prod_id in (30505101, 30505102) then '030'
                             when a.prod_id=30503001 then '040'
                             when a.prod_id=30501801 then '050'
                             when a.prod_id=30504001 then '060'
                             when a.prod_id=50001010 then '070'
                             when a.prod_id in (30512002, 30512001) then '080'
                             when a.prod_id in (30502401, 30502801, 50002002, 50002001, 50002003) then '100'
                             when a.prod_id=30513001 then '110'
                             when a.prod_id=30501204 then '120'
                             when a.prod_id in (50001405 ,50001402) then '131'
                             when a.prod_id in (50001406, 50001410, 50001407, 50001408, 50001411, 50001412, 50001413, 50001414, 50001416, 50001403) then '132'
                        end   
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                      from 
                        bass2.dw_product_func_$op_month a,
                        bass2.dw_product_$op_month b
                      where
                            b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.user_id=b.user_id 
                        and replace(char(date(a.expire_date)),'-','')>='$timestamp'
                        and a.prod_id in (30500401 ,30500301 , 30505101 ,30505102 ,30503001 ,30501801 ,30504001 ,50001010 ,                                                                              
                                      30512002 ,30512001 , 99001688 ,30502401 ,30502801 ,50002002 ,50002001 ,50002003 ,                                          
                                      30513001 ,30501204 , 50001405 ,50001402 ,50001406 ,50001410 ,50001407 ,50001408 ,
                                      50001411 ,50001412 ,50001413 ,50001414 ,50001416 ,50001403)
                union
                select
                                       distinct a.user_id
                                       ,case when a.sprom_id=99001688 then '090'
                                             when a.sprom_id in (50001405, 50001402) then '131'
                                             else '132'
                                        end      
                                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                                      from 
                                        bass2.dw_product_sprom_$op_month a,
                                        bass2.dw_product_$op_month b
                                      where b.userstatus_id in (1,2,3,6,8)
                                        and b.usertype_id in (1,2,9)
                                        and a.user_id=b.user_id 
                                        and a.active_mark=1
                						            and a.sprom_id in(50001405,50001402,50001406,50001410,50001407,50001408,
                                                          50001411,50001412,50001413,50001414,50001416,50001403,99001688)
                with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle

     
#360  �ֻ�Ǯ��
#����ע���û���

  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp3
                      select
                       distinct a.user_id
                       ,'070'
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                      from  bass2.DW_PRODUCT_REGSP_$op_month a,
                        bass2.dw_product_$op_month b
                      where b.userstatus_id in (1,2,3,6,8)
                        and b.usertype_id in (1,2,9)
                        and a.BUSI_TYPE ='140'
                        and b.usertype_id in (1,2,9)
                        and replace(char(date(a.expire_date)),'-','')>='$timestamp'
                        and a.user_id=b.user_id with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle



  #��ʧ�û���
  set handle [aidb_open $conn]
	set sql_buff "insert into  session.g_s_22032_month_tmp1
	              select user_id,bus_func_flag,cust_brnd_id,0,0,1,0,0,0
	              from 
	                (select user_id,bus_func_flag,cust_brnd_id
                     from session.g_s_22032_month_tmp2
                   except
                   select user_id,bus_func_flag,cust_brnd_id
                    from  session.g_s_22032_month_tmp3
                   )a"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	


	
	
	#07���ܷ�
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp1
                      select
                        a.user_id
                        ,case when a.item_id in (80000187)            then  '030'
                              when a.item_id in (80000009)            then  '040'
                              when a.item_id in (80000017)            then  '050'
                              when a.item_id in (80000012,80000132)   then  '080'
                              when a.item_id in (80000478)            then  '090'
                              when a.item_id in (80000480)            then  '100'
                              when a.item_id in (80000455)            then  '110'
                              when a.item_id in (80000477)            then  '120'
                         end
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                        ,0
                        ,0
                        ,0
                        ,0
                        ,sum(a.fact_fee)
                        ,0
                      from 
                        bass2.dw_acct_shoulditem_$op_month a,
                        bass2.dw_product_$op_month b
                      where
                        a.item_id in (80000187,80000009,80000017,80000012,
                                      80000132,80000478,80000480,80000455,80000477) 
                        and a.user_id=b.user_id 
                      group by 
                        a.user_id
                        ,case when a.item_id in (80000187)            then  '030'
                              when a.item_id in (80000009)            then  '040'
                              when a.item_id in (80000017)            then  '050'
                              when a.item_id in (80000012,80000132)   then  '080'
                              when a.item_id in (80000478)            then  '090'
                              when a.item_id in (80000480)            then  '100'
                              when a.item_id in (80000455)            then  '110'
                              when a.item_id in (80000477)            then  '120'
                         end
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2') "                      
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle	
	
  #08���·�
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22032_month_tmp1
                      select
                        a.user_id
                        ,case
                          when a.item_id in (80000146,80000188,80000194) then '030'
                          when a.item_id in (80000159,80000141) then '070' 
                          when a.item_id in (80000167,80000143,80000170) then '080' 
                         end
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2')
                       ,0
                       ,0
                       ,0
                       ,0
                       ,0
                       ,sum(a.fact_fee)
                      from
                        bass2.dw_acct_shoulditem_$op_month a,
                        bass2.dw_product_$op_month b
                      where
                        a.item_id in (80000146,80000188,80000194,80000159,
                                      80000141,80000167,80000143,80000170) 
                        and a.user_id=b.user_id 
                      group by 
                        a.user_id
                        ,case
                          when a.item_id in (80000146,80000188,80000194) then '030'
                          when a.item_id in (80000159,80000141) then '070' 
                          when a.item_id in (80000167,80000143,80000170) then '080' 
                         end
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2') "                      
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
	


	#12:���ܵ������
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_22032_MONTH
                      select
                        $op_month
                        ,'$op_month'
                        ,bus_func_flag
                        ,cust_brnd_id
                        ,char(sum(register_user_num))
                        ,char(sum(add_register_user_num))
                        ,char(sum(lost_register_user_num))
                        ,char(sum(month_fee_usr_num))
                        ,char(sum(func_fee)*100)
                        ,char(sum(month_fee)*100)
                      from
                        session.g_s_22032_month_tmp1
                      where bus_func_flag is not null
                      group by 
                        bus_func_flag
                        ,cust_brnd_id "                     
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
