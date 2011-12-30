######################################################################################################
#�ӿ����ƣ����ſͻ�M2M��Ʒʹ�����
#�ӿڱ��룺22037
#�ӿ�˵������¼���ſͻ�ҵ����M2M��Ʒ��ʹ�������
#��������: G_S_22037_MONTH.tcl
#��������: ����22037������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #�������һ�� $op_monthdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $op_time
        puts $op_month 
      
      scan   $op_time "%04s-%02s-%02s" year month day
      set    la   "-"
      
      set thismonth [string range $op_month 4 5]
      set thisyear  [string range $op_month 0 3] 

      #�õ�����1�ŵ�����
      puts $op_month
      #����	$last_month	
      set last_month [GetLastMonth [string range $op_month 0 5]]

      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay 


      #�õ��ϸ��µ�1������
    	set sql_buff "select date('$ThisMonthFirstDay')-1 month from bass2.dual"
	    puts $sql_buff
	    set LastMonthFirstDay [get_single $sql_buff]
	    puts $LastMonthFirstDay
      set LastMonthYear [string range $LastMonthFirstDay 0 3]
      set LastMonth [string range $LastMonthFirstDay 5 6]
	    
      #�õ��¸��µ�1������
    	set sql_buff "select date('$ThisMonthFirstDay')+1 month from bass2.dual"
	    puts $sql_buff
	    set NextMonthFirstDay [get_single $sql_buff]
	    puts $NextMonthFirstDay
      set NextMonthYear [string range $NextMonthFirstDay 0 3]
      set NextMonth [string range $NextMonthFirstDay 5 6]

	    
     
       
  #ɾ����������
	set sql_buff "delete from bass1.g_s_22037_month where time_id=$op_month"
	puts $sql_buff
	exec_sql $sql_buff
	 
 
  #������ʱ��
	set sql_buff "declare global temporary table session.g_s_22037_month_tmp
	              (FAB_TYPE           CHARACTER(3),
                 ENT_NUM            bigint,
                 ENTPERSON_NUM      bigint,
                 UNENTPERSON_NUM    bigint,
                 ENTPERSON_NEWNUM   bigint,
                 ENTPERSON_LOSTNUM  bigint,
                 USER_NUM           bigint,
                 USER_NUMLJ         bigint,
                 USER_NUMJF         bigint,
                 USER_NUMJFLJ       bigint,
                 UPSMS_COUNT        bigint,
                 DOWNSMS_COUNT      bigint
                )  
                partitioning key 
                (FAB_TYPE)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	exec_sql $sql_buff
	
  #03	����������
  #04	���Ÿ��˶����ͻ���
  #05	�Ǽ��Ÿ��˶����ͻ���
  #06	���������û���
  #07	�˶��û���
  #08	����ʹ�ÿͻ���
  #09	�ۼ�ʹ�ÿͻ���
  #10	���¼Ʒѿͻ���
  #11	�ۼƼƷѿͻ���
  #12	���ж���ҵ����
  #13	���ж���ҵ����



#===================================================================================================    
#H001 ����ͨ	����ͨ����������	                      ��
#H003 ����ͨ	���У����Ÿ��˿ͻ���	                  ��
#H004 ����ͨ	     �Ǽ��Ÿ��˿ͻ���	                  ��
#H005 ����ͨ	����ͨ���������û���	                  ��
#H006 ����ͨ	����ͨ�˶��û���	                      ��
#H007 ����ͨ	����ͨ����ʹ�ÿͻ���	                  ��
#H008 ����ͨ	����ͨ�ۼ�ʹ�ÿͻ���	                  ��
#H009 ����ͨ	����ͨ���¼Ʒѿͻ���	                  ��
#H010 ����ͨ	����ͨ�ۼƼƷѿͻ���	                  ��
#H014 ����ͨ	--�������ж���ҵ����	                  MB
#H015 ����ͨ	--�������ж���ҵ����	                  MB
#===================================================================================================
#H031	УѶͨ	УѶͨ����������	                      ��	
#H033	УѶͨ	���У����Ÿ��˿ͻ���	                  ��	
#H034	УѶͨ	       �Ǽ��Ÿ��˿ͻ���	                ��	
#H035	УѶͨ	УѶͨ���������û���	                  ��	
#H036	УѶͨ	УѶͨ�˶��û���	                      ��	
#H037	УѶͨ	У��ͨ����ʹ�ÿͻ���	                  ��	
#H038	УѶͨ	У��ͨ�ۼ�ʹ�ÿͻ���	                  ��	
#H039	УѶͨ	УѶͨ���¼Ʒѿͻ���	                  ��	
#H040	УѶͨ	У��ͨ�ۼƼƷѿͻ���	                  ��	
#H044	УѶͨ	--�������ж���ҵ����	                  MB	
#H045	УѶͨ	--�������ж���ҵ����	                  MB	
#===================================================================================================
#H046	����ͨ	��������ͨ����ͨ����������	            ��	
#H048	����ͨ  ���У� ���Ÿ��˿ͻ���	                  ��	
#H049	����ͨ       �Ǽ��Ÿ��˿ͻ���	                  ��	
#H050	����ͨ  ��������ͨ����ͨ���������û���	        ��	
#H051	����ͨ  ��������ͨ����ͨ�˶��û���	            ��	
#H052	����ͨ  ��������ͨ����ͨ����ʹ�ÿͻ���	        ��	
#H053	����ͨ  ��������ͨ����ͨ�ۼ�ʹ�ÿͻ���	        ��	
#H054	����ͨ  ��������ͨ����ͨ���¼Ʒѿͻ���	        ��	
#H055	����ͨ  ��������ͨ����ͨ�ۼƼƷѿͻ���	        ��	
#H059	����ͨ  --�������ж���ҵ����	                  MB	
#H060	����ͨ  --�������ж���ҵ����	                  MB	
#===================================================================================================
#H061	ũ��ͨ	ũ��ͨ����������	                      ��	
#H064	ũ��ͨ	���У����Ÿ��˿ͻ���	                  ��	
#H065	ũ��ͨ	     �Ǽ��Ÿ��˿ͻ���	                  ��	
#H067	ũ��ͨ	ũ��ͨ���������û���	                  ��	
#H069	ũ��ͨ	ũ��ͨ�˶��û���	                      ��	
#H073	ũ��ͨ	ũ��ͨ����ʹ�ÿͻ���	                  ��	
#H075	ũ��ͨ	ũ��ͨ�ۼ�ʹ�ÿͻ���	                  ��	
#H077	ũ��ͨ	ũ��ͨ����ʹ�ÿͻ���	                  ��	
#H079	ũ��ͨ	ũ��ͨ�ۼƼƷѿͻ���	                  ��	
#H084	ũ��ͨ	--�������ж���ҵ����	                  MB	
#H085	ũ��ͨ	--�������ж���ҵ����	                  MB	
#===================================================================================================
#����ͨ	H016	����ͨ����������	                      ��	        
#����ͨ	H017	����ͨ�����û���	                      ��	        
#����ͨ	H018	���У�  ���Ÿ��˿ͻ���	                ��	        
#����ͨ	H019	      �Ǽ��Ÿ��˿ͻ���	                ��	        
#����ͨ	H020	����ͨ���������û���	                  ��	        
#����ͨ	H021	����ͨ�˶��û���	                      ��	        
#����ͨ	H022	����ͨ����ʹ�ÿͻ���	                  ��	        
#����ͨ	H023	����ͨ�ۼ�ʹ�ÿͻ���	                  ��	        
#����ͨ	H024	����ͨ���¼Ʒѿͻ���	                  ��	        
#����ͨ	H025	����ͨ�ۼƼƷѿͻ���	                  ��	        
#����ͨ	H029	--�������ж���ҵ����	                  MB	        
#����ͨ	H030	--�������ж���ҵ����	                  MB	     
#===================================================================================================

#������ҵӦ��
#H001 ����ͨ	����ͨ����������	                      ��    
#H031	УѶͨ	УѶͨ����������	                      ��
#H046	����ͨ	��������ͨ����ͨ����������	            ��	
#H061	ũ��ͨ	ũ��ͨ����������	                      ��	
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ


    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                   select '110',
                          count(distinct enterprise_id)
                     from bass2.dw_enterprise_sub_$op_month
                    where service_id = '911' and rec_status = 1 and enterprise_id not in ('891910006274')"
  	puts $sql_buff
	  exec_sql $sql_buff
	
	
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                   select case b.apptype_id
                             when 1 then '110'
                             when 2 then '120'
                             when 3 then '140'
                             when 4 then '210'  
                          end,
                          count(distinct a.enterprise_id)
                   from bass2.dw_enterprise_industry_apply b,bass2.dw_enterprise_member_mid_$op_month a 
                   where a.dummy_mark = 0
                         and b.op_time = '$ThisMonthFirstDay'
                         and b.apptype_id in (3) and a.user_id = b.user_id
                   group by case b.apptype_id
                             when 1 then '110'
                             when 2 then '120'
                             when 3 then '140'
                             when 4 then '210'  
                           end;"
	puts $sql_buff
	exec_sql $sql_buff


	
	
set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                  select  '120',count(distinct a.ENTERPRISE_ID)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a 
                   where b.MOBILE_NUM = a.product_no    
                   and b.OPERATE_TYPE = '��ǩԼ' and b.OPERATE_DATE <'$NextMonthFirstDay' and b.OPERATE_DATE >= '$ThisMonthFirstDay'
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%')"
	puts $sql_buff
	exec_sql $sql_buff
                            
                            
                            
                            	

#����ͨ	H016	����ͨ����������	 ��
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENT_NUM)
                   select
                        '100',
                        count(distinct a.enterprise_id)
                      from 
                        bass2.dw_enterprise_sub_$op_month a,bass2.dw_enterprise_msg_$op_month b
                     where a.SERVICE_ID = '945' and a.enterprise_id = b.enterprise_id
                           and a.enterprise_id not in ('891880005002')"
	puts $sql_buff
	exec_sql $sql_buff


#H033	УѶͨ	���У� ���Ÿ��˿ͻ���	  ��	
#H034	УѶͨ	     �Ǽ��Ÿ��˿ͻ���	  ��	
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

#select count(distinct user_id) from bass2.dw_enterprise_membersub_$op_month where order_id in (select  order_id  from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and 
#enterprise_id not in ('891910006274')) and rec_status = 1  with ur




 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                       select '110', count(distinct a.user_id)
                   from  
                        bass2.dw_enterprise_membersub_$op_month a,
                        bass2.dw_enterprise_member_mid_$op_month b
                   left join
                        (select distinct enterprise_id 
                           from bass2.dw_enterprise_sub_$op_month 
                          where service_id = '911' and rec_status = 1 and 
                                enterprise_id not in ('891910006274')
                        ) c
                     on b.enterprise_id = c.enterprise_id
                  where a.USER_ID = b.USER_ID and
                        order_id in (select  order_id  from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and 
                        enterprise_id not in ('891910006274')) and 
                        rec_status = 1 and c.enterprise_id is not null
                        group by c.enterprise_id;"
	puts $sql_buff
	exec_sql $sql_buff

  set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                       select '110', count(distinct a.user_id) result
						           	from
								           bass2.dw_enterprise_membersub_$op_month a
						           	left join
							           	bass2.dw_enterprise_member_mid_$op_month b
							           on
							           	a.USER_ID = b.USER_ID
						           	left join
						          		(select distinct enterprise_id from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and enterprise_id not in ('891910006274')) c
							           on
							            b.enterprise_id = c.enterprise_id 
						           	where
							              	order_id in (select  order_id  from bass2.dw_enterprise_sub_$op_month where service_id = '911' and rec_status = 1 and enterprise_id not in ('891910006274')) and rec_status = 1
                              and c.enterprise_id is null ;"
	puts $sql_buff
	exec_sql $sql_buff

  
	
	
	


                          
                          
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
#                       select '110', count(distinct b.user_id)
#                   from bass2.dw_enterprise_industry_apply b left outer join 
#                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
#				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
#                   left outer join bass2.dw_product_$op_month c
#                   on a.user_id = c.user_id
#                   where b.apptype_id in (1)
#                     and b.op_time ='$ThisMonthFirstDay' and c.ENTERPRISE_MARK = 1;"
#	puts $sql_buff
#	exec_sql $sql_buff
#
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
#                       select '110', count(distinct b.user_id)
#                   from bass2.dw_enterprise_industry_apply b left outer join 
#                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
#				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
#                   left outer join bass2.dw_product_$op_month c
#                   on a.user_id = c.user_id
#                   where b.apptype_id in (1)
#                     and b.op_time ='$ThisMonthFirstDay' and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null ) ;"
#	puts $sql_buff
#	exec_sql $sql_buff




#H048	����ͨ	���У�  ���Ÿ��˿ͻ���	     ��	
#H049	����ͨ	      �Ǽ��Ÿ��˿ͻ���	     ��	
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                   select  '120', 
                           count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a ,bass2.dw_product_$op_month c
                   where b.MOBILE_NUM = a.product_no
                   and b.MOBILE_NUM = c.product_no  
                   and b.OPERATE_TYPE = '��ǩԼ' 
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%')
                  and c.ENTERPRISE_MARK = 1;"
	puts $sql_buff
	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                   select  '120', 
                           count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a ,bass2.dw_product_$op_month c
                   where b.MOBILE_NUM = a.product_no
                   and b.MOBILE_NUM = c.product_no  
                   and b.OPERATE_TYPE = '��ǩԼ' 
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%')
                     and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null );"
	puts $sql_buff
	exec_sql $sql_buff


#===================================================================================================   
#H064	ũ��ͨ	���У� ���Ÿ��˿ͻ���	��	
#H065	ũ��ͨ	     �Ǽ��Ÿ��˿ͻ���	��	
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                       select '140', count(distinct b.user_id)
                   from bass2.dw_enterprise_industry_apply b left outer join 
                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
                   left outer join bass2.dw_product_$op_month c
                   on a.user_id = c.user_id
                   where b.apptype_id in (3)
                     and b.op_time ='$ThisMonthFirstDay' and c.ENTERPRISE_MARK = 1;"
	puts $sql_buff
	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                       select '140', count(distinct b.user_id)
                   from bass2.dw_enterprise_industry_apply b left outer join 
                   (select user_id as user_id,level_def_mode as level_def_mode,ent_city_id as ent_city_id 
				           from bass2.dw_enterprise_member_mid_$op_month where dummy_mark = 0 group by user_id,level_def_mode,ent_city_id) a on a.user_id = b.user_id
                   left outer join bass2.dw_product_$op_month c
                   on a.user_id = c.user_id
                   where b.apptype_id in (3)
                     and b.op_time ='$ThisMonthFirstDay' and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null );"
	puts $sql_buff
	exec_sql $sql_buff


#===================================================================================================   
#H003 ����ͨ	���У����Ÿ��˿ͻ���	                  ��
#H004 ����ͨ	     �Ǽ��Ÿ��˿ͻ���	                  ��	
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                       select '210', count(distinct b.user_id)
              from  bass2.DW_PRODUCT_REGSP_$op_month b left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              left outer join bass2.dw_product_$op_month c on b.user_id = c.user_id 
              where a.dummy_mark = 0 
                    and b.sts=1 
                    and b.SP_CODE in ('400002','901848','900139','810611')
                    and b.VALID_DATE <'$NextMonthFirstDay' and b.EXPIRE_DATE >= '$NextMonthFirstDay'
                    and c.ENTERPRISE_MARK = 1;"
	puts $sql_buff
	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                       select '210', count(distinct b.user_id)
              from  bass2.DW_PRODUCT_REGSP_$op_month b left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              left outer join bass2.dw_product_$op_month c on b.user_id = c.user_id 
              where a.dummy_mark = 0 
                    and b.sts=1 
                    and b.SP_CODE in ('400002','901848','900139','810611')
                    and b.VALID_DATE <'$NextMonthFirstDay' and b.EXPIRE_DATE >= '$NextMonthFirstDay'
                    and (c.ENTERPRISE_MARK = 0 or c.ENTERPRISE_MARK is null );"
	puts $sql_buff
	exec_sql $sql_buff
	
	
#����ͨ	H018	���У�  ���Ÿ��˿ͻ���	                ��	        
#����ͨ	H019	      �Ǽ��Ÿ��˿ͻ���	                ��	        
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
#                   select '100', count(distinct b.user_id)
#                   from bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
#                   left outer join bass2.dw_product_$op_month c
#                   on  b.user_id =c.user_id
#                   where a.sprom_id=90004001 and a.busi_type=945
#                   and a.user_id not in ('1160046359') and  c.ENTERPRISE_MARK = 1;"
#	puts $sql_buff
#	exec_sql $sql_buff

    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NUM)
                   select '100', count(distinct a.user_id)
                   from bass2.dw_enterprise_extsub_rela_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
                   left outer join bass2.dw_product_$op_month c
                   on  b.user_id =c.user_id
                   where a.SERVICE_ID = '945' 
                     and a.enterprise_id not in ('891880005002','891910006274') 
                     and a.rec_status=1 and c.ENTERPRISE_MARK = 1 ;"
	puts $sql_buff
	exec_sql $sql_buff
							                
							                
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
                   select '100', count(distinct a.user_id)
                   from bass2.dw_enterprise_extsub_rela_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
                   left outer join bass2.dw_product_$op_month c
                   on  b.user_id =c.user_id
                   where a.SERVICE_ID = '945' 
                     and a.enterprise_id not in ('891880005002','891910006274') 
                     and a.rec_status=1 and c.ENTERPRISE_MARK is null ;"
	puts $sql_buff
	exec_sql $sql_buff

							                
#    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UNENTPERSON_NUM)
#                  select '100', count(distinct b.user_id)
#                   from bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month a left outer join bass2.dw_enterprise_member_mid_$op_month b on a.user_id = b.user_id 
#                   left outer join bass2.dw_product_$op_month c
#                   on  b.user_id =c.user_id
#                   where a.sprom_id=90004001 and a.busi_type=945
#                   and a.user_id not in ('1160046359') ;"
#	puts $sql_buff
#	exec_sql $sql_buff
	
#===================================���������û���==============================================   
#����ͨ	H005	����ͨ���������û���	��	0

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
                select '210',   
                     count(distinct b.user_id)
              from  bass2.DW_PRODUCT_REGSP_$op_month b left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              where a.dummy_mark = 0
                    and b.sts=1 
                    and b.SP_CODE in ('400002','901848','900139','810611')
                    and b.STS_DATE>='$ThisMonthFirstDay' and b.STS_DATE<'$NextMonthFirstDay'; "
	puts $sql_buff
	exec_sql $sql_buff


#H035	УѶͨ	УѶͨ���������û���	 ��	 
  set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
              select 
                     '110',
                     count(distinct a.user_id)
              from bass2.dw_enterprise_member_mid_$op_month a,
			             (select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$ThisMonthFirstDay' and apptype_id in (1) group by user_id
                    except
                    select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$LastMonthFirstDay' and apptype_id in (1) group by user_id
                   ) b,
				          bass2.dw_product_$op_month c
              where a.dummy_mark = 0
                and a.user_id = b.user_id  
                and a.user_id = c.user_id    
             with ur;"
	puts $sql_buff
	exec_sql $sql_buff

 
             
             
                                    
                        

#H050	����ͨ  ��������ͨ����ͨ���������û���	        ��	 
   set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
                  select  '120',
                          count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a 
                   where b.MOBILE_NUM = a.product_no    
                   and b.OPERATE_TYPE = '��ǩԼ' and b.OPERATE_DATE <'$NextMonthFirstDay' and b.OPERATE_DATE >= '$ThisMonthFirstDay'
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%');"
	puts $sql_buff
	exec_sql $sql_buff

  
#����ͨ	H020	����ͨ���������û���	                  ��	
    set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_NEWNUM)
                   select '100',
                         count(distinct a.user_id)
                   from 
                   (select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')
                   except
                   select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$last_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')) a left outer join bass2.dw_enterprise_member_mid_200807 b            
                   on a.user_id =b.user_id                 
                   group by  case 
                                  when b.level_def_mode = 1 then 888
                                  else value(int(b.ent_city_id),891)
                         end"      
	puts $sql_buff
	exec_sql $sql_buff

#===================================ͨ�˶��û���==============================================   
#����ͨ	H006	����ͨ�˶��û���	��	0    
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
                select '210',   
                     count(distinct b.user_id)
              from  (select user_id from bass2.DW_PRODUCT_REGSP_${LastMonthYear}${LastMonth}  
                     where  sts=1 and SP_CODE in ('400002','901848','900139','810611')
                     and VALID_DATE <'$ThisMonthFirstDay' and EXPIRE_DATE >= '$ThisMonthFirstDay' 
                     group by user_id
                     except 
                     select user_id from bass2.DW_PRODUCT_REGSP_$op_month 
                     where  sts=1 and SP_CODE in ('400002','901848','900139','810611')
                     and VALID_DATE <'$NextMonthFirstDay' and EXPIRE_DATE >= '$ThisMonthFirstDay'
                     group by user_id
                    )b     
              left outer join bass2.dw_enterprise_member_mid_$op_month a on a.user_id = b.user_id 
              where a.dummy_mark = 0;"
	puts $sql_buff
	exec_sql $sql_buff
                       
                       
#H036	УѶͨ	УѶͨ�˶��û���	                      ��	
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
               select 
                     '110',
                     count(distinct a.user_id)
              from bass2.dw_enterprise_member_mid_$op_month a,
			             (select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$LastMonthFirstDay' and apptype_id in (1) group by user_id
                    except
                    select user_id as user_id from bass2.dw_enterprise_industry_apply where op_time='$ThisMonthFirstDay' and apptype_id in (1) group by user_id
                   ) b,
				          bass2.dw_product_$op_month c
              where a.dummy_mark = 0
                and a.user_id = b.user_id  
                and a.user_id = c.user_id    
             with ur;"
	puts $sql_buff
	exec_sql $sql_buff





#H051	����ͨ  ��������ͨ����ͨ�˶��û���	            ��	    
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
                   select '120',
                          count(distinct a.user_id)
                   from bass2.DWD_BANK_$op_month b,bass2.dw_enterprise_member_mid_$op_month a 
                   where b.MOBILE_NUM = a.product_no    
                   and b.OPERATE_TYPE = '�ѽ�Լ' and b.OPERATE_DATE <'$NextMonthFirstDay' and b.OPERATE_DATE >= '$ThisMonthFirstDay'
                   and (b.MOBILE_NUM like '134%' 
                     or b.MOBILE_NUM like '135%' 
                     or b.MOBILE_NUM like '136%' 
                     or b.MOBILE_NUM like '137%' 
                     or b.MOBILE_NUM like '138%' 
                     or b.MOBILE_NUM like '139%'
                     or b.MOBILE_NUM like '158%' 
                     or b.MOBILE_NUM like '150%' 
                     or b.MOBILE_NUM like '159%');"
	puts $sql_buff
	exec_sql $sql_buff



#����ͨ	H021	����ͨ�˶��û���	                      ��
   set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,ENTPERSON_LOSTNUM)
                 select '100',
                         count(distinct a.user_id)
                   from 
                   (select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$last_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')
                   except
                   select 
                                         user_id
                                         from 
                                      bass2.DW_PRODUCT_BUSI_SPROM_DM_$op_month
                                      where  sprom_id=90004001 and  busi_type=945
                                      and  user_id not in ('1160046359')    ) a left outer join bass2.dw_enterprise_member_mid_200807 b            
                   on a.user_id =b.user_id  "
	puts $sql_buff
	exec_sql $sql_buff
                         
                         
                         
                         
                         
#=================================����ʹ�ÿͻ���==============================================   
#H037	  УѶͨ	  У��ͨ����ʹ�ÿͻ���	          ��	
#H052	  ����ͨ	  ��������ͨ����ͨ����ʹ�ÿͻ���	��	
#H073	  ũ��ͨ	  ũ��ͨ����ʹ�ÿͻ���	          ��	
#H007   ����ͨ	  ����ͨ����ʹ�ÿͻ���	          ��
#H022   ����ͨ		����ͨ����ʹ�ÿͻ���	          ��
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUM)
                    select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end,
                          count(distinct a.user_id)
                   from bass2.dw_enterprise_industry_apply a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                        on a.user_id=b.user_id
                   where a.op_time = '$ThisMonthFirstDay'
                     and a.apptype_id in (1,2,3,4,5)
                   group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end;"
	puts $sql_buff
	exec_sql $sql_buff

#add by zhanght on 2009.06.17
#ũ��ͨ	  ũ��ͨ����ʹ�ÿͻ��� ���У�12582�û���
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUM)
                    select '582',
                          count(distinct a.user_id)
                   from bass2.dw_enterprise_industry_apply a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                        on a.user_id=b.user_id
                   where a.op_time = '$ThisMonthFirstDay'
                     and a.apptype_id = 3
                   with ur"
	puts $sql_buff
	exec_sql $sql_buff



#=================================ͨ�ۼ�ʹ�ÿͻ���=============================================   
#H038	УѶͨ	У��ͨ�ۼ�ʹ�ÿͻ���	          ��	
#H053	����ͨ	��������ͨ����ͨ�ۼ�ʹ�ÿͻ���	��	
#H075	ũ��ͨ	ũ��ͨ�ۼ�ʹ�ÿͻ���	          ��	
#H008 ����ͨ	����ͨ�ۼ�ʹ�ÿͻ���	          ��
#H023 ����ͨ	����ͨ�ۼ�ʹ�ÿͻ���	          ��
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUMLJ)
                   select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end,
                            count(distinct a.user_id)
                     from (select user_id,city_id,apptype_id
                           from bass2.dw_enterprise_industry_apply
                           where op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                             and apptype_id in (1,2,3,4,5)
                           group by user_id,city_id,apptype_id
                           having count(distinct op_time) >= 3
                           union all
                           select a.user_id,a.city_id,a.apptype_id
                           from bass2.dw_enterprise_industry_apply a left outer join bass2.dw_product_$op_month b
                             on a.user_id = b.user_id
                           where a.op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                             and a.apptype_id in (1,2,3,4,5)
                             
                           group by a.user_id,a.city_id,a.apptype_id) a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b
                               on a.user_id=b.user_id
                     group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140' 
                              when 4 then '210'
                              when 5 then '100'
                          end;"
	puts $sql_buff
	exec_sql $sql_buff




#===============================���¼Ʒѿͻ���=============================================   
#УѶͨ	H039	УѶͨ���¼Ʒѿͻ���	          ��	        
#����ͨ	H054	��������ͨ����ͨ���¼Ʒѿͻ���	��	  
#����ͨ	H009	����ͨ���¼Ʒѿͻ���	          ��	          
#����ͨ	H024	����ͨ���¼Ʒѿͻ���	          ��
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUMJF)
                   select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end,
                          count(distinct a.user_id)
                   from (select user_id,city_id,apptype_id
                         from bass2.dw_enterprise_industry_apply
                         where op_time = '$ThisMonthFirstDay'
                           and apptype_id in (1,2,3,4,5)
                           and fee > 0
                         group by user_id,city_id,apptype_id
                        ) a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                   on a.user_id=b.user_id
                   group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end;"
	puts $sql_buff
	exec_sql $sql_buff
                          
                          
#===============================�ۼƼƷѿͻ���============================================   
#H040	УѶͨ	У��ͨ�ۼƼƷѿͻ���	          ��	
#H055	����ͨ	��������ͨ����ͨ�ۼƼƷѿͻ���	��
#H079	ũ��ͨ	ũ��ͨ�ۼƼƷѿͻ���          	��
#H010 ����ͨ	����ͨ�ۼƼƷѿͻ���	          ��
#H025 ����ͨ	����ͨ�ۼƼƷѿͻ���	          ��
#110-У��ͨ #120-����ͨ #130-����ͨ #140-ũ��ͨ #150-�ǹ�ͨ #160-��óͨ #170-ҽ��ͨ #180-����ͨ #190-����ͨ #200-����ͨ #210-����ͨ

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,USER_NUMJFLJ)
                   select case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end,
                          count(distinct a.user_id)
                   from (select user_id,city_id,apptype_id
                         from bass2.dw_enterprise_industry_apply
                         where op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                           and apptype_id in (1,2,3,4,5)
                           and fee > 0
                         group by user_id,city_id,apptype_id
                         having count(distinct op_time) >= 3
                         union all
                         select a.user_id,a.city_id,a.apptype_id
                         from bass2.dw_enterprise_industry_apply a,bass2.dw_product_$op_month b
                         where a.op_time between '$thisyear-01-01' and '$ThisMonthFirstDay'
                           and a.apptype_id in (1,2,3,4,5)
                           and a.fee > 0
                           and a.user_id = b.user_id
                         group by a.user_id,a.city_id,a.apptype_id) a left outer join bass2.DW_ENTERPRISE_MEMBER_MID_$op_month b 
                   on a.user_id=b.user_id
                   group by case a.apptype_id
                              when 1 then '110'
                              when 2 then '120'
                              when 3 then '140'
                              when 4 then '210'
                              when 5 then '100'
                          end;"                         
	puts $sql_buff
	exec_sql $sql_buff
                          

                          
#===============================�����ж���ҵ����===========================================   
#����ͨ	H059	        --�������ж���ҵ����	MB	0	          
#����ͨ	H060	        --�������ж���ҵ����	MB	0	 

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UPSMS_COUNT)
                   select '120',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and sp_code = '931027'  and b.CALLTYPE_ID <> 1"
	puts $sql_buff
	exec_sql $sql_buff

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,DOWNSMS_COUNT)
                   select '120',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and sp_code = '931027'  and b.CALLTYPE_ID = 1;"
	puts $sql_buff
	exec_sql $sql_buff


#ũ��ͨ	H084	--�������ж���ҵ����	MB	0	      
#ũ��ͨ	H085	--�������ж���ҵ����	MB	0	������
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UPSMS_COUNT)
                   select '140',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and SER_CODE='12582' and b.CALLTYPE_ID <> 1"
	puts $sql_buff
	exec_sql $sql_buff
                     
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,DOWNSMS_COUNT)
                   select '140',
                     sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and SER_CODE='12582' and b.CALLTYPE_ID = 1;"
	puts $sql_buff
	exec_sql $sql_buff
                     
                     
                     
                       
                       
#H014 ����ͨ	--�������ж���ҵ����	                  MB
#H015 ����ͨ	--�������ж���ҵ����	                  MB
 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,UPSMS_COUNT)
                   select '210',
                   sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and b.sp_code in ('901848','400002','900139') and b.CALLTYPE_ID <> 1"
	puts $sql_buff
	exec_sql $sql_buff

 set sql_buff "insert into session.g_s_22037_month_tmp(FAB_TYPE,DOWNSMS_COUNT)
                   select '210',
                   sum(b.counts)
              from bass2.dw_enterprise_member_mid_$op_month a,bass2.dw_newbusi_ismg_$op_month b
              where  a.user_id = b.user_id and b.sp_code in ('901848','400002','900139') and b.CALLTYPE_ID = 1;"
	puts $sql_buff
	exec_sql $sql_buff








	#============����ʽ���в�������============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_22037_MONTH 
                  select
                  ${op_month},
                  '${op_month}',
                  FAB_TYPE,
                  value(char(ENT_NUM          ),'0'),
                  value(char(ENTPERSON_NUM    ),'0'),
                  value(char(UNENTPERSON_NUM  ),'0'),
                  value(char(ENTPERSON_NEWNUM ),'0'),
                  value(char(ENTPERSON_LOSTNUM),'0'),
                  value(char(USER_NUM         ),'0'),
                  value(char(USER_NUMLJ       ),'0'),
                  value(char(USER_NUMJF       ),'0'),
                  value(char(USER_NUMJFLJ     ),'0'),
                  value(char(UPSMS_COUNT      ),'0'),
                  value(char(DOWNSMS_COUNT    ),'0')
                  from
                  (
                   select
                     FAB_TYPE  as FAB_TYPE,
                     sum(ENT_NUM          ) as ENT_NUM          ,
                     sum(ENTPERSON_NUM    ) as ENTPERSON_NUM    ,
                     sum(UNENTPERSON_NUM  ) as UNENTPERSON_NUM  ,
                     sum(ENTPERSON_NEWNUM ) as ENTPERSON_NEWNUM ,
                     sum(ENTPERSON_LOSTNUM) as ENTPERSON_LOSTNUM,
                     sum(USER_NUM         ) as USER_NUM         ,
                     sum(USER_NUMLJ       ) as USER_NUMLJ       ,
                     sum(USER_NUMJF       ) as USER_NUMJF       ,
                     sum(USER_NUMJFLJ     ) as USER_NUMJFLJ     ,
                     sum(UPSMS_COUNT      ) as UPSMS_COUNT      ,
                     sum(DOWNSMS_COUNT    ) as DOWNSMS_COUNT        
                                     
                  from 
                      session.g_s_22037_month_tmp
                  group by FAB_TYPE
                )aa"   
           puts $sql_buff                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ʽ���в�������"






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
		WriteTrace "$errmsg" 2005
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
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
