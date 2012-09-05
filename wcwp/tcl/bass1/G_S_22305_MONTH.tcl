######################################################################################################
#�ӿ����ƣ��ƶ�400ҵ�����»���
#�ӿڱ��룺22305
#�ӿ�˵�������ӿ��ϱ��ƶ�400ҵ���ҵ������ʹ�������
#��������: G_S_22305_MONTH.tcl
#��������: ����22305������
#��������: ��
#Դ    ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2009-11-22
#�����¼��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2009-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        #----���������һ��---#,��ʽ yyyymmdd
        puts $last_month_day
        
        set thisyear [string range $op_time 0 3]
        puts $thisyear
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set objecttable "G_S_22305_MONTH"
        #set db_user $env(DB_USER)
       
        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


  #����G_S_22305_MONTH��session������session.tmp22305
	set sql_buff "declare global temporary table session.tmp22305
	        (
						  enterprise_id  		char(20)   not null,
						  ent_busi_id			  char(4)    not null,
						  manage_mod			  char(1)    not null,
							upmessage					bigint,
							downmessage				bigint,
							local_counts			bigint,
							chang_counts			bigint,
							local_duration		bigint,
							chang_duration		bigint
                )
               partitioning key (ENTERPRISE_ID) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	puts $sql_buff
	exec_sql $sql_buff
	puts "����G_S_22305_MONTH��session������session.tmp22305"
	

  #��������ҵ����ʱ��,����session.tmp22305_temp1
	set sql_buff "declare global temporary table session.tmp22305_temp1
	        (
						  service_id  		char(20),
						  config_value		varchar(100),
						  bass1_value			varchar(100),
							plan_id			    bigint
                )
               partitioning key (plan_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	puts $sql_buff
	exec_sql $sql_buff
	puts "����G_S_22305_MONTH��session������session.tmp22305"



  #����user_id����ҵ����ʱ������session.tmp22305_temp2
	set sql_buff "declare global temporary table session.tmp22305_temp2
	        (
						  user_id						char(20)   not null,
						  ent_busi_id			  char(4)    not null,
						  manage_mod			  char(1)    not null,
							upmessage					bigint,
							downmessage				bigint,
							local_counts			bigint,
							chang_counts			bigint,
							local_duration		bigint,
							chang_duration		bigint
                )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
 	puts $sql_buff
	exec_sql $sql_buff
	puts "����G_S_22305_MONTH��session������session.tmp22305_temp2"



  #�������弯��ҵ�����ͽṹ��Ϣ����
	set sql_buff "insert into session.tmp22305
	        (
						enterprise_id,
						ent_busi_id,
						manage_mod,
						upmessage,
						downmessage,
						local_counts,
						chang_counts,
						local_duration,
						chang_duration
            )
						select distinct
							a.enterprise_id   as enterprise_id,
							'1520'            as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as upmessage,
							0 as downmessage,
							0 as local_counts,
							0 as chang_counts,
							0 as local_duration,
							0 as chang_duration
						from bass2.dw_enterprise_extsub_rela_${op_month} a,
						(select * from bass2.dim_service_config where config_id=1000027) b
						where a.service_id = b.service_id
						  and a.service_id = '931'
						"
          
  puts $sql_buff        
	exec_sql $sql_buff
	puts "���뵽session���е�ҵ��ṹ��Ϣ���"	


  ## 931	�ƶ�400			1520	�ƶ�400


  #���ɼ���ҵ����ʱ��
	set sql_buff "insert into session.tmp22305_temp1
	        (
						service_id,
						config_value,
						bass1_value,
					  plan_id
            )
            select 
              a.service_id,
              a.config_value,
              '1520',
              c.plan_id 
            from 
						(select * from bass2.dim_service_config where config_id=1000027) a,
						(SELECT service_id,int(plan_id) plan_id FROM bass2.DIM_ENT_GROUP_PLAN) c
						where a.service_id = '931'
						  and a.service_id = c.service_id"
          
  puts $sql_buff        
	exec_sql $sql_buff
	puts "���뵽session.tmp22305_temp1����ҵ��������ʱ�����"	


  #ͳ�����С����ж�������
	set sql_buff "insert into session.tmp22305_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							upmessage,
							downmessage,
							local_counts,
							chang_counts,
							local_duration,
							chang_duration
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							sum(case when calltype_id=0 then counts else 0 end) as upmessage,
							sum(case when calltype_id=1 then counts else 0 end) as downmessage,
							0 as local_counts,
							0 as chang_counts,
							0 as local_duration,
							0 as chang_duration
						from bass2.dw_newbusi_ismg_${op_month} a,
						     session.tmp22305_temp1 b
						where a.plan_id = b.plan_id
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end"
          
  puts $sql_buff        
	exec_sql $sql_buff
	puts "���뵽session.tmp22305_temp2���е����С����ж�������ָ����Ϣ���"	
		


  #ͳ�Ʊ����������д���/��;�������д���/������������ʱ��/��;��������ʱ��
	set sql_buff "insert into session.tmp22305_temp2
	        (
						  user_id,
						  ent_busi_id,
						  manage_mod,
							upmessage,
							downmessage,
							local_counts,
							chang_counts,
							local_duration,
							chang_duration
            )
						select 
							a.user_id      as user_id,
							b.bass1_value  as ent_busi_id,
							case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end as manage_mod,
							0 as upmessage,
							0 as downmessage,
							sum(case when a.tolltype_id =0 and a.calltype_id<>1 then call_counts else 0 end) as local_counts,
							sum(case when a.tolltype_id<>0 and a.calltype_id<>1 then call_counts else 0 end) as chang_counts,
							sum(case when a.tolltype_id =0 and a.calltype_id<>1 then call_duration else 0 end) as local_duration,
							sum(case when a.tolltype_id<>0 and a.calltype_id<>1 then call_duration else 0 end) as chang_duration
						from bass2.dw_call_${op_month} a,
						     session.tmp22305_temp1 b
						where a.plan_id = b.plan_id
					 group by a.user_id,b.bass1_value,
					 		case
								when upper(b.config_value)='MAS' then '1'
								when upper(b.config_value)='ADC' then '2'
								else '3'
							end
					"
          
  puts $sql_buff        
	exec_sql $sql_buff
	puts "���뵽session.tmp22305_temp2���е�ͳ�Ʊ����������д���/��;�������д���/������������ʱ��/��;��������ʱ�����"	
	

  #����enterprise_id������ָ�����ݲ�����ʱ��
	set sql_buff "insert into session.tmp22305
	        (
						enterprise_id,
						ent_busi_id,
						manage_mod,
						upmessage,
						downmessage,
						local_counts,
						chang_counts,
						local_duration,
						chang_duration
            )
						select 
							a.enterprise_id                 as enterprise_id,
							b.ent_busi_id                   as ent_busi_id,
							b.manage_mod                    as manage_mod,
							sum(upmessage) 			      as upmessage,
							sum(downmessage) 		      as downmessage,
							sum(local_counts) 				as local_counts,
							sum(chang_counts) 			  as chang_counts,
							sum(local_duration) 	    as local_duration,
							sum(chang_duration)       as chang_duration
						from bass2.dw_enterprise_member_mid_${op_month} a,
						     session.tmp22305_temp2 b
						where a.user_id = b.user_id
					 group by a.enterprise_id,b.ent_busi_id,b.manage_mod"
          
  puts $sql_buff        
	exec_sql $sql_buff
	puts "���뵽session.tmp22305����ָ����Ϣ�����������"		


	#����Ŀ���
	set sql_buff "insert into bass1.G_S_22305_MONTH
	        (
	            time_id,
							enterprise_id,
							ent_busi_id,
							manage_mod,
							upmessage,
							downmessage,
							local_counts,
							chang_counts,
							local_duration,
							chang_duration
            )
						select 
						  $op_month,
						  enterprise_id,
						  ent_busi_id,
						  manage_mod,
							char(sum(upmessage)) 			      as upmessage,
							char(sum(downmessage)) 		      as downmessage,
							char(sum(local_counts)) 				as local_counts,
							char(sum(chang_counts)) 			  as chang_counts,
							char(BIGINT(ROUND(SUM(local_duration)/60.0,0))) 	as local_duration,
							char(BIGINT(ROUND(SUM(chang_duration)/60.0,0)))   as chang_duration
						from session.tmp22305
					group by enterprise_id,ent_busi_id,manage_mod"
          
	  puts $sql_buff        
		exec_sql $sql_buff
		puts "���뵽Ŀ�����bass1.G_S_22305_MONTH ����Ϣ���"	

   
    #�޳����Լ���
    set sql_buff " delete from  BASS1.G_S_22305_MONTH where time_id = $op_month and enterprise_id in ('891910006274','891910006688','891910006714','891910006932')"
    puts $sql_buff      
    exec_sql $sql_buff      



	aidb_close $handle

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
#--------------------------------------------------------------------------------------------------------------

##~   20120903�� 22305 �� ���ߣ�

  ##~   delete from (
  ##~   select *from app.sch_control_task where control_code like '%22305%MONTH%'
  ##~   ) t 
  
  
##~   CONTROL_CODE                                       BEFORE_CONTROL_CODE                               
##~   -------------------------------------------------- --------------------------------------------------
##~   BASS1_EXP_G_S_22305_MONTH                          BASS1_G_S_22305_MONTH.tcl                         
##~   BASS1_G_S_22305_MONTH.tcl                          BASS2_Dw_enterprise_extsub_rela_ds.tcl            
##~   BASS1_G_S_22305_MONTH.tcl                          BASS2_Dw_newbusi_ismg_dt.tcl                      
##~   BASS1_G_S_22305_MONTH.tcl                          BASS2_Dw_call_dt.tcl                              
##~   BASS1_G_S_22305_MONTH.tcl                          BASS2_Dw_enterprise_member_mid_ms.tcl   

  ##~   delete from (
    ##~   select *from app.sch_control_before where control_code like 'BASS1%22305%MONTH%'
  ##~   ) t 
  
  
  