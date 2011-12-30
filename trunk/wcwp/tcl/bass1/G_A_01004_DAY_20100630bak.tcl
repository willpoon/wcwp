######################################################################################################
#�ӿ����ƣ����ſͻ�
#�ӿڱ��룺01004
#�ӿ�˵����ָ����֯�������й��ƶ�ǩ��Э��,������ʹ���й��ƶ�ͨ�Ų�Ʒ�ͷ���
#          �����й��ƶ��������ſͻ���ϵ����ķ��˵�λ���������Ĳ�ҵ���λ��
#��������: G_A_01004_DAY.tcl
#��������: ����01004������
#��������: ��
#Դ    ��1.bass2.dwd_enterprise_msg_yyyymmdd
#          2.bass1.dwd_enterprise_manager_rela_yyyymmdd
#          3.bass2.dw_enterprise_member_ds
#          4.bass1.g_a_01001_day
#          5.BASS2.DW_ENTERPRISE_ACCOUNT_YYYYMMDD
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ����ҵ���������ͱ��롱��ҵ���������Ͳ�������"��ҵ���������ͱ���"����Ҫ��ͳһ�99��
#�޸���ʷ: 2009-06-09 zhanght �޸ļ������ (0 A1;1 A2;3 B1;4 B2),�޸ļ���ͳһ���ѱ�־
#          20090818  liuzhilong ֻȡ60���ַ�  ,a.group_name as enterprise_name
###        20090910  ����enter_type_id(���ſͻ���������)�ֶΣ��޸�ent_scale_id(���ż�ֵ�������)ӳ���ϵ
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	      #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        #���� yyyy-mm-dd
        set optime $op_time
        puts $optime
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        puts $op_month
        set last_month [GetLastMonth [string range $op_month 0 5]]
        puts $last_month
        
        #ɾ����������
  	set sql_buff "delete from bass1.g_a_01004_day where time_id=$timestamp"
    puts $sql_buff      
    exec_sql $sql_buff      


    #������ʱ��(ͳ�Ƹü���Ա����)
	  set sql_buff "declare global temporary table session.g_a_01004_tmp1
	              (
	                 enterprise_id       varchar(20),
                   numbers             int
	              )
	              partitioning key
	              (enterprise_id)
	              using hashing
	              with replace on commit preserve rows not logged in tbs_user_temp"
    puts $sql_buff      
    exec_sql $sql_buff      

    #������ʱ��(ͳ�Ƹü��Ŷ�Ӧ�Ĵ�ͻ�����)
	  set sql_buff "declare global temporary table session.g_a_01004_tmp2
	              (
	                 enterprise_id       varchar(20),
                   manager_id          varchar(20)
	              )
	              partitioning key
	              (enterprise_id)
	              using hashing
	              with replace on commit preserve rows not logged in tbs_user_temp"
    puts $sql_buff      
    exec_sql $sql_buff      
  
   #����ʱ���������(ͳ�Ƹü���Ա����)
	 set sql_buff "insert into  session.g_a_01004_tmp1
                    select 
                      enterprise_id
                      ,count(distinct cust_id)
                    from 
                      bass2.dw_enterprise_member_ds
                    group by 
                      enterprise_id "               
    puts $sql_buff      
    exec_sql $sql_buff      
        
   #����ʱ���������(ͳ�Ƹü��Ŷ�Ӧ�Ĵ�ͻ�����)
	 set sql_buff "insert into  session.g_a_01004_tmp2
                    select 
                      enterprise_id
                      ,char(max(manager_id))
                    from 
                      bass2.dwd_enterprise_manager_rela_${timestamp} 
                    group by 
                      enterprise_id"             
    puts $sql_buff      
    exec_sql $sql_buff      
 
               
        #������ʱ������(ͳ�Ƹü���Ա����)
	  set sql_buff "\
	     create index  session.idx_tmp1_ei on session.g_a_01004_tmp1(enterprise_id) "               
    puts $sql_buff      
    exec_sql $sql_buff      
          
        #������ʱ������(ͳ�Ƹü��Ŷ�Ӧ�Ĵ�ͻ�����)
	  set sql_buff "\
	     create index  session.idx_tmp2_ei on session.g_a_01004_tmp2(enterprise_id) "               
    puts $sql_buff      
    exec_sql $sql_buff      
  
        
        #��������
#    20090818  ֻȡ60���ַ�  ,a.group_name as enterprise_name
        set sql_buff "insert into bass1.g_a_01004_day
                   select
                     ${timestamp}
                     ,a.enterprise_id
                     ,case 
                        when a.group_level=1 then '1'
                        when a.group_level=2 then '2'
                        else '3' 
                      end as ent_def_mode
		                 ,' ' as prt_grp_cust_id
                     ,substr(a.group_name,1,60) as enterprise_name
                     ,value(substr(a.owner_name,1,20),' ') as owner_name 
                     ,value(a.net_address,' ')
                     ,value(a.fax_id, ' ') as fax_no
                     ,case  group_level
                            when 0 then '4'
													  when 1 then '4'
													  when 2 then '5'
													  when 3 then '6'
													  when 4 then '7'
													  when 5 then '8'
													  else '9'
                      end as ent_scale_id
                     ,value(char(b.numbers),'0') as member_nums
                     ,'99' as ent_region_type
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0043',char(a.vocation)),'99') as ent_industry_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0020',char(a.region_specia)),'99') as grp_area_spec_id
                     ,value(char(c.manager_id),' ') as ent_manager_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.group_city)),'13101') as cmcc_id
                     ,value(replace(char(date(a.create_date)),'-',''),'20100101') as create_date
                     ,' ' as  linkman_name
                     ,value(substr(a.phone_id,1,20),' ') as telephone_num
                     ,value(substr(a.phone_id,1,15),' ') as mobile_num
                     ,' ' as linkman_title
                     ,value(a.fax_id,' ') as linkman_fax
                     ,coalesce(a.email,' ') as linkman_mail
                     ,value(a.group_address,' ') as linkman_addr
                     ,value(char(a.group_postcode),' ') as post_code
                     ,case
                        when a.group_status=0 then '20'
                        else '11'
                      end 
                     ,case when f.enterprise_id is null then '0' else '1' end as unite_pay_flag
                     ,' ' as ind_res_schema
                     ,' ' as crt_chnl_id
                     ,' '  as enter_type_id 
                   from bass2.dwd_enterprise_msg_${timestamp} a
                    left join  session.g_a_01004_tmp2 c
                     on a.enterprise_id=c.enterprise_id 
                    left join  session.g_a_01004_tmp1 b
                     on a.enterprise_id=b.enterprise_id 
                    left join (select distinct ENTERPRISE_ID
                                from BASS2.DW_ENTERPRISE_ACCOUNT_${timestamp}
                                  where REC_STATUS=1
                               ) f
                     on a.enterprise_id=f.enterprise_id
                   except
                   select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, OWNER_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                       ENT_MANAGER_ID, CMCC_ID, CREATE_DATE, LINKMAN_NAME, 
                       TELEPHONE_NUM, MOBILE_NUM, LINKMAN_TITLE, LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG, IND_RES_SCHEMA, CRT_CHNL_ID,enter_type_id
                   	from
                   	(
                   select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, OWNER_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                       ENT_MANAGER_ID, CMCC_ID, CREATE_DATE, LINKMAN_NAME, 
                       TELEPHONE_NUM, MOBILE_NUM, LINKMAN_TITLE, LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG, IND_RES_SCHEMA, CRT_CHNL_ID,enter_type_id,row_number()over(partition by enterprise_id order by time_id desc) row_id
                     from BASS1.G_A_01004_DAY 
                     where time_id<${timestamp}
                     ) k
                     where k.row_id=1   
                     
                     
                      with ur
                   "
    puts $sql_buff      
    exec_sql $sql_buff      
	
	
    #������ʱ��(ͳ�Ƹü���Ա����)
	  set sql_buff "declare global temporary table session.g_a_01004_tmp10
	              (
	                 enterprise_id       varchar(20)
	              )
	              partitioning key
	              (enterprise_id)
	              using hashing
	              with replace on commit preserve rows not logged in tbs_user_temp"
    puts $sql_buff      
    exec_sql $sql_buff      

	
	#����ɾ���ļ������� 
  set sql_buff "insert into session.g_a_01004_tmp10 
               select distinct enterprise_id  from bass2.dwd_enterprise_msg_his_${timestamp} where group_status = 0
               except 
               select distinct a.enterprise_id from
                 (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= ${timestamp}) a,
                 (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                                 where time_id<=${timestamp} 
															                                group by enterprise_id)b
                where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20' ;" 
    puts $sql_buff      
    exec_sql $sql_buff      


	#����ɾ���ļ�������
  set sql_buff "insert into session.g_a_01004_tmp10 
               select aa.enterprise_id from 
                 (
                   select distinct enterprise_id  from bass2.dwd_enterprise_msg_his_${timestamp} where group_status = 0
                   except 
                   select distinct enterprise_id  from bass2.dwd_enterprise_msg_${timestamp} )aa,
				         (select distinct a.enterprise_id from
                   (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= ${timestamp} ) a,
                   (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                                 where time_id<=${timestamp}  
                                                              group by enterprise_id)b
                where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20')bb
				where aa.enterprise_id = bb.enterprise_id ;" 
    puts $sql_buff      
    exec_sql $sql_buff      
  
  
#    20090818  ֻȡ60���ַ�  ,a.group_name as enterprise_name
        set sql_buff "insert into bass1.g_a_01004_day
                     select
                     ${timestamp}
                     ,a.enterprise_id
                     ,case 
                        when a.group_level=1 then '1'
                        when a.group_level=2 then '2'
                        else '3' 
                      end as ent_def_mode
		             ,' ' as prt_grp_cust_id
                     ,substr(a.group_name,1,60) as enterprise_name
                     ,value(substr(a.owner_name,1,20),' ') as owner_name 
                     ,value(a.net_address,' ')
                     ,value(a.fax_id, ' ') as fax_no
                     ,case  group_level
                            when 0 then '4'
													  when 1 then '4'
													  when 2 then '5'
													  when 3 then '6'
													  when 4 then '7'
													  when 5 then '8'
													  else '9'
                      end as ent_scale_id
                     ,'0' as member_nums
                     ,'99' as ent_region_type
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0043',char(a.vocation)),'99') as ent_industry_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0020',char(a.region_specia)),'99') as grp_area_spec_id
                     ,' ' as ent_manager_id
                     ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.group_city)),'13101') as cmcc_id
                     ,value(replace(char(date(a.create_date)),'-',''),'20100101') as create_date
                     ,' ' as  linkman_name
                     ,value(substr(a.phone_id,1,20),' ') as telephone_num
                     ,value(substr(a.phone_id,1,15),' ') as mobile_num
                     ,' ' as linkman_title
                     ,value(a.fax_id,' ') as linkman_fax
                     ,coalesce(a.email,' ') as linkman_mail
                     ,value(a.group_address,' ') as linkman_addr
                     ,value(char(a.group_postcode),' ') as post_code
                     ,'11'
                     ,case when b.enterprise_id is null then '0' else '1' end
                     ,' ' as ind_res_schema
                     ,' ' as crt_chnl_id
                     ,' ' as enter_type_id
                   from 
                     (select a.* from 
                                (select * from bass2.dwd_enterprise_msg_his_${timestamp})  a,
                                (select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_${timestamp}  group by enterprise_id) b
                           where a.done_date = b.done_date and a.enterprise_id = b.enterprise_id and rec_status = 0 and  a.enterprise_id in (select * from session.g_a_01004_tmp10) 
                     )a 
                     left outer join (select distinct ENTERPRISE_ID
                                       from BASS2.DW_ENTERPRISE_ACCOUNT_${timestamp}
                                        where REC_STATUS=1
                                      ) b
                     on a.enterprise_id=b.enterprise_id
                     
                      except
                     select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, OWNER_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                       ENT_MANAGER_ID, CMCC_ID, CREATE_DATE, LINKMAN_NAME, 
                       TELEPHONE_NUM, MOBILE_NUM, LINKMAN_TITLE, LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG, IND_RES_SCHEMA, CRT_CHNL_ID,enter_type_id
                   	from
                   	(
                   select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, OWNER_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                       ENT_MANAGER_ID, CMCC_ID, CREATE_DATE, LINKMAN_NAME, 
                       TELEPHONE_NUM, MOBILE_NUM, LINKMAN_TITLE, LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG, IND_RES_SCHEMA, CRT_CHNL_ID,enter_type_id,row_number()over(partition by enterprise_id order by time_id desc) row_id
                     from BASS1.G_A_01004_DAY 
                     where time_id<${timestamp}
                     ) k
                     where k.row_id=1   
                     
                      with ur
                     
                     
                     "
    puts $sql_buff      
    exec_sql $sql_buff      


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
