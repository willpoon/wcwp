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
#--------------------------ȫ������������20100625------------------------------
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
        
				set app_name "G_A_01004_DAY.tcl"        
        
        #ɾ����������
  	set sql_buff "delete from bass1.g_a_01004_day where time_id=$timestamp"
    puts $sql_buff      
    exec_sql $sql_buff      

   #     #ɾ����ʱ������(g_a_01004_tmp1)
	 # set sql_buff "\
	 #    drop index  bass1.idx_01004tmp1_ei  "               
   # puts $sql_buff      
   # exec_sql $sql_buff    

        #ɾ����ʱ������(g_a_01004_tmp2)
	 # set sql_buff "\
	 #    drop index  bass1.idx_01004tmp2_ei  "               
   # puts $sql_buff      
   # exec_sql $sql_buff    
        
    #�����ʱ��(ͳ�Ƹü���Ա����)
        set sql_buff "alter table bass1.g_a_01004_tmp1 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff

    #�����ʱ��(ͳ�Ƹü��Ŷ�Ӧ�Ĵ�ͻ�����)
        set sql_buff "alter table bass1.g_a_01004_tmp2 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff

  
   #����ʱ���������(ͳ�Ƹü���Ա����)
	 set sql_buff "insert into  bass1.g_a_01004_tmp1
                    select 
                      enterprise_id
                      ,count(distinct cust_id)
                    from 
                      bass2.dw_enterprise_member_ds
                    group by 
                      enterprise_id with ur "               
    puts $sql_buff      
    exec_sql $sql_buff      
        
   #����ʱ���������(ͳ�Ƹü��Ŷ�Ӧ�Ĵ�ͻ�����)
	 set sql_buff "insert into  bass1.g_a_01004_tmp2
                    select 
                      enterprise_id
                      ,char(max(manager_id))
                    from 
                      bass2.dwd_enterprise_manager_rela_${timestamp} 
                    group by 
                      enterprise_id with ur "             
    puts $sql_buff      
    exec_sql $sql_buff      
 
               
        #������ʱ������(ͳ�Ƹü���Ա����)
	  #set sql_buff "\
	  #   create index  bass1.idx_01004tmp1_ei on bass1.g_a_01004_tmp1(enterprise_id) "               
    #puts $sql_buff      
    #exec_sql $sql_buff      
          
        #������ʱ������(ͳ�Ƹü��Ŷ�Ӧ�Ĵ�ͻ�����)
	  #set sql_buff "\
	  #   create index  bass1.idx_01004tmp2_ei on bass1.g_a_01004_tmp2(enterprise_id) "               
    #puts $sql_buff      
    #exec_sql $sql_buff      

  exec db2 connect to bassdb user bass1 using bass1
  exec db2 runstats on table bass1.g_a_01004_tmp1 with distribution and detailed indexes all
  exec db2 runstats on table bass1.g_a_01004_tmp2 with distribution and detailed indexes all  
  exec db2 terminate  

    #�����ʱ��(g_a_01004_tmp3)
        set sql_buff "alter table bass1.g_a_01004_tmp3 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff        
        #�������ݣ����� ��ϵ������/��ϵ�˹̶��绰/��ϵ���ƶ��绰/��ϵ��ְ��/��ϵ�˴���/��ϵ�˵���/ͨ�ŵ�ַ/��������
        #�� ��ϵ������ �����⣿
        # 20090818  ֻȡ60���ַ�  ,a.group_name as enterprise_name
        
        set sql_buff "insert into bass1.g_a_01004_tmp3
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
                     ,case group_level
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
                     ,value(substr(e.cust_name,1,20),' ')    as  linkman_name
                     ,value(substr(e.office_phone,1,20),' ') as telephone_num
                     ,value(substr(e.link_phone,1,15),' ')   as mobile_num
                     ,value(substr(e.position,1,20),'')      as linkman_title
                     ,value(e.fax,' ') as linkman_fax
                     ,coalesce(e.email,' ') as linkman_mail
                     ,value(e.post_address,' ') as linkman_addr
                     ,value(char(e.post_code),' ') as post_code
                     ,case
                        when a.group_status=0 then '20'
                        else '11'
                      end 
                     ,case when f.enterprise_id is null then '0' else '1' end as unite_pay_flag
                     ,' ' as ind_res_schema
                     ,' ' as crt_chnl_id
                     ,''  as enter_type_id 
                   from bass2.dwd_enterprise_msg_${timestamp} a
                    left join  bass1.g_a_01004_tmp2 c on a.enterprise_id=c.enterprise_id 
                    left join  bass1.g_a_01004_tmp1 b on a.enterprise_id=b.enterprise_id 
                    left join (select distinct ENTERPRISE_ID
                                from BASS2.DW_ENTERPRISE_ACCOUNT_${timestamp} where REC_STATUS=1
                               ) f on a.enterprise_id=f.enterprise_id
                    left join bass2.dwd_cust_msg_${timestamp} e on a.cust_id=e.cust_id
                    with ur 
 					"
        puts $sql_buff
        exec_sql $sql_buff    

#/**
#458	�漰����������������������������ͨ������������ֱ�����������޶���
#1��(02004)�û��ӿڡ�����������ʶ����ͨ������������ֱ����������
#�����¹�����д�ض������ʶ��
#��վ�����ߡ����š�wap�������ն˵���������ֱ�������ֱ��Ӧ��д
#'BASS1_WB', 'BASS1_HL','BASS1_SM','BASS1_WP','BASS1_ST', 'BASS1_DS' 
#(�ַ����ִ�Сд)�� ʡ��˾����ռ������6�����������á�
#2�� (01004)���˿ͻ���(01004)���ſͻ���(02013) IPֱͨ���̶��û���
#������������ʶ����������������ʶ���μ�(02004)�û��ӿ��ϱ���	
#1.7.3	2011-5-17	����������20110601����Ч
#**/
#add     

#2011-06-01 21:57:54 trans channel
#add

set sql_buff "alter table bass1.G_A_01004_DAY_CHNL_MID activate not logged initially with empty table"
exec_sql $sql_buff


        
set sql_buff "
insert into G_A_01004_DAY_CHNL_MID
select 
         a.TIME_ID
        ,a.ENTERPRISE_ID
        ,a.ENT_DEF_MODE
        ,a.PRT_GRP_CUST_ID
        ,a.ENTERPRISE_NAME
        ,a.OWNER_NAME
        ,a.NET_ADDRESS
        ,a.FAX_NO
        ,a.ENT_SCALE_ID
        ,a.MEMBER_NUMS
        ,a.ENT_REGION_TYPE
        ,a.ENT_INDUSTRY_ID
        ,a.GRP_AREA_SPEC_ID
        ,a.ENT_MANAGER_ID
        ,a.CMCC_ID
        ,a.CREATE_DATE
        ,a.LINKMAN_NAME
        ,a.TELEPHONE_NUM
        ,a.MOBILE_NUM
        ,a.LINKMAN_TITLE
        ,a.LINKMAN_FAX
        ,a.LINKMAN_MAIL
        ,a.LINKMAN_ADDR
        ,a.POST_CODE
        ,a.CUST_STATU_TYP_ID
        ,a.UNITE_PAY_FLAG
        ,a.IND_RES_SCHEMA
        ,case when b.CHANNEL_ID is null then 'BASS1_DS' 
        		else char(c.channel_id) end CHANNEL_ID 
        ,a.ENTER_TYPE_ID
from bass1.g_a_01004_tmp3 a
left join  bass2.dwd_enterprise_manager_rela_${timestamp} c on a.ENTERPRISE_ID = c.ENTERPRISE_ID
left join (select distinct CHANNEL_ID  from bass1.G_I_06021_MONTH )  b on char(c.CHANNEL_ID) = b.CHANNEL_ID
where a.time_id = ${timestamp}

"
exec_sql $sql_buff

	set sql_buff "\
		DELETE FROM bass1.g_a_01004_tmp3 
		"
  exec_sql $sql_buff

	set sql_buff "
	insert into  bass1.g_a_01004_tmp3 
	select * from G_A_01004_DAY_CHNL_MID
	"
exec_sql $sql_buff

        
        #�����������ȥ����֮ǰ�����ݣ��ڴ˽�����ʱ��
        set sql_buff "alter table bass1.g_a_01004_tmp4 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff

	set sql_buff "insert into bass1.g_a_01004_tmp4      
			(
			TIME_ID
			,ENTERPRISE_ID
			,ENT_DEF_MODE
			,PRT_GRP_CUST_ID
			,ENTERPRISE_NAME
			,NET_ADDRESS
			,FAX_NO
			,ENT_SCALE_ID
			,MEMBER_NUMS
			,ENT_REGION_TYPE
			,ENT_INDUSTRY_ID
			,GRP_AREA_SPEC_ID
			,CMCC_ID
			,CREATE_DATE
			,TELEPHONE_NUM
			,MOBILE_NUM
			,LINKMAN_FAX
			,LINKMAN_MAIL
			,LINKMAN_ADDR
			,POST_CODE
			,CUST_STATU_TYP_ID
			,UNITE_PAY_FLAG
			,CRT_CHNL_ID
			,ENTER_TYPE_ID
			)
	select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                        CMCC_ID, CREATE_DATE, 
                       TELEPHONE_NUM, MOBILE_NUM,  LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG,  CRT_CHNL_ID,enter_type_id
                   	from
                   	(
                   select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                        CMCC_ID, CREATE_DATE, 
                       TELEPHONE_NUM, MOBILE_NUM,  LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG,  CRT_CHNL_ID,enter_type_id,row_number()over(partition by enterprise_id order by time_id desc) row_id
                     from BASS1.G_A_01004_DAY 
                     where time_id<${timestamp}
                     ) k
                     where k.row_id=1   
                     with ur
                "
        puts $sql_buff
        exec_sql $sql_buff
                        
        set sql_buff "insert into bass1.g_a_01004_day
        					 select 
							         TIME_ID
							        ,ENTERPRISE_ID
							        ,ENT_DEF_MODE
							        ,PRT_GRP_CUST_ID
							        ,ENTERPRISE_NAME
							        ,NET_ADDRESS
							        ,FAX_NO
							        ,ENT_SCALE_ID
							        ,MEMBER_NUMS
							        ,ENT_REGION_TYPE
							        ,ENT_INDUSTRY_ID
							        ,GRP_AREA_SPEC_ID
							        ,CMCC_ID
							        ,CREATE_DATE
							        ,TELEPHONE_NUM
							        ,MOBILE_NUM
							        ,LINKMAN_FAX
							        ,LINKMAN_MAIL
							        ,LINKMAN_ADDR
							        ,POST_CODE
							        ,CUST_STATU_TYP_ID
							        ,UNITE_PAY_FLAG
							        ,CRT_CHNL_ID
							        ,ENTER_TYPE_ID       
							        from  bass1.g_a_01004_tmp3					         				                    
                   except
                   select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                        CMCC_ID, CREATE_DATE, 
                       TELEPHONE_NUM, MOBILE_NUM,  LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG,  CRT_CHNL_ID,enter_type_id
                       from bass1.g_a_01004_tmp4
                    with ur   
                   "
    puts $sql_buff      
    exec_sql $sql_buff      
	
        #ɾ����ʱ������(g_a_01004_tmp10)
	  #set sql_buff "\
	  #   drop index  bass1.idx_01004tmp10_ei  "               
    #puts $sql_buff      
    #exec_sql $sql_buff    
    	
    #�����ʱ��(ͳ�Ƹü���Ա����)
        set sql_buff "alter table bass1.g_a_01004_tmp10 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff

	
	#����ɾ���ļ������� 
  set sql_buff "insert into bass1.g_a_01004_tmp10 
               select distinct enterprise_id  from bass2.dwd_enterprise_msg_his_${timestamp} where group_status = 0
               except 
               select distinct enterprise_id from
								(
								select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
								from bass1.G_A_01004_DAY 
								where time_id <= ${timestamp} 
								) t where t.rn = 1 and  cust_statu_typ_id = '20'
								"
    puts $sql_buff      
    exec_sql $sql_buff      


	#����ɾ���ļ�������
  set sql_buff "insert into bass1.g_a_01004_tmp10 
               select aa.enterprise_id from 
                 (
                   select distinct enterprise_id  from bass2.dwd_enterprise_msg_his_${timestamp} where group_status = 0
                   except 
                   select distinct enterprise_id  from bass2.dwd_enterprise_msg_${timestamp} )aa,
				         (
				         select distinct enterprise_id from
								(
									select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
									from bass1.G_A_01004_DAY 
									where time_id <= ${timestamp} 
								) t where t.rn = 1 and  cust_statu_typ_id = '20') bb
				where aa.enterprise_id = bb.enterprise_id
				" 
    puts $sql_buff      
    exec_sql $sql_buff      

        #������ʱ������(g_a_01004_tmp10)
	  #set sql_buff "\
	  #   create index  bass1.idx_01004tmp10_ei on bass1.g_a_01004_tmp10(enterprise_id) "               
    #puts $sql_buff      
    #exec_sql $sql_buff     
    
        #ɾ����ʱ������(g_a_01004_tmp11)
	  #set sql_buff "\
	  #   drop index  bass1.idx_01004tmp11_ei  "               
    #puts $sql_buff      
    #exec_sql $sql_buff      
      
		#�����ʱ��	
        set sql_buff "alter table bass1.g_a_01004_tmp11 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff
    #��������Ŀ�꼯����Ϣ    
    set sql_buff "    
		    insert into bass1.g_a_01004_tmp11       
				select 
  	     a.ENTERPRISE_ID
        ,a.GROUP_NAME
        ,a.PASSWORD
        ,a.GROUP_LEVEL
        ,a.GROUP_TYPE
        ,a.REGION_SPECIA
        ,a.GROUP_STATUS
        ,a.LEVEL_DEF_MODE
        ,a.REC_STATUS
        ,a.VOCATION
        ,a.VOCATION2
        ,a.VOCATION3
        ,a.GROUP_COUNTRY
        ,a.GROUP_PROVINCE
        ,a.GROUP_CITY
        ,a.REGION_TYPE
        ,a.REGION_DETAIL
        ,a.GROUP_ADDRESS
        ,a.GROUP_POSTCODE
        ,a.POST_PROVINCE
        ,a.POST_CITY
        ,a.POST_ADDRESS
        ,a.POST_POSTCODE
        ,a.PHONE_ID
        ,a.FAX_ID
        ,a.IDEN_ID
        ,a.IDEN_NBR
        ,a.OWNER_NAME
        ,a.TAX_ID
        ,a.NET_ADDRESS
        ,a.PAY_TYPE
        ,a.EMAIL
        ,a.CREATE_DATE
        ,a.DONE_DATE
        ,a.VALID_DATE
        ,a.EXPIRE_DATE
        ,a.OP_ID
        ,a.ORG_ID
        ,a.SO_NBR
        ,a.CUST_ID
        ,a.NOTES
        ,a.VPMN_ID
        ,a.EXT_FIELD1
        ,a.EXT_FIELD2
        ,a.EXT_FIELD3
        ,a.EXT_FIELD4
        ,a.EXT_FIELD5
        ,a.EXT_FIELD6
        ,a.EXT_FIELD7
        ,a.EXT_FIELD8
        ,a.EXT_FIELD9
        ,a.EXT_FIELD10
				from bass2.dwd_enterprise_msg_his_${timestamp} a,
        (select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_${timestamp}  group by enterprise_id) b
        where a.done_date = b.done_date 
        			and a.enterprise_id = b.enterprise_id and rec_status = 0 
        			and  a.enterprise_id in (select t.enterprise_id from bass1.g_a_01004_tmp10 t) 
				with ur 
				"
        puts $sql_buff
        exec_sql $sql_buff
        
        #������ʱ������(g_a_01004_tmp11)
	  #set sql_buff "\
	  #   create index  bass1.idx_01004tmp11_ei on bass1.g_a_01004_tmp11(enterprise_id) "               
    #puts $sql_buff      
    #exec_sql $sql_buff       


  exec db2 connect to bassdb user bass1 using bass1
  exec db2 runstats on table bass1.g_a_01004_tmp11 with distribution and detailed indexes all
  exec db2 terminate  
  
  
    #�����ʱ��(g_a_01004_tmp3)
        set sql_buff "alter table bass1.g_a_01004_tmp12 activate not logged initially with empty table"
        puts $sql_buff
        exec_sql $sql_buff    
#       ' ' as crt_chnl_id  --> 'BASS1_DS' as crt_chnl_id                                         
#    20090818  ֻȡ60���ַ�  ,a.group_name as enterprise_name
        set sql_buff "insert into bass1.g_a_01004_tmp12
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
                     ,case group_level
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
                     ,value(substr(e.cust_name,1,20),' ')    as  linkman_name
                     ,value(substr(e.office_phone,1,20),' ') as telephone_num
                     ,value(substr(e.link_phone,1,15),' ')   as mobile_num
                     ,value(substr(e.position,1,20),' ')     as linkman_title
                     ,value(e.fax,' ') as linkman_fax
                     ,coalesce(e.email,' ') as linkman_mail
                     ,value(e.post_address,' ') as linkman_addr
                     ,value(char(e.post_code),' ') as post_code
                     ,'11'
                     ,case when b.enterprise_id is null then '0' else '1' end
                     ,' ' as ind_res_schema
                     ,'BASS1_DS' as crt_chnl_id
                     ,' ' as enter_type_id
                   from  bass1.g_a_01004_tmp11   a 
                     left outer join (select distinct ENTERPRISE_ID
                                       from BASS2.DW_ENTERPRISE_ACCOUNT_${timestamp}
                                        where REC_STATUS=1
                                      ) b
                     on a.enterprise_id=b.enterprise_id
                     left join bass2.dwd_cust_msg_${timestamp} e on a.cust_id=e.cust_id
                     with ur 
									"
        puts $sql_buff
        exec_sql $sql_buff    			
        
				set sql_buff "insert into bass1.g_a_01004_day
        					 select distinct
							         TIME_ID
							        ,ENTERPRISE_ID
							        ,ENT_DEF_MODE
							        ,PRT_GRP_CUST_ID
							        ,ENTERPRISE_NAME
							        ,NET_ADDRESS
							        ,FAX_NO
							        ,ENT_SCALE_ID
							        ,MEMBER_NUMS
							        ,ENT_REGION_TYPE
							        ,ENT_INDUSTRY_ID
							        ,GRP_AREA_SPEC_ID
							        ,CMCC_ID
							        ,CREATE_DATE
							        ,TELEPHONE_NUM
							        ,MOBILE_NUM
							        ,LINKMAN_FAX
							        ,LINKMAN_MAIL
							        ,LINKMAN_ADDR
							        ,POST_CODE
							        ,CUST_STATU_TYP_ID
							        ,UNITE_PAY_FLAG
							        ,CRT_CHNL_ID
							        ,ENTER_TYPE_ID       
							        from  bass1.g_a_01004_tmp12		         				                    
                   except
                   select TIME_ID, ENTERPRISE_ID, ENT_DEF_MODE, PRT_GRP_CUST_ID, 
                       ENTERPRISE_NAME, NET_ADDRESS, FAX_NO, ENT_SCALE_ID, 
                       MEMBER_NUMS, ENT_REGION_TYPE, ENT_INDUSTRY_ID, GRP_AREA_SPEC_ID, 
                        CMCC_ID, CREATE_DATE, 
                       TELEPHONE_NUM, MOBILE_NUM,  LINKMAN_FAX, 
                       LINKMAN_MAIL, LINKMAN_ADDR, POST_CODE, CUST_STATU_TYP_ID, 
                       UNITE_PAY_FLAG,  CRT_CHNL_ID,enter_type_id
                       from bass1.g_a_01004_tmp4
                    with ur
                     "
    puts $sql_buff      
    exec_sql $sql_buff      

  #exec db2 connect to bassdb user bass1 using bass1
  #exec db2 runstats on table bass1.g_a_01004_day with distribution and detailed indexes all
  #exec db2 terminate  

  #����01004����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select enterprise_id,count(*) cnt from bass1.g_a_01004_day
	              where time_id =$timestamp
	             group by enterprise_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "01004�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }
















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
