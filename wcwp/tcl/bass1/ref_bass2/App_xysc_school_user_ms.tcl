#****************************************************************************************
# ** ��������: App_xysc_school_user_ms.tcl
# ** ������: 2011�����ƶ�У԰�г�ר��-1 ѧ���û�ʶ��ģ���Ż�
# ** ��������: ��
# ** ����ʾ��: ds App_xysc_school_user_ms_0927.tcl 2011-06-01
# ** ����ʱ��: 2011-8-8 17:55
# ** �� �� ��: ֣����
# ** ��    ��: 
# ** ��    ע��1.2011���ص�Ӧ�ö�У԰�û�ʶ��������Ż������ǵ�ԭ�г����Ӵ���˽��Ż���������д������
#               ���Ǻ������кܶ������ԭ�г���Ϊǰ�ã���˽�ԭ��ģ�ͳ�������ΪApp_xysc_school_user_ms_1.tcl,
#               ���µ��Ż���������ΪApp_xysc_school_user_ms.tcl
# **           2.���д���У԰����V���û��嵥�����嵥����Ŀ����ʽ���룻ͬʱ����ѧ��֤ע����û����뵽���У�
#	**					   ѧ��֤ע���û���У԰����V���û������ظ�ʱ����У԰����V���û���ѧУ���Ź�����Ϊ�������е�ѧУ����
# **           3.���쳣�û������޳���������ȫ��ͨ�û�������10�����ϡ�ARPUֵ����200��Ԥ�Ῠ�û���
# ** �޸���ʷ:
# **           �޸�����      �޸���      �޸�����
# **           -----------------------------------------------
# **           2011-9-27     fuzl      ��У԰V�����û�����ѧУ���д���
# ** Copyright(c) 2011 AsiaInfo Technologies (China), Inc.
# ** All Rights Reserved.
#****************************************************************************************

proc deal {p_optime p_timestamp} {

	global env
  #���ݿ����Ӿ��
	global conn   ;
  #���ݿ�������
	global handle ;
  #�������ݲ������
	set handle [aidb_open $conn] ;

	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg  1000
	  	return -1
	}

	if { [exec_sql $p_optime $p_timestamp] != 0 } {
	  #�ر����ݲ������
		aidb_close $handle ;
		#���ݿ�ع�
		aidb_roll $conn    ;
		return -1
	}

  aidb_commit $conn
	aidb_close $handle

	return 0
}

proc exec_sql {p_optime p_timestamp} {
 	   global env
		 global conn
		 global handle

     set db_schema  "bass2"
 	   set date_optime	[ai_to_date $p_optime]
		 scan $p_optime "%04s-%02s-%02s" year month day
		 set  db_schema  "bass2"
		 #��ȡǰһ����ʱ���ǰ������ʱ��������ȡ�û�ARPU ֵ
     set p_optime1                [ai_minusmonths $p_optime 1]
     set p_optime2                [ai_minusmonths $p_optime 2]
  	 scan   $p_optime1 "%04s-%02s-%02s" year1 month1 day2
		 scan   $p_optime1 "%04s-%02s-%02s" year2 month2 day2

     ##Դ��
		 set dw_vpmn_member_yyyymm   							"$db_schema.dw_vpmn_member_$year$month"
		 set dw_enterprise_msg_yyyymm							"$db_schema.dw_enterprise_msg_$year$month"
		 set dw_enterprise_vpmn_rela_yyyymm 			"$db_schema.dw_enterprise_vpmn_rela_$year$month"
	   set dw_product_yyyymm       							"$db_schema.dw_product_$year$month";
		 set dw_cust_yyyymm          							"$db_schema.dw_cust_$year$month"
		 set dw_product_yyyymm1      							"$db_schema.dw_product_$year1$month1"
		 set dw_product_yyyymm2      							"$db_schema.dw_product_$year2$month2"
     set dw_res_msisdn_yyyymm    							"$db_schema.dw_res_msisdn_$year$month"
     set dim_xysc_lac_cell_info               "$db_schema.dim_xysc_lac_cell_info"
     set dw_call_cell_yyyymm                  "$db_schema.dw_call_cell_$year$month"
     
     ##�м��
		 set dw_xysc_school_real_user_dt_tmp    	"$db_schema.dw_xysc_school_real_user_dt_tmp"
		 set dw_xysc_school_real_user_dt_tmp_0  	"$db_schema.dw_xysc_school_real_user_dt_tmp_0"
		 set dw_xysc_school_real_user_dt_tmp_1  	"$db_schema.dw_xysc_school_real_user_dt_tmp_1"
		 
     #ģ���                                	
     set dw_xysc_school_real_user_dt_yyyymm 	"$db_schema.dw_xysc_school_real_user_dt_yyyymm"
     
     #Ŀ���                                	
     set target_table                       	"$db_schema.dw_xysc_school_real_user_dt_$year$month"
     
##******************************************************************************************************************
## Step0. �����м��                             
##******************************************************************************************************************
     # �������ݵı�ĸ�ʽ
	   set sql_buf "drop table $dw_xysc_school_real_user_dt_tmp"
     puts $sql_buf
  	 if [catch { aidb_sql $handle $sql_buf } errmsg ] {
		 		puts $errmsg
		 }
	   set sql_buf "create table $dw_xysc_school_real_user_dt_tmp  like bass2.dw_xysc_school_real_user_dt_yyyymm in TBS_BASS_MINER
									index in TBS_INDEX partitioning key (user_id,product_no) using hashing not logged initially"
		 puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	trace_sql $errmsg 0001
	      	puts "errmsg:$errmsg  "
	      	return -1
	      }
	  ##������ʱ��,���ֹ�����ѧУ�Ƿ�Ϊϵͳ�Ѿ�ά������ѧУ 
	   set sql_buf "drop table $dw_xysc_school_real_user_dt_tmp_0"
     puts $sql_buf
  	 if [catch { aidb_sql $handle $sql_buf } errmsg ] {
		 		puts $errmsg
		 }  
 	   set sql_buf "CREATE TABLE bass2.dw_xysc_school_real_user_dt_tmp_0 (
			    user_id      varchar(20),                                      
          product_no   varchar(15),                                      
          school_name  varchar(128),                                     
          phone_type   varchar(2),                                       
          mark         smallint                                                            
         )                                                                 
          data capture none in tbs_bass_miner                                               
          index in tbs_index                                              
          partitioning key                                                 
         (user_id,product_no                                                     
          ) using hashing"
     puts $sql_buf
	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	   trace_sql $errmsg 1300
	   	   puts "errmsg:$errmsg"
	   	   return -1
	   }
	  ##������ʱ��,���ֹ�����ѧУ�Ƿ�Ϊϵͳ�Ѿ�ά������ѧУ
	   set sql_buf "drop table $dw_xysc_school_real_user_dt_tmp_1"
     puts $sql_buf
  	 if [catch { aidb_sql $handle $sql_buf } errmsg ] {
		 		puts $errmsg
		 }   
 	   set sql_buf "CREATE TABLE bass2.dw_xysc_school_real_user_dt_tmp_1 (
			    user_id        varchar(20) ,                                      
          product_no     varchar(15) ,                                      
          school_name	   varchar(128),  
          call_counts	   integer     ,
          call_duration  integer                                                        
         )                                                                 
          data capture none in tbs_bass_miner                                               
          index in tbs_index                                              
          partitioning key                                                 
         (user_id,product_no                                                     
          ) using hashing"
     puts $sql_buf
	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	   trace_sql $errmsg 1300
	   	   puts "errmsg:$errmsg"
	   	   return -1
	   }	   
	   
##*******************************************************************************************************************
## Step1.  ��һЩ������ѧ���������û��ȷ�����ʱ������ȫ��ͨ��ARPUֵ������������û���
##*******************************************************************************************************************
    #�½���ʱ�����ڴ�����޳����û�
		 set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp20"
		 puts $sql_buf
	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg "
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp20 (
			         user_id		  	varchar(20)
			         ,product_no	  varchar(20)
			         ,type          varchar(50)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "
		 puts $sql_buf
	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	   trace_sql $errmsg 1002
	   	   puts "errmsg:$errmsg"
	   	   return -1
	   }
     #1������ȫ��ͨ�����嵥 brand_id=1
		 set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20 ( user_id
		                                                              ,product_no
		                                                              ,type
		                                                             )
		              select a.user_id
		                    ,a.product_no
		                    ,'ȫ��ͨ'
			            from
			                $target_table a
			             inner join
			              	(select user_id
			                 from $dw_product_yyyymm b
			                 where b.crm_brand_id1=1 and b.userstatus_id in (1,2,3,6,8) and b.usertype_id  in (1,2,9)
			                ) b on  b.user_id=a.user_id
		             where a.phone_type='S'"
		 puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	   trace_sql $errmsg 1003
	   	   puts "errmsg:$errmsg"
	   	   return -1
	   }
     #2������������ƽ��ARPUֵ����200�ĺ����嵥
		 set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20(user_id
																																,product_no
																																,type)
								select user_id
								        ,product_no
								        ,'ARPUֵ����'
							  from (select  a.user_id ,
							   							a.product_no,
								   						(b.fact_fee+c.fact_fee+d.fact_fee)/3 as fee_avg
 							        from $target_table a
 							          inner join
 							             $dw_product_yyyymm b on a.user_id=b.user_id
	 		  			          inner  join
	 		  				           $dw_product_yyyymm1 c on a.user_id=c.user_id
	 		  				        inner join
	 		  				           $dw_product_yyyymm2 d on a.user_id=d.user_id
	 		  				      where a.phone_type='S' and b.userstatus_id in (1,2,3,6,8) and b.usertype_id  in (1,2,9)
	 		  				) a
	 		  				where a.fee_avg>=200"
		puts $sql_buf
	  if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	  trace_sql $errmsg 1004
	   	  puts "errmsg:$errmsg"
	   	  return -1
	  }
    #3����������С��12������35   0,100 ����ѧ����Χ��
	  set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20 (user_id
																																	,product_no
																																	,type
																																 )
								select a.user_id
								 				,a.product_no
								 				,'�����쳣'
			 					from $target_table a,$dw_product_yyyymm b
			 					where a.user_id=b.user_id
			 					 		  and b.userstatus_id in (1,2,3,6,8)
			 					 		  and b.usertype_id in (1,2,9)
			                and (b.age<>0 and (b.age<12 or b.age>35) and b.age<>100)
			                and a.phone_type='S' "
	  puts $sql_buf
	  if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	  trace_sql $errmsg 1005
	   	  puts "errmsg:$errmsg"
	   	  return -1
	  }
    #4������10������
	  set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20 (user_id
	  																															,product_no
	  																															,type
																																	)
								select a.user_id
											 ,a.product_no
											 ,'����10������'
			          from $target_table  a
			          	inner join
			          		 $dw_product_yyyymm b on b.user_id=a.user_id
			         where  b.user_online_id>=22 
			               and b.userstatus_id in (1,2,3,6,8)
			               and b.usertype_id in (1,2,9)
			               and a.phone_type='S'"
		puts $sql_buf
	  if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	  trace_sql $errmsg 1006
	   	  puts "errmsg:$errmsg"
	   	  return -1
	  }
#5��ɸѡԤ�Ῠ�嵥  --����stat_market_0126.tcl�е�Ԥ�ῨĿ��ͻ���ȡ�ھ�
	  set  sql_buf " insert into  bass2.xysc_school_real_user_2_tmp20 (user_id
	  																																 ,product_no
	  																																 ,type
																																		)
        					select a.user_id
        								,a.product_no
        								,'Ԥ�Ῠ�û�'
        					from $target_table a
        						inner join
        						  (select b.user_id
        						   from  $dw_res_msisdn_yyyymm c
        						  	 inner join
        						  		   $dw_product_yyyymm b on b.product_no=c.product_no
										  where  b.userstatus_id in (1,2,3,6,8) and	b.test_mark <> 1
        						  		  and  b.usertype_id in (1,2,9)and c.purpose=3
               			  ) d  on a.user_id=d.user_id
               	where a.phone_type='S' "

		puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1007
        puts "errmsg:$errmsg"
        return -1
    }

##*******************************************************************************************************************
## Step2.  ��һЩ������ѧ���������û��ӽ�������޳�
##*******************************************************************************************************************

	  set  sql_buf "delete from $target_table a
	  							where exists (select user_id from (select distinct user_id from bass2.xysc_school_real_user_2_tmp20) b where a.user_id=b.user_id )"

    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	   trace_sql $errmsg 1008
	   	   puts "errmsg:$errmsg"
	   	   return -1
	  }  

##*******************************************************************************************************************
## Step3.  ��У԰V���û�����ѧ��֤�������û�������ѧ���û��ϲ�����Ϊ�µ��û���
##*******************************************************************************************************************
   #1. ��Ӽ���V���û�����ѧ��֤ע����û��� ע�����ڼ�ΪV���û�����ѧ��֤�������������V��Ϊ׼
     #1.1  ��У԰V��������ѧУ���ʶ��δά����Ϊ1,�Ѿ�ά����ѧУΪ0
		 set sql_buf "insert into $dw_xysc_school_real_user_dt_tmp_0  (user_id
		 																														  ,product_no
		 																														  ,school_name
		 																														  ,phone_type
		 																														  ,mark
		 																														  )
		  	select a.user_id
              ,a.product_no
              ,coalesce(d.school_name,c.enterprise_name)
              ,'S' AS phone_type
              ,case when d.school_name is null then 1 else 0 end  mark
        from $dw_vpmn_member_yyyymm a
          inner join
        		 $dw_enterprise_vpmn_rela_yyyymm b on a.vpmn_id =b.vpmn_id
          inner join
        		(select enterprise_id,enterprise_name
        		 from $dw_enterprise_msg_yyyymm
        		 where enterprise_name like '%ѧУ%' or	enterprise_name like '%ѧԺ%' or enterprise_name like '%��ѧ%'
        		) c on c.enterprise_id=b.enterprise_id
        	left join 
        	  (select distinct school_name from $dim_xysc_lac_cell_info)	d on c.enterprise_name=d.school_name"
		 puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }
	   #1.2 ��ѧ��֤ʶ������û����ʶ��ͬ��  
	   #ע�����ڼ�ΪV���û�����ѧ��֤�������������V��Ϊ׼       	 
		 set sql_buf "insert into $dw_xysc_school_real_user_dt_tmp_0  (user_id
		 																														  ,product_no
		 																														  ,school_name
		 																														  ,phone_type
		 																														  ,mark
		 																														  )
		  	select a.user_id
		  	      ,a.product_no
		  	      ,'δ֪'
		  	      ,'S' AS phone_type
		  	      ,1
		  	from  $dw_product_yyyymm a,
		  				$dw_cust_yyyymm  b
		  	where a.cust_id=b.cust_id  and b.iden_id=4 
		  	      and not exists (select * from  $dw_xysc_school_real_user_dt_tmp_0 b where a.user_id=b.user_id ) "
	   puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }
   #2. ��δ������ѧУ���û���ͨ����ѧУ��վС����ͨ���������У԰��Ψһ����
     #2.1���û��ڸ���ѧУ��ͨ��������л���      
		 set sql_buf "insert into $dw_xysc_school_real_user_dt_tmp_1 (  user_id      
                                                                   ,product_no   	   
                                                                   ,school_name	 
                                                                   ,call_counts	 
                                                                   ,call_duration
		 																														  )
		  	select a.user_id
		  	      ,a.product_no
		  	      ,c.school_name
		  	      ,sum(b.call_counts     )
		  	      ,sum(b.call_duration )
        from  bass2.dw_xysc_school_real_user_dt_tmp_0 a
          left join 
              (select user_id
                     ,lac_id
                     ,cell_id
                     ,sum(call_counts) call_counts
                     ,sum(call_duration_m) call_duration
               from $dw_call_cell_yyyymm b 
               group by user_id,lac_id,cell_id
               ) b  on a.user_id=b.user_id
          left join 
             (select distinct lac_id,cell_id,school_name,over_flag 
              from $dim_xysc_lac_cell_info
        	    )c on b.lac_id=c.lac_id and b.cell_id=c.cell_id
        where a.mark=1  and  c.school_name is not null 
        group by a.user_id     
                ,a.product_no 
                ,c.school_name"
	   puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }
     #2.2 ���û�������Ψһ��У��      
		 set sql_buf "insert into $dw_xysc_school_real_user_dt_tmp (  user_id      
                                                                  ,product_no   	   
                                                                  ,school_name  
                                                                  ,phone_type   
		 																														 )
		  	select a.user_id
		  	      ,a.product_no
		  	      ,a.school_name
		  	      ,'S'
        from  (select a.user_id      
                     ,a.product_no   	   
                     ,a.school_name	 
                     ,row_number() over (partition by a.user_id order by a.call_counts desc,a.call_duration desc) as row_rank
               from $dw_xysc_school_real_user_dt_tmp_1 a                   
               ) a
        where a.row_rank=1"
	   puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }	      
     #2.3 ���û�������Ψһ��У��      
		 set sql_buf "insert into $dw_xysc_school_real_user_dt_tmp (  user_id      
                                                                  ,product_no   	   
                                                                  ,school_name  
                                                                  ,phone_type   
		 																														 )
		  	select a.user_id
		  	      ,a.product_no
		  	      ,a.school_name
		  	      ,'S'
        from $dw_xysc_school_real_user_dt_tmp_0 a
        where mark=0"
	   puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg"
	       return -1
	      }	      
##*******************************************************************************************************************
## Step4.  �����ݲ��뵽�����
##*******************************************************************************************************************

		 set sql_buf "insert into $target_table (user_id
		 																				,product_no
		 																				,school_name
		 																				,phone_type
		 																				)
		  	select   user_id
                ,product_no
                ,school_name
                ,phone_type
		  	from  $dw_xysc_school_real_user_dt_tmp a
		    where  not exists (select user_id from $target_table b where a.user_id=b.user_id)"
	   puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }

##*******************************************************************************************************************
## Step5.  ��������з��������û��޳�
##*******************************************************************************************************************
		 set sql_buf "delete from $target_table a
		              where not exists (select 1 from  $dw_product_yyyymm b where a.user_id=b.user_id and b.userstatus_id in (1,2,3,6,8) and b.usertype_id in (1,2,9)) "
	   puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }
	
    return 0
}



