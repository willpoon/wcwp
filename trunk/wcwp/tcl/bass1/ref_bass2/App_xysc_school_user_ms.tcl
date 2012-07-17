#****************************************************************************************
# ** 程序名称: App_xysc_school_user_ms.tcl
# ** 程序功能: 2011西藏移动校园市场专题-1 学生用户识别模型优化
# ** 运行粒度: 月
# ** 运行示例: ds App_xysc_school_user_ms_0927.tcl 2011-06-01
# ** 创建时间: 2011-8-8 17:55
# ** 创 建 人: 郑冬冬
# ** 问    题: 
# ** 备    注：1.2011年重点应用对校园用户识别进行了优化，考虑到原有程序庞大，因此将优化部分另外写程序处理。
#               但是后续又有很多程序以原有程序为前置，因此将原有模型程序名改为App_xysc_school_user_ms_1.tcl,
#               将新的优化程序命名为App_xysc_school_user_ms.tcl
# **           2.集中处理校园集团V网用户清单，将清单按照目标表格式插入；同时将持学生证注册的用户加入到表中；
#	**					   学生证注册用户和校园集团V网用户出现重复时，以校园集团V网用户的学校集团归属作为程序结果中的学校名称
# **           3.对异常用户进行剔除处理：包括全球通用户、网龄10年以上、ARPU值超过200、预提卡用户等
# ** 修改历史:
# **           修改日期      修改人      修改内容
# **           -----------------------------------------------
# **           2011-9-27     fuzl      对校园V网的用户归属学校进行处理
# ** Copyright(c) 2011 AsiaInfo Technologies (China), Inc.
# ** All Rights Reserved.
#****************************************************************************************

proc deal {p_optime p_timestamp} {

	global env
  #数据库连接句柄
	global conn   ;
  #数据库操作句柄
	global handle ;
  #创建数据操作句柄
	set handle [aidb_open $conn] ;

	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg  1000
	  	return -1
	}

	if { [exec_sql $p_optime $p_timestamp] != 0 } {
	  #关闭数据操作句柄
		aidb_close $handle ;
		#数据库回滚
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
		 #获取前一个月时间和前两个月时间用于提取用户ARPU 值
     set p_optime1                [ai_minusmonths $p_optime 1]
     set p_optime2                [ai_minusmonths $p_optime 2]
  	 scan   $p_optime1 "%04s-%02s-%02s" year1 month1 day2
		 scan   $p_optime1 "%04s-%02s-%02s" year2 month2 day2

     ##源表
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
     
     ##中间表
		 set dw_xysc_school_real_user_dt_tmp    	"$db_schema.dw_xysc_school_real_user_dt_tmp"
		 set dw_xysc_school_real_user_dt_tmp_0  	"$db_schema.dw_xysc_school_real_user_dt_tmp_0"
		 set dw_xysc_school_real_user_dt_tmp_1  	"$db_schema.dw_xysc_school_real_user_dt_tmp_1"
		 
     #模板表                                	
     set dw_xysc_school_real_user_dt_yyyymm 	"$db_schema.dw_xysc_school_real_user_dt_yyyymm"
     
     #目标表                                	
     set target_table                       	"$db_schema.dw_xysc_school_real_user_dt_$year$month"
     
##******************************************************************************************************************
## Step0. 创建中间表                             
##******************************************************************************************************************
     # 创建备份的表的格式
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
	  ##创建临时表,区分归属的学校是否为系统已经维护到的学校 
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
	  ##创建临时表,区分归属的学校是否为系统已经维护到的学校
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
## Step1.  将一些不符合学生特征的用户先放入临时表，例如全球通、ARPU值过大等条件的用户；
##*******************************************************************************************************************
    #新建临时表，用于存放需剔除的用户
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
     #1、加入全球通号码清单 brand_id=1
		 set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20 ( user_id
		                                                              ,product_no
		                                                              ,type
		                                                             )
		              select a.user_id
		                    ,a.product_no
		                    ,'全球通'
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
     #2、加入三月内平均ARPU值超过200的号码清单
		 set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20(user_id
																																,product_no
																																,type)
								select user_id
								        ,product_no
								        ,'ARPU值过高'
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
    #3、加入年龄小于12，大于35   0,100 纳入学生范围内
	  set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20 (user_id
																																	,product_no
																																	,type
																																 )
								select a.user_id
								 				,a.product_no
								 				,'年龄异常'
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
    #4、网龄10年以上
	  set sql_buf "insert into bass2.xysc_school_real_user_2_tmp20 (user_id
	  																															,product_no
	  																															,type
																																	)
								select a.user_id
											 ,a.product_no
											 ,'网龄10年以上'
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
#5、筛选预提卡清单  --参照stat_market_0126.tcl中的预提卡目标客户提取口径
	  set  sql_buf " insert into  bass2.xysc_school_real_user_2_tmp20 (user_id
	  																																 ,product_no
	  																																 ,type
																																		)
        					select a.user_id
        								,a.product_no
        								,'预提卡用户'
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
## Step2.  将一些不符合学生特征的用户从结果表中剔除
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
## Step3.  将校园V网用户和以学生证入网的用户于已有学生用户合并，作为新的用户集
##*******************************************************************************************************************
   #1. 添加集团V网用户和用学生证注册的用户。 注：存在即为V网用户又是学生证入网的情况，以V往为准
     #1.1  对校园V网归属的学校打标识，未维护的为1,已经维护的学校为0
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
        		 where enterprise_name like '%学校%' or	enterprise_name like '%学院%' or enterprise_name like '%大学%'
        		) c on c.enterprise_id=b.enterprise_id
        	left join 
        	  (select distinct school_name from $dim_xysc_lac_cell_info)	d on c.enterprise_name=d.school_name"
		 puts $sql_buf
		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
	       trace_sql $errmsg 1001
	       puts "errmsg:$errmsg  "
	       return -1
	      }
	   #1.2 对学生证识别出的用户打标识，同上  
	   #注：存在既为V网用户又是学生证入网的情况，以V往为准       	 
		 set sql_buf "insert into $dw_xysc_school_real_user_dt_tmp_0  (user_id
		 																														  ,product_no
		 																														  ,school_name
		 																														  ,phone_type
		 																														  ,mark
		 																														  )
		  	select a.user_id
		  	      ,a.product_no
		  	      ,'未知'
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
   #2. 对未归属到学校的用户，通过在学校基站小区的通话情况进行校园的唯一归属
     #2.1将用户在各个学校的通话情况进行汇总      
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
     #2.2 将用户归属到唯一的校区      
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
     #2.3 将用户归属到唯一的校区      
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
## Step4.  将数据插入到结果表
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
## Step5.  将结果表中非在网的用户剔除
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



