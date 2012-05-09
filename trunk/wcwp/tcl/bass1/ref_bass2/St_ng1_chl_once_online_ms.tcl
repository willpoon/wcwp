#****************************************************************************************
# ** 程序名称: St_ng1_chl_once_online_ms.tcl
# ** 程序功能: 渠道客户重入网分析相关指标
# ** 运行粒度: 月
# ** 运行示例: ngds St_ng1_chl_once_online_ms.tcl 2009-11-01                           
# ** 创建时间: 2009-11-12 11:21:44
# ** 创 建 人: LiChunCai
# 
#
#****************************************************************************************
proc deal {p_optime p_timestamp} {

 	   global conn
 	   global handle

 	   if [catch {set handle [aidb_open $conn]} errmsg] {
 	   	trace_sql $errmsg 1000
 	   	return -1
 	   }

 	   if {[St_ng1_chl_once_online $p_optime $p_timestamp ]!= 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }

 	   aidb_commit $conn
 	   aidb_close $handle

 	   return 0
 }

proc St_ng1_chl_once_online { p_optime p_timestamp } {
	global conn
	global handle
	global env

	#获取数据库用户
	set DB2_USER         "bass2"
	set TBS_USER_TEMP    "tbs_user_temp"
	#获取数据月份yyyymm
	set op_month [ string range $p_optime 0 3 ][ string range $p_optime 5 6 ]
	#获取  年 月 日
	scan   $p_optime      "%04s-%02s-%02s" year month day 
	
	##############################################################################################
	#获取数据月份上月当日yyyy-mm-dd
	#set ls_p_optime [ clock format [ clock scan "${p_optime} - 1 months" ] -format "%Y-%m-%d" ]
	#获取数据月份上月yyyymm
	#set ls_op_month [ string range $ls_p_optime 0 3 ][ string range $ls_p_optime 5 6 ]
	#获取数据日期yyyymmdd
	#set op_date [ string range $p_optime 0 3 ][ string range $p_optime 5 6 ][ string range $p_optime 8 9 ]
	#获取数据月份1号yyyy-mm-01
	#set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	#获取数据月份下月1号yyyy-mm-01
	#set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]
	#获取数据月份上月1号yyyy-mm-01
	#set ls_month_01 [ clock format [ clock scan "${op_month_01} - 1 months" ] -format "%Y-%m-01" ]
	#获取数据月份本月末日yyyy-mm-31
	#set ls_month_day [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]
	#获取数据日期dd
	#set day [ string range $p_optime 8 9 ]
	################################################################################################
	#源表
	set dw_channel_msg_yyyymm          "dw_channel_msg_${op_month}"
	set dw_product_yyyymm              "dw_product_$year$month"
	set dw_cust_yyyymm                 "dw_cust_$year$month"
	#定义目标表
	set St_ng1_chl_once_online_yyyymm  "${DB2_USER}.St_ng1_chl_once_online_$year$month"
# #===============================================================================================
# # step 1: 建立临时结果表
# #===============================================================================================
	set sql_buf "
	declare global temporary table session.St_ng1_chl_once_online_tmp
	(
  op_time                  date,         
  city_id                  varchar(7),   
  county_id                varchar(20),   
  brand_id                 smallint,     
  position_type            smallint,     
  channel_id               varchar(20),  
  channel_name             varchar(50),  
  channel_sort             smallint,     
  channel_type_id          smallint,     
  object_work_model        smallint,     
  flag_id                  smallint,     
  excl_flag                smallint,    
  cust_type_id             smallint,     
  cust_class_id            smallint,     
  renet_id                 smallint,     
  cust_new_nums            integer,       
  renet_user_nums          integer       
                                     
  
	) 
	with replace on commit preserve rows not logged
	in ${TBS_USER_TEMP}
	"
  
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		trace_sql $errmsg 10010
		puts "errmsg:$errmsg"
		return -1
	} 


# #===============================================================================================
# # step 2: 取有关数据放入临时结果表
# #===============================================================================================

	set sql_buf "
	insert into session.St_ng1_chl_once_online_tmp
	(
   op_time                  ,              
   city_id                  ,              
   county_id                ,              
   brand_id                 ,              
   position_type            ,              
   channel_id               ,              
   channel_name             ,              
   channel_sort             ,              
   channel_type_id          ,              
   object_work_model        ,              
   flag_id                  ,              
   excl_flag                ,              
   cust_type_id             ,              
   cust_class_id            ,              
   renet_id                 ,              
   cust_new_nums            ,              
   renet_user_nums                        
                                                       
  )
  select 
 date('$p_optime') ,
 a.city_id,
 a.county_id,
 a.brand_id,
  case 
	 	  when b.position_type_id=1 or b.position_type_id=2 then 3
	 		when b.position_type_id=4 or b.position_type_id=5 then 4 
	 		when b.position_type_id=5 then 5
			when b.position_type_id=6 then 6				   
      else -1
 end  ,
 char(e.channel_id),
 e.channel_name,
 case when e.channel_type =1 then 1         
      when e.channel_type in(2,3,7) then 2  
      when e.channel_type in(5,6) then 3    
      when e.channel_type =4 then 4         
 end ,                                       
case
			when b.sub_channel_type_id=90154 then 102
			when b.sub_channel_type_id=90155 then 103
			when b.sub_channel_type_id=90153 then 105
			else 101
 end,
b.operate_mode_id ,
b.join_boss_mark  ,
b.sole_mark ,
a.custtype_id,
a.custclass_id,
case when c.iden_nbr_nums is null then 0
     when c.iden_nbr_nums=2 then 1
     when c.iden_nbr_nums=2 then 2
     when c.iden_nbr_nums=3 then 3
     when c.iden_nbr_nums>=4 and c.iden_nbr_nums<6 then 4
     when c.iden_nbr_nums>=4 and c.iden_nbr_nums<6 then 5
     when c.iden_nbr_nums>=6 and c.iden_nbr_nums<10 then 6
     when c.iden_nbr_nums>=10 and c.iden_nbr_nums<25 then 7
     when c.iden_nbr_nums>=25  then 8
     else -1
 end,

 count(distinct a.user_id),
 count(distinct c.cust_id)
     
from
	 (select * from  $dw_product_yyyymm a where a.month_new_mark=1 and a.test_mark=0 and a.free_mark=0) a
	 left join $dw_channel_msg_yyyymm b on a.channel_id=b.channel_id 
	 left join  ( select k.cust_id,j.iden_nbr,j.iden_nbr_nums from $dw_cust_yyyymm k
	 join (select iden_nbr,count(*) iden_nbr_nums from $dw_cust_yyyymm group by iden_nbr having count(*)>1) j
	 on k.iden_nbr=j.iden_nbr) c on a.cust_id=c.cust_id
	 join dim_pub_channel_ext_ng1 e on a.channel_id=e.channel_id 
	 group by
	 date('$p_optime') ,
 a.city_id,
 a.county_id,
 a.brand_id,
  case 
	 	  when b.position_type_id=1 or b.position_type_id=2 then 3
	 		when b.position_type_id=4 or b.position_type_id=5 then 4 
	 		when b.position_type_id=5 then 5
			when b.position_type_id=6 then 6				   
      else -1
 end  ,
 char(e.channel_id),
 e.channel_name,
 case when e.channel_type =1 then 1         
      when e.channel_type in(2,3,7) then 2  
      when e.channel_type in(5,6) then 3    
      when e.channel_type =4 then 4         
 end  ,                                       
case
			when b.sub_channel_type_id=90154 then 102
			when b.sub_channel_type_id=90155 then 103
			when b.sub_channel_type_id=90153 then 105
			else 101
 end ,
b.operate_mode_id ,
b.join_boss_mark  ,
b.sole_mark ,
a.custtype_id,
a.custclass_id,
case when c.iden_nbr_nums is null then 0
     when c.iden_nbr_nums=2 then 1
     when c.iden_nbr_nums=2 then 2
     when c.iden_nbr_nums=3 then 3
     when c.iden_nbr_nums>=4 and c.iden_nbr_nums<6 then 4
     when c.iden_nbr_nums>=4 and c.iden_nbr_nums<6 then 5
     when c.iden_nbr_nums>=6 and c.iden_nbr_nums<10 then 6
     when c.iden_nbr_nums>=10 and c.iden_nbr_nums<25 then 7
     when c.iden_nbr_nums>=25  then 8
     else -1
 end
  
  "
	
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		trace_sql $errmsg 10080
		puts "errmsg:$errmsg"
		return -1
	}
	
	

# #===============================================================================================
# # step 6: 删除目标表
# #===============================================================================================
	set sql_buf  "
	drop table ${St_ng1_chl_once_online_yyyymm}
	"
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		puts "errmsg:$errmsg"
		
	}
	aidb_commit $conn

# #===============================================================================================
# # step 7: 创建目标表
# #===============================================================================================
    set sql_buf "
	create table ${St_ng1_chl_once_online_yyyymm} like St_ng1_chl_once_online_yyyymm
	in tbs_cdr_data
  index in tbs_index
  partitioning key (op_time,city_id) using hashing
  not logged initially
                    "
	puts ${sql_buf}
	
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		trace_sql $errmsg 10140
		puts "errmsg:$errmsg"
		return -1
	}
	aidb_commit $conn
         





# #===============================================================================================
# # step 9: 往结果表插入数据
# #===============================================================================================
	set sql_buf "
	insert into ${St_ng1_chl_once_online_yyyymm}
	(
  op_time                  ,              
   city_id                  ,              
   county_id                ,              
   brand_id                 ,              
   position_type            ,              
   channel_id               ,              
   channel_name             ,              
   channel_sort             ,              
   channel_type_id          ,              
   object_work_model        ,              
   flag_id                  ,              
   excl_flag                ,              
   cust_type_id             ,              
   cust_class_id            ,              
   renet_id                 ,              
   cust_new_nums            ,              
   renet_user_nums            
                          
  ) select 
   op_time                  ,              
   city_id                  ,              
   county_id                ,              
   brand_id                 ,              
   position_type            ,              
   channel_id               ,              
   channel_name             ,              
   channel_sort             ,              
   channel_type_id          ,              
  value(object_work_model,-1)        ,              
   value(flag_id,1)                  ,              
   value(excl_flag,-1)               ,              
   cust_type_id             ,              
   cust_class_id            ,              
   renet_id                 ,              
   sum(cust_new_nums )      ,              
   sum(renet_user_nums)                from session.St_ng1_chl_once_online_tmp where renet_id<>8
   group by
   op_time                  ,              
   city_id                  ,              
   county_id                ,              
   brand_id                 ,              
   position_type            ,              
   channel_id               ,              
   channel_name             ,              
   channel_sort             ,              
   channel_type_id          ,              
   object_work_model        ,              
   flag_id                  ,              
   excl_flag                ,              
   cust_type_id             ,              
   cust_class_id            ,              
   renet_id                      
  
	"
	puts ${sql_buf}
	if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
		trace_sql $errmsg 10150
		puts "errmsg:$errmsg"
		return -1
	}
	aidb_commit $conn
	 
	return 0
}