######################################################################################################
#接口名称：用户状态
#接口编码：02008
#接口说明：用户的当前状态。新增用户状态的分类取值"9000 无效修正" 
#          是为了避免部分省数据修正过程中对一级经营分析系统相关指标<如"离网用户数">的严重负面影响。。
#程序名称: G_A_02008_DAY.tcl
#功能描述: 生成02008的数据
#运行粒度: 日
#源    表：1.bass2.dw_product_bass1_yyyymmdd
#          2.bass1.INT_02004_02008_YYYYMM
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：一个电话号码不能同时对应多个非离网的用户ID校验
#修改历史: add by zhanght on 2009.05.18
###20091126 用 dw_product_bass1_ 替换原来的用户表
#  20100120 修改在网用户口径userstatus_id in (1,2,3,6,8),包括数据卡用户sim_code='1'
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

	set sql_buff "\
	DELETE FROM $db_user.G_A_02008_DAY where time_id=$Timestamp"
	exec_sql $sql_buff
	##~   #temp process ! 20120519: 相同号码对应不同再网user_id!删除一条！
	##~  20120531  boss已经修复，可以把代码屏蔽
	##~   set sql_buff "
		##~   delete from (
		##~   select * from 
		##~   $db_user.INT_02004_02008_${op_month}
		##~   where
		##~   op_time=$Timestamp
		##~   and usertype_flag=1
		##~   and user_id = '89160002171967'
		##~   ) t						   
	##~   " 
	##~   exec_sql $sql_buff




	set sql_buff "insert into $db_user.G_A_02008_DAY
	            select
                         $Timestamp
                     	,user_id
                     	,usertype_id
                     	from
                     	(
                          select
                           user_id
                           ,usertype_id
                          from 
                           $db_user.INT_02004_02008_${op_month}
                          where
                           op_time=$Timestamp
                           and usertype_flag=1
                        )a"
        exec_sql $sql_buff





	
#一个电话号码不能同时对应多个非离网的用户ID校验	 add by zhanght on 2009.05.18
	
	
	set sql_buff "ALTER TABLE PRODUCT_XHX3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        exec_sql $sql_buff



#	set handle [aidb_open $conn]
#	set sql_buff "insert into  PRODUCT_XHX3
#  select a.product_no from 
#  (select a.user_id,a.product_no,usertype_id,sim_code from
#   (select time_id,user_id,product_no,usertype_id,sim_code from G_A_02004_DAY where time_id <= $Timestamp ) a,
#   (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$Timestamp 
#     group by user_id)b
#    where a.time_id=b.time_id and a.user_id=b.user_id 
#   )a,
#  (select a.user_id,usertype_id from
#   (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <= $Timestamp ) a,
#   (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<=$Timestamp 
#       group by user_id)b
#    where a.time_id=b.time_id and a.user_id=b.user_id
#   )b
# where a.user_id = b.user_id and a.usertype_id <> '3' and a.sim_code <> '1' and 
#       b.usertype_id not in ('2010','2020','2030','1021','9000')
# group by a.product_no
# having count(a.user_id) > 1 
# with ur"
 

	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        exec_sql $sql_buff

 
	set sql_buff "insert into CHECK_0200402008_DAY_1
select user_id,product_no,usertype_id,sim_code from
 (
 select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
 where time_id<=$Timestamp
 ) k
 where k.row_id=1 
 and k.usertype_id <> '3'
 with ur"
        exec_sql $sql_buff


	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        exec_sql $sql_buff


	set sql_buff "insert into CHECK_0200402008_DAY_2
   select user_id,usertype_id from
   (
   select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02008_DAY
   where time_id<=$Timestamp
   ) k
   where k.row_id=1 
   and k.usertype_id not in ('2010','2020','2030','9000') with ur"
        exec_sql $sql_buff


 
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        exec_sql $sql_buff


	set sql_buff "insert into CHECK_0200402008_DAY_4
	select a.product_no from CHECK_0200402008_DAY_1 a,
         CHECK_0200402008_DAY_2 b
         where a.user_id=b.user_id
  with ur       
	
	"
        exec_sql $sql_buff

	
	set sql_buff " 
 insert into  PRODUCT_XHX3
  select product_no
   from  CHECK_0200402008_DAY_4
   group by product_no
   having count(*) >=2 
   with ur"
         exec_sql $sql_buff


	set sql_buff "ALTER TABLE product_xhx4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        exec_sql $sql_buff

#	set handle [aidb_open $conn]
#	set sql_buff "insert into product_xhx4 select distinct user_id from G_A_02004_DAY where product_no in (select product_no from PRODUCT_XHX3) with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#	set handle [aidb_open $conn]
#	set sql_buff "delete from G_A_02008_DAY where user_id in (select distinct user_id from product_xhx4) with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#	set handle [aidb_open $conn]
#	set sql_buff "insert into G_A_02008_DAY select distinct $Timestamp,user_id,'2020' from G_A_02004_DAY where product_no in (select product_no from PRODUCT_XHX3) with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
	


	set sql_buff "insert into product_xhx4  select distinct k.user_id from
(
select user_id,row_number()over(partition by product_no order by int(create_date) desc) row_id from G_A_02004_DAY 
where product_no in (select product_no from PRODUCT_XHX3) 
) k
where k.row_id<>1 with ur"
        exec_sql $sql_buff

##为了更好地甄别哪个用户有效，暂不删除原来的状态。
##~   set sql_buff "delete from G_A_02008_DAY where user_id in (select distinct user_id from product_xhx4) and time_id = $Timestamp with ur"

        exec_sql $sql_buff
	set sql_buff "insert into G_A_02008_DAY select distinct $Timestamp,user_id,'2020' from G_A_02004_DAY where user_id in(select user_id from product_xhx4) with ur"
        exec_sql $sql_buff


####################################################################
#每天自动调整(用户未经过销户状态.直接置为0(历史离网) 这种情况在02008中无法处理 ,故在这里修正) 20091027
####################################################################

	#情况临时表
	set sql_buff " delete from BASS1.TEMP_CHECK_02008"
        exec_sql $sql_buff


	#全关联，存储用户状态和资料
	set sql_buff "
				insert into BASS1.TEMP_CHECK_02008
				select  value(char(a.user_id),b.user_id) user_id
				       ,b.usertype_id  bass1_usertype_id   
				       ,a.userstatus_id
				       ,a.usertype_id
				from bass2.dw_product_bass1_$Timestamp a
				full join (select user_id , usertype_id 
										from 	(select user_id , usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id   
				         					  from bass1.g_a_02008_day
				           					where time_id<=$Timestamp ) t
				           	where t.row_id=1 ) b on a.user_id=b.user_id
			"
        exec_sql $sql_buff



	#清空临时表2
	set sql_buff "DELETE FROM BASS1.TEMP_CHECK_02008_D WHERE TIME_ID = $Timestamp"
        exec_sql $sql_buff



	#一经状态是在网,二经状态已经离网或状态不存在的置为'2020'状态
	set sql_buff "
				insert into BASS1.TEMP_CHECK_02008_D
					select 
					 $Timestamp,
					 a.user_id,
					 '2020'
					from BASS1.TEMP_CHECK_02008 a
				 where bass1_usertype_id is not null 
					 and bass1_usertype_id NOT IN ('2010','2020','2030','9000') 
					 and (a.userstatus_id not in (1,2,3,6,8) or a.userstatus_id is null)
			"
        exec_sql $sql_buff




	#先删除02008接口表中将要修正的数据
	set sql_buff "
				delete from bass1.g_a_02008_day 
				where user_id in (select user_id from BASS1.TEMP_CHECK_02008_D where time_id=$Timestamp)
				  and time_id=$Timestamp
			"
        exec_sql $sql_buff



	#02008接口表中插入修正数据
	set sql_buff "
				insert into bass1.g_a_02008_day
				select time_id,user_id,usertype_id from BASS1.TEMP_CHECK_02008_D where time_id=$Timestamp
			"
 exec_sql $sql_buff

##----------------end------------------

	##~   set sql_buff "
##~   delete from (
##~   select * from                     
 ##~   bass1.g_a_02008_day
 ##~   where user_id = '89160000184970'
 ##~   and time_id = $Timestamp
##~   ) t 
	##~   "
 ##~   exec_sql $sql_buff

  ##~   #1.检查chkpkunique
  ##~   set tabname "G_S_22080_DAY"
        ##~   set pk                  "OP_TIME"
        ##~   chkpkunique ${tabname} ${pk} ${timestamp}	
	return 0
}
