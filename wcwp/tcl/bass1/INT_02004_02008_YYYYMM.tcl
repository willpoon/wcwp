######################################################################################################
#接口名称：
#接口编码：
#接口说明：
#程序名称: INT_02004_02008_YYYYMM.tcl
#功能描述: 生成02004,02008的增量数据的user_id
#运行粒度: 日
#源    表：1.bass2.dw_product_yyyymmdd
#          2.bass1.g_a_02004_day
#          3.bass1.G_A_02008_DAY
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 去掉“冷冻期”，修改“停机保号”口径为 userstatus_id=9 。修改时间：2009-05-31 修改人：zhanght
###20091126 用 dw_product_bass1_ 替换原来的用户表
#  20100125 修改在网用户口径userstatus_id in (1,2,3,6,8)
#  20100128 修改口径 when userstatus_id=9 then '1033'为 when userstatus_id=9 then '2030' 归为冷冻期
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.INT_02004_02008_${op_month} where op_time=$Timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
     
        #----建立临时表-----#
        set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.int_02004_02008
                     (
                      user_id     varchar(20) not null , 
                      brand_id    varchar(1) not null ,
                      usertype_id varchar(4) not null, 
                      brand_flag int not null,
                      usertype_flag int not null				
                      )
                      partitioning key 
                      (user_id)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
        puts "建临时表结束"



        #--插入02004的增量数据
        set handle [aidb_open $conn]

	set sql_buff "Insert into session.int_02004_02008
                   select
                     user_id
                     ,brand_id
                     ,'0000'
                     ,1
                     ,0
                   from
                     (
                       select
                         user_id
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2') as brand_id
                         ,case
                            when test_mark=1 then '3'
                            when free_mark=1 then '2'
                            else '1'
                          end
                       from 
                         bass2.dw_product_bass1_${Timestamp} 
                       where 
                         userstatus_id in (1,2,3,6,8)
                         and usertype_id in (1,2,9)
                       except
                       select a.user_id,a.brand_id,usertype_id from $db_user.g_a_02004_day a,
                           (
                             select max(time_id) as time_id,user_id from $db_user.g_a_02004_day 
                             where time_id<$Timestamp 
                             group by user_id
                            )b
                            where a.user_id = b.user_id and a.time_id=b.time_id
                     )aa"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	puts "插入02004结束"
	
	#---插入02008的增量数据
	set handle [aidb_open $conn]        
	set sql_buff "Insert into session.int_02004_02008
                    select
                     user_id
                     ,'0'
                     ,usertype_id
                     ,0
                     ,1
                     from
                     (
                       select
                         user_id
                         ,case 
                            when userstatus_id=1 and stopstatus_id=0 then '1010'
                            when userstatus_id=1 and stopstatus_id between 11 and 16 then '1031'
                            when userstatus_id=1 and stopstatus_id in (28,29) then '1032'
                            when userstatus_id=9 then '2030'
                            when userstatus_id=1 and stopstatus_id not in (0,11,12,13,14,15,16,28,29) then '1039' 
                            when userstatus_id in (3,6) then '1022'
                            when userstatus_id in (2)   then '1021'
                            when userstatus_id=4 then '2010'
                            when userstatus_id=8 then '1040'
                            when userstatus_id in (5,7) then '2020' 
                            else '1039' 
                     	   end as usertype_id
                         from 
                           bass2.dw_product_bass1_${Timestamp} 
                         where 
                           userstatus_id<>0
                           and usertype_id in (1,2,9)
                       except
                        select a.user_id,a.usertype_id from $db_user.G_A_02008_DAY a,
                           (select max(time_id) as time_id,user_id from $db_user.G_A_02008_DAY 
                            where time_id<$Timestamp 
                            group by user_id
                            )b
                           where a.user_id = b.user_id and a.time_id=b.time_id
                       )aa"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	puts "插入02008结束"
	
	#---汇总数据----#
	set handle [aidb_open $conn]

	set sql_buff "Insert into $db_user.INT_02004_02008_${op_month}
                      select
                        $Timestamp 
                        ,user_id 
                        ,max(brand_id ) 
                        ,max(usertype_id)  
                        ,max(brand_flag)
                        ,max(usertype_flag) 
                      from 
                        session.int_02004_02008
                      group by user_id"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	
	puts "汇总结束"

		
	aidb_close $handle

	return 0
}
###########参考##################
#0	历史用户
#1	正常用户
#2	营业预销户
#3	帐务预销户
#4	营业销户
#5	帐务销户
#6	管理预销户
#7	管理销户
#8	保留期
#9	保号期
#not in ('2010','2020','2030','1040','1021','9000')
#(1,2,3,6)
# ,case 
#    when userstatus_id=1 and stopstatus_id=0 then '1010' --正常--
#    when userstatus_id=1 and stopstatus_id in (28,29) then '1032' --高额停机--
#    when userstatus_id=1 and stopstatus_id in (2,3) then '1033'   --停机保号--
#    when userstatus_id=1 and stopstatus_id not in (0,2,3,28,29) then '1031' --欠停--
#    when userstatus_id in (3,6) then '1022' --欠费预销号--
#    when userstatus_id=4 then '2010' --主动销号--
#    when userstatus_id=8 then '1040' --保留期--
#    when userstatus_id=9 then '2030' --冷冻期
#    when userstatus_id in (5,7) then '2020' --被动销号--
#    else '1039' --其他原因停机--
#   end as usertype_id
#0	未停机状态                       -1	[Null]	未知用户状态
#1	连带停机                         0	1	正常用户    
#2	挂失停机                         1	0	历史用户    
#3	营业报停                         2	2	营业预销户  
#11	帐务连带单停                     3	5	帐务销户    
#12	帐务连带停                       4	7	管理销户    
#13	信誉单停(预付单停)               40	8	保留期      
#14	信誉度停(预付停)                 41	9	保号期      
#15	欠费单停                         100	3	帐务预销户  
#16	欠费停                           101	4	营业销户    
#26	管理停(用户违章停)               102	6	管理预销户  
#27	管理(用户违章停) 联带停
#28	高额停
#29	高额联带停
#31	IPBUS帐户封锁状态(业务受理)
#32	IPBUS帐户封锁状态(帐务管理)
#33	IPBUS帐户封锁状态(管理类)
#
#(0,'历史用户');  
#(1,'正常用户');  
#(2,'营业预销户');
#(3,'帐务预销户');
#(4,'营业销户');  
#(5,'帐务销户');  
#(6,'管理预销户');
#(7,'管理销户');  
#(8,'保留期');    
#(9,'保号期');   
#################################