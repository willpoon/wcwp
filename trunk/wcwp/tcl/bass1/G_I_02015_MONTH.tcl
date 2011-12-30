######################################################################################################
#接口单元名称：用户选择移动数据流量套餐
#接口单元编码：02015
#接口单元说明：截至当月底最后一天24时，所有订购移动数据流量套餐的有效用户的订购关系记录
#程序名称: G_I_02015_MONTH.tcl
#功能描述: 生成02015的数据
#运行粒度: 月
#源    表：1.bass2.dwd_product_sprom_active_yyyymmdd(用户套餐关系(在用))
#          2.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2010-05-24
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        #----求上月最后一天---#,格式 yyyymmdd
        puts $last_month_day
        
        set thisyear [string range $op_time 0 3]
        puts $thisyear
        
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
       
        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #程序名称
	    set app_name "G_I_02015_MONTH.tcl"

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02015_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

####90004024	5元数据流量套餐
####90004025	20元数据流量套餐
####90004026	50元数据流量套餐
####90004027	100元数据流量套餐
####90004028	200元数据流量套餐
####99001676	10元数据流量上网包(飞信\100M)
####99001677	20元数据流量上网包(飞信\200M)
####90004050	300元数据流量套餐
####90008961	3个月50元数据流量套餐第一个月收150元
####90004047	50元数据流量套餐(上网卡省内\省际)
####90004048	80元数据流量套餐(上网卡省内\省际)
####90004049	120元数据流量套餐(上网卡省内\省际)
####90004052	50元数据流量套餐(上网卡)
####90004053	100元数据流量套餐(上网卡)
####90004054	200元数据流量套餐(上网卡)
####90004055	300元数据流量套餐(上网卡)
####90004023	10元数据流量套餐
####90004305	免费数据流量套餐(活动70M)
####73900001	专项移动数据流量套餐 G3 5元套餐
####73900002	专项移动数据流量套餐 G3 10元套餐
####73900003	专项移动数据流量套餐 G3 20元套餐
####73900004	专项移动数据流量套餐 G3 50元套餐
####73900005	专项移动数据流量套餐 G3 100元套餐
####73900006	专项移动数据流量套餐 G3 200元套餐

    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02015_month
	     (time_id,prod_id,user_id,valid_date)
		select
		    $op_month,
			case when b.prod_id in (90004024,73900001) then '0'
			     when b.prod_id in (90004023,73900002) then '1'
			     when b.prod_id in (90004025,73900003) then '2'
			     when b.prod_id in (90004026,73900004) then '3'
			     when b.prod_id in (90004027,73900005) then '4'
			     when b.prod_id in (90004028,73900006) then '5'
			 else '6'
		    end prod_id,
			 a.user_id,
			 replace(char(date(a.valid_date)),'-','') valid_date
		from bass2.dwd_product_sprom_active_${last_month_day} a,
			 bass2.dim_product_item b
		where a.sprom_id=b.prod_id
		  and b.prod_id in (90004024,90004025,90004026,90004027,90004028,99001676,99001677,90004050,90008961,90004047,90004048,90004049,90004052,90004053,90004054,90004055,90004023,90004305,73900001,73900002,73900003,73900004,73900005,73900006)
		  and replace(char(date(a.valid_date)),'-','')<='${last_month_day}'
		  and replace(char(date(a.expire_date)),'-','')>'${last_month_day}'
		"
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #处理业务口径错误
  #建立临时表
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_02015_month_tmp
	             (
							  time_id     integer         not null,
							  prod_id     character(1)    not null,
							  user_id     character(20)   not null,
							  valid_date  character(8)    not null
                )
                partitioning key
                (user_id)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp
	          "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_02015_month_tmp
					select time_id,prod_id,user_id,valid_date 
					from 
					(
					    select time_id,prod_id,user_id,valid_date,row_number() over(partition by user_id order by valid_date desc ) row_id
					     from bass1.g_i_02015_month
					    where user_id in 
					    (
					        select user_id from 
					        (
					            select prod_id,user_id,count(*) cnt from bass1.g_i_02015_month
					            where time_id =$op_month
					            group by prod_id,user_id
					            having count(*)>1
					        ) b
					    )
					    and time_id =$op_month
					) c
					where c.row_id=1
	      "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  set handle [aidb_open $conn]
	set sql_buff "delete from  bass1.g_i_02015_month
					where user_id in 
					(
					    select user_id from 
					    (
					    select prod_id,user_id,count(*) cnt from bass1.g_i_02015_month
					    where time_id =$op_month
					    group by prod_id,user_id
					    having count(*)>1
					) b
					)
					and time_id =$op_month
	      "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  set handle [aidb_open $conn]
	set sql_buff "insert into  bass1.g_i_02015_month
					select * from session.g_i_02015_month_tmp
	      "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #进行主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select prod_id,user_id,count(*) cnt from bass1.g_i_02015_month
	              where time_id =$op_month
	             group by prod_id,user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02015接口订购移动数据流量套餐主键唯一性校验未通过"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


	return 0
}