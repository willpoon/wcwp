######################################################################################################
#接口名称： 上网本业务绑定关系
#接口编码：02063
#接口说明：上网本业务绑定关系 每日全量抽取数据。
#程序名称: G_I_02063_DAY.tcl
#功能描述:
#运行粒度: 日
#源    表：1.
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：20090828
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	     #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   


        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
			 #上月  yyyymm
			 set last_month [GetLastMonth [string range $op_month 0 5]]
			 #上上月 yyyymm
			 set last_last_month [GetLastMonth [string range $last_month 0 5]]


  #删除本期数据
	set sql_buff "delete from bass1.G_I_02063_DAY where time_id=$timestamp"
	puts $sql_buff
	exec_sql $sql_buff


	#============向正式表中插入 数据============================
	#  busi_type='737' 上网本业务
	set sql_buff "
	 insert into bass1.G_I_02063_DAY (
							 TIME_ID          
							,INTRA_PRODUCT_NO	
							,PRODUCT_NO	     
							,VALID_DATE	     
							,STS	              )
    select $timestamp
    			,t.ext_holds5
					,t.id_value  
					,replace(char(date(valid_date)),'-','')
					,case when t.sts in (1,5) then '1'       
					      when t.sts=2 then '2'
					      when t.sts=3 then '3'
					 else '2'  end
    from  (select ext_holds5
            			,id_value
            			,valid_date
            			,sts  
            			,row_number() over(partition by ext_holds5 order by valid_date desc ) row_id
					 from bass2.dwd_product_regsp_$timestamp
					 where busi_type='737' and ext_holds5 is not null) t
		where t.row_id=1				 
								 "
   puts $sql_buff
   exec_sql $sql_buff

	return 0
}



#内部函数部分
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

