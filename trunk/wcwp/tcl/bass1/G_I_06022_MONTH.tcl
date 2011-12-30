######################################################################################################
#接口名称：实体渠道购建或租赁信息
#接口编码：06022
#接口说明：记录实体渠道物业来源信息，涉及自营厅、委托经营厅或社会代理网点（自建他营类）
#程序名称: G_I_06022_MONTH.tcl
#功能描述: 生成06022的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-11-9
#问题记录：
#修改历史:
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #删除正式表本月数据
    set sql_buff "delete from bass1.G_I_06022_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff




    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_I_06022_MONTH
		(	 	TIME_ID
			 ,CHANNEL_ID
			 ,OWNER_TYPE
			 ,BUY_MONTH
			 ,FC_LIC
			 ,TD_LIC
			 ,GS_LIC
			 ,BUY_CHARGE
			 ,RENT_BEGIN_DATE
			 ,RENT_END_DATE
			 ,AV_PRICES
		  )
	SELECT
	   $op_month
		,	trim(char(channel_id))
		,	case when PROPERTY_SRC_TYPE in (2,3) then '1'
					  when PROPERTY_SRC_TYPe=1 then '2'
						else '3 '
		 	end OWNER_TYPE
		,	case when PROPERTY_SRC_TYPE in (2,3) then left(replace(char(a.create_date),'-',''),6) 
					 else '000101' 
		 	end BUY_MONTH
		,	'' FC_LIC
		,	'' TD_LIC
		,	'' GS_LIC
		,	'' BUY_CHARGE
		,	case when PROPERTY_SRC_TYPE not in (2,3) or PROPERTY_SRC_TYPE is null then left(replace(char(a.create_date),'-',''),8) 
					  else '00010101' 
		 	end RENT_BEGIN_DATE
		,	case when PROPERTY_SRC_TYPE not in (2,3) or PROPERTY_SRC_TYPE is null then '20300101' 
		        else '00010101'
		  end RENT_END_DATE
		,	'' AV_PRICES 
		FROM BASS2.DW_CHANNEL_INFO_$op_month A 
		WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102)
	with ur
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



