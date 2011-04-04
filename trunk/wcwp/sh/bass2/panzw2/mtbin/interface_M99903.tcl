#/*******************************************************************************
#	源程序名：interface_M99903.tcl
#	数据流向：
#	创建日期：2010-11-19 
#	编写人：  panzhiwei
#	简要说明：
#	修改日志：ds interface_M99903.tcl 2010-11-00
#*******************************************************************************/
proc deal {p_optime p_timestamp} {

	global env

	global conn

	global handle
	
	

	set handle [aidb_open $conn]

	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	
	  	return -1
	}

	if { [exec_sql $p_optime $p_timestamp] != 0 } {
		aidb_close $handle
		aidb_roll $conn
		return -1
	}

	return 0
}

proc exec_sql {p_optime p_timestamp} {

	global env

	global conn

	global handle

	set db_user $env(DB_USER)
	set yyyymm [string range $p_timestamp 0 5]
		#获取数据月份1号yyyymm01
	set op_month_01 [ string range $p_timestamp 0 5]01
	#获取数据月份下月1号yyyymm01
	set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y%m01" ]
		#获取数据月份当月末日yyyymm31
	set last_month_date [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y%m%d" ]
	set sFileName "/bassapp/bass2/panzw2/mtbin/data/M99903${p_timestamp}000000.AVL"

  puts $p_optime
  puts $p_timestamp


	set f [open ${sFileName} w]
	set sqlbuf "
		select distinct 
         SCHOOL_ID SCHMARKER
        ,SCHOOL_NAME SCHNAME
        ,CITY_ID REGION
        ,SCH_SIZE_STUS STUDENT_NUM
        ,SCHOOL_ADDRESS SCHADDRESS
        ,1 PARENT_SCHOOL_IND
        ,int(rand(1)*5) MARKETING_AREA
        ,ENTERPRISE_ID GROUPCODE
        ,int(rand(1)*5) ADMIN_DEPT
        ,SCH_SIZE_TEAS EMPLOYEE_NUM
        ,SCHOOL_TYPE RUN_TYPE
        ,$yyyymm OP_TIME
       from  bass2.Dim_xysc_maintenance_info 
	"

    puts $sqlbuf
										
  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		puts $errmsg
		return -1
	}		
			
	set cnt 0
	while {[set p_row [aidb_fetch $handle]] != ""} {
		set result00 [lindex $p_row 0]
    set result01 [lindex $p_row 1]
    set result02 [lindex $p_row 2]
    set result03 [lindex $p_row 3]
    set result04 [lindex $p_row 4]
    set result05 [lindex $p_row 5]
    set result06 [lindex $p_row 6]
    set result07 [lindex $p_row 7]
    set result08 [lindex $p_row 8]
    set result09 [lindex $p_row 9]
    set result10 [lindex $p_row 10]
    set result11 [lindex $p_row 11]

		
puts $f "${result00}\$${result01}\$${result02}\$${result03}\$${result04}\$${result05}\$${result06}\$${result07}\$${result08}\$${result09}\$${result10}\$${result11}"		


		set cnt [expr $cnt + 1]
		
	}
	
	close $f
	
	 exec sh interface_chk.sh M99903 ${p_timestamp} ${cnt}	
   exec sh ftp_interface.sh  172.16.5.43 load load M99903 ${p_timestamp} 
   	
	aidb_close $handle
		
	return 0
}
