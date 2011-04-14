
#################################################################
# ��������: WriteTrace
# ��������: ���Ѿ��򿪵�trace�ļ�дһ����¼
# �������: errmsg ��д��trace�ļ����ַ���; line �кŻ����Ǽ���ı��
# �������:
# ����ֵ:   ��
#################################################################

proc WriteTrace { msg line } {
	global trace_fd
	global script

	global program_name
	global data_time

	global conn

	set handle2 [aidb_open $conn]
   	set sqlbuf "select module,program_name,control_code from app.sch_control_map where program_name='$program_name' and module=2"

   	if [catch { aidb_sql $handle2 $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1001
		return -1
	}

	aidb_commit $conn

	if [catch {
		set sof_data [aidb_fetch $handle2]
		set control_code [lindex $sof_data 2]

		if { $control_code == "" } {
			puts "Warning: You have not set the schedule name in table app.sch_control_map"
			return -1
		}
	} errmsg ] {
			aidb_close $handle2
			return -1
	}
	aidb_close $handle2
        
	set op_time [string range $data_time 0 3]-[string range $data_time 4 5]-[string range $data_time 6 7]
	if {$line=="1000"} {
		set grade 1
	} else {
		set grade 2
	}
#	#�Դ�����Ϣ���м򻯴���(����[IBM][CLI Driver][DB2/SUN64]��ЩSQL����)
#	set Strlen [string length ${msg}]
#	set newmsg  [string range ${msg} 29 $Strlen]
#	set msg ${newmsg}
        #ȥ����line����Ķ���
#       set sql_buff "insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) values ('$control_code','int -s $program_name',$grade,'Error line:${line}; Error info:${msg}',current timestamp,-1)"
	if { [ string last "'" $msg ] != -1 } {
		set msg_list [ split $msg "'" ]
		set msg_list_length [ llength $msg_list ]
		for { set i 0 } { $i < $msg_list_length } { incr i } {
			set msg_list_content($i) [ lindex $msg_list $i ]
			append msg_content $msg_list_content($i)
		}
		set msg $msg_content
	}
	
	set handle3 [aidb_open $conn]
	set sql_buff "insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) values ('$control_code','int -s $program_name',$grade,' Error info:${msg}',current timestamp,-1)"

	if [ catch { aidb_sql $handle3 $sql_buff } errmsg ] {
		puts $errmsg
		aidb_close $handle3
		return -1
	}
	aidb_commit $conn

	puts $trace_fd "file=$script,line=$line,msg=$msg \n"
	flush $trace_fd
}
#################################################################
# ��������: WriteAlarm
# ��������: ��澯�����һ���澯��¼
# �������: app_name �˲��ļ��� p_optime ��ǰʱ��(yyyy-mm-dd) grade �澯���� alarmcontent �澯����
# �������:
# ����ֵ:   ��
#################################################################
proc WriteAlarm {app_name p_optime grade alarmcontent} {
	 #WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	global program_name
	global conn

	if { [ string last "'" $alarmcontent ] != -1 } {
		set msg_list [ split $alarmcontent "'" ]
		set msg_list_length [ llength $msg_list ]
		for { set i 0 } { $i < $msg_list_length } { incr i } {
			set msg_list_content($i) [ lindex $msg_list $i ]
			append msg_content $msg_list_content($i)
		}
		set alarmcontent $msg_content
	}

	set control_code "BASS1_${program_name}"
	set handle [aidb_open $conn]
	set sql_buff "\
	      insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) 
	      values 
	      (
	        '$control_code',
	        'int -s $program_name',
	        $grade,
	        '${alarmcontent}',
	        current timestamp,
	        -1
	      )"
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	

	
}



#������

for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)

#ϵͳ����
   exec db2 connect to bassdb user bass2 using bass2
   exec db2 runstats on table bass2.dw_product_20070701 with distribution and detailed indexes all

#����sql��ѯ�����tcl
{
    set sql_buf " select count(0) from dim_pub_city "
  	puts $sql_buf
    aidb_sql $handle  $sql_buf
    #һ����¼
    puts [aidb_fetch $handle]
    #������¼
		while {[set row [aidb_fetch $handle]] != "" } {
    			puts $row
    }
        aidb_close $handle   
}        

#�˽�
proc aidb_fetch {handle} {
  return  [db2_fetchrow $handle]
}

{
	     set handle [aidb_open $conn]
	     set sqlbuf "....."

	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	      }
	      while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 1]
	      }
	      aidb_commit $conn
	      aidb_close $handle
}
	
#conn �ĸ�ֵ	
if [catch {set conn [aidb_connect $db $username $passwd] } errmsg ] 

#handle �ĸ�ֵ
 set handle [aidb_open $conn]
 
#��ʼ�����Ի��� 

set aidbtclpath "/aitools"
set aidbtclpath "/bassapp/tcl/aiomnivision/aitools"
load "$aidbtclpath/lib/libdb2tcl.so"
source "$aidbtclpath//bin//aidb_db2_webapp.tcl"
source "/bassapp/tcl/aiomnivision/aitools/bin/aidb_db2.tcl"
#ȫ�ֱ���:
puts $env(DATABASE)
puts $env(AITOOLSPATH)
puts $env(DB_USER)
puts $env(AGENTLOGDIR)

#����:
set db bassdb
set username bass2
set passwd bass2
set conn [aidb_connect $db $username $passwd]
#����db2sql1�ִ�(����ID)
�Ͽ�����
aidb_disconnect $conn

$dbhandle ~ $conn  ���Ӿ��
$handle �������
set  handle  [aidb_open $dbhandle]
#or
set  handle  [aidb_open $conn]



 set handle [aidb_open $conn]
    set sql_buf " select count(0) from dim_pub_city "
  	puts $sql_buf
    aidb_sql $handle  $sql_buf
    #һ����¼
    puts [aidb_fetch $handle]
    #������¼
		while {[set row [aidb_fetch $handle]] != "" } {
    			puts $row
    }
        aidb_close $handle   


#conn �� handle ֮��Ĵ�����ϵ:
conn :db2sql1
handle:db2sql1.65537

% set conn [aidb_connect $db $username $passwd]
#aidb_disconnect $conn
db2sql1        
%  set handle [aidb_open $conn]
db2sql1.65537      
%set sql_buf " select count(0) from dim_pub_city "
select count(0) from dim_pub_city 
%aidb_sql $handle  $sql_buf
-1
% puts [aidb_fetch $handle]
8
aidb_commit $conn
aidb_close $handle   


% set conn [aidb_connect $db $username $passwd]
db2sql1
% set conn [aidb_connect $db $username $passwd]
db2sql2
% set handle [aidb_open db2sql1] 
db2sql1.65537
% set handle [aidb_open $conn]
db2sql2.131073

���ϣ�ÿһ�����ݿ����Ӿ��$conn�����ݿ�������$handle����һ��Ψһ�ı�ʶ��
����$handle������$conn ��������Ϊ: set handle [aidb_open $conn] .
�ر����ӵķ�����
aidb_disconnect $conn
���پ���ķ�����
aidb_close $handle   





#!/bassapp/tcl/aiomnivision/aitools/bin/tclsh

#command \
exec tclsh8.4 "$0" "$@"
if {$tcl_version >8.0} {
        encoding system identity
        fconfigure stdout -encoding identity
}

# �������

proc report_log { ddh rwh suc_code } {
    global env
    global script
    global tracefd
  
    set self_code "00"     
    set self_name $script

    set sys_fwlx_dm $self_code
    set s_program_name $self_name

    switch $suc_code {
        0 {
            set  sys_clzt_dm 04 
            set  s_notes "FAIL TO CONNECT TO THE DATABASE!!"
        }
        3 {
            set  sys_clzt_dm 03 
            set  s_notes "SUCCEED!!"
        }
        4 {
            set  sys_clzt_dm 04 
            set  s_notes "FAIL!!"
        }
        default {
            set  sys_clzt_dm 04 
            set  s_notes "FAIL!!"
        }
    }

    if {! [info exists env(AGENTLOGDIR)]} {
        puts "Error: cannot get env varialbe AGENTLOGDIR"
        exit -1
    }
    set logdir $env(AGENTLOGDIR)

    if {[file exists $logdir] == 0 } {
        file mkdir $logdir  
    }

    set len [string length $logdir]
    if { [string index $logdir [expr $len - 1]] != "/" } {
        set logdir "$logdir/"
    }

    set logfile "$logdir$ddh.log"
    set tmpfile "$logdir$ddh.log.tmp"

    set fd [open $logfile w]
    set buff "update SYS_TASK_RUNLOG set PROGRAM_NAME=\"$self_name\",SYS_CLZT_DM=\"$sys_clzt_dm\",NOTES=\"$s_notes\" where DDH=$ddh\n"
    set buff "$sys_clzt_dm$buff"
    puts $fd $buff 
    puts $tracefd $buff 
    close $fd

    set fd [open $tmpfile w]
    puts $fd $buff 
    close $fd
}

proc get_trace_name { tastname } {
    global env
   
    if {! [info exists env(AGENTTRACEDIR)]} {
        puts "Error: cannot get env varialbe AGENTTRACEDIR"
        exit -1
    }
    set tracedir $env(AGENTTRACEDIR)
  
    set len [string length $tracedir]
    if { [string index $tracedir [expr $len - 1]] != "/" } {
        set tracedir "$tracedir/"
    }

    if { [set ind [string last "/" $tastname ]] != -1 } {
        set tastname [string range $tastname [expr $ind+1] end]
    } 

    set tracedir "$tracedir"

    if {[file exists $tracedir] == 0 } {
        file mkdir $tracedir  
    }

    return "$tracedir/$tastname.trace" 
  
}

proc open_trace { tastname } {
    set filename [get_trace_name $tastname]
    return [open $filename w+]
}

proc trace_sql {errmsg line} {
    global tracefd
    global script
    global op_time
    global timestamp

    global conn
    global handle

    set date [ clock seconds ]
    set currentDate [ clock format $date -format "%Y-%m-%d %H:%M:%S" ]

    if {$line=="1000"} {
        set grade 1
    } else {
        set grade 2
    }
    
    #set sql_buff "insert into app.sch_control_alarm (dealcode,pvalue1,pvalue2,pvalue3,grade,content,alarmtime,userid,flag) values ('ds','$script','$op_time','',$grade,'Error line:${line}; Error info:${errmsg}',current timestamp,'bass2',-1)"
    
    set sql_buff "insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) values ('BASS2_$script','ds ${script} ${op_time}',$grade,'error line:${line}; error info:${errmsg}',current timestamp,-1)"
    
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
        return 0
    }
    
    aidb_commit $conn

    puts $tracefd "the time is:  $currentDate \n"
    puts $tracefd "file=$script,line=$line,err=$errmsg \n"
    flush $tracefd
}

# ��ʼ��

if {! [info exists env(DATABASE)]} {
    puts "Error: can not get env varialbe DATABASE"
    exit -1
}

if {! [info exists env(AITOOLSPATH)]} {
    set aidbtclpath "/aitools"
} else {
    set aidbtclpath $env(AITOOLSPATH) 
}

if { $env(DATABASE) == "db2" } {
    load "$env(AITOOLSPATH)/lib/libdb2tcl.so"
    source "$aidbtclpath/bin/aidb_db2.tcl"
} elseif { $env(DATABASE) == "oracle" } {
    load "$aidbtclpath//lib//libOratcl2.5.so" 
    source "$aidbtclpath//bin//aidb_ora.tcl"
} else {
    puts "we only support database db2 and oracle now "
    exit -1
}

if {$argc < 6} {
    puts "Usage : dss -s \[script\] -d \[conn string\] -t \[op_time\] -p \[timestamp\] -u \[ddh\] -v \[rwh\]"
    exit -1
}

set arg(-s)  ""
set arg(-d)  ""
set arg(-t)  ""
set arg(-p)  ""
set arg(-u)  ""
set arg(-v)  ""

#���������в���

for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
    set flag [lindex $argv $i]
    set value [lindex $argv [expr $i + 1] ] 
    if { [string index $flag 0] == "-" } {
        set arg($flag) $value
    }  
}

set script $arg(-s)
set dbstr $arg(-d)
set op_time $arg(-t)
set timestamp $arg(-p)
set ddh $arg(-u)
set rwh $arg(-v)

set pos1 [string first "/" $dbstr] 
set pos2 [string first "@" $dbstr] 

set username [string range $dbstr 0 [expr $pos1 - 1] ]
set passwd [string range $dbstr [expr $pos1 + 1] [expr $pos2 -1 ]]
set db [string range $dbstr [expr $pos2 + 1] end ]

#������ʼ

set tracefd [open_trace $script]

if [catch {set conn [aidb_connect $db $username $passwd] } errmsg ] {
    trace_sql "$errmsg,conn failed!\n" 5000
    report_log $ddh $rwh 0
    exit -1
}

set strBeginTime [ clock format [clock seconds] -format "%Y-%m-%d %H:%M:%S" ]
#trace_sql "Run $arg(-s) begin at $strBeginTime" 5000

source $env(HOME)/tcl/$script
#source $script

if {[deal $op_time $timestamp] < 0 } {
    trace_sql "EXECUTE FAILED!" 5001
    report_log $ddh $rwh 4
    exit -1
}

#trace_sql "EXECUTE SUCCESSFULLY!" 5002
report_log $ddh $rwh 3
catch {aidb_disconnect $conn} errmsg

set strBeginTime [ clock format [clock seconds] -format "%Y-%m-%d %H:%M:%S" ]
#trace_sql "Run $arg(-s) end at $strBeginTime" 5000

puts "successful"


#trace dir 
/bassapp/bass2/trace


trace_sql:
    set sql_buff "insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) values ('BASS2_$script','ds ${script} ${op_time}',$grade,'error line:${line}; error info:${errmsg}',current timestamp,-1)"


SYS_TASK_RUNLOG
����
trace ����ʲô�õ�?


#tcl ͷ
#!/bassapp/tcl/aiomnivision/aitools/bin/tclsh

#command \
exec tclsh8.4 "$0" "$@"

if { [string range [string tolower $tabname] 3 1] == "s"}{
	set sql_buf "
		select ${pk},count(0)  
		from bass1.${tabname} 
		group by ${pk} having count(0) > 1
		"	
	}
else {
	set sql_buf "
		select ${pk},count(0)  
		from bass1.${tabname}   
		where time_id =$timestamp		
		group by ${pk} having count(0) > 1
		"		
	}
proc chkpkunique{tabname pk }{
	set sql_buf "
		select ${pk},count(0)  
		from bass1.${tabname} 
		where time_id =$timestamp
		group by ${pk} having count(0) > 1
		"
	puts $sql_buf
	set CHECK_VAL1 [get_single $sql_buf ]
	puts $CHECK_VAL1
	set CHECK_VAL1 [format "%.3f" [expr ${CHECK_VAL1} /1.00]]
	puts $CHECK_VAL1
	if {[format %.3f [expr ${CHECK_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "�ӿ� ${tabname} ����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
    return -1	        
	}
		puts "�ӿ� ${tabname} ����Ψһ��У��OK!"
	return 0
}
	
invoke:
#tabname����schema
set tabname "g_a_01002_day"
set pk 			"cust_id"
chkpkunique ${tabname} ${pk}

proc chknull{tabname col}{
	set sql_buf "
		select count(0)  
		from bass1.${tabname} 
		where time_id =$timestamp
		and ${col} is null
		"	
	puts $sql_buf
	set CHECK_VAL1 [get_single $sql_buf ]
	puts $CHECK_VAL1
	set CHECK_VAL1 [format "%.3f" [expr ${CHECK_VAL1} /1.00]]
	puts $CHECK_VAL1
	if {[format %.3f [expr ${CHECK_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "�ֶ� ${tabname}.${col} �ǿ�У��δͨ��!"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
    return -1	        
	}
		puts "�ֶ� ${tabname}.${col} �ǿ�У��ok!"
	return 0
		
	}
#tabname����schema
set tabname "g_a_01002_day"
set col 			"cust_id"
chknull ${tabname} ${col}


chkfkvalid{maintabname fk dimtabname reftabname refkey}{
	sql_buf "
	select count(0) cnt
	from ( select distinct ${fk} fk from ${maintabname} where time_id = $timestamp ) a 
	where a.fk not in 
		( 
	 	select ${refkey} from ${dimtabname} where reftabname = '${reftabname}'
		)
	"
	
	puts $sql_buf
	set CHECK_VAL1 [get_single $sql_buf ]
	puts $CHECK_VAL1
	set CHECK_VAL1 [format "%.3f" [expr ${CHECK_VAL1} /1.00]]
	puts $CHECK_VAL1
	if {[format %.3f [expr ${CHECK_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "�ֶ� ${maintabname}.${fk} ������úϷ���У��δͨ��!"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
    return -1	        
	}
		puts "�ֶ� ${maintabname}.${fk} ������úϷ���У��OK!"
	return 0
	}

#tabname����schema
set tabname "g_a_01002_day"
set fk 			"cust_id"
set dimtabname 			"dim_bass1_std1"
set reftabname 			"bass1_std1_001"
set  refkey 			"code"
chkfkvalid ${tabname} ${col}

#################################################################
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

proc get_single_val {MySQL col} {

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
	if [catch {set result [lindex [aidb_fetch $handle] $col]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}		




#------------------------�ڲ���������--------------------------#	
#  get_row ���� SQL���Џ�
proc get_row {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
	puts "----------------------------------------------------------------------------------- "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	set p_row [aidb_fetch $handle]
	aidb_commit $conn
	aidb_close $handle
	return $p_row
}


   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  
	 