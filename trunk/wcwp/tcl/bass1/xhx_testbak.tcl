#################################################################
#int -s xhx_testbak.tcl
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  set enterprise_id  "891891001076"
  set product_no     "13989008798"
  
  #申明全局变量
  global Pro_listTmp (1000,1000)

  #建立临时表,存放一个集团用户之间的通话记录
	set sql_buff "declare global temporary table session.dmrn_ent_msg_xhx
	              (   
	                  enterprise_id       varchar(20)     ,  
	                  product_no1         varchar(20)     ,  
                    product_no2         varchar(20)        
                )  
                partitioning key 
                (enterprise_id,
                 product_no1)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	exec_sql $sql_buff
	
  #存放通话记录(存放与peoduct_no有关的所有通话)
	set sql_buff "declare global temporary table session.dmrn_ent_msg_rcd
	              (   
	                  product_no1         varchar(20)     ,  
                    product_no2         varchar(20)        
                )  
                partitioning key 
                (product_no1)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	exec_sql $sql_buff


  #存放通话记录(去重)
	set sql_buff "declare global temporary table session.dmrn_ent_msg_rcd1
	              (   
	                  product_no1         varchar(20)     ,   
                    product_no2         varchar(20)      
                )  
                partitioning key 
                (product_no1)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	exec_sql $sql_buff



  #存放通话记录(再去重)
	set sql_buff "declare global temporary table session.dmrn_ent_msg_rcd2
	              (   
	                  product_no1         varchar(20)     ,   
                    product_no2         varchar(20)     
                             
                )  
                partitioning key 
                (product_no1)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	exec_sql $sql_buff


  #存放通话记录(再去重)
	set sql_buff "declare global temporary table session.dmrn_ent_msg_rcd3
	              (   
	                  product_no1         varchar(20)     ,   
                    product_no2         varchar(20)     
                             
                )  
                partitioning key 
                (product_no1)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	exec_sql $sql_buff



  set sql_buff "insert into session.dmrn_ent_msg_xhx 
                select a.enterprise_id,a.product_no,a.opp_number 
                  from bass2.dw_call_opposite_200804 a,bass2.dw_enterprise_msg_200804 b
                where a.netenter_mark=1 and a.enterprise_id=b.enterprise_id and a.enterprise_id = '$enterprise_id';"
	puts $sql_buff
	exec_sql $sql_buff

  #将所有与product_no有关的通话记录放入dmrn_ent_msg_rcd
  set sql_buff "insert into session.dmrn_ent_msg_rcd1 
                select product_no1,product_no2 
                  from session.dmrn_ent_msg_xhx 
                where product_no1 = '$product_no';"
	puts $sql_buff
	exec_sql $sql_buff
                
  set sql_buff "insert into session.dmrn_ent_msg_rcd1 
                select product_no2,product_no1 
                  from session.dmrn_ent_msg_xhx 
                where product_no2 = '$product_no';"
	puts $sql_buff
	exec_sql $sql_buff
	
	
  set sql_buff "insert into session.dmrn_ent_msg_rcd2 
                select distinct product_no1,product_no2 
                  from session.dmrn_ent_msg_rcd1
                 order by product_no1,product_no2;"
	puts $sql_buff
	exec_sql $sql_buff
	

	#取得与product_no相关的通话记录
  set sql_buff "select count(distinct product_no2) 
                  from session.dmrn_ent_msg_rcd1;"
	puts $sql_buff
	set group_maxnum [get_single $sql_buff]
	puts $group_maxnum

  set Index 0    
  set Space " "  



  #取得与product_no相关的通话记录
	set handle [aidb_open $conn]
  set sql_buff "select distinct product_no2 
                  from session.dmrn_ent_msg_rcd1 
                 order by product_no2;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
  aidb_commit $conn	
  
   while {[set this_row [aidb_fetch $handle]] != ""} {
        set tem_product_no   [lindex $this_row 0]  
   	    set Index [expr  $Index+1]
   	    
   	    set product_list($Index) $tem_product_no
   	      	    
        #将所有与product_no有关的通话记录放入dmrn_ent_msg_rcd
        set sql_buff "insert into session.dmrn_ent_msg_rcd1 
                      select a.product_no1,a.product_no2 
                        from session.dmrn_ent_msg_xhx a,
                             session.dmrn_ent_msg_rcd2  b 
                       where a.product_no1 = '$tem_product_no' and a.product_no2 = b.product_no2;"
	      #puts $sql_buff
      	exec_sql $sql_buff
                
        set sql_buff "insert into session.dmrn_ent_msg_rcd1 
                      select a.product_no2,a.product_no1 
                        from session.dmrn_ent_msg_xhx a,
                             session.dmrn_ent_msg_rcd2  b 
                       where a.product_no2 = '$tem_product_no' and a.product_no1 = b.product_no2;"
	      #puts $sql_buff
      	exec_sql $sql_buff
    	  } 
    	  

   for {set y 1} {$y<=$group_maxnum} {incr y} {
      for {set x 1} {$x<$y} {incr x} {
      	set product_list2($product_list($y),$product_list($x)) "0"
        }
     }
 	
      	
  #插入除了product_no之外的两两通话记录
        set sql_buff "insert into session.dmrn_ent_msg_rcd2 
                      select distinct product_no1,product_no2 
                        from session.dmrn_ent_msg_rcd1 
                       where product_no1 <> '$product_no' and product_no2 <> '$product_no';"
	      puts $sql_buff
      	exec_sql $sql_buff


  #插入除了product_no之外的两两通话记录
        set sql_buff "insert into session.dmrn_ent_msg_rcd2 
                      select distinct product_no1,product_no2 
                        from session.dmrn_ent_msg_rcd1 
                       where product_no1 <> '$product_no' and product_no2 <> '$product_no';"
	      puts $sql_buff
      	exec_sql $sql_buff




  #插入除了product_no之外的两两通话记录
        set sql_buff "insert into session.dmrn_ent_msg_rcd3
                      select distinct product_no1,product_no2 
                        from session.dmrn_ent_msg_rcd1 
                       where product_no1 <> '$product_no' and product_no2 <> '$product_no' and product_no1 < product_no2;"
	      puts $sql_buff
      	exec_sql $sql_buff

  #插入除了product_no之外的两两通话记录
        set sql_buff "insert into session.dmrn_ent_msg_rcd3
                      select distinct product_no2,product_no1
                        from session.dmrn_ent_msg_rcd1 
                       where product_no1 <> '$product_no' and product_no2 <>  '$product_no' and product_no1 > product_no2;"
	      puts $sql_buff
      	exec_sql $sql_buff



       

  #取得除了product_no之外的两两通话记录
	set handle [aidb_open $conn]
  set sql_buff "select distinct product_no1,product_no2 
                  from session.dmrn_ent_msg_rcd3 
                 order by product_no1;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
  aidb_commit $conn	
  
  
  
   while {[set this_row [aidb_fetch $handle]] != ""} {
   	
        set tem_product_no1   [lindex $this_row 0]  
        set tem_product_no2   [lindex $this_row 1]  
        #puts $tem_product_no1$Space$tem_product_no2
        
   	    set product_list2($tem_product_no2,$tem_product_no1) "1"
   	    #puts $tem_product_no1$Space$tem_product_no2$Space$product_list2($tem_product_no2,$tem_product_no1)
    	  } 
    	  
    	  
   for {set y 1} {$y<=$group_maxnum} {incr y} {
   	  set tmpstr ""
      for {set x 1} {$x<$y} {incr x} {
      	set tmpstr $tmpstr$Space$product_list2($product_list($y),$product_list($x)) 
        }
      puts $product_list($y)$Space$Space$tmpstr
     }
    	
    
    
    
   set group_count "0" 
   set group_list($group_count) ""

   for {set y 2} {$y<=$group_maxnum} {incr y} {
   	 for {set x 2} {$x<=$group_maxnum} {incr x} {
     	  set Pro_listTmp($x) $product_list($x)
     	}
     puts "y:$y"
     puts "group_maxnum:$group_maxnum"
     puts "group_count:$group_count"
     get_group_list $y,$group_maxnum,$group_count	
    } 
    	
    	   
  aidb_close $handle
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
#--------------------------------------------------------------------------------------------------------------
#set RESULT_VAL [get_single $sql_buff]
#puts "10:梦网彩信计费量  $RESULT_VAL"


#pro2D_list 二维三角矩阵  
#Index 序号
#号码列表
#group_list 圈子列表

#返回1算圈子 
proc get_group_list {Index ProCount group_count} {
  
   puts "Index:$Index"
   puts "ProCount:$ProCount"
   puts "group_count:$group_count"
   
 
   puts "00000"
   set ProCountTmp "0"
   set Space " "
   set group_list($group_count) $group_list($group_count)$Space$Pro_listTmp($Index)
   puts "1111"
   puts $group_list($group_count)
   
   for {set y [expr $Index+1]} {$y<=$ProCount} {incr y} {
   	    if { $product_list2($Pro_listTmp($Index),$Pro_listTmp($y)) == 1 } {
      	set Pro_listTmp($y) $Pro_listTmp($y)
      	set ProCountTmp [expr  $ProCountTmp+1] 
       }
     } 
     
   for {set y 1} {$y<=$ProCountTmp} {incr y} {
     get_group_list $y $ProCountTmp $group_count	
    } 
  
  if { $ProCountTmp == 0 } {
  	  set group_count [expr  $group_count+1]   
  	  set group_list($group_count) ""
  	}
 
  
  return $ProCountTmp	
}





#================调试20080806==============
#proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#
#  set enterprise_id  "891891001076"
#  set product_no     "13989008798"
#  
#  #申明全局变量
#   global group
#   global group_list
#   
#   
#   #赋值 
#   set group_count "0" 
#   set group(1) "group(1)"
#   set group_list(0,0) "group_list(0,0)"
#   set group_list(0,1) "group_list(0,1)"
#   set group_list(1,0) "group_list(1,0)"
#   set group_list(1,1) "group_list(1,1)"
#
#   
#   #调用函数
#   get_group_list
#   
#	return 0
#}
#
#
##pro2D_list 二维三角矩阵  
##Index 序号
##号码列表
##group_list 圈子列表
#
##返回1算圈子 
#proc get_group_list {  } {
#  
#   
#   global group_list 
#   global group
#   
#   puts "group(1):$group(1)"                
#   puts "group_list(0,0):$group_list(0,0)"
#   puts "group_list(0,1):$group_list(0,1)"
#   puts "group_list(1,0):$group_list(1,0)"  
#   puts "group_list(1,1):$group_list(1,1)"
#   
#   return 0	
#}