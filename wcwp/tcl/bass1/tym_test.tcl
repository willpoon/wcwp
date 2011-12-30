proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  set enterprise_id  "891891001076"
  set product_no     "13989008798"
  
  #申明全局变量
   global group
   global group_list
   global product_list2
   #赋值 
   set group_count "0" 
   set group(1) "group(1)"
   set group_list(0,0) "group_list(0,0)"
   set group_list(0,1) "group_list(0,1)"
   set group_list(1,0) "group_list(1,0)"
   set group_list(1,1) "group_list(1,1)"
   
   set fuck [exec /bassapp/backapp/src/C_FUNCTION/encode 12#^p]
   puts "fuck=${fuck}"

   set fuckme [exec /bassapp/backapp/src/C_FUNCTION/decode ${fuck}] 
   puts "fuckme=${fuckme}"
   #调用函数
   get_group_list
   
	return 0
}


#pro2D_list 二维三角矩阵  
#Index 序号
#号码列表
#group_list 圈子列表

#返回1算圈子 
proc get_group_list {  } {
  
   
   global group_list 
   global group
   
   puts "group(1):$group(1)"                
   puts "group_list(0,0):$group_list(0,0)"
   puts "group_list(0,1):$group_list(0,1)"
   puts "group_list(1,0):$group_list(1,0)"  
   puts "group_list(1,1):$group_list(1,1)"
   
   return 0	
}