proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  set enterprise_id  "891891001076"
  set product_no     "13989008798"
  
  #����ȫ�ֱ���
   global group
   global group_list
   global product_list2
   #��ֵ 
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
   #���ú���
   get_group_list
   
	return 0
}


#pro2D_list ��ά���Ǿ���  
#Index ���
#�����б�
#group_list Ȧ���б�

#����1��Ȧ�� 
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