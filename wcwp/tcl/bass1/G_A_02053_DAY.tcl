######################################################################################################
#�ӿ����ƣ���ֵҵ�񶩹���ϵ
#�ӿڱ��룺02053
#�ӿ�˵�����ϱ����š�ȫ���ֻ�����ȫ��139�ֻ�����ҵ��Ķ�����ϵ
#��������: G_A_02053_DAY.tcl
#��������: ����02053������
#��������: ��
#Դ    ��1.bass2.dw_product_bass1_yyyymmdd
#          2.bass2.DWD_PRODUCT_REGSP_yyyymmdd
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-05-23
#�����¼��bill_flag �ֶ�ȡֵ������
#�޸���ʷ: 2009-06-19 liuzhilong ����APPLY_TYPE ���������
#           2009-09-01 liuzhilong �Ż����� except �ĳ� except all
#           20091126 �� dw_product_bass1_ �滻ԭ�����û���
#           20100531 liuqf busi_type ��integer---->varchar(20),���� apply_type �ֶεĹ淶����
#           20101209 liuqf �����ֹ�����BOSS�Ǳ�����SPע����Ϣ�����������
#           2011-05-12 11:38:38 panzhiwei  bass2.dwd_i_user_radius_order_$timestamp -> bass2.DW_I_USER_RADIUS_ORDER_DS
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
	      puts $op_time
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
		set app_name "G_A_02053_DAY.tcl"        
    #ɾ����������
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_a_02053_day where time_id=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
          
#  set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.g_a_02053_day
#	             select k.TIME_ID, k.PRODUCT_NO, k.PRODUCT_NO_3, k.BUSI_TYPE, k.SP_CODE, 
#                      k.SERV_CODE, k.STS, k.VALID_DATE, k.EXPIRE_DATE, k.BILL_FLAG, k.REMARK, k.APPLY_TYPE
#               from
#               (
#                select $timestamp TIME_ID,
#                       b.product_no,
#                       a.id_value PRODUCT_NO_3,
#                       case when a.busi_type=115 then '09'
#                            when a.busi_type=119 then '15'
#                            when a.busi_type=130 then '14'
#                       end as BUSI_TYPE,
#                       char(a.sp_code) SP_CODE,
#                       a.serv_code,
#                       case when a.sts=1 then '0'
#                            when a.sts in(2,5,6) then '1'
#                            when a.sts=3 then '2'
#                       end as STS,
#                       substr(char(a.valid_date),1,4)||substr(char(a.valid_date),6,2)||substr(char(a.valid_date),9,2) VALID_DATE,
#                       substr(char(a.expire_date),1,4)||substr(char(a.expire_date),6,2)||substr(char(a.expire_date),9,2) EXPIRE_DATE,
#                       case when a.bill_flag=0 then '0'
#				                   when a.bill_flag=1 then '1'
#					               	 when a.bill_flag=2 then '2'
#					               	 when a.bill_flag=3 then '3'
#					               	 when a.bill_flag=4 then '4'
#					                 else '0'
#				               end bill_flag,
#                       case when (a.remark is null or a.remark like '%��ϵͳ����%' ) then '1'
#                            when lower(a.remark) like '%cboss%' then '2'
#                            else '3'
#                       end as REMARK,
#                       char(a.apply_type) APPLY_TYPE ,
#                       row_number()over(partition by PRODUCT_NO, a.id_value, case when a.busi_type=115 then '09'
#                            when a.busi_type=119 then '15'
#                            when a.busi_type=130 then '14'
#                       end, SP_CODE, SERV_CODE order by $timestamp desc) row_id
#                from bass2.DWD_PRODUCT_REGSP_$timestamp a
#                 left outer join bass2.dw_product_bass1_$timestamp b
#                  on a.user_id=b.user_id
#                where a.busi_type in (115,119,130) 
#                  and a.sts=1
#               ) k   
#              where k.row_id=1 
#                with ur"   
#                        
#	aidb_commit $conn
#	aidb_close $handle
          
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_02053_day
	           	      select f.TIME_ID, f.PRODUCT_NO, f.PRODUCT_NO_3, f.BUSI_TYPE, f.SP_CODE, 
             f.SERV_CODE, f.STS, f.VALID_DATE, f.EXPIRE_DATE, f.BILL_FLAG, f.REMARK, f.APPLY_TYPE
		  from
		  (
		     select k.TIME_ID, k.PRODUCT_NO, k.PRODUCT_NO_3, k.BUSI_TYPE, k.SP_CODE, 
             k.SERV_CODE, k.STS, k.VALID_DATE, k.EXPIRE_DATE, k.BILL_FLAG, k.REMARK, k.APPLY_TYPE,row_number()over(partition by k.PRODUCT_NO, k.PRODUCT_NO_3, k.BUSI_TYPE, k.SP_CODE, 
             k.SERV_CODE order by $timestamp desc) row_id    
               from
               (
	              select $timestamp time_id,
                       b.product_no,
                       a.id_value product_no_3,
                       case when a.busi_type='115' then '09'
                            when a.busi_type='119' then '15'
                            when a.busi_type='130' then '14'
                       end busi_type,
                       char(a.sp_code) sp_code,
                       a.serv_code,
                       case when a.sts=1 then '0'
                            when a.sts in(2,5,6) then '1'
                            when a.sts=3 then '2'
                       end STS,
                       substr(char(a.valid_date),1,4)||substr(char(a.valid_date),6,2)||substr(char(a.valid_date),9,2) valid_date,
                       substr(char(a.expire_date),1,4)||substr(char(a.expire_date),6,2)||substr(char(a.expire_date),9,2) expire_date,
                       case when a.bill_flag=0 then '0'
				            when a.bill_flag=1 then '1'
					        when a.bill_flag=2 then '2'
					        when a.bill_flag=3 then '3'
					        when a.bill_flag=4 then '4'
					    else '0'
				       end bill_flag,
                       case when (a.remark is null or a.remark like '%��ϵͳ����%' ) then '1'
                            when lower(a.remark) like '%cboss%' then '2'
                        else '3'
                       end remark,
                       case when a.apply_type=1 then '01'
						     when a.apply_type=2 then '02'
						     when a.apply_type=3 then '03'
						     when a.apply_type=4 then '04'
						     when a.apply_type=5 then '05'
						     when a.apply_type=6 then '06'
						     when a.apply_type=7 then '07'
						     when a.apply_type=8 then '08'
						     when a.apply_type=9 then '09'
						     when a.apply_type between 10 and 14 then char(a.apply_type)
						else '00'  
					   end apply_type
                from bass2.dwd_product_regsp_$timestamp a
                 inner join (select distinct USER_ID from bass2.dw_i_user_radius_order_ds) c on a.user_id=c.USER_ID 
                 left outer join bass2.dw_product_bass1_$timestamp b on a.user_id=b.user_id
                where a.busi_type in ('115','119','130')
              except all
              select $timestamp time_id,product_no, product_no_3, busi_type, sp_code, 
                  serv_code, sts, valid_date, expire_date, bill_flag, remark,  apply_type 
              from
               (   
                select $timestamp time_id, product_no, product_no_3, busi_type, sp_code, 
                  serv_code, sts, valid_date, expire_date, bill_flag, remark,  apply_type ,
                  row_number()over(partition by product_no order by time_id desc) row_id
                from bass1.g_a_02053_day 
               ) m
               where m.row_id=1
                  ) k   
  ) f
  where f.row_id=1
              with ur"   

	
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}      
	aidb_commit $conn
	aidb_close $handle


  #20101209 liuqf �����ֹ�����BOSS�Ǳ�����SPע����Ϣ�����������
  set handle [aidb_open $conn]
	set sql_buff "update g_a_02053_day set expire_date=valid_date
			where expire_date<valid_date 
			and time_id = $timestamp
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



  #����02053����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select product_no,product_no_3,busi_type,sp_code,serv_code,count(*) cnt from bass1.g_a_02053_day
	              where time_id =$timestamp
	             group by product_no,product_no_3,busi_type,sp_code,serv_code
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02053�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }


	return 0
}