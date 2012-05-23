######################################################################################################
#�������ƣ�INT_CHECK_TD_MONTH.tcl
#У��ӿڣ�TD��У�� 'R144','R145','R146','R149','R150','R151','R152'
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2009-09-14
#�����¼��
#�޸���ʷ: 20100127 �޸������û��ھ� usertype_id not in ('2010','2020','2030','9000')
#          20100322 ȡ���ӿ�22204��22205��22206��22207�漰����У�����22204�е�R147/R148
##~   20120523 ����2012���°�У�飬��дR147,R148У�飬����2У��ӱ�����ɾ������� �Ƴ���
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
				#���� yyyymm
				set op_month [ string range $optime_month 0 3 ][ string range $optime_month 5 6 ]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #��ȡ�����·�1��yyyy-mm-01
				set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
				#��ȡ�����·�����1��yyyy-mm-01
				set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]
				#��ȡ�����·ݱ���ĩ��yyyy-mm-31
				set op_month_last_iso [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]
				#��ȡ�����·ݱ���ĩ��yyyymm31
				set op_month_last [ string range $op_month_last_iso 0 3 ][ string range $op_month_last_iso 5 6 ][ string range $op_month_last_iso 8 9 ]
        #������
        set app_name "INT_CHECK_TD_MONTH.tcl"

 puts " ɾ��������"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R144','R145','R146','R149','R150','R151','R152') "        
	  exec_sql $sqlbuf


 puts "R144	��	TD�����еĺ����������»�����������������ΪTD����ĺ���������ƫ��5%��"
   set sqlbuf " 
						select val1
									,val2
									,decimal(round(val1/1.00/val2-1,4),9,4) rate 
						from 
						( select count(distinct product_no) val1
							from bass1.g_s_04017_day
							where time_id/100=$op_month and mns_type='1' ) M
						,
						( select count(distinct product_no) val2
						  from bass1.g_s_21003_month
						  where time_id=$op_month and mns_type='1' ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R144',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.05||$RESULT_VAL3<-0.05 } {
		set grade 2
	  set alarmcontent " R144У�鲻ͨ�� "
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R145	��	TD�����еļƷ�ʱ�����»�����������������ΪTD����ļƷ�ʱ����ƫ��5%��"
   set sqlbuf " 
				select val1
							,val2
							,decimal(round(val1/1.00/val2-1,4),9,4) rate 
		    from 
					( select sum(bigint(BASE_BILL_DURATION)) val1
						from bass1.g_s_04017_day
						where time_id/100=$op_month and mns_type='1' ) M
					,
					( select sum(bigint(BASE_BILL_DURATION)) val2
					  from bass1.g_s_21003_month
					  where time_id=$op_month and mns_type='1' ) N
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R145',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL3>0.05||$RESULT_VAL3<-0.05 } {
		set grade 2
	  set alarmcontent " R145 У�鲻ͨ�� "
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

 puts "R146	��	����ҵ����ʹ����������������ΪTD����� (�������͡���;���͡���������) �û���ϱ�����TD���������ж�Ӧ��(�������͡���;���͡���������)�û�����д���"
##   set sqlbuf " 
##					select count(*) val1 
##					from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
##									from bass1.g_s_21003_month
##									where time_id=$op_month and mns_type='1'
##												and (product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID) not in (
##																	select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
##																	from bass1.g_s_04017_day
##																	where time_id/100=$op_month and mns_type='1'
##																	group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID )
##									group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID  ) M
##    "

set sqlbuf " 
select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=$op_month and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=$op_month and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur
"

   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]

 puts " ��У��ֵ����У����� "   
	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R146',$RESULT_VAL1,0,0,0) "        
	  exec_sql $sqlbuf  

 # У��ֵ����ʱ�澯	
	if {$RESULT_VAL1>0} {
		set grade 2
	  set alarmcontent " R146 У�鲻ͨ�� "
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 


 
### puts " ����R147 R148У���õ���TD�м�� BASS1.td_user "
### 
### puts " Step0. �������TD�м��ʱ �õ��ĵ���ʱ��  ��ɾ������ td_user������"
### set sqlbuf "ALTER TABLE BASS1.td_user_1  ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE "
### exec_sql $sqlbuf     
### set sqlbuf "ALTER TABLE BASS1.td_user_2  ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE "
### exec_sql $sqlbuf     
### set sqlbuf "ALTER TABLE BASS1.td_user_3  ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE "
### exec_sql $sqlbuf      
### set sqlbuf "ALTER TABLE BASS1.td_user_4  ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE "
### exec_sql $sqlbuf     
### set sqlbuf "delete from bass1.td_user where time_id=$op_month  "
### exec_sql $sqlbuf     
### 
###  
### puts " Step1. ���ݿ� td���� ʹ��TD�������� "
### set sqlbuf "
###			insert into  BASS1.td_user_1  
###			select t.user_id
###						,t.usertype_id
###						,a.sim_code 
###						,a.product_no
###					 ,case when a.usertype_id in ('1','2') then '0' else '1' end  test_flag
###					 ,case when c.product_no is not null  then '1' else '0' end td_call_flag
###					 ,case when d.product_no is not null  then '1' else '0' end td_gprs_flag
###			from 	(select user_id , usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id   
###			       from bass1.g_a_02008_day
###			       where time_id<=$op_month_last ) t
###			inner join (select user_id , create_date ,product_no,sim_code,usertype_id
###												,row_number() over(partition by user_id order by time_id desc ) row_id   
###			       			from bass1.g_a_02004_day
###			            where time_id<=$op_month_last ) a on t.user_id=a.user_id  
###			left  join (select distinct product_no from bass1.g_s_04017_day 
###									where time_id/100=$op_month and roam_type_id NOT IN('122','202','302','401') ) c on a.product_no=c.product_no
###			left join (select distinct product_no from bass1.G_S_04002_DAY   where time_id/100=$op_month  and mns_type='1'
###									union 
###								 select distinct product_no from bass1.G_S_04018_DAY   where time_id/100=$op_month  and mns_type='1') d on d.product_no=a.product_no
###			where t.row_id=1 and a.row_id=1
###			"
### exec_sql $sqlbuf     		
###
### puts " Step2. ʹ��TD�ն˵Ŀͻ��� "
### set sqlbuf "
###			insert into BASS1.td_user_2 
###			select distinct a.user_id
###			from 	  bass1.G_S_02047_MONTH a
###			      , bass2.DIM_TACNUM_DEVID d
###			      , bass2.DIM_DEVICE_PROFILE e			
###			where  a.time_id=$op_month
###					  and d.time_id=$op_month
###					  and e.time_id='$op_month'
###					  and (substr(a.imei,1,8) = d.tac_num or substr(a.imei,1,6)=d.tac_num)
###					  and d.dev_id = e.DEVICE_ID
###					  and e.value in ('001001008','001001006','001012004') 
###			"
### exec_sql $sqlbuf 
###
### puts " Step3. ʹ��TD��ɫ���ܷ�"
### set sqlbuf "
###				insert into BASS1.td_user_3
###				select  value(a.user_id,b.user_id)
###						  ,case when a.user_id is not null then '1' else '0' end  td_teshefee_flag
###						  ,case when b.user_id is not null then '1' else '0' end  td_teshefunc_flag	  
###				from  ( select distinct user_id
###							  from bass1.G_S_03004_MONTH 
###								 where time_id=$op_month
###								 and ACCT_ITEM_ID in ('0522','0523','0524','0525','0526','0620','0643')  ) a
###				full join (select distinct user_id 
###										from  (select user_id, BUSI_CODE,VALID_DATE,INVALID_DATE
###																,row_number() over(partition by user_id,busi_code order by time_id desc ) row_id
###													from bass1.g_a_02011_day  
###													where busi_code in ('090','100','110','120') and time_id<=$op_month_last ) t
###										where row_id=1 and int(VALID_DATE)<=$op_month_last and int(INVALID_DATE)>$op_month_last  ) b on a.user_id=b.user_id
###			"
### exec_sql $sqlbuf 
### 				
### puts " Step4. ��ͨTD������ר���ײ͵Ŀͻ���"
### set sqlbuf "
###				insert into  BASS1.td_user_4						
###				select distinct user_id
###				from bass1.g_i_02010_month  a ,bass1.g_i_02001_month  b 
###				where a.time_id=$op_month
###						  and b.time_id=$op_month
###						  and a.PLAN_ID=b.PLAN_ID
###						  and b.td_plan_flag='2' 		
###				"
### exec_sql $sqlbuf 					
###						
### puts " Step5. ����TD�м�� BASS1.td_user "
### set sqlbuf "
###					 insert into BASS1.td_user 
###					 select  $op_month
###					 				,a.user_id
###					 				,a.usertype_id
###					 				,a.sim_code
###					 				,a.product_no
###					 				,a.test_flag
###					 				,case when a.td_call_flag='1' then '1' 
###					 				      when a.td_gprs_flag='1' then '1' 
###					 				      when b.user_id is not null then '1' 
###					 				      when c.td_teshefee_flag='1' and c.td_teshefunc_flag='1' then '1'
###					 				      when d.user_id is not null then '1'
###					 				      when substr(a.product_no,1,3) in ('188','147') then '1'
###					 				 else '0' end  td_user_flag
###					 			 ,value(a.td_call_flag,'0') td_call_flag
###					 			 ,value(a.td_gprs_flag,'0') td_gprs_flag 
###					 			 ,case when b.user_id is not null then '1'  else '0' end  td_term_flag
###					 			 ,value(c.td_teshefee_flag,'0') td_teshefee_flag
###					 			 ,value(c.td_teshefunc_flag,'0') td_teshefunc_flag
###					 			 ,case when d.user_id is not null then '1' else '0' end   td_book_flag   
###					from BASS1.td_user_1 a
###					left join BASS1.td_user_2 b	on a.user_id=b.user_id
###					left join BASS1.td_user_3 c	on a.user_id=c.user_id 
###					left join BASS1.td_user_4 d	on a.user_id=d.user_id 		
###				"
### exec_sql $sqlbuf 	 
###
###
### puts "R147	��	�Ӹ�TD�굥���м�����ġ�TD�ͻ�����������ܽӿ��еġ�TD�ͻ�������ƫ����5%����"
###   set sqlbuf " 
###					select M.val1
###								,N.val2
###								,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate
###					from 
###							( select count(distinct user_id) val1
###								from  BASS1.td_user  
###								where  time_id=$op_month and td_user_flag='1' and 
###								usertype_id  NOT IN ('2010','2020','2030','9000') and test_flag='0' ) M
###						,( select int(td_customer_cnt) val2
###								from bass1.g_s_22204_month
###								where time_id=$op_month ) N
###    "
###   set p_row [get_row $sqlbuf]
###   set RESULT_VAL1 [lindex $p_row 0]
###	 set RESULT_VAL2 [lindex $p_row 1]
###	 set RESULT_VAL3 [lindex $p_row 2]  
###
### puts " ��У��ֵ����У����� "   
###	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R147',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
###	  exec_sql $sqlbuf  
###
### # У��ֵ����ʱ�澯	
###	if {$RESULT_VAL3>0.05||$RESULT_VAL3<-0.05 } {
###		set grade 2
###	  set alarmcontent " R147 У�鲻ͨ�� "
###	  WriteAlarm $app_name $optime $grade ${alarmcontent}
### }
###
### puts "R148	��	��TD�굥���м�����ġ�ʹ�ù�TD�����TD�ͻ���������ܽӿ��еġ�ʹ�ù�TD�����TD�ͻ�������ƫ����5%����"
###   set sqlbuf " 
###					select M.val1
###								,N.val2
###								,decimal(round(M.val1/1.00/N.val2-1,4),9,4) rate
###					from 
###							( select count(distinct user_id) val1
###								from BASS1.td_user 
###								where time_id=$op_month 
###											and (td_call_flag='1' or td_gprs_flag='1' or (td_teshefee_flag='1' and td_teshefunc_flag='1') )
###											and usertype_id  NOT IN ('2010','2020','2030','9000') and test_flag='0' ) M
###						,( select int(td_tnet_use_cnt) val2
###								from bass1.g_s_22204_month
###								where time_id=$op_month ) N
###    "
###   set p_row [get_row $sqlbuf]
###   set RESULT_VAL1 [lindex $p_row 0]
###	 set RESULT_VAL2 [lindex $p_row 1]
###	 set RESULT_VAL3 [lindex $p_row 2]  
###
### puts " ��У��ֵ����У����� "   
###	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R148',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
###	  exec_sql $sqlbuf  
###
### # У��ֵ����ʱ�澯	
###	if {$RESULT_VAL3>0.05||$RESULT_VAL3<-0.05 } {
###		set grade 2
###	  set alarmcontent " R148 У�鲻ͨ�� "
###	  WriteAlarm $app_name $optime $grade ${alarmcontent}
###  }	  
###
###
###
### puts " ����R149 R150 R151 R152 R153 У���õ��� ��ʱ�� bass1.td_check_func_fee "
### 
### puts " Step0. ɾ������ td_user������ "
### set sqlbuf "delete from bass1.td_check_func_fee where time_id=$op_month  "
### exec_sql $sqlbuf     
###
###  
### puts " Step1. ���뵱������ bass1.td_check_func_fee "
### set sqlbuf "
###				insert into BASS1.td_check_func_fee
###				select $op_month
###							,M.user_id
###							,M.share_fee_flag   
###							,M.meeting_fee_flag 
###							,M.leave_fee_flag   
###							,M.cr_fee_flag      
###							,value(N.share_func_flag   ,0)
###							,value(N.meeting_func_flag ,0)
###							,value(N.leave_func_flag   ,0)
###							,value(N.cr_func_flag      ,0)
###				from ( select  user_id
###											  ,count(distinct case when ACCT_ITEM_ID in ('0526','0620') then  user_id end )  share_fee_flag
###											  ,count(distinct case when ACCT_ITEM_ID in ('0523','0524') then  user_id end )  meeting_fee_flag
###											  ,count(distinct case when ACCT_ITEM_ID in ('0522') 			  then  user_id end )  leave_fee_flag
###											  ,count(distinct case when ACCT_ITEM_ID in ('0525','0643') then  user_id end )  cr_fee_flag
###								from bass1.G_S_03004_MONTH 
###								where time_id=$op_month
###											and ACCT_ITEM_ID in ('0522','0523','0524','0525','0526','0620','0643')  
###								group by user_id		) M
###				left join ( select  user_id
###														  ,count(distinct case when busi_code in ('120') then  user_id end )  share_func_flag
###														  ,count(distinct case when busi_code in ('090') then  user_id end )  meeting_func_flag
###														  ,count(distinct case when busi_code in ('100') then  user_id end )  leave_func_flag
###														  ,count(distinct case when busi_code in ('110') then  user_id end )  cr_func_flag
###											from  (select user_id, BUSI_CODE,VALID_DATE,INVALID_DATE
###																	,row_number() over(partition by user_id,busi_code order by time_id desc ) row_id
###														from bass1.g_a_02011_day  
###														where busi_code in ('090','100','110','120') and time_id<=$op_month_last ) b
###											where b.row_id=1 and int(VALID_DATE)<=$op_month_last and int(INVALID_DATE)>$op_month_last
###											group by user_id ) N on M.user_id=N.user_id
###   "
### exec_sql $sqlbuf    
###  
### puts "R149	��ϸ�˵�������Ƶ�����ܷ�(526)����Ƶ����ͨ�ŷ�(620)���û�����Ȼ���û���ͨҵ������ʷ�����ڱ����ڿ�ͨ����Ƶ����(120)����"
###   set sqlbuf " 
###					select count(*) 
###					from bass1.td_check_func_fee 
###					where share_fee_flag=1 and share_func_flag=0 and time_id=$op_month
###    "
###   set p_row [get_row $sqlbuf]
###   set RESULT_VAL1 [lindex $p_row 0]
###
### puts " ��У��ֵ����У����� "   
###	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R149',$RESULT_VAL1,0,0,0) "        
###	  exec_sql $sqlbuf  
###
### # У��ֵ����ʱ�澯	
###	if {$RESULT_VAL1>0} {
###		set grade 2
###	  set alarmcontent " R149 ������ У�鲻ͨ�� "
###	  WriteAlarm $app_name $optime $grade ${alarmcontent}	  
###	}
### 		
### puts "R150	��ϸ�˵�������Ƶ����ҵ���ܷ�(523)����Ƶ����᳡���ܷ�(524)���û�����Ȼ���û���ͨҵ������ʷ�����ڱ����ڿ�ͨ����Ƶ����(090)����"
###   set sqlbuf " 
###						select count(*) 
###						from bass1.td_check_func_fee 
###						where meeting_fee_flag=1 and meeting_func_flag=0 and time_id=$op_month
###    "
###   set p_row [get_row $sqlbuf]
###   set RESULT_VAL1 [lindex $p_row 0]
###
### puts " ��У��ֵ����У����� "   
###	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R150',$RESULT_VAL1,0,0,0) "        
###	  exec_sql $sqlbuf  
###
### # У��ֵ����ʱ�澯	
###	if {$RESULT_VAL1>0} {
###		set grade 2
###	  set alarmcontent " R150 ������ У�鲻ͨ�� "
###	  WriteAlarm $app_name $optime $grade ${alarmcontent}	  
###	}
###
### puts "R151	��ϸ�˵�������Ƶ���Թ��ܷ�(522)���û�����Ȼ���û���ͨҵ������ʷ�����ڱ����ڿ�ͨ����Ƶ����(100)���� "
###   set sqlbuf " 
###						select count(*) 
###						from bass1.td_check_func_fee 
###						where leave_fee_flag=1 and leave_func_flag=0 and time_id=$op_month
###    "
###   set p_row [get_row $sqlbuf]
###   set RESULT_VAL1 [lindex $p_row 0]
###
### puts " ��У��ֵ����У����� "   
###	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R151',$RESULT_VAL1,0,0,0) "        
###	  exec_sql $sqlbuf  
###
### # У��ֵ����ʱ�澯	
###	if {$RESULT_VAL1>0} {
###		set grade 2
###	  set alarmcontent " R151 ������ У�鲻ͨ�� "
###	  WriteAlarm $app_name $optime $grade ${alarmcontent}	  
###	}
###
### puts "R152	��ϸ�˵����ж�ý����幦�ܷ�(525)���ý�������Ϣ��(643)���û�����Ȼ���û���ͨҵ������ʷ�����ڱ����ڿ�ͨ����ý�����(110)���� "
###   set sqlbuf " 
###						select count(*) 
###						from bass1.td_check_func_fee 
###						where cr_fee_flag=1 and cr_func_flag=0 and time_id=$op_month
###    "
###   set p_row [get_row $sqlbuf]
###   set RESULT_VAL1 [lindex $p_row 0]
###
### puts " ��У��ֵ����У����� "   
###	  set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R152',$RESULT_VAL1,0,0,0) "        
###	  exec_sql $sqlbuf  
###
### # У��ֵ����ʱ�澯	
###	if {$RESULT_VAL1>0} { 
###		set grade 2
###	  set alarmcontent " R152 ������ У�鲻ͨ�� "
###	  WriteAlarm $app_name $optime $grade ${alarmcontent}	  
###	}				  
###	
	
	return 0
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
		WriteTrace"$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	set p_row [aidb_fetch $handle]
	aidb_commit $conn
	aidb_close $handle
	return $p_row
}

#   exec_sql ִ��SQL
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
	puts "----------------------------------------------------------------------------------- "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace"$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
