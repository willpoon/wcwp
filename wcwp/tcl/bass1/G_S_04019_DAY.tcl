
######################################################################################################		
#�ӿ�����: "���ʳ�;��������"                                                               
#�ӿڱ��룺04019                                                                                          
#�ӿ�˵����"��¼���ۺ�Ĺ��ʳ�;����������ֱ�����ʳ�;������IP��12593ҵ�񻰵�����������������������"
#��������: G_S_04019_DAY.tcl                                                                            
#��������: ����04019������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110727
#�����¼��
#�޸���ʷ: 1. panzw 20110727	1.7.4 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {




##~   # ͨ�� while ѭ��
##~   # set i 0 ���������������� 0 Ϊ ����
	##~   set i 1
##~   # ���������������� , $i<= n   ,  n Խ��Խ��Զ
	##~   while { $i<=60 } {
	        ##~   set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time >= "2012-06-01" && $op_time <= "2012-07-10" } {
		##~   puts $op_time
		##~   Deal_proc04019 $op_time $optime_month	
	##~   }
		##~   incr i
	##~   }
	
		##~   set op_time 2012-07-11
		Deal_proc04019 $op_time $optime_month	



      ##~   set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      ##~   puts $timestamp
    ##~   #�ϸ��� yyyymm
    
            ##~   global app_name

		##~   set app_name "G_S_04019_DAY.tcl"        

  ##~   #ɾ����������
	##~   set sql_buff "delete from bass1.G_S_04019_DAY where time_id=$timestamp"
	##~   exec_sql $sql_buff


##~   #����ά���е�
##~   #040	�۰�̨
##~   #050	����
##~   #051	�����պ���
##~   #052	��������

##~   #2100	    IP����
##~   #2101	17950
##~   #2102	17951
##~   #2103	12593
##~   #2199	        ����

##~   #01	����/�ƶ�ɳ������	
##~   #02	����/�ƶ�ɳ������	
##~   #03	��ת	
##~   #

##~   #CALLTYPE_ID	CALLTYPE_NAME
##~   #0	����
##~   #1	����
##~   #2	��������ת
##~   #3	��������ת
##~   #4	���ŷ���
##~   #5	���Ž���


##~   #ROAMTYPE_ID	ROAMTYPE_NAME
##~   #0	����
##~   #1	ʡ������
##~   #2	ʡ������
##~   #3	��������
##~   #4	ʡ������
##~   #5	�۰�̨��������
##~   #6	ʡ�ڱ߽�����
##~   #7	ʡ�ʱ߽�����
##~   #8	ʡ�ʱ߽�����
##~   #9	�Ǹ۰�̨��������
##~   #100	��������(���ʵ�Ե����)
##~   #101	��������(���ʵ�Ե����)
##~   #

#20120710 add: when substr(opp_number,1,2) = '00'    then '1000' 	
	##~   set sql_buff "
	##~   insert into G_S_04019_DAY
	##~   select 
	##~   $timestamp time_id
	##~   ,a.product_no
	##~   ,value(a.imei,'0') imei
	##~   ,case 
		##~   when tolltype_id in (3,4,5,103,104,105) then '040'
		##~   when tolltype_id in (6,7,12,13,106,107,112,113) then '050'
		##~   when tolltype_id in (8,9,10,11,108,109,110,111) then '051'
		##~   when tolltype_id in (99,999) then '052'
	##~   end TOLL_TYPE
	##~   ,case 
	##~   when substr(opp_number,1,2) = '00'    then '1000' 	
	##~   when substr(opp_number,1,5) = '17950' then '2101' 
	##~   when substr(opp_number,1,5) = '17951' then '2102' 
	##~   when substr(opp_number,1,5) = '12593' then '2103' 
	##~   else '2199' end	IP_TYPE
	##~   ,opp_city_id  B_AREA_CD
	##~   ,case 
		##~   when calltype_id  = 0 then '01'
		##~   when calltype_id  = 1 then '02'
		##~   when calltype_id  in (2,3) then '03' 
	##~   end  CALL_TYPE_CD
	##~   ,replace(char(date(START_TIME)),'-','') BEGIN_DT 
	##~   ,replace(char(time(START_TIME)),'.','') BEGIN_TIME
	##~   ,char(call_duration_m) BILL_DUR
	##~   ,char(call_duration) CALL_DUR
	##~   ,char(int(basecall_fee*100)) BASE_CALL_FEE
	##~   ,char(int(toll_fee*100)) TOLL_FEE
	##~   from bass2.Cdr_call_dtl_$timestamp a 
	##~   where roamtype_id in (0,1,2,4,6,7,8)
	##~   and tolltype_id in (3,4,5,103,104,105,6,7,12,13,106,107,112,113,8,9,10,11,108,109,110,111,99,999)
	##~   and calltype_id in (0,1,2,3)
  ##~   "
	##~   exec_sql $sql_buff

##~   #20110828
	##~   set sql_buff "
	##~   update (select * from G_S_04019_DAY where time_id = $timestamp) a 
	##~   set B_AREA_CD = '1209'
	##~   where time_id = $timestamp
	##~   and B_AREA_CD  in (select distinct B_AREA_CD
		##~   from G_S_04019_DAY where time_id = $timestamp
		##~   except 
		##~   select country_code from  bass1.dim_country_city_code
		##~   )
  ##~   "
	##~   exec_sql $sql_buff

  ##~   #2.��� b_area_cd
	##~   set sql_buff "
	##~   select count(0) from (
		##~   select distinct B_AREA_CD
		##~   from G_S_04019_DAY where time_id = $timestamp
		##~   except 
		##~   select country_code from  bass1.dim_country_city_code 
		##~   ) a with ur
	            ##~   "
##~   chkzero2 $sql_buff "invalid B_AREA_CD!"

	return 0
}



proc Deal_proc04019 { op_time optime_month } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #�ϸ��� yyyymm
    
            global app_name

		set app_name "G_S_04019_DAY.tcl"        

  #ɾ����������
	set sql_buff "delete from bass1.G_S_04019_DAY where time_id=$timestamp"
	exec_sql $sql_buff


#����ά���е�
#040	�۰�̨
#050	����
#051	�����պ���
#052	��������

#2100	    IP����
#2101	17950
#2102	17951
#2103	12593
#2199	        ����

#01	����/�ƶ�ɳ������	
#02	����/�ƶ�ɳ������	
#03	��ת	
#

#CALLTYPE_ID	CALLTYPE_NAME
#0	����
#1	����
#2	��������ת
#3	��������ת
#4	���ŷ���
#5	���Ž���


#ROAMTYPE_ID	ROAMTYPE_NAME
#0	����
#1	ʡ������
#2	ʡ������
#3	��������
#4	ʡ������
#5	�۰�̨��������
#6	ʡ�ڱ߽�����
#7	ʡ�ʱ߽�����
#8	ʡ�ʱ߽�����
#9	�Ǹ۰�̨��������
#100	��������(���ʵ�Ե����)
#101	��������(���ʵ�Ե����)
#

##~   20120710 add: when substr(opp_number,1,2) = '00'    then '1000' 	
	set sql_buff "
	insert into G_S_04019_DAY
	select 
	$timestamp time_id
	,a.product_no
	,value(a.imei,'0') imei
	,case 
		when tolltype_id in (3,4,5,103,104,105) then '040'
		when tolltype_id in (6,7,12,13,106,107,112,113) then '050'
		when tolltype_id in (8,9,10,11,108,109,110,111) then '051'
		when tolltype_id in (99,999) then '052'
	end TOLL_TYPE
	,case 
	when substr(opp_number,1,2) = '00'    then '1000' 	
	when substr(opp_number,1,5) = '17950' then '2101' 
	when substr(opp_number,1,5) = '17951' then '2102' 
	when substr(opp_number,1,5) = '12593' then '2103' 
	else '2199' end	IP_TYPE
	,opp_city_id  B_AREA_CD
	,case 
		when calltype_id  = 0 then '01'
		when calltype_id  = 1 then '02'
		when calltype_id  in (2,3) then '03' 
	end  CALL_TYPE_CD
	,replace(char(date(START_TIME)),'-','') BEGIN_DT 
	,replace(char(time(START_TIME)),'.','') BEGIN_TIME
	,char(call_duration_m) BILL_DUR
	,char(call_duration) CALL_DUR
	,char(int(basecall_fee*100)) BASE_CALL_FEE
	,char(int(toll_fee*100)) TOLL_FEE
	from bass2.Cdr_call_dtl_$timestamp a 
	where roamtype_id in (0,1,2,4,6,7,8)
	and tolltype_id in (3,4,5,103,104,105,6,7,12,13,106,107,112,113,8,9,10,11,108,109,110,111,99,999)
	and calltype_id in (0,1,2,3)
with ur
  "
	exec_sql $sql_buff


##~   ����ά���е�
##~   040	�۰�̨
##~   050	����
##~   051	�����պ���
##~   052	��������

#����
	set sql_buff "
	insert into G_S_04019_DAY
	select 
	$timestamp time_id
	,a.product_no
	,value(a.imei,'0') imei
	,case 
		when b.COUNTRY_CODE in ('852','853','886') then '040'
		when b.COUNTRY_CODE in ('1','61','81','82') then '051'
		else '052'
	end TOLL_TYPE
	,case 
	when substr(opp_number,1,2) = '00'    then '1000' 	
	when substr(opp_number,1,5) = '17950' then '2101' 
	when substr(opp_number,1,5) = '17951' then '2102' 
	when substr(opp_number,1,5) = '12593' then '2103' 
	else '2199' end	IP_TYPE
	,opp_city_id  B_AREA_CD
	,case 
		when calltype_id  = 0 then '01'
		when calltype_id  = 1 then '02'
		when calltype_id  in (2,3) then '03' 
	end  CALL_TYPE_CD
	,replace(char(date(START_TIME)),'-','') BEGIN_DT 
	,replace(char(time(START_TIME)),'.','') BEGIN_TIME
	,char(call_duration_m) BILL_DUR
	,char(call_duration) CALL_DUR
	,char(int(basecall_fee*100)) BASE_CALL_FEE
	,char(int(toll_fee*100)) TOLL_FEE
	from bass2.Cdr_call_dtl_$timestamp a
	left join (select * from  bass1.dim_country_city_code)	 b on char(a.opp_city_id) = b.COUNTRY_CODE
	where roamtype_id in (0,1,2,4,6,7,8)
	and substr(a.opp_number,1,2) = '00'
	and tolltype_id in (0,1,2)
	and calltype_id in (1)
with ur
"
	exec_sql $sql_buff



#20110828
	set sql_buff "
	update (select * from G_S_04019_DAY where time_id = $timestamp) a 
	set B_AREA_CD = '1209'
	where time_id = $timestamp
	and B_AREA_CD  in (select distinct B_AREA_CD
		from G_S_04019_DAY where time_id = $timestamp
		except 
		select country_code from  bass1.dim_country_city_code
		)
  "
	exec_sql $sql_buff

  #2.��� b_area_cd
	set sql_buff "
	select count(0) from (
		select distinct B_AREA_CD
		from G_S_04019_DAY where time_id = $timestamp
		except 
		select country_code from  bass1.dim_country_city_code 
		) a with ur
	            "
chkzero2 $sql_buff "invalid B_AREA_CD!"

	return 0
}