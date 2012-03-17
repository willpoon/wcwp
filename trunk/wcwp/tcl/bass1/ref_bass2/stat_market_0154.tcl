#======================================================================================
#��Ȩ������Copyright (c) 2010,AsiaInfo.Report.System
#��������: stat_market_0154.tcl
#������: �±���ȫ��ͨȫ��ͳһ�ײͿͻ��������
#����Ŀ��: ֻץȡ�������û����
#ȫ��ͨȫ��ͳһ�ײͿͻ�����������λ���� 
#���������ͻ�����������λ����           
#���д����ͻ�����������λ����           
#����ȫ��ͨ�ײͿͻ�����������λ����     
#���������ͻ�����������λ����           
#���д����ͻ�����������λ����           
#ͨ�����Ű�����������λ����             
#ͨ��10086���߰�����������λ����        
#ͨ��Ӫҵ��������������λ����           
#ͨ��WAP����Ӫҵ��������������λ����    
#ͨ����վ������������λ����             
#�����ײ�-58Ԫ ��������                 
#�����ײ�-88Ԫ ��������                 
#�����ײ�-128Ԫ ��������                
#�����ײ�-58Ԫ ��������                 
#�����ײ�-88Ԫ ��������                 
#�����ײ�-128Ԫ ��������                
#�����ײ�-158Ԫ ��������                
#�����ײ�-188Ԫ ��������                
#�����ײ�-288Ԫ ��������                
#�����ײ�-388Ԫ ��������                
#�����ײ�-588Ԫ ��������                
#�����ײ�-888Ԫ ��������                
#�����ײ�-58Ԫ ��������                 
#�����ײ�-88Ԫ ��������                 
#�����ײ�-128Ԫ ��������                
#���Ű� ��������                        
#���Ű� ��������                        
#ȫ��ͨ����� ��������                  
#ȫ��ͨ�Ķ��� ��������                  
#ȫ��ͨ���ְ� ��������                  
#ȫ��ͨ�����Ѷ�� ��������              
#����ά��: ȫ��
#��������: ��
#����ʾ��: crt_basetab.sh stat_market_0154.tcl 2011-07-01
#����ʱ��: 2011-7-20
#�� �� ��: Asiainfo-Linkage
#��������:
#�޸���ʷ:���Ű�/���Ű�...�˴����߼��ϴ������⣬ͨ���������������� 2011-8-11 ����offer_id��������
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_market_0154 $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
 }

proc stat_market_0154 {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #���ڴ���
    set    date_optime  [ai_to_date $p_optime]
    scan   $p_optime    "%04s-%02s-%02s" year month day
	  #��ȡ�����·�yyyymm
	  set op_month [ string range $p_optime 0 3][string range $p_optime 5 6 ]
	  #��ȡ�����·�1��yyyy-mm-01
	  set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	  #��ȡ�����·�����1��yyyy-mm-01
	  set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]
	  #��ȡ�����·ݵ���ĩ��yyyymm31
	  set last_month_date [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]



    #1/���ȫ��ͨȫ��ͳһ�ײͿͻ������û��嵥��ʱ��
    #stat_market_0155�������õ���Щ�嵥��������ͳ��
    set sql_buf "alter table bass2.stat_market_0154_user_01 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #�����û��嵥�б�
    set sql_buf "
    insert into BASS2.stat_market_0154_user_01
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=1
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c
	where a.product_instance_id=c.user_id
		and a.rn=1
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }

    
    #2/���ȫ��ͨȫ��ͳһ�ײʹ����ͻ������û��嵥��ʱ��
    #stat_market_0155�������õ���Щ�嵥��������ͳ��
    set sql_buf "alter table bass2.stat_market_0154_user_02 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #�����û��嵥�б�
    set sql_buf "
    insert into BASS2.stat_market_0154_user_02
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=1
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c,
				 bass2.dim_qqt_month d
	where a.product_instance_id=c.user_id
		and a.rn=1
		and a.valid_date=d.month_id
		and a.create_date<>a.valid_date
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1		
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #3/�������ȫ��ͨ�ײͿͻ������û��嵥��ʱ��
    #stat_market_0155�������õ���Щ�嵥��������ͳ��
    set sql_buf "alter table bass2.stat_market_0154_user_03 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #�����û��嵥�б�
    set sql_buf "
    insert into BASS2.stat_market_0154_user_03
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=2
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c
	where a.product_instance_id=c.user_id
		and a.rn=1
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #4/�������ȫ��ͨȫ��ͳһ�ײʹ����ͻ������û��嵥��ʱ��
    #stat_market_0155�������õ���Щ�嵥��������ͳ��
    set sql_buf "alter table bass2.stat_market_0154_user_04 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #�����û��嵥�б�
    set sql_buf "
    insert into BASS2.stat_market_0154_user_04
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=2
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c,
				 bass2.dim_qqt_month d
	where a.product_instance_id=c.user_id
		and a.rn=1
		and a.valid_date=d.month_id
		and a.create_date<>a.valid_date
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #���������ʱ��
    set sql_buf "
    declare global temporary table session.stat_market_0154_mid
    (
			time_id          date,
			city_id          varchar(7),
			result1          integer,
			result2          integer,
			result3          integer,
			result4          integer,
			result5          integer,
			result6          integer,
			result7          integer,
			result8          integer,
			result9          integer,
			result10         integer,
			result11         integer,
			result12         integer,
			result13         integer,
			result14         integer,
			result15         integer,
			result16         integer,
			result17         integer,
			result18         integer,
			result19         integer,
			result20         integer,
			result21         integer,
			result22         integer,
			result23         integer,
			result24         integer,
			result25         integer,
			result26         integer,
			result27         integer
    )
    partitioning key(time_id) using hashing
    with replace on commit preserve rows not logged in tbs_user_temp
    "
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }


    
    #------------------------------------------------------------------------------
    #1��ȫ��ͨȫ��ͳһ�ײͿͻ�����������λ����
    #2�����������ͻ�����������λ����
    #3�����д����ͻ�����������λ����
    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result1
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_01
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result3
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_02
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




    #4������ȫ��ͨ�ײͿͻ�����������λ����
    #5�����������ͻ�����������λ����
    #6�����д����ͻ�����������λ����
    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result4
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_03
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result6
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_04
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




    #07�������ײ�-58Ԫ ��������
    #08�������ײ�-88Ԫ ��������
    #09�������ײ�-128Ԫ ��������
    #10�������ײ�-58Ԫ ��������  
		#11�������ײ�-88Ԫ ��������  
		#12�������ײ�-128Ԫ �������� 
		#13�������ײ�-158Ԫ �������� 
		#14�������ײ�-188Ԫ �������� 
		#15�������ײ�-288Ԫ �������� 
		#16�������ײ�-388Ԫ �������� 
		#17�������ײ�-588Ԫ �������� 
		#18�������ײ�-888Ԫ �������� 
    #19�������ײ�-58Ԫ ��������    
    #20�������ײ�-88Ԫ ��������    
    #21�������ײ�-128Ԫ ��������

    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
				,result12
				,result13
				,result14
				,result15
				,result16
				,result17
				,result18
				,result19
				,result20
				,result21
    )
   select $date_optime,
          city_id,
          count(distinct case when  offer_id in (111090001348,111090001331) then  user_id end),
          count(distinct case when  offer_id in (111090001349,111090001332) then  user_id end),
          count(distinct case when  offer_id in (111090001350,111090001333) then  user_id end),
          count(distinct case when  offer_id in (111090001334,111090001351) then  user_id end),
          count(distinct case when  offer_id in (111090001335,111090001352) then  user_id end),
          count(distinct case when  offer_id in (111090001336,111090001353) then  user_id end),
          count(distinct case when  offer_id in (111090001337,111090001354) then  user_id end),
          count(distinct case when  offer_id in (111090001338,111090001355) then  user_id end),
          count(distinct case when  offer_id in (111090001339,111090001356) then  user_id end),
          count(distinct case when  offer_id in (111090001340,111090001357) then  user_id end),
          count(distinct case when  offer_id in (111090001341,111090001358) then  user_id end),
          count(distinct case when  offer_id in (111090001342,111090001359) then  user_id end),
          count(distinct case when  offer_id in (111090001343,111090001360) then  user_id end),
          count(distinct case when  offer_id in (111090001344,111090001361) then  user_id end),
          count(distinct case when  offer_id in (111090001345,111090001362) then  user_id end)
		from BASS2.stat_market_0154_user_01
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


		#22.���Ű�
		#23.���Ű�
		#24.ȫ��ͨ�����
		#25.ȫ��ͨ�Ķ���
		#26.ȫ��ͨ���ְ�
		#27.ȫ��ͨ�����Ѷ��
    set sql_buf "
    insert into session.stat_market_0154_mid
     (
				time_id
				,city_id
				,result22
				,result23
				,result24
				,result25
				,result26
				,result27
      )
			select $date_optime,
			       a.city_id,
            count(distinct case when  offer_id in (111090001363) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001364) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001365) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001366) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001367) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001368) then  product_instance_id end)
			 from (
			 select product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_$op_month a,
              bass2.dw_product_$op_month c
        where a.product_instance_id=c.user_id
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
			    and c.test_mark<>1
			    and a.offer_id in (111090001363,111090001364,111090001365,111090001366,111090001367,111090001368)
            ) a
			where a.row_id=1
	group by a.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



####����ȫ����һ����¼

   
    #------------------------------------------------------------------------------
    #1��ȫ��ͨȫ��ͳһ�ײͿͻ�����������λ����
    #2�����������ͻ�����������λ����
    #3�����д����ͻ�����������λ����
    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result1
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_01
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result3
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_02
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




    #4������ȫ��ͨ�ײͿͻ�����������λ����
    #5�����������ͻ�����������λ����
    #6�����д����ͻ�����������λ����
    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result4
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_03
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result6
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_04
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




    #07�������ײ�-58Ԫ ��������
    #08�������ײ�-88Ԫ ��������
    #09�������ײ�-128Ԫ ��������
    #10�������ײ�-58Ԫ ��������  
		#11�������ײ�-88Ԫ ��������  
		#12�������ײ�-128Ԫ �������� 
		#13�������ײ�-158Ԫ �������� 
		#14�������ײ�-188Ԫ �������� 
		#15�������ײ�-288Ԫ �������� 
		#16�������ײ�-388Ԫ �������� 
		#17�������ײ�-588Ԫ �������� 
		#18�������ײ�-888Ԫ �������� 
    #19�������ײ�-58Ԫ ��������    
    #20�������ײ�-88Ԫ ��������    
    #21�������ײ�-128Ԫ ��������

    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
				,result12
				,result13
				,result14
				,result15
				,result16
				,result17
				,result18
				,result19
				,result20
				,result21
    )
   select $date_optime,
          '890',
          count(distinct case when  offer_id in (111090001348,111090001331) then  user_id end),
          count(distinct case when  offer_id in (111090001349,111090001332) then  user_id end),
          count(distinct case when  offer_id in (111090001350,111090001333) then  user_id end),
          count(distinct case when  offer_id in (111090001334,111090001351) then  user_id end),
          count(distinct case when  offer_id in (111090001335,111090001352) then  user_id end),
          count(distinct case when  offer_id in (111090001336,111090001353) then  user_id end),
          count(distinct case when  offer_id in (111090001337,111090001354) then  user_id end),
          count(distinct case when  offer_id in (111090001338,111090001355) then  user_id end),
          count(distinct case when  offer_id in (111090001339,111090001356) then  user_id end),
          count(distinct case when  offer_id in (111090001340,111090001357) then  user_id end),
          count(distinct case when  offer_id in (111090001341,111090001358) then  user_id end),
          count(distinct case when  offer_id in (111090001342,111090001359) then  user_id end),
          count(distinct case when  offer_id in (111090001343,111090001360) then  user_id end),
          count(distinct case when  offer_id in (111090001344,111090001361) then  user_id end),
          count(distinct case when  offer_id in (111090001345,111090001362) then  user_id end)
		from BASS2.stat_market_0154_user_01
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


		#22.���Ű�
		#23.���Ű�
		#24.ȫ��ͨ�����
		#25.ȫ��ͨ�Ķ���
		#26.ȫ��ͨ���ְ�
		#27.ȫ��ͨ�����Ѷ��
    set sql_buf "
    insert into session.stat_market_0154_mid
     (
				time_id
				,city_id
				,result22
				,result23
				,result24
				,result25
				,result26
				,result27
      )
			select $date_optime,
			       '890',
            count(distinct case when  offer_id in (111090001363) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001364) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001365) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001366) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001367) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001368) then  product_instance_id end)
			 from (
			 select product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_$op_month a,
              bass2.dw_product_$op_month c
        where a.product_instance_id=c.user_id
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
			    and c.test_mark<>1
			    and a.offer_id in (111090001363,111090001364,111090001365,111090001366,111090001367,111090001368)
            ) a
			where a.row_id=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




	# #===============================================================================================
	# # step : ɾ���������������
	# #===============================================================================================
		set sql_buf "delete from bass2.stat_market_0154 where time_id = ${date_optime}"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn



	##=====================================================
	## step 2: ���ɽ������ ���ȫ����Ʒ�Ƶ�ץȡ
	##=====================================================
    set sql_buf "
    insert into bass2.stat_market_0154
    select 
				$date_optime
				,city_id
				,sum(value(result1,0))
				,sum(value(result1,0))-sum(value(result3,0))
				,sum(value(result3,0))
				,sum(value(result4,0))
				,sum(value(result4,0))-sum(value(result6,0))
				,sum(value(result6,0))
				,sum(value(result7,0))
				,sum(value(result8,0))
				,sum(value(result9,0))
				,sum(value(result10,0))
				,sum(value(result11,0))
				,sum(value(result12,0))
				,sum(value(result13,0))
				,sum(value(result14,0))
				,sum(value(result15,0))
				,sum(value(result16,0))
				,sum(value(result17,0))
				,sum(value(result18,0))
				,sum(value(result19,0))
				,sum(value(result20,0))
				,sum(value(result21,0))
				,sum(value(result22,0))
				,sum(value(result23,0))
				,sum(value(result24,0))
				,sum(value(result25,0))
				,sum(value(result26,0))
				,sum(value(result27,0))
		  from session.stat_market_0154_mid
	group by city_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



  #Step.�ռ���Ϣ
	exec db2 "connect to bassdb user bass2 using bass2"
	exec db2 "runstats on table bass2.stat_market_0154 with distribution and detailed indexes all"
	exec db2 "terminate"

    return 0
}


