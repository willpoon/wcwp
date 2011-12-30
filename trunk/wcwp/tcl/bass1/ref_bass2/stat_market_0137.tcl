#======================================================================================
#��Ȩ������Copyright (c) 2010,AsiaInfo.Report.System
#��������: stat_market_0137.tcl
#������: ��/�ܱ���ȫ��ͨȫ��ͳһ�ײͿͻ��������
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
#����ʾ��: crt_basetab.sh stat_market_0137.tcl 2011-06-12
#����ʱ��: 2011-5-31
#�� �� ��: Asiainfo-Linkage
#��������:
#�޸���ʷ:
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_market_0137 $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
 }

proc stat_market_0137 {p_optime} {
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


    #Step1.���������ʱ��
    set sql_buf "
    declare global temporary table session.stat_market_0137_mid
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
			result27         integer,
			result28         integer,
			result29         integer,
			result30         integer,
			result31         integer,
			result32         integer
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


    #ץȡÿ��ָ��ֵ�����м��
    #1��ȫ��ͨȫ��ͳһ�ײͿͻ�����������λ����
    #2�����������ͻ�����������λ����
    #3�����д����ͻ�����������λ����
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result1
				,result2
				,result3
    )
    select $date_optime,
           c.city_id,
		       count(distinct product_instance_id),
		       count(distinct case when a.valid_type=1 then product_instance_id  end),
		       count(distinct product_instance_id)-count(distinct case when a.valid_type=1 then product_instance_id  end)
		from 
		   (
        select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_ds a,		
		           bass2.dim_qqt_offer_id b
					where a.offer_id=b.offer_id
					  and b.index=1
					  and date(a.create_date)=${date_optime}
					  and date(a.expire_date)>${date_optime}
			  ) a,
			  bass2.dw_product_$year$month$day c
		where a.product_instance_id=c.user_id
		  and a.rn=1
	group by c.city_id
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
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result4
				,result5
				,result6
    )
    select $date_optime,
           c.city_id,
		       count(distinct product_instance_id),
		       count(distinct case when a.valid_type=1 then product_instance_id  end),
		       count(distinct product_instance_id)-count(distinct case when a.valid_type=1 then product_instance_id  end)
		from bass2.dw_product_ins_off_ins_prod_ds a,
		     bass2.dim_qqt_offer_id b,
		     bass2.dw_product_$year$month$day c
		where a.product_instance_id=c.user_id
		  and a.offer_id=b.offer_id
		  and b.index=2
		  and date(a.create_date)=${date_optime}
		  and date(a.expire_date)>${date_optime}
	group by c.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #7��ͨ�����Ű�����������λ����                
    #8��ͨ��10086���߰�����������λ����           
    #9��ͨ��Ӫҵ��������������λ����              
    #10��ͨ��WAP����Ӫҵ��������������λ����       
    #11��ͨ����վ������������λ����  
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
    )
    select $date_optime,
           e.city_id,
			     count(distinct case when a.op_id = 10000047 then a.product_instance_id end),
			     count(distinct case when a.org_id=11111124 then a.product_instance_id end),
			     count(distinct case when a.op_id not in  (10000047,10000475) and a.org_id<>11111124 then a.product_instance_id end),
			     0,
			     count(distinct case when a.op_id = 10000475 then a.product_instance_id end)
			from bass2.dw_product_ord_cust_dm_${op_month} a,
			     bass2.dw_product_ins_off_ins_prod_ds b,
			     bass2.dw_product_ord_offer_dm_${op_month} c,
			     bass2.dim_qqt_offer_id d,
			     bass2.dw_product_$year$month$day e
			where a.product_instance_id=e.user_id
			  and a.product_instance_id=b.product_instance_id
			  and a.customer_order_id=c.customer_order_id
			  and b.offer_id=d.offer_id
			  and d.offer_id=c.offer_id
			  and date(b.create_date)=${date_optime}
			  and a.op_time=${date_optime}
			  and c.op_time=${date_optime}
			  and c.state=1
			  and d.index=1
		 group by e.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #12�������ײ�-58Ԫ ��������
    #13�������ײ�-88Ԫ ��������
    #14�������ײ�-128Ԫ ��������
    #15�������ײ�-58Ԫ ��������  
		#16�������ײ�-88Ԫ ��������  
		#17�������ײ�-128Ԫ �������� 
		#18�������ײ�-158Ԫ �������� 
		#19�������ײ�-188Ԫ �������� 
		#20�������ײ�-288Ԫ �������� 
		#21�������ײ�-388Ԫ �������� 
		#22�������ײ�-588Ԫ �������� 
		#23�������ײ�-888Ԫ �������� 
    #24�������ײ�-58Ԫ ��������    
    #25�������ײ�-88Ԫ ��������    
    #26�������ײ�-128Ԫ ��������     		
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
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
				,result22
				,result23
				,result24
				,result25
				,result26
    )
			select $date_optime,
			       a.city_id,
            count(distinct case when  a.offer_id in (111090001348,111090001331) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001349,111090001332) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001350,111090001333) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001334,111090001351) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001335,111090001352) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001336,111090001353) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001337,111090001354) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001338,111090001355) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001339,111090001356) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001340,111090001357) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001341,111090001358) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001342,111090001359) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001343,111090001360) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001344,111090001361) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dim_qqt_offer_id b,
              bass2.dw_product_$year$month$day c
        where a.product_instance_id=c.user_id
          and a.offer_id=b.offer_id
          and b.index=1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
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






    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result27
				,result28
				,result29
				,result30
				,result31
				,result32
    )
			select $date_optime,
			       a.city_id,
            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dw_product_$year$month$day c
        where a.product_instance_id=c.user_id
          and c.test_mark<>1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
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







###����һ��ȫ�������ݼ�¼---------------------
    #ץȡÿ��ָ��ֵ�����м��
    #1��ȫ��ͨȫ��ͳһ�ײͿͻ�����������λ����
    #2�����������ͻ�����������λ����
    #3�����д����ͻ�����������λ����
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result1
				,result2
				,result3
    )
    select $date_optime,
           '890',
		       count(distinct a.product_instance_id),
		       count(distinct case when a.valid_type=1 then a.product_instance_id  end),
		       count(distinct a.product_instance_id)-count(distinct case when a.valid_type=1 then a.product_instance_id  end)
		from 
		  (
        select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_ds a,		
		           bass2.dim_qqt_offer_id b
					where a.offer_id=b.offer_id
					  and b.index=1
					  and date(a.create_date)=${date_optime}
					  and date(a.expire_date)>${date_optime}
			  ) a
		where a.rn=1
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
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result4
				,result5
				,result6
    )
    select $date_optime,
           '890',
		       count(distinct a.product_instance_id),
		       count(distinct case when a.valid_type=1 then a.product_instance_id  end),
		       count(distinct a.product_instance_id)-count(distinct case when a.valid_type=1 then a.product_instance_id  end)
		from (
        select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_ds a,		
		           bass2.dim_qqt_offer_id b
					where a.offer_id=b.offer_id
					  and b.index=2
					  and date(a.create_date)=${date_optime}
					  and date(a.expire_date)>${date_optime}
			  ) a
		where a.rn=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #7��ͨ�����Ű�����������λ����                
    #8��ͨ��10086���߰�����������λ����           
    #9��ͨ��Ӫҵ��������������λ����              
    #10��ͨ��WAP����Ӫҵ��������������λ����       
    #11��ͨ����վ������������λ����  
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
    )
    select $date_optime,
           '890',
			     count(distinct case when a.op_id = 10000047 then a.product_instance_id end),
			     count(distinct case when a.org_id=11111124 then a.product_instance_id end),
			     count(distinct case when a.op_id not in  (10000047,10000475) and a.org_id<>11111124 then a.product_instance_id end),
			     0,
			     count(distinct case when a.op_id = 10000475 then a.product_instance_id end)
			from bass2.dw_product_ord_cust_dm_${op_month} a,
			     bass2.dw_product_ins_off_ins_prod_ds b,
			     bass2.dw_product_ord_offer_dm_${op_month} c,
			     bass2.dim_qqt_offer_id d
			where a.product_instance_id=b.product_instance_id
			  and a.customer_order_id=c.customer_order_id
			  and b.offer_id=d.offer_id
			  and d.offer_id=c.offer_id
			  and date(b.create_date)=${date_optime}
			  and a.op_time=${date_optime}
			  and c.op_time=${date_optime}
			  and c.state=1
			  and d.index=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #12�������ײ�-58Ԫ ��������
    #13�������ײ�-88Ԫ ��������
    #14�������ײ�-128Ԫ ��������
    #15�������ײ�-58Ԫ ��������  
		#16�������ײ�-88Ԫ ��������  
		#17�������ײ�-128Ԫ �������� 
		#18�������ײ�-158Ԫ �������� 
		#19�������ײ�-188Ԫ �������� 
		#20�������ײ�-288Ԫ �������� 
		#21�������ײ�-388Ԫ �������� 
		#22�������ײ�-588Ԫ �������� 
		#23�������ײ�-888Ԫ �������� 
    #24�������ײ�-58Ԫ ��������    
    #25�������ײ�-88Ԫ ��������    
    #26�������ײ�-128Ԫ ��������     		
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
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
				,result22
				,result23
				,result24
				,result25
				,result26
    )
			select $date_optime,
			       '890',
            count(distinct case when  a.offer_id in (111090001348,111090001331) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001349,111090001332) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001350,111090001333) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001334,111090001351) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001335,111090001352) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001336,111090001353) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001337,111090001354) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001338,111090001355) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001339,111090001356) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001340,111090001357) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001341,111090001358) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001342,111090001359) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001343,111090001360) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001344,111090001361) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dim_qqt_offer_id b
        where a.offer_id=b.offer_id
          and b.index=1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
            ) a
			where a.row_id=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }





    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result27
				,result28
				,result29
				,result30
				,result31
				,result32
    )
			select $date_optime,
			       '890',
            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dw_product_$year$month$day c
        where a.product_instance_id=c.user_id
          and c.test_mark<>1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
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
		set sql_buf "delete from bass2.stat_market_0137 where time_id = ${date_optime}"
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
    insert into bass2.stat_market_0137
    select 
				$date_optime,
				 city_id
				,sum(value(result1,0))
				,sum(value(result2,0))
				,sum(value(result3,0))
				,sum(value(result4,0))
				,sum(value(result5,0))
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
				,sum(value(result28,0))
				,sum(value(result29,0))
				,sum(value(result30,0))
				,sum(value(result31,0))
				,sum(value(result32,0))
		  from session.stat_market_0137_mid
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
	exec db2 "runstats on table bass2.stat_market_0137 with distribution and detailed indexes all"
	exec db2 "terminate"

    return 0
}


