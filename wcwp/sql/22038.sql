G_S_22038_DAY

CREATE TABLE "BASS1   "."G_S_22038_DAY_20111231"  (
                  "TIME_ID" INTEGER , 
                  "BILL_DATE" CHAR(8) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "INCOME" CHAR(15) NOT NULL , 
                  "D_COMM_USERS" CHAR(10) NOT NULL , 
                  "M_COMM_USERS" CHAR(10) NOT NULL , 
                  "D_VOICE_USERS" CHAR(10) NOT NULL , 
                  "M_VOICE_USERS" CHAR(10) NOT NULL , 
                  "D_INCR_USERS" CHAR(10) NOT NULL , 
                  "M_INCR_USERS" CHAR(10) NOT NULL , 
                  "W_COMM_NUMS" CHAR(10) NOT NULL , 
                  "D_INCR_FEE" CHAR(15) NOT NULL , 
                  "D_VOICE_INCR_FEE" CHAR(15) NOT NULL , 
                  "D_SMS_FEE" CHAR(15) NOT NULL , 
                  "D_OTHER_FEE" CHAR(15) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

MSISDN	BILL_ITEM	LOAD_FEE	REGION_CODE	GEN_DATE
98910037691	80000797	103810000	891	2011-12-23 11:10
98910037662	80000797	84800000	891	2011-12-23 11:10
98910037653	80000797	13000000	891	2011-12-23 11:10
98910037946	80000798	51072000	891	2011-12-27 11:29
98910037868	80000798	61248000	891	2011-12-27 11:40
98910037992	80000797	218006200	891	2011-12-28 09:58


insert into G_S_22038_DAY_20111231
select * from G_S_22038_DAY


                   
select
                   coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2'),
                   bigint(sum(a.fact_fee)*100),
                   0,0,0,0,0, 0,0,0,0,0, 0
                 from 
                    bass2.dw_acct_shoulditem_today_20111223 a,
                    bass2.dw_product_20111223  b
                 where
                   a.user_id=b.user_id
                   and   b.product_no in ('98910037662','98910037691','98910037653')
                   and a.ITEM_ID in (80000797)
                 group by
                   b.brand_id 

1	2	3	4	5	6	7	8	9	10	11	12	13
2	201610000	0	0	0	0	0	0	0	0	0	0	0


update (select * from G_S_22038_DAY where  time_id = 20111223 ) a 
set income = char(bigint(income)-201610000)
where BRAND_ID = '2' and income = '413869654'

select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0,0,0
                from
                  bass2.dw_acct_shoulditem_today_20111223 a,
                  bass2.dw_product_20111223  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100 in (4,5,6,7)
                  and a.user_id=b.user_id
                   and   b.product_no in ('98910037662','98910037691','98910037653')
                   and a.ITEM_ID in (80000797)		  
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2') 
		  
		  

select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0,0
                from
                  bass2.dw_acct_shoulditem_today_20111223 a,
                  bass2.dw_product_20111223  b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100=5
                  and a.user_id=b.user_id
		                     and   b.product_no in ('98910037662','98910037691','98910037653')
                   and a.ITEM_ID in (80000797)		
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')

 select 
                   COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2'),
                   0,0,0,0,0,0,0,0,0,0,
                   sum(a.fact_fee)*100,
                   0
                from
                  bass2.dw_acct_shoulditem_today_20111223 a,
                  bass2.dw_product_20111223 b
                where
                  int(coalesce(bass1.fn_get_all_dim('BASS_STD1_0074',char(a.item_id)),'0901'))/100=6
                  and a.user_id=b.user_id
                  		                     and   b.product_no in ('98910037662','98910037691','98910037653')
                   and a.ITEM_ID in (80000797)		
                group by 
                  COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
		  
		  
--27

                   
select
                   coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2'),
                   bigint(sum(a.fact_fee)*100),
                   0,0,0,0,0, 0,0,0,0,0, 0
                 from 
                    bass2.dw_acct_shoulditem_today_20111227 a,
                    bass2.dw_product_20111227  b
                 where
                   a.user_id=b.user_id
                   and   b.product_no in ('98910037946','98910037868')
                   and a.ITEM_ID in (80000798)
                 group by
                   b.brand_id 
	1	2	3	4	5	6	7	8	9	10	11	12	13
	2	112320000	0	0	0	0	0	0	0	0	0	0	0

update (select * from G_S_22038_DAY where  time_id = 20111227 ) a 
set income = char(bigint(income)-112320000)
where BRAND_ID = '2' and income = '320755720'



--28

select
                   coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(b.brand_id)),'2'),
                   bigint(sum(a.fact_fee)*100),
                   0,0,0,0,0, 0,0,0,0,0, 0
                 from 
                    bass2.dw_acct_shoulditem_today_20111228 a,
                    bass2.dw_product_20111228  b
                 where
                   a.user_id=b.user_id
                   and   b.product_no in ('98910037992')
                   and a.ITEM_ID in (80000797)
                 group by
                   b.brand_id 



1	2	3	4	5	6	7	8	9	10	11	12	13
2	218006200	0	0	0	0	0	0	0	0	0	0	0


update (select * from G_S_22038_DAY where  time_id = 20111228 ) a 
set income = char(bigint(income)-218006200)
where BRAND_ID = '2' and income = '407818214'



/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_22038_DAY 2011-12-23 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_22038_DAY 2011-12-27 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_22038_DAY 2011-12-28 &


