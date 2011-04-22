--G_S_04002_DAY
select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_04002_DAY'

--G_S_04002_DAY	95829.84 635661807
--G_S_04002_DAY	49864.68 330794180


    rename BASS1.G_S_04002_DAY to G_S_04002_DAY_BAK
    create table BASS1.G_S_04002_DAY like BASS1.G_S_04002_DAY_BAK 
     DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 ROAM_LOCN,  
                 ROAM_TYPE_ID,  
                 APNNI,  
                 START_TIME)   
     IN TBS_APP_BASS1 INDEX IN TBS_INDEX 
                   
	  insert into BASS1.G_S_04002_DAY 
	  select * from  BASS1.G_S_04002_DAY_BAK
	  where time_id >= 20101101
	  --about 15min
	 db2 RUNSTATS ON TABLE BASS1.G_S_04002_DAY	with distribution and detailed indexes all  
	  drop table BASS1.G_S_04002_DAY_BAK



--04005


RUNSTATS ON TABLE BASS1.G_S_04005_DAY	with distribution and detailed indexes all  

select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_04005_DAY'

--G_S_04005_DAY	92232.18
--G_S_04005_DAY	33963.28



    rename BASS1.G_S_04005_DAY to G_S_04005_DAY_BAK
    create table BASS1.G_S_04005_DAY like BASS1.G_S_04005_DAY_BAK 
                 DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 SP_CODE,  
                 OPPOSITE_NO)   
                   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY ; 
                   
	  insert into BASS1.G_S_04005_DAY 
	  select * from  BASS1.G_S_04005_DAY_BAK
	  where time_id >= 20101001
	  --about 10min

RUNSTATS ON TABLE BASS1.G_S_04005_DAY	with distribution and detailed indexes all  

select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            
AND tabschema = 'BASS1'

G_S_04005_DAY	159753255
G_S_04005_DAY_BAK	433834695

drop table BASS1.G_S_04005_DAY_BAK

/**
--G_S_21003_MONTH


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_21003_MONTH'

--G_S_21003_MONTH	37259.53

    rename BASS1.G_S_21003_MONTH to G_S_21003_MONTH_BAK
    create table BASS1.G_S_21003_MONTH like BASS1.G_S_21003_MONTH_BAK 
     DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 ROAM_LOCN,  
                 ROAM_TYPE_ID,  
                 APNNI,  
                 START_TIME)   
     IN TBS_APP_BASS1 INDEX IN TBS_INDEX 
                   
	  insert into BASS1.G_S_21003_MONTH 
	  select * from  BASS1.G_S_21003_MONTH_BAK
	  where time_id >= 20101101
	  
	  RUNSTATS ON TABLE BASS1.G_S_21003_MONTH	with distribution and detailed indexes all  
	  drop table BASS1.G_S_21003_MONTH_BAK
**/
