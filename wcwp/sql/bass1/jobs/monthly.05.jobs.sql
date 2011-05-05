05001,05002 入库

出月报

  --1>分割21003 : split -7000000 s_13100_201101_21003_00_001.dat
/**
update  app.sch_control_task
set time_value = 510
where  time_value = 112
and control_code = 'BASS1_G_S_05001_MONTH.tcl'

        
update  app.sch_control_task
set time_value = 510
where  time_value = 112
and control_code = 'BASS1_G_S_05002_MONTH.tcl'
**/


10号前接口：
ls -alrt  *03007*dat *21010*dat *21013*dat *21014*dat *21015*dat *05001*dat \
*05002*dat *05003*dat *22013*dat *22021*dat *22025*dat *22032*dat *22033*dat \
*22039*dat *22041*dat *22042*dat *22043*dat  

15号前接口：
ls -lart *22049*dat *22050*dat *22052*dat *22055*dat *22056*dat *22061*dat \
*22062*dat *22063*dat *22064*dat *22065*dat  





--重运出数代码
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月3日前'
)

--重运校验代码
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月3日前'
)
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
)
and flag = 0
and date(endtime) < current date
)


--删除 export_yyyymm 下的数据，然后：

--重运导出程序
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = '每月3日前'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) < current date
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')

--不导出 05001-05002 , 除非这两个数已经拿到。
BASS1_G_S_05001_MONTH.tcl
BASS1_G_S_05002_MONTH.tcl

