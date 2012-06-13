gensql(){
name=$1
echo " delete from  app.sch_control_map where PROGRAM_NAME = '${name}.tcl';"
echo " insert into app.sch_control_map values (2,'${name}.tcl' , 'BASS1_${name}.tcl') ;"
echo " delete from  bass1.int_program_data where PROGRAM_NAME = '${name}.tcl';"
echo " insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , '${name}.tcl' , '${name}.BASS1' , '${name}_e' , '${name}_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';"
echo " "
echo " insert into app.sch_control_before values "
echo "  ('BASS1_${name}.tcl' , 'BASS1_G_S_22038_DAY.tcl') "
echo " ;"
echo " "
echo " delete from  app.sch_control_task where control_code in ( 'BASS1_${name}.tcl');"
echo " insert into app.sch_control_task values('BASS1_${name}.tcl' , 1 , 2 , 'int -s ${name}.tcl' ,10000,-1,'通信客户数日变动率 ≤ 10%' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');"
}

INT_CHECK_R012_DAY.tcl

delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_R012_DAY.tcl';
insert into app.sch_control_map values (2,'INT_CHECK_R012_DAY.tcl' , 'BASS1_INT_CHECK_R012_DAY.tcl') ;
delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_R012_DAY.tcl';
insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_R012_DAY.tcl' , 'INT_CHECK_R012_DAY.BASS1' , 'INT_CHECK_R012_DAY_e' , 'INT_CHECK_R012_DAY_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';

insert into app.sch_control_before values 
 ('BASS1_INT_CHECK_R012_DAY.tcl' , 'BASS1_G_S_22038_DAY.tcl') 
;

delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_R012_DAY.tcl');
insert into app.sch_control_task values('BASS1_INT_CHECK_R012_DAY.tcl' , 1 , 2 , 'int -s INT_CHECK_R012_DAY.tcl' ,10000,-1,'通信客户数日变动率 ≤ 10%' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');



INT_CHECK_R147R148_DAY.tcl


 delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_R147R148_DAY.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_R147R148_DAY.tcl' , 'BASS1_INT_CHECK_R147R148_DAY.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_R147R148_DAY.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_R147R148_DAY.tcl' , 'INT_CHECK_R147R148_DAY.BASS1' , 'INT_CHECK_R147R148_DAY_e' , 'INT_CHECK_R147R148_DAY_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
  ('BASS1_INT_CHECK_R147R148_DAY.tcl' , 'BASS1_G_S_22202_DAY.tcl') 
, ('BASS1_INT_CHECK_R147R148_DAY.tcl' , 'BASS1_G_S_22203_DAY.tcl') 
 ;
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_R147R148_DAY.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_R147R148_DAY.tcl' , 1 , 2 , 'int -s INT_CHECK_R147R148_DAY.tcl' ,10000,-1,'使用TD网络客户数在T网上计费时长' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
 
 
 
 INT_CHECK_R263_DAY.tcl
 
 
 
  delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_R263_DAY.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_R263_DAY.tcl' , 'BASS1_INT_CHECK_R263_DAY.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_R263_DAY.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_R263_DAY.tcl' , 'INT_CHECK_R263_DAY.BASS1' , 'INT_CHECK_R263_DAY_e' , 'INT_CHECK_R263_DAY_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
  ('BASS1_INT_CHECK_R263_DAY.tcl' , 'BASS1_G_S_22012_DAY.tcl') 
 , ('BASS1_INT_CHECK_R263_DAY.tcl' , 'BASS1_G_S_04005_DAY.tcl') 
 , ('BASS1_INT_CHECK_R263_DAY.tcl' , 'BASS1_G_S_21007_DAY.tcl') 
 ;
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_R263_DAY.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_R263_DAY.tcl' , 1 , 2 , 'int -s INT_CHECK_R263_DAY.tcl' ,10000,-1,'详单计算出的短信计费量与汇总接口上报的短信计费量之间的平衡关系' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
 
 
 
  delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_R276R277_DAY.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_R276R277_DAY.tcl' , 'BASS1_INT_CHECK_R276R277_DAY.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_R276R277_DAY.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_R276R277_DAY.tcl' , 'INT_CHECK_R276R277_DAY.BASS1' , 'INT_CHECK_R276R277_DAY_e' , 'INT_CHECK_R276R277_DAY_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
  ('BASS1_INT_CHECK_R276R277_DAY.tcl' , 'BASS1_G_S_22066_DAY.tcl') 
 ;
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_R276R277_DAY.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_R276R277_DAY.tcl' , 1 , 2 , 'int -s INT_CHECK_R276R277_DAY.tcl' ,10000,-1,'短信营业厅的放号量 = 0 /  定制终端销售量 = 0' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
 
 
 INT_CHECK_R284R285_DAY
 
 
  delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_R284R285_DAY.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_R284R285_DAY.tcl' , 'BASS1_INT_CHECK_R284R285_DAY.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_R284R285_DAY.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_R284R285_DAY.tcl' , 'INT_CHECK_R284R285_DAY.BASS1' , 'INT_CHECK_R284R285_DAY_e' , 'INT_CHECK_R284R285_DAY_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
   ('BASS1_INT_CHECK_R284R285_DAY.tcl' , 'BASS1_G_I_22420_DAY.tcl') 
,  ('BASS1_INT_CHECK_R284R285_DAY.tcl' , 'BASS1_G_S_22421_DAY.tcl') 
,  ('BASS1_INT_CHECK_R284R285_DAY.tcl' , 'BASS1_G_A_02004_DAY.tcl') 
 ;
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_R284R285_DAY.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_R284R285_DAY.tcl' , 1 , 2 , 'int -s INT_CHECK_R284R285_DAY.tcl' ,10000,-1,'垃圾短信“用户标识”都应该在“用户”表中存在' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
 
 
 
 
 
 
 
 
 insert into app.sch_control_before values 
 ('BASS1_EXP_G_S_22038_DAY','BASS1_INT_CHECK_R012_DAY.tcl') 
;

 insert into app.sch_control_before values 
  ( 'BASS1_EXP_G_S_22202_DAY','BASS1_INT_CHECK_R147R148_DAY.tcl') 
, ( 'BASS1_EXP_G_S_22203_DAY','BASS1_INT_CHECK_R147R148_DAY.tcl') 
 ;
 
 
  insert into app.sch_control_before values 
   ( 'BASS1_EXP_G_S_22012_DAY','BASS1_INT_CHECK_R263_DAY.tcl') 
 ;
 
 
  insert into app.sch_control_before values 
  ( 'BASS1_EXP_G_S_22066_DAY','BASS1_INT_CHECK_R276R277_DAY.tcl') 
 ;
 
 
  insert into app.sch_control_before values 
   ( 'BASS1_EXP_G_I_22420_DAY','BASS1_INT_CHECK_R284R285_DAY.tcl') 
,  ( 'BASS1_EXP_G_S_22421_DAY','BASS1_INT_CHECK_R284R285_DAY.tcl') 
 ;
 
 
 
 
INT_CHECK_GRP_ORD_DAY.tcl


INT_CHECK_GRP_ORD_DAY


 delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_GRP_ORD_DAY.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_GRP_ORD_DAY.tcl' , 'BASS1_INT_CHECK_GRP_ORD_DAY.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_GRP_ORD_DAY.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_GRP_ORD_DAY.tcl' , 'INT_CHECK_GRP_ORD_DAY.BASS1' , 'INT_CHECK_GRP_ORD_DAY_e' , 'INT_CHECK_GRP_ORD_DAY_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
   ('BASS1_INT_CHECK_GRP_ORD_DAY.tcl' , 'BASS1_G_A_22036_DAY.tcl') 
,  ('BASS1_INT_CHECK_GRP_ORD_DAY.tcl' , 'BASS1_G_A_01004_DAY.tcl') 
 ;
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_GRP_ORD_DAY.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_GRP_ORD_DAY.tcl' , 1 , 2 , 'int -s INT_CHECK_GRP_ORD_DAY.tcl' ,10000,-1,'集客日接口月校验' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');



INT_CHECK_GRP_INC_MONTH.tcl

INT_CHECK_GRP_INC_MONTH



 delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_GRP_INC_MONTH.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_GRP_INC_MONTH.tcl' , 'BASS1_INT_CHECK_GRP_INC_MONTH.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_GRP_INC_MONTH.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_GRP_INC_MONTH.tcl' , 'INT_CHECK_GRP_INC_MONTH.BASS1' , 'INT_CHECK_GRP_INC_MONTH_e' , 'INT_CHECK_GRP_INC_MONTH_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
  ('BASS1_INT_CHECK_GRP_INC_MONTH.tcl' , 'BASS1_G_S_03004_MONTH.tcl') 
, ('BASS1_INT_CHECK_GRP_INC_MONTH.tcl' , 'BASS1_G_I_02049_MONTH.tcl') 
, ('BASS1_INT_CHECK_GRP_INC_MONTH.tcl' , 'BASS1_G_S_03017_MONTH.tcl') 
, ('BASS1_INT_CHECK_GRP_INC_MONTH.tcl' , 'BASS1_G_S_03018_MONTH.tcl') 
 ;
 
 
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_GRP_INC_MONTH.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_GRP_INC_MONTH.tcl' , 1 , 2 , 'int -s INT_CHECK_GRP_INC_MONTH.tcl' ,10000,-1,'集客月接口月校验' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
 
 
 
 INT_CHECK_R262_MONTH
 
 
 
  delete from  app.sch_control_map where PROGRAM_NAME = 'INT_CHECK_R262_MONTH.tcl';
 insert into app.sch_control_map values (2,'INT_CHECK_R262_MONTH.tcl' , 'BASS1_INT_CHECK_R262_MONTH.tcl') ;
 delete from  bass1.int_program_data where PROGRAM_NAME = 'INT_CHECK_R262_MONTH.tcl';
 insert into bass1.int_program_data select distinct SEQUENCE_ID,PROGRAM_TYPE , 'INT_CHECK_R262_MONTH.tcl' , 'INT_CHECK_R262_MONTH.BASS1' , 'INT_CHECK_R262_MONTH_e' , 'INT_CHECK_R262_MONTH_f' from bass1.int_program_data where PROGRAM_NAME = 'G_S_22204_MONTH.tcl';
 
 insert into app.sch_control_before values 
  ('BASS1_INT_CHECK_R262_MONTH.tcl' , 'BASS1_G_S_21003_MONTH.tcl') 
 ;
 
 delete from  app.sch_control_task where control_code in ( 'BASS1_INT_CHECK_R262_MONTH.tcl');
 insert into app.sch_control_task values('BASS1_INT_CHECK_R262_MONTH.tcl' , 1 , 2 , 'int -s INT_CHECK_R262_MONTH.tcl' ,10000,-1,'2012版校验-其他' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
 
 