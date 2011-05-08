CREATE FUNCTION bass1.get_before(p_control_code varchar(128))
RETURNS
TABLE (control_code varchar(128),before_control_code varchar(128))
RETURN
select control_code,before_control_code from app.sch_control_before where control_code = p_control_code

CREATE FUNCTION bass1.get_after(p_before_control_code varchar(128))
RETURNS
TABLE (control_code varchar(128),before_control_code varchar(128))
RETURN
select control_code,before_control_code from app.sch_control_before where before_control_code = p_before_control_code


select * from  table( bass1.get_before('BASS1_G_I_03007_MONTH.tcl')) a 
select * from  table( bass1.get_after('BASS1_G_I_03007_MONTH.tcl')) a 

