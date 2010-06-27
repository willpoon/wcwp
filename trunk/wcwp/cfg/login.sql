--usually for debuging 
--WHENEVER SQLERROR EXIT 5 ROLLBACK
--WHENEVER OSERROR EXIT 10 ROLLBACK
--set echo on 
set pagesize 0
--set linesize 190
define _editor=vi
--set sqlprompt "&_user> "

--http://www.oracle-base.com/dba/miscellaneous/login.sql
SET FEEDBACK OFF
SET TERMOUT OFF

define Y=&_user
COLUMN X NEW_VALUE Y

SELECT LOWER(USER || '@' || instance_name) X FROM v$instance;
SET SQLPROMPT '&Y> '
--ALTER SESSION SET NLS_DATE_FORMAT='DD-MON-YYYY HH24:MI:SS'; 
--ALTER SESSION SET NLS_TIMESTAMP_FORMAT='DD-MON-YYYY HH24:MI:SS.FF'; 

SET TERMOUT ON
SET FEEDBACK ON
set serverout on
--SET LINESIZE 100
--set echo off
--test echo off

--testing statements on login
--exec dbms_output.put_line('hiilooo');
host echo &_user > /tmp/orauser
