--~   db2 -tvf syscall.sql

CREATE OR REPLACE FUNCTION systemCall( command VARCHAR(2000) )
   RETURNS INTEGER
   SPECIFIC systemCall 
   EXTERNAL NAME 'syscall!systemCall'
   LANGUAGE C 
   PARAMETER STYLE SQL 
   DETERMINISTIC 
   NOT FENCED 
   RETURNS NULL ON NULL INPUT 
   NO SQL 
   EXTERNAL ACTION 
   NO SCRATCHPAD 
   DISALLOW PARALLEL 
   FINAL CALL; 
 
values systemCall('dir . > c:\ccoutput\syscallOutPut.txt');
