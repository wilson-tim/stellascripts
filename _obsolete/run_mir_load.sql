/* run_mir_load.sql 
used to execute stored proc pl/sql wrapper that runs the java stored procedure
to load mir files into database for stella system */


set serveroutput on size 1000000
/* need to have serveroutput on so that the returned log name can be shown */
set linesize 200

declare 
result varchar2(2000);

begin
  -- redirect java std out to console 
  -- dbms_java.set_output(1000000);
  -- Call the function
  -- most of the params are only used for running outside of a stored procedure
  p_common.set_debug_mode('OFF');
  result := p_stella_get_data.run_mir_load( 'na',   --driverclass
                                         'na',   --connectionurl
                                         'na',   --dbuserid
                                         'na',   --dbuserpwd
                                         'none',   --singlefilename
                                         'LIVE'); --runmode
 dbms_output.put_line('Result of mir load:');
 dbms_output.put_line(substr(result,1,200));                                           
 dbms_output.put_line(substr(result,201,400));                                           
end;
/
exit;
