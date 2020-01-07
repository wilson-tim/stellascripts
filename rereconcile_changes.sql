/* 
used to execute stored proc pl/sql that runs the rereconcile chnaged bookings job for stella
*/

set serveroutput on size 1000000
/* need to have serveroutput on so that the returned log name can be shown */
set linesize 200

declare 

p_no_of_rows NUMBER;
p_sqlerrm varchar2(500);
p_valid BOOLEAN;
begin
 p_common.set_debug_mode('ON');
 stella.sp_rereconcile_stella_pnr( null,   -- specific season type
                                                null,   -- specific season year
                                                1,      -- num of days to go back
                                                 'N',   --log only mode
                                                p_sqlerrm,
                                                p_valid,
                                                p_no_of_rows
 );
 dbms_output.put_line('Result of rereconcile:');
 dbms_output.put_line(substr(p_sqlerrm,1,200));                                           
 dbms_output.put_line('No of rows :'||p_no_of_rows);                                           

end;
/
exit;
