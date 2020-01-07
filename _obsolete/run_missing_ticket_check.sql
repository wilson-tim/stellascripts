/* run missing ticket check sql_
used to execute stored proc pl/sql that runs the missing ticket report for stella
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
 stella.p_stella_reconciliation.missing_ticket_report( null,
                                                null,
                                                null,
                                                p_sqlerrm,
                                                p_no_of_rows,
                                                p_valid );
 dbms_output.put_line('Result of missing ticket check:');
 dbms_output.put_line(substr(p_sqlerrm,1,200));                                           
 dbms_output.put_line('No of rows :'||p_no_of_rows);                                           

end;
/
exit;
