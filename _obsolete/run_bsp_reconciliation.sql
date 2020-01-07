/* run bsp to stella reconciliation
used to execute stored proc pl/sql 
*/

--set serveroutput on size 1000000
set linesize 200

declare 

p_no_of_rows NUMBER;
p_sqlerrm varchar2(500);
p_valid BOOLEAN;
begin
 --p_common.set_debug_mode('ON');
 stella.p_bsp_reconciliation.bsp_reconcile(null, --airline num
                                                null, --bsp trans id for indiv. trans
                                                null, -- yymm transaction month/yy
                                                 p_sqlerrm,
                                                 p_no_of_rows,
                                                 p_valid);
 dbms_output.put_line('Result of bsp reconc.:');
 dbms_output.put_line(substr(p_sqlerrm,1,200));                                           
 dbms_output.put_line('No of rows :'||p_no_of_rows);                                           

end;
/
exit;
