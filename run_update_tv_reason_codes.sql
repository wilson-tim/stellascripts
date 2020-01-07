set serveroutput on size 1000000
BEGIN
  -- Call the procedure
  /* script to change stella discrepancies enmass from TV reason code to DN
    so that they disappear from users exception screens    
    this is run regularly because users don't care about TV exceptions    
    
    Leigh mar 2004  
  */

  stella.sp_update_pnr_reason_codes('TV', -- reason code to look for
                             'DN'  -- reason code to change to
                             );
                             
END;
/
exit;

