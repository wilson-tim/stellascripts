package uk.co.firstchoice.stella;

import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import uk.co.firstchoice.util.StringUtils;
import uk.co.firstchoice.util.businessrules.NumberProcessor;

public class TimSandbox
{
  /** Creates a new instance of TimSandbox */
  public TimSandbox() {
  }

  /** Hello world! */    
  public static String world()
  {
    return "Hello world!";
  }

  public static String fpRecord(String recordText)
  {
    String group = "";
    String recBookingRef = "";
    String patternString = "NONREFAGT[A-Z]{1,3}[0-9]{1,10}";
    
    Pattern pattern = Pattern.compile(patternString);
    Matcher matcher = pattern.matcher(recordText);
    
    if (matcher.find()) {
        
        String fprefs = recordText.substring(matcher.start() + 9, matcher.end()).trim();
        group         = fprefs.replaceAll("[0-9]", "");
        recBookingRef = fprefs.replaceAll("[A-Z]", "");
    }

    return "Group: " + group + "\n" + "Booking ref: " + recBookingRef;
  }
  
  public static String fpRecordOld(String recordText)
  {
    String group = "";
    String recBookingRef = "";

    int refPos = recordText.indexOf("NONREFAGT");
    
    if ((refPos > 0) && (recordText.length() > refPos + 9)) {
        if ((recordText.charAt(refPos + 9) >= 'A') &&
                (recordText.charAt(refPos + 9) <= 'Z')) {
    
            // locate ;S or if ;S not found than total length
            int delimPos = StringUtils.searchStringOccur(
                recordText, "/", 2);

            if (delimPos < 0) {
                delimPos = recordText.indexOf("/GBP"); // returns -1, if not found
            }

            if (delimPos < 0) {
                delimPos = recordText.indexOf(";S"); // returns -1, if not found
            }

            if (delimPos < 0) {
                delimPos = recordText.indexOf(";P"); // returns -1, if not found
            }

            if (delimPos < 0) {
                delimPos = recordText.length();
            }
    
            String fprefs = recordText.substring(refPos + 9, delimPos).trim();
            group         = fprefs.replaceAll("[0-9\\+]", "");
            recBookingRef = fprefs.replaceAll("[A-Z\\+]", "");
    
        }
    }

    return "Group: " + group + "\n" + "Booking ref: " + recBookingRef;
  }

  public static String rmRecord(String recordText)
  {
    String group = "";
    String recBookingRef = "";
    String patternString = "#D[A-Z]{1,3}[0-9]{1,10}";
    
    Pattern pattern = Pattern.compile(patternString);
    Matcher matcher = pattern.matcher(recordText);
    
    if (matcher.find()) {
        
        String rmrefs = recordText.substring(matcher.start() + 2, matcher.end());
        group         = rmrefs.replaceAll("[0-9]", "");
        recBookingRef = rmrefs.replaceAll("[A-Z]", "");
    }

    return "Group: " + group + "\n" + "Booking ref: " + recBookingRef;
  }
  
  public static String rmRecordOld(String recordText)
  {
    String group = "";
    String recBookingRef = "";
    
    if ((recordText.substring(0, 6).equals("RM ##D")) || (recordText.substring(0, 5).equals("RM #D"))) {
    
        String rmrefs = recordText.replaceAll("RM ##D","").replaceAll("RM #D","").trim();
        if (group.equals("")) {
            group         = rmrefs.replaceAll("[0-9]", "");
        }
        if (recBookingRef.equals("")) {
            recBookingRef = rmrefs.replaceAll("[A-Z]", "");
        }
        
    }
        
    return "Group: " + group + "\n" + "Booking ref: " + recBookingRef;
  }
  
  public static boolean taxAmt(String taxAmt)
  {
        taxAmt = taxAmt.replaceAll("EXEMPT","0.00");
        return NumberProcessor.validateStringAsNumber(taxAmt);
  }  
  
  public static String recComm(String recordText)
  {
    
		String recCommAmt = "0.0";
        String recCommPct = "0.0";
            
        int startPos = StringUtils.searchStringOccur(
                recordText, "*", 2); // start position
                                             // for amount / pct

        if (startPos < 0) {
            // Assuming recordText starts with "FM" so not startPos = 0
            startPos = 1;
        }
        
        int endPos = recordText.indexOf(";"); // returns -1
                                                   // if not
                                                   // found
        if (endPos < 0) {
            endPos = recordText.length();
        } // Here if no semicolon means it is manual commission
          // eg FM*M*8

        if (recordText.substring(endPos - 1, endPos)
                .equalsIgnoreCase("P")) { // "P" commission pct
            recCommPct = recordText.substring(
                    startPos + 1, endPos - 1);
        } else if (recordText.substring(endPos - 1, endPos)
                .equalsIgnoreCase("A")) { // "A" commission amt
            recCommAmt = recordText.substring(
                    startPos + 1, endPos - 1);
        } else { // other
            recCommAmt = recordText.substring(
                    startPos + 1, endPos);
        }
        
        return ("recCommAmt: " + recCommAmt + " recCommPct: " + recCommPct);
        
  }
  
  public static String collectionAmt(String recordText)
  {
    String lineData = recordText;
    String oneLine  = recordText;
    String collectionAmt = "0.00";
   

    if ((lineData.length() > 3) 
            && (oneLine.substring(0, 3).equalsIgnoreCase("FPO"))) {

        int slashPos = oneLine.indexOf("/GBP");  // pos 26
        int endPos   = oneLine.indexOf(";"); // either -1 for not fo

        // The end position of Amount is either up to first
        // semicolon or up to end of line length
        if (slashPos > 0) {
            if (endPos > 0) {
                collectionAmt = oneLine.substring(slashPos + 4, endPos);
            } else {
                collectionAmt = oneLine.substring(slashPos + 4,	oneLine.length()); 
            }
        }
    }
    
    return ("collectionAmt: " + collectionAmt);
    
  }
  
  public static String recExchangeTicketNo(String recordText)
  {
    String recExchangeTicketNo = "";
    String recExchangeTicketNos = "";
    String maxConjExchTktNum = "";
    int dashPos = 0;
    int dashOccur = 0;
    int foLen = 0;
    int foEndPos = 0;

    String foRecord = "";

    dashOccur = 1;
    dashPos = StringUtils.searchStringOccur(
    recordText, "-", dashOccur);
    if (dashPos == 5) {
         // e.g. FO006-2570769969-70LON24MAY18/91212295/006-25707699694E1234*I;S4-8;P3
         foRecord = recordText.substring(6);
    } else {
         // e,g, :FO1572570111863LON30APR1891278401
         foRecord = recordText.substring(5);
    }    

    foLen = foRecord.length();

    // Loop to the end of the exchange ticket number
    if (foLen > 10) {
        foEndPos = 0;
        while ((foEndPos < foLen) && (recExchangeTicketNos.equals(""))) {
            if ((foRecord.charAt(foEndPos) >= 'A') && (foRecord.charAt(foEndPos) <= 'Z')) {
                recExchangeTicketNos = foRecord.substring(0, foEndPos);
            } else {
                foEndPos = foEndPos + 1;
            }
        }
    }    

   // Check for exchange ticket number range
   dashOccur = 1;
   dashPos = StringUtils.searchStringOccur(
        recExchangeTicketNos, "-", dashOccur);
    if (dashPos > 0) {
        recExchangeTicketNo = recExchangeTicketNos.substring(0,dashPos);
        if ((recExchangeTicketNos.substring(dashPos + 1,
             dashPos + 3)).equalsIgnoreCase("00")) {
                 maxConjExchTktNum = Long.toString(Long
                     .parseLong(recExchangeTicketNo) + 1);
        } else {
             maxConjExchTktNum = recExchangeTicketNo.substring(0, 8)
                 + recExchangeTicketNos.substring(
                     dashPos + 1, dashPos + 3);
        }
    } else {
        recExchangeTicketNo = recExchangeTicketNos;
        maxConjExchTktNum = recExchangeTicketNo;
    } // If no conjunctive ticket it will be defaulted to
     // ticket number}
 
    return("recExchangeTicketNo: " + recExchangeTicketNo);
  }

}



