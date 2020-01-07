package uk.co.firstchoice.stella;

import java.io.*;
import java.sql.*;
import java.util.Date;

import uk.co.firstchoice.util.*;

import uk.co.firstchoice.util.businessrules.DateProcessor;
import uk.co.firstchoice.util.businessrules.NumberProcessor;




/*

Datawarehouse live server connection URL : jdbc:oracle:thin:@dwlive_en1:1521:dwl

Required inputs

Usage Option
-m run mode   (L for Live or T for Test - so can get correct
         parameters from app registry)
if internal connection not to be used:
-d driverClass   ( Driver Class for connecting to ORACLE/SYBASE )
-c connectionURL ( Connects to the database where tables are kept)
-u userid        ( User id  for connecting to database )
-p passwd        ( Password for connecting to database )
-f single filename (run for a single file only instead of all pending)
-x execute database updates - set to NOUPDATE if no update of database needed - just for testing/debug
 */
/**
 * Class to load flat files from BSP tickets financial system
 * into Stella database
 *
 * Creation date: (14/03/03 16:38:07 pM)
 * Contact Information: FirstChoice
 * @version 1.3
 * @author Leigh Ashton
 */

/* change history
 v1.3 Leigh nov03 Added breakdown of tax parts e.g. ub tax
 v1.4 Jyoti Jan05 Change validation for BAR record type and added new validation for new record type BMI
 V1.5 Jyoti Dec05 changed program to incorporate new BSP record sequence
 *  eg: of of record sequence : BFH - BCH - BOH - --------- BOT-BOH-------BCT-BCH-BH -----------BOH ------BOT-BCT-BFT
 *  Previous format had only once BCH
 *
 *V1.6 Jyoti Jan07 Mdofied Validation to allow H&J and Citalia BSP Hot files to allow to be loaded in stella database
 *v1.7 Leigh Sep07 Modified validation to allow a BKP record to come without a BKF preceeding it
 *
 */


public class StellaBSPLoad {


    // Class constants
    private static final String programName = "StellaBSPLoad";
    private static final String programShortName = "BSPLOAD1";
    private static final String programVersion = "1.4";


    // The application object. Provides logging, properties etc
    private static final Application application = new Application(
    programName,
    "Routine to load data from BSP into  data warehouse"
    + " stella system");



    private static    int recSequence = 0;  //used to validate record sequence in file read
    private static    int prevHighSequence = 0;//used to validate record sequence in file read

    private static    String stage; // used to denote place in code where any exception happened


    //next variables used to store data passed to database
    private static        String recFileName = "";
    private static        String recAgentID = "";
    private static        String recDate    = "";
    private static        String recTicketingAirline = "";
    private static        String recCRSCode = "";
    private static        long recTicketNo = 0;
    private static        String recTransCode = "";
    private static        String recConjunctionInd = "";
    private static        double recCommissionableAmt =0;
    private static        double recTaxTotal = 0;  // tax total
    private static        double recUBTax = 0;
    private static        double recGBTax = 0;
    private static        double recRemainingTax = 0;
    private static        double recAirlinePenalty = 0;
    private static        double recBalancePayable = 0;
    private static        double recNetFareAmt =0;
    private static        double recEffectiveCommissionAmt =0;
    private static        String recCcyCode = "";
    private static        double recCommissionAmt =0;
    private static        String recNetRemitInd = "N";




    private static         int countInsertedTrans = 0;
    private static         boolean hadBKS24 = false;
    private static         boolean hadBKS39 = false;
    private static         boolean hadBKS30 = false;
    private static         boolean processedThisTrans = false;
    private static        String wConjunctionInd = "";
    private static        double totalNetFareAmt = 0;
    private static        double totalCommissionAmt = 0;
    private static        double totalTaxTotal = 0;
    private static        double totalCommissionableAmt = 0;
    private static        double totalAirlinePenalty = 0;
    private static        double totalBalancePayable = 0;
    private static        double totalLateReportingAmt = 0;
    private static        double totalEffectiveCommissionAmt = 0;
    private static        double totalDocumentAmt = 0;

    private static        boolean updateDatabase = true;

    private static        double discrepancyAmt =0;
    private static        double totalDiscrepancyAmt =0;

    /** main method ,
     *   called from outside ie.command line.
     *   set up and then loop through all files to be processed
     * @param args arguments from command line incl. database connection , -u user id, -p password,
     * -m mode (TEST or LIVE)
     */
    public static void main(String[] args)
    throws SQLException {


        String   driverClass    = "";
        String   connectionURL  = "";
        String   dbUserID       = "";
        String   dbUserPwd      = "";

        String runMode = "LIVE";
        String singleFileName = "none";
        String updateDB = "";


        // start processing



        System.out.println(programName + " " + programVersion + " started");

        if( args.length == 0 ) {
            System.out.println( "Error: main method. No runtime args specified" );
            System.exit(1);
            return;
        }


        // show run-time params
        String   argvalue     = "" ;
        String   argset       = "";
        for (int i = 0; i < args.length; i++) {
            System.out.print(args[i] + " ");

            // If the argument is -e20 then -e is the exp_id and 20 is the passed argument value

            argvalue = args[i];

            if ((argvalue.length() > 2) && ((argvalue.substring(0,1)).equals("-"))) {

                // split the argument into switch and value
                argset = argvalue.substring(0,2);
                argvalue   = argvalue.substring(2,argvalue.length()  );

                if (argset.equals("-m")) {
                    runMode = argvalue;
                    System.out.println("Runmode:" + runMode);
                }
                else if (argset.equals("-f")) {
                    // run for one file only, not all contents of directory
                    singleFileName = argvalue;
                }
                else if (argset.equals("-d")) {
                    // driverclass
                    driverClass = argvalue;

                }
                else if (argset.equals("-c")) {
                    // connection url
                    connectionURL = argvalue;
                }
                else if (argset.equals("-u")) {
                    dbUserID = argvalue;
                }
                else if (argset.equals("-p")) {
                    dbUserPwd = argvalue;
                }
                else if (argset.equals("-x")) {
                    updateDB = argvalue;
                    if (updateDB.equals("NOUPDATE")) {
                        updateDatabase = false;
                        System.out.println("NO DATABASE UPDATES SET!!!!!!!");

                    }
                    else{
                        updateDatabase = true;
                    }
                }

                else {
                    // unknown parameter
                    System.out.println( "Error: main method. Unrecognised args specified:" + argset + argvalue );
                    System.exit(1);
                    return;
                }

            }
        }

        // actually run the method to do the processing
        String retCode = runBSPLoad( driverClass, connectionURL, dbUserID, dbUserPwd, singleFileName, runMode);
        System.out.println("end, return was:" +retCode);
        if (retCode.substring(0,2).equals("OK") ) { System.exit(0);}
        else { System.exit(1);}

        return;

    }

    // end of main method

    /** runBSPLoad method to load data from bsp files
     * @param driverClass jdbc driver class for database interaction
     * @param connectionURL database conn url
     * @param dbUserID database user id to use
     * @param dbUserPwd database user password
     * @param singleFileName if want to run in singlefile mode then specify a filename
     * (without directory)
     * here, otherwise pass ""
     * @param runMode Test or Live : determines if in debug mode
     * @return return code "OK"  for success, "Error,...." if failure
     */
    public static String runBSPLoad(String driverClass,String  connectionURL,
    String  dbUserID,String  dbUserPwd,String  singleFileName,String runMode)
    throws SQLException {





        String dirData = "", dirBackup = "",  logPath = "",  dirError = "";


        boolean debugMode = false;
        int numFilesRead = 0, numFilesError = 0, numFilesSuccess = 0;

        String logFileName = "";
        String returnStr = "";

        String logFileReturnValue = "";
        String logLevel =""; // min logging level to be logged

        // Create a instance of main class
        StellaBSPLoad   f =  new StellaBSPLoad();




        // create database connection

        // the dbmanager trys to connect to the internal oracle connection
        // first before using these dbconnection parameters
        // better to use internal connection if possible
        // but leave these params avail in case want to run
        // from outside Oracle java stored procedures.

        stage = "setup vars";
        //DBManager dbManager = new DBManager(
        //                                   "jdbc:oracle:thin:@dwdev:1521:dwd",
        //                                   "oracle.jdbc.driver.OracleDriver",
        //                                   "stella",
        //                                    "???????");

        System.out.println( "Database:" + connectionURL +","+ driverClass +","+ dbUserID +","+ dbUserPwd);
        DBManager dbManager = new DBManager(
        connectionURL,
        driverClass,
        dbUserID,
        dbUserPwd);

        try {
            // set up the application object.
            application.setLogger(programName);
            application.setLoggerLevel(Level.ALL.toString()); // default to log all levels
            application.setAccessMode(dbManager.connect());
            stage = "setup registry" ;
            application.setRegistry(programShortName,runMode,"ALL",dbManager.getConnection());

            // find the log file path and create the logger
            // if running from client PC then use local paths
            // otherwise must be running on server

            try {
                // get the correct build path
                stage = "get registry";
                if (application.getAccessMode() == AccessMode.CLIENT) {
                    logPath   = application.getRegisteryProperty("LocalLogFilePath");
                    dirData   = application.getRegisteryProperty("LocalFilePath");
                    dirBackup = application.getRegisteryProperty("LocalBackupPath");
                    dirError  = application.getRegisteryProperty("LocalErrorPath");


                    application.log.info("Database:" + connectionURL +","+ driverClass +","+ dbUserID +","+ dbUserPwd);

                } else {
                    logPath   = application.getRegisteryProperty("ServerLogFilePath");
                    dirData   = application.getRegisteryProperty("ServerFilePath");
                    dirBackup = application.getRegisteryProperty("ServerBackupPath");
                    dirError  = application.getRegisteryProperty("ServerErrorPath");


                }


                logLevel = application.getRegisteryProperty("LogLevel");



            } catch (PropertyNotFoundException ex) {

                application.log.severe( "Error: unable to find registry property. " + ex.getMessage() );

                return "Error: unable to find registry property. " + ex.getMessage();
            }
            //fileUtils myFU = new fileUtils();
            application.setLoggerLevel(logLevel);

            application.setLogSysOut(false); // don't log to sysout as well as to file
            //get bst time here

            logFileName =  programShortName.toLowerCase() + "_" +  ( FileUtils.fileGetTimeStamp()) + ".log";
            application.log.config("Log level is:" + logLevel);
            application.log.config("Logfile is: " + logFileName);
            System.out.println("Logfile is: " + logFileName);
            application.log.setLoggerFile(new File(logPath, logFileName));


            logFileReturnValue = "|Lf#" + logPath +"/" +  logFileName;
            // returned at end of program so that calling program knows log file name
            //
            // Write out header to the log file
            //
            application.log.info(programName);
            application.log.config("Runmode:" + runMode);
            application.log.info("START " + java.util.Calendar.getInstance().getTime());
            application.log.config("Access mode:" + application.getAccessMode());

            try {
                application.log.config("Name         => " + application.getRegisteryProperty("Name"));
                application.log.config("Version      => " + application.getRegisteryProperty("Version"));
                application.log.config("LogPath      => " + logPath );
                application.log.config("FilePath     => " + dirData );
                application.log.config("Backuppath   => " + dirBackup );
                application.log.config("Errorpath    => " + dirError );



            } catch (PropertyNotFoundException ex) {
                //return null;

                return "Error: unable to find registry property.(2nd attempt) " + ex.getMessage() + logFileReturnValue;
            }

            if (application.getRegisteryProperty("DebugMode").equals("Y") ) {
                application.log.info("Debug mode set");
                debugMode = true;}
            else {
                debugMode = false;
            }


            stage = "check dirs";

            // now check all file permissions and directories

            File fileDataDirectory = new File(dirData);
            File backupDataDirectory = new File(dirBackup);
            File errorDataDirectory = new File(dirError);
            String newFileName = "";
            if (fileDataDirectory.isDirectory()) {
                application.log.info("data :" + fileDataDirectory.getAbsolutePath() +
                " exists OK.");
            }
            else {
                returnStr = "ERROR data :" + dirData + " does not exist.";
                application.log.severe(returnStr);
                return returnStr +logFileReturnValue;
            }
            if (backupDataDirectory.isDirectory()) {
                application.log.info("data :" + backupDataDirectory.getAbsolutePath() +
                " exists OK.");
            }
            else {

                returnStr = "ERROR data :" + dirBackup + " does not exist.";
                application.log.severe(returnStr);
                return returnStr +logFileReturnValue;

            }

            if (errorDataDirectory.isDirectory()) {
                application.log.info("data :" + errorDataDirectory.getAbsolutePath() +
                " exists OK.");
            }
            else {
                returnStr = "ERROR data :" + dirError + " does not exist.";
                application.log.severe(returnStr);
                return returnStr + logFileReturnValue;
            }

            // get contents of data directory
            stage = "list files";

            String[] contents = fileDataDirectory.list();
            application.log.info(contents.length + " files in data directory:");
            if (debugMode) {
                application.log.info("List of files to be proc:");
                for (int i=0; i < contents.length; i++) {
                    File indFile = new File(fileDataDirectory.getAbsolutePath(), contents[i]);
                    application.log.info(contents[i] + "  " + new Date(indFile.lastModified()));
                }
            }




            stage = "process files";
            File fileToProcess = null;
            for (int i=0; i < contents.length; i++) {


                // now process each file
                if (singleFileName.equals( "none")) {
                    // normal run mode, process ALL files in dir
                    fileToProcess = new File(fileDataDirectory.getAbsolutePath(),contents[i]);
                }
                else {
                    // single file name passed from command line parameters,
                    // check it exists
                    application.log.info("Single file mode:" + singleFileName);
                    fileToProcess = new File(fileDataDirectory, singleFileName);
                }

                if (fileToProcess.exists()) {
                    // ok
                }
                else {

                    returnStr = "ERROR datafile :" + fileToProcess + " does not exist.";
                    application.log.severe(returnStr);
                    return returnStr + logFileReturnValue;
                }
                if (fileToProcess.canRead()) {
                    // ok
                }
                else {

                    returnStr = "ERROR datafile :" + fileToProcess +
                    " cannot be read.";
                    application.log.severe(returnStr);
                    return returnStr + logFileReturnValue;

                }
                application.log.fine(""); // blank line to separate files in log
                application.log.info("File " + (i+1) + " :" + fileToProcess + " "
                + fileToProcess.length() + " bytes" + " at:"   + java.util.Calendar.getInstance().getTime());


                // now have valid filehandle




                stage = "about to processFile";

                int intReturn = f.processFile(fileToProcess, debugMode, dbManager.getConnection());

                stage = "eval return from processFile";

                // 0 for success
                // 1 for error

                switch (intReturn) {
                    case 0:
                        // success, move file to backup area
                        // check to see if it exists already, if so then add a suffix
                    {
                        File moveFile = new File(backupDataDirectory, fileToProcess.getName());
                        if (moveFile.exists() ) {
                            moveFile = new File(backupDataDirectory, fileToProcess.getName() + ".1");
                        }
                        if (fileToProcess.renameTo( moveFile)) {
                            // successful move

                        } else {
                            returnStr = "ERROR datafile :" + fileToProcess +" cannot be moved to backup.";
                            application.log.severe(returnStr);
                            application.log.info("data NOT rolled back");
                            return returnStr + logFileReturnValue;

                        }
                        numFilesSuccess ++;
                        application.log.fine(fileToProcess + " processed OK");
                        break;
                    }
                    case 1:
                        // critical error , log and stop run
                        application.log.info("data rolled back");
                        dbManager.getConnection().rollback();
                        returnStr = "CRITICAL ERROR datafile :" + fileToProcess +
                        " failed, renamed as error file";
                        // rename with error suffix and move to error directory
                        application.log.info("ERROR datafile :" + fileToProcess +
                        " failed, renamed as error file");
                        if ( !fileToProcess.getName().substring(0,3).equals("err") ) {
                            newFileName = "err_" + fileToProcess.getName();
                        }
                        else {
                            newFileName = fileToProcess.getName();
                        }

                        File moveFile = new File(errorDataDirectory, newFileName);
                        if (fileToProcess.renameTo(moveFile )) {
                            // successful move
                            application.log.info(fileToProcess + " renamed as error, moved to error area");
                        } else {
                            returnStr = "ERROR datafile :" + fileToProcess +" cannot be renamed as error file";
                            application.log.severe(returnStr);
                            return returnStr + logFileReturnValue;

                        }
                        application.log.severe(returnStr);
                        return returnStr + logFileReturnValue;



                    default:
                        returnStr = "ERROR invalid return from processFile :" + fileToProcess +	intReturn;
                        application.log.severe(returnStr);
                        application.log.info("data rolled back");
                        dbManager.getConnection().rollback();
                        return returnStr + logFileReturnValue;

                } // end case


                if (!singleFileName.equals("none")) {break;
                }

            } // end for loop through files




            numFilesRead = contents.length;

        } catch (Exception ex) {
            application.log.info("data rolled back");
            dbManager.getConnection().rollback();
            returnStr = "Failed while " + stage + ". [Exception " +
            ex.getClass() + " " + ex.getMessage() + "]";
            application.log.severe(returnStr);
            application.log.severe(String.valueOf(ex));
            ex.printStackTrace();
            ByteArrayOutputStream ostr = new ByteArrayOutputStream();
            ex.printStackTrace(new PrintStream(ostr));
            application.log.severe(ostr.toString());

            return returnStr + logFileReturnValue;
        }

        dbManager.getConnection().commit();


        dbManager.shutDown();


        if (numFilesRead == 0) {
            application.log.warning("WARNING!!! No files were found to process");
        }

        application.log.info("");
        application.log.info("Files Read:" + numFilesRead);
        if (!singleFileName.equals("none")) {
            application.log.info("Was in SingleFileMode:" + singleFileName);
        }
        application.log.info("Files in success:" + numFilesSuccess);
        application.log.info("Files in error:" + numFilesError);




        application.log.info("COMPLETE " + java.util.Calendar.getInstance().getTime());

        application.log.close();


        // end of class - report to console as well as log

        System.out.println("Files Read:" + numFilesRead);
        System.out.println("Files in success:" + numFilesSuccess);
        System.out.println("Files in error:" + numFilesError);
        System.out.println("Num BSP records inserted:" + countInsertedTrans );

        System.out.println(programName + " ended");

        return "OK"  + logFileReturnValue;


    }






    /** read and process each individual file
     * @param fileToProcess filename of an individual file to be processed
     * @param showContents true if debug on, false if not on
     * @param conn the connection object
     * @return 0 - success, 1 - critical error
     */

    public static int processFile(File fileToProcess, boolean showContents, Connection conn)
    throws SQLException {
        /** read and process each individual file */


        String stage = "start of processFile";
        application.log.info("JDBC Autocommit mode was:" + conn.getAutoCommit() + ", BUT now switching to OFF" );
        conn.setAutoCommit(false);

        BufferedReader fileIn = null;
        try {


            FileReader theFile;

            PreparedStatement lookupStmt;

            int recsRead = 0;
            String fileName = "";
            ResultSet rs;
            application.log.finest( "File: " + fileToProcess.getName() );

            // now actually process the file


            stage = "setting up file to process";

            // working variables

            theFile = new FileReader( fileToProcess );
            fileIn  = new BufferedReader( theFile );
            BSPRecord fileRec = new BSPRecord();
            BSPRecord prevRecord = new BSPRecord();
            String expectedRecType="";
            recsRead = 0;
            String message ="";
            int highTrans = 0;
            int prevHighTrans = 0;
            double tax1 = 0;  // used for tax total
            double tax2 = 0;  // used for tax total
            double airlinePenalty = 0;

            double tranRemittanceAmt =0;
            String recTaxType = "";
            int countBKS24 = 0;

            String prevBKSType = "";

            boolean checkDup = false;
            // variables used to input data to database
            String filePeriod= "";

            recFileName = fileToProcess.getName();

            stage = "about to read file";
            fileRec = readRecord(fileIn, showContents, fileToProcess, recsRead);
            recsRead ++;

            prevHighTrans = 0; // BKT record transaction counter

            expectedRecType = "BFH";
            if (!fileRec.recordID.equals(expectedRecType) ) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " invalid batch header record (s/be " +  expectedRecType + "):" + fileRec.recordID );
                return 1;
            }
            application.log.info("File Header record:" + fileRec.recordText);

            // now should get BCH
           /* prevRecord = fileRec;
            fileRec = readRecord(fileIn, showContents, fileToProcess, recsRead);
            recsRead ++;

            isRecordTypeSeqValid(prevRecord.recordID,fileRec.recordID, fileRec.recordText, fileToProcess.getName());
            application.log.info("Batch Hdr:" + fileRec.recordText);  // BCH record
            filePeriod = fileRec.recordText.substring(17,23);  // in format yymmdd
            if (!NumberProcessor.validateStringAsNumber(filePeriod)) {
                application.log.severe("ERROR datafile :" + fileToProcess + " file date non-numeric:" + filePeriod + " (record was:" + fileRec.recordText + ")");
                return 1;}
            application.log.info("covers period to " + filePeriod);
         */


            // check to see if bsp_transaction table already has an entry in it for
            // this filename
          /*  int countFiles = 0;
            try {

                fileName = fileToProcess.getName();

                lookupStmt = conn.prepareStatement
                (             " SELECT count(*) count" +
                " FROM bsp_transaction " +
                " WHERE bsp_period_ending_date = ? ");


                lookupStmt.clearParameters();
                lookupStmt.setDate(1,new java.sql.Date( DateProcessor.parseDate(filePeriod,"yyMMdd").getTime()));

                rs = lookupStmt.executeQuery();

                if (rs.next()) {
                    countFiles = rs.getInt("count");
                }

                if (countFiles > 0 ) {
                    // have alread seen this filename, ERROR
                    application.log.severe("CRITICAL ERROR datafile :" + fileToProcess + " " + filePeriod + " data already exists in BSP data. Processed already?" );
                    return 1;
                }


            }
            catch (SQLException ex) {
                ex.printStackTrace();
                throw new PropertyNotFoundException("SQL Error while retrieving filename count: "
                + " filename was "  + fileName
                + " SQLErr " + ex.getErrorCode()
                + " SQLMsg " + ex.getMessage());
            } // end try/catch for sql to get countof filenames
            */
            // now should get BOH
            prevRecord = fileRec;
            fileRec = readRecord(fileIn, showContents, fileToProcess, recsRead);
            recsRead ++;

            if (fileRec.recordID == null ) {
                // unexpected EOF
                message =   "ERROR i/o error datafile :" + fileToProcess + " unexpected end of file,last record was " + prevRecord.recordText ;
                application.log.severe(message );
                return 1;
            }

            if (!isRecordTypeSeqValid(prevRecord.recordID,fileRec.recordID, fileRec.recordText, fileToProcess.getName())) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " invalid record sequence (prev was " + prevRecord.recordID + "), current is " + fileRec.recordText);
                return 1;
            }

            stage = "about to loop read records";
            // now repeat til file trailer record met, or end of file
            // should be loop of one or a number of BOH records
            //while ( !fileRec.recordID.equals("BCT") ) {
              while ( !fileRec.recordID.equals("BFT") ) {  // read till last footer line

                 stage = "about to process BCH";
                 if (fileRec.recordID.equals("BCH") ) {

                isRecordTypeSeqValid(prevRecord.recordID,fileRec.recordID, fileRec.recordText, fileToProcess.getName());
                application.log.info("Batch Hdr:" + fileRec.recordText);  // BCH record
                filePeriod = fileRec.recordText.substring(17,23);  // in format yymmdd
                if (!NumberProcessor.validateStringAsNumber(filePeriod)) {
                    application.log.severe("ERROR datafile :" + fileToProcess + " file date non-numeric:" + filePeriod + " (record was:" + fileRec.recordText + ")");
                    return 1;}


                 }

                 // office batch header, BOH
                 stage = "about to process boh";
                 if (fileRec.recordID.equals("BOH") ) { // check if file is already loaded , check once

                 	// get iata number , this one is moved up before if
                    recAgentID = fileRec.recordText.substring(13,21);
                    if (!NumberProcessor.validateStringAsNumber(recAgentID)) {
                        application.log.severe("ERROR datafile :" + fileToProcess + " BOH agentID non-numeric:" + recAgentID + " (record was:" + fileRec.recordText + ")");
                        return 1;}


                    stage = "checking if file has already been loaded";
                    if (!checkDup) {
                    recDate = fileRec.recordText.substring(21,27);
                     application.log.info("covers period to " + filePeriod);

                    checkDup = true;  // don't  do it again for another BOH
                  // check to see if bsp_transaction table already has an entry in it for this filename
                   //  H&J and Citalia files are comign as a seperate BSP file and they have to be loaded , change validation to include iata number ,
                    // so key in select below will be (bsp period ending date + iata no )
                                        int countFiles = 0;
                                try {
                                        fileName = fileToProcess.getName();
                                        lookupStmt = conn.prepareStatement
                                        (             " SELECT count(*) count" +
                                                      " FROM bsp_transaction " +
                                                      " WHERE bsp_period_ending_date = ? and " +
													  " iata_num = ? ");

                                        lookupStmt.clearParameters();
                                        lookupStmt.setDate(1,new java.sql.Date( DateProcessor.parseDate(recDate,"yyMMdd").getTime()));
										lookupStmt.setString(2,recAgentID.trim());

                                        rs = lookupStmt.executeQuery();

                                        if (rs.next()) {
                                            countFiles = rs.getInt("count");
                                        }
                                        if (countFiles > 0 ) {
                                            // have alread seen this filename, ERROR
                                            application.log.severe("CRITICAL ERROR datafile :" + fileToProcess + " " + filePeriod + " data already exists in BSP data. Processed already?" );
                                            return 1;
                                        }
                                    } catch (SQLException ex) {
                                        ex.printStackTrace();
                                        throw new PropertyNotFoundException("SQL Error while retrieving filename count: "
                                        + " filename was "  + fileName
                                        + " SQLErr " + ex.getErrorCode()
                                        + " SQLMsg " + ex.getMessage());
                                        } // end try/catch for sql to get countof filenames
                    } // checkDup , do onle once


                    //recDate = fileRec.recordText.substring(21,27);
                    /* 7/12/05 Jyoti , commented so new format bsp file can go through
                    if (!recDate.equals(filePeriod) ) {
                        application.log.severe("ERROR datafile :" + fileToProcess + " Different period date (" + recDate + ") in BOH to file header (" + filePeriod + ")");
                        return 1;
                    } */
                }  // end BOH processing

                if (fileRec.recordID.equals("BKT") ) {
                    // transaction header
                    // first validate
                    //check sequence of transactions is correct , should increment by one each BKT record
                    stage = "about to process " + fileRec.recordID;
                    if (hadBKS24 && !processedThisTrans ) {
                        application.log.severe("ERROR datafile :" + fileToProcess + " BKT trans but haven't finished prcoessing last trans:" + highTrans + " vs." + prevHighTrans + " (record was:" + fileRec.recordText + ")");
                        return 1;
                    }

                    hadBKS24 = false;
                    hadBKS39 = false;
                    hadBKS30 = false;
                    processedThisTrans = false;

                    // reset database variables



                    recTicketingAirline = "";
                    recCRSCode = "";
                    recTicketNo = 0;
                    recTransCode = "";
                    wConjunctionInd = "";
                    recCommissionableAmt =0;
                    recTaxTotal = 0;
                    recGBTax = 0;
                    recUBTax = 0;
                    recRemainingTax = 0;
                    recAirlinePenalty = 0;
                    recBalancePayable = 0;
                    recNetFareAmt =0;
                    recCcyCode = "";
                    recCommissionAmt =0;
                    recEffectiveCommissionAmt =0;
                    recNetRemitInd = "N";






                    highTrans = fileRec.transSeq;
                    if (highTrans != prevHighTrans + 1) {
                        application.log.severe("ERROR datafile :" + fileToProcess + " BKT transactions out of seq:" + highTrans + " vs." + prevHighTrans + " (record was:" + fileRec.recordText + ")");
                        return 1;
                    }

                    prevHighTrans = highTrans;

                    recTicketingAirline = fileRec.recordText.substring(50,55).trim();
                    if (!NumberProcessor.validateStringAsNumber(recTicketingAirline)) {
                        application.log.severe("ERROR datafile :" + fileToProcess + " BKT airline non-numeric:" + recTicketingAirline + " (record was:" + fileRec.recordText + ")");
                        return 1;
                    }

                    recCRSCode = fileRec.recordText.substring(100,104).trim(); // source of transaction

                    recNetRemitInd = fileRec.recordText.substring(43,45).trim();
                    if (recNetRemitInd.equals("NR")) {
                        recNetRemitInd = "Y";
                    }
                    else {
                        recNetRemitInd = "N";
                    }
                    recTaxTotal = 0;  // reset
                    recUBTax = 0;
                    recGBTax = 0;
                    recRemainingTax = 0;
                    recNetFareAmt =0;
                    recCommissionableAmt = 0;
                    recCommissionAmt = 0;
                    recEffectiveCommissionAmt =0;
                    recAirlinePenalty = 0;
                    recBalancePayable = 0;

                } // end of BKT processing



                if (fileRec.recordID.equals("BKS") ) {
                    // ticket id record
                    // first validate

                    stage = "about to process " + fileRec.recordID;
                    // it should be same sequence as corresponding BKT record above it
                    if (fileRec.transSeq !=  highTrans)  {
                        application.log.severe("ERROR datafile :" + fileToProcess + " BKT seq num out of sequence:" + fileRec.transSeq + " vs. " + highTrans + " (record was:" + fileRec.recordText + ")");
                        return 1;
                    }



                    // 24 indicates the first one in a batch

                    if (fileRec.bksType.equals("24")) {
                        // ticketID record
                        // now populate necessary fields
                        countBKS24 ++;

                        wConjunctionInd = fileRec.recordText.substring(61,64).trim();
                        if (wConjunctionInd.equals("CNJ")  ) {
                            // conjunction ticket, need to process what we already have within
                            // this BKT batch
                            // before we can process this conjunction one
                            // call stored proc here -- needs to be in a subroutine so can be
                            // called from
                            // different places
                            // again need to validate flags
                            // now insert record to database
                            wConjunctionInd = ""; // only the second in the pair should be set to Y in database
                            application.log.finest("loading cnj");
                            if (!insertTransaction(conn, showContents)) {
                                application.log.severe("ERROR datafile :" + fileToProcess + " error inserting transaction to database (cnj)");
                                return 1;
                            }
                            processedThisTrans = false; // we have processed the previous 24, but not this one
                            wConjunctionInd = "CNJ"; // only the second in the pair should be set to Y in database
                            recTaxTotal = 0;  // reset amts to 0 for conjunction
                            recGBTax = 0;
                            recUBTax = 0;
                            recRemainingTax = 0;
                            recNetFareAmt =0;
                            recCommissionableAmt = 0;
                            recCommissionAmt = 0;
                            recEffectiveCommissionAmt =0;
                            recAirlinePenalty = 0;
                            recBalancePayable = 0;

                        }
                        recTicketNo = fileRec.ticketNo;
                        recTransCode = fileRec.recordText.substring(92,95).trim();
                        hadBKS24 = true;

                    } // end of 24 BKS rec


                    else if (fileRec.bksType.equals("30")) {
                        // document amounts record
                        // can get more than one record in succession if more than 2 taxes are applicable
                        // amt fields are reset each BKT record
                        if (!prevBKSType.equals("24") && !prevBKSType.equals("30")) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS seq type invalid - out of sequence?:"+ fileRec.bksType + ", BKS24 was missing (record was:" + fileRec.recordText + ")");
                            return 1;
                        }

                        // now check we are talking about same ticket as the bks24 above it
                        if (fileRec.ticketNo != recTicketNo ) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS30 seq tkt invalid - diff to bks24:"+ recTicketNo + ", (record was:" + fileRec.recordText + ")");
                            return 1;
                        }

                        if (fileRec.transSeq !=  highTrans)  {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS30 trans seq num out of sequence:" + fileRec.transSeq + " vs. " + highTrans + " (record was:" + fileRec.recordText + ")");
                            return 1;
                        }

                        // this is the published fare
                        recCommissionableAmt += ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(41,52).trim());

                        // this is the selling fare
                        recNetFareAmt += ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(52,63).trim());

                        // potentially two amounts representing taxes on the record

                        // we don't care what parts make up the tax, just the total
                        // but need to exclude CP tax type which is for airline penalty and is recorded separately
                        recTaxType = fileRec.recordText.substring(64,66).trim();
                        application.log.fine("taxtype:" + recTaxType + tax1);
                        if (!recTaxType.equals("CP")) {
                            tax1 = ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(72,83).trim());
                            if (recTaxType.equals("UB")) {
                                recUBTax += tax1;
                            }
                            else {
                                if (recTaxType.equals("GB")) {
                                    recGBTax += tax1;
                                }
                                else {
                                    recRemainingTax += tax1;
                                }
                            }
                        }
                        else {
                            // CP airline penalty -- only usually used for RFND transactions for Galileo sourced refunds
                            airlinePenalty = airlinePenalty + ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(72,83).trim());
                        }
                        application.log.fine("taxtype:" + recTaxType + tax1);

                        recTaxType = fileRec.recordText.substring(83,85).trim();
                        if (!recTaxType.equals("CP")) {
                            tax2 = ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(91,102).trim());
                            if (recTaxType.equals("UB")) {
                                recUBTax += tax2;
                            }
                            else {
                                if (recTaxType.equals("GB")) {
                                    recGBTax += tax2;
                                }
                                else {
                                    recRemainingTax += tax2;
                                }
                            }
                        }
                        else {
                            // CP airline penalty
                            airlinePenalty = airlinePenalty + ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(91,102).trim());
                        }
                        //application.log.fine("taxtype:" + recTaxType + tax2);

                        // capture the balance payable field -- called the document total in the bsp spec
                        recBalancePayable += ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(102,113).trim());



                        totalLateReportingAmt += ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(113,124).trim());

                        // there can be two or more bks30 records if more than 2 applicable taxes
                        // so need to get total of these records
                        recTaxTotal += tax1 + tax2;
                        tax1 = 0;
                        tax2 = 0;
                        recAirlinePenalty += airlinePenalty;
                        airlinePenalty = 0;





                        recCcyCode = fileRec.recordText.substring(129,132).trim();


                        hadBKS30 = true;


                    } // end of BKS30

                    else if (fileRec.bksType.equals("39")) {
                        // commission record

                        // amt fields are reset each BKT record
                        if (!prevBKSType.equals("30") ) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS seq type invalid - out of sequence 39?:"+ fileRec.bksType + ", BKS30 was missing (record was:" + fileRec.recordText + "), prev was:" + prevBKSType);
                            return 1;
                        }

                        // now check we are talking about same ticket as the bks24 above it
                        if (fileRec.ticketNo != recTicketNo ) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS39 seq tkt invalid - diff to bks24:"+ recTicketNo + ", (record was:" + fileRec.recordText + ")");
                            return 1;
                        }

                        if (fileRec.transSeq !=  highTrans)  {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS39 trans seq num out of sequence:" + fileRec.transSeq + " vs. " + highTrans + " (record was:" + fileRec.recordText + ")");
                            return 1;
                        }


                        recCommissionAmt = ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(55,66));
                        recEffectiveCommissionAmt = ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(93,104));
                        // commission stored as debit (-ve) in file





                        if (!fileRec.recordText.substring(129,132).trim().equals(recCcyCode) ) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS39 ccy different to bks30: vs. " + recCcyCode + " (record was:" + fileRec.recordText + ")");
                            return 1;
                        }

                        hadBKS39 = true;


                    } // endof bks39 record


                    else {
                        // some other unexpected BKS rec type -- skip it, we don't need it

                    }

                    prevBKSType = fileRec.bksType;  // store the BKS type last encountered so can validate flow
                } // end of BKS processing


                if (fileRec.recordID.equals("BKP") ) {
                    // first validate



                    stage = "about to process " + fileRec.recordID;
                    if (fileRec.transSeq !=  highTrans)  {
                        application.log.severe("ERROR datafile :" + fileToProcess + " BKP trans seq num out of sequence:" + fileRec.transSeq + " vs. " + highTrans + " (record was:" + fileRec.recordText + ")");
                        return 1;
                    }

                    // we sometimes get more than one bkp record in succession. this seems to be where
                    // there are multiple payment types. We don't care about this, so need to skip
                    // successive ones
                    // but ensure we do actually do the insert once -- when we encounter the CA one



                    if (fileRec.bksType.equals("84")  ) {
                        if (fileRec.recordText.substring(25,27).trim().equals("CA") ) {


                            // cash form of payment record -- present for every transaction
                            // total for this transaction
                            // use as control check for each transaction
                            // remittance amt should be document amt less effective commission
                            if (!wConjunctionInd.equals("CNJ")) {
                                tranRemittanceAmt =  ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(111,122).trim());
                                discrepancyAmt    = tranRemittanceAmt -
                                ((Math.round((recBalancePayable * 100)) / (double)100) + (Math.round((recEffectiveCommissionAmt * 100)) / (double)100));
                                discrepancyAmt = ((Math.round((discrepancyAmt * 100)) / (double)100));
                                if (discrepancyAmt != 0) {
                                    // mismatch between document total and remittance amt
                                    application.log.finest("recbalpayable" + recBalancePayable);
                                    application.log.finest("receffectivecomm" + recEffectiveCommissionAmt);
                                    application.log.finest("bkp84 remittance amt:" + tranRemittanceAmt);
                                    application.log.severe("datafile :" + fileToProcess +
                                    " BKP remittance cross check discrep:"
                                    + tranRemittanceAmt + " vs. " +(recBalancePayable + recEffectiveCommissionAmt) + " (tkt: " + recTicketNo + " filerecord was:" + fileRec.recordText + "), discrep was:" + discrepancyAmt + ". Due to credit card internet payment?");
                                    //return 1; // allow to continue - this is only a warning
                                    totalDiscrepancyAmt += discrepancyAmt;
                                }
                            } // if cnj



                            if (!processedThisTrans) {

                                // insert the record here

                                // first validate we have encountered all necessary types of record to get all data
                                if (wConjunctionInd.equals("CNJ")) {
                                    if (!hadBKS24) {
                                        application.log.severe("ERROR datafile :" + fileToProcess + " BKP, but conjn. tkt insuffient recs encountered to form data" + " (record was:" + fileRec.recordText + ")");
                                        return 1;
                                    }
                                }
                                else   {
                                    if (!hadBKS24 || !hadBKS39 || !hadBKS30) {
                                        application.log.severe("ERROR datafile :" + fileToProcess + " BKP, but insuffient recs encountered to form data" + " (record was:" + fileRec.recordText + ")");
                                        return 1;
                                    }
                                }

                                // now insert record to database
                                if (!insertTransaction(conn, showContents)) {
                                    application.log.severe("ERROR datafile :" + fileToProcess + " error inserting transaction to database (bkp)");
                                    return 1;
                                }


                            }  // end if processed this trans

                            if (countBKS24 != countInsertedTrans ) {
                                application.log.severe("ERROR datafile :" + fileToProcess +
                                " end of file, not all bks24 loaded (inserted " + countInsertedTrans +
                                " vs. bks24:" + countBKS24 + ")");
                                return 1;
                            }




                        } // if CA
                    }  // if 84



                } // end of BKP processing

                if (fileRec.recordID.equals("BKI") ) {
                    // itinerary record
                    // not used, so skip
                } // end of BKI processing
                if (fileRec.recordID.equals("BAR") ) {
                    // payment record
                    // not used, so skip
                } // end of BAR processing



                // end of BOH iteration
                // now read the next record which should either be another BOH,
                // or a BCT (if approaching end of file)
                stage = "about to read next rec";
                prevRecord = fileRec;
                fileRec = readRecord(fileIn, showContents, fileToProcess, recsRead);
                recsRead ++;

                if (fileRec.recordID == null ) {
                    // unexpected EOF
                    message =   "ERROR i/o error datafile :" + fileToProcess + " unexpected end of file,last record was " + prevRecord.recordText ;
                    application.log.severe(message );
                    return 1;
                }

                if (!isRecordTypeSeqValid(prevRecord.recordID,fileRec.recordID, fileRec.recordText, fileToProcess.getName())) {
                    application.log.severe("ERROR i/o error datafile :" + fileToProcess + " invalid record sequence (prev was " + prevRecord.recordID + "), current is " + fileRec.recordText);
                    return 1;
                }



            } // end while within loop of BOH






            // must have now a BCT record, previous will have been BOT

            // get any values needed from BCT



            // read next line
            stage = "end of reading loop";
            /*
            prevRecord = fileRec;
            fileRec = readRecord(fileIn, showContents, fileToProcess, recsRead);
            recsRead ++;
            */

            // should be a BFT
            if (fileRec.recordID == null ) {
                // unexpected EOF
                message =   "ERROR i/o error datafile :" + fileToProcess + " unexpected end of file,last record was " + prevRecord.recordText ;
                application.log.severe(message );
                return 1;
            }

            if (!isRecordTypeSeqValid(prevRecord.recordID,fileRec.recordID, fileRec.recordText, fileToProcess.getName())) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " invalid record sequence (prev was " + prevRecord.recordID + "), current is " + fileRec.recordText);
                return 1;
            }


            application.log.fine("BFT reached:" + fileRec.recordText);

            totalEffectiveCommissionAmt = (Math.round((totalEffectiveCommissionAmt * 100)) / (double)100);
            totalAirlinePenalty = (Math.round((totalAirlinePenalty * 100)) / (double)100);
            totalTaxTotal = (Math.round((totalTaxTotal * 100)) / (double)100);
            totalLateReportingAmt = (Math.round((totalLateReportingAmt * 100)) / (double)100);
            totalDocumentAmt =  (Math.round((totalDocumentAmt * 100)) / (double)100);
            totalCommissionableAmt =  (Math.round((totalCommissionableAmt * 100)) / (double)100);

            // can do totals checks here
            if (totalEffectiveCommissionAmt != ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(58,73)) ) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " EFFECTIVE COMM TOTALS NOT MATCHING (on BFT record) " + totalEffectiveCommissionAmt + "vs. "
                + ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(58,73)) );
                return 1;
            }

            // Total Tax/Miscellaneous Fee Amount
            if  ( (Math.round(((totalAirlinePenalty + totalTaxTotal) * 100)) / (double)100) != ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(73,88)) ) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " TAX + PENALTIES TOTALS NOT MATCHING (on BFT record) " + (totalAirlinePenalty + totalTaxTotal) + " vs. "
                + ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(73,88)) );
                application.log.info("total of tax           :" + totalTaxTotal);
                application.log.info("total of airlinepenalty:" + totalAirlinePenalty);
                return 1;
            }

            // late reporting penalty
            if (totalLateReportingAmt != ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(88,103)) ) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " late reporting amt TOTALS NOT MATCHING (on BFT record) " + totalLateReportingAmt + "vs. "
                + ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(88,103)) );
                return 1;
            }
            // gross amt
            if (totalDocumentAmt != ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(28,43)) ) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " gross amt TOTALS NOT MATCHING (on BFT record) " + totalDocumentAmt
                + "vs. " + ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(28,43)) );
                return 1;
            }

            // remittance amt -- this is the amount that we get invoiced for by BSP
            double fileTotalRemittance =0;
            fileTotalRemittance = ConversionBSPAmt.parseLastCharAmt(fileRec.recordText.substring(43,58));

            // now should be EOF, have already had a null
            fileRec = readRecord(fileIn, showContents, fileToProcess, recsRead);


            if (fileRec != null) {
                // eof not reached
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " end of file expected, but got "+ fileRec.recordText);
                return 1;
            }

            // now have finished the whole file

            // now check checksums against what we have read in the file
            application.log.info("Count of inserted trans:" + countInsertedTrans);
            application.log.info("Count of BKS/24 trans  :" + countBKS24);
            application.log.info("total of net fare      :" + totalNetFareAmt);
            application.log.info("total of comm amt      :" + totalCommissionAmt);
            application.log.info("total of tax           :" + totalTaxTotal);
            application.log.info("total of commable amt  :" + totalCommissionableAmt);
            application.log.info("total of airlinepenalty:" + totalAirlinePenalty);
            application.log.info("total of rfnd bal payab:" + totalBalancePayable);
            application.log.info("total of late report   :" + totalLateReportingAmt        );
            application.log.info("total of effectcomm amt:" + totalEffectiveCommissionAmt);
            application.log.info("total of document amt  :" + totalDocumentAmt);
            application.log.info("total of bkp discreps  :" + totalDiscrepancyAmt);
            application.log.info("file says total of remittance amt  :" + fileTotalRemittance);

            // gross amt
            discrepancyAmt = totalCommissionableAmt - (totalDocumentAmt - totalAirlinePenalty - totalTaxTotal);
            discrepancyAmt = ((Math.round((discrepancyAmt * 100)) / (double)100));
            if (discrepancyAmt != 0 ) {
                application.log.severe("ERROR i/o error datafile :" + fileToProcess + " commable amt cross check TOTALS NOT MATCHING (after BFT record) discrep:" + discrepancyAmt );
                return 1;
            }

            // note that total effective commission is always negative
            discrepancyAmt = fileTotalRemittance - (totalDocumentAmt + totalEffectiveCommissionAmt);
            discrepancyAmt = ((Math.round((discrepancyAmt * 100)) / (double)100));
            if (discrepancyAmt != 0) {
                // this can happen remittance amt is not the same as the cash amount from bkp records
                // according to bsp spec this can happen if credit card internet transactions are made -- they shouldn't be!
                application.log.severe("i/o error datafile :" + fileToProcess + " total remittance cross check TOTALS NOT MATCHING (after BFT record). Could be because of credit card transactions");
                application.log.severe("That total remittance discrepancy was :" + discrepancyAmt);
                application.log.info("Program continued");
            }




            if (!updateDatabase) {
                System.out.println("NO DATABASE UPDATES WERE DONE!!!!!!!");
            }


            if (countBKS24 != countInsertedTrans ) {
                application.log.severe("ERROR datafile :" + fileToProcess +
                " end of file, not all bks24 loaded " + countInsertedTrans +
                " vs. " + countBKS24);
                return 1;
            }





            ////////////////////////////////////////////////////////
            ////////////////////////////////////////////////////////
            ////////////////////////////////////////////////////////
            stage = "finished with file";

            // no errors , processing OK, return OK return code
            conn.commit();
            return 0;

        } // end main try of this method


        catch( IOException e )
        {  System.out.println( e );
           application.log.severe("ERROR i/o error datafile :" + fileToProcess );
           application.log.severe(String.valueOf(e));
           return 3;
        }

        catch (Exception ex) {
            String returnStr = "SYSTEM FAILURE File:" + fileToProcess.getName() + " failed while " + stage + ". [Exception " +
            ex.getClass() + " " + ex.getMessage() + "]";
            application.log.severe(returnStr);
            //application.log.severe( ex.fillInStackTrace().toString());

            ex.printStackTrace();
            ByteArrayOutputStream ostr = new ByteArrayOutputStream();
            ex.printStackTrace(new PrintStream(ostr));
            application.log.severe(ostr.toString());
            return 3;
        } // try catch inside processFile

        finally {
            // Close the stream
            try {
                if(fileIn != null ) {
                    fileIn.close( );}
            }
            catch( IOException e )
            { System.out.println( e );
              application.log.severe("ERROR i/o error closing datafile (2) :" + fileToProcess );
              return 3;
            }
        } // end main catch of this processFile method










    }  // end of processFile method







    /** reads a record from input file and performs some basic validation on it
     * e.g. of minimum length, in correct numerical sequence
     * @param fileIn Buffered reader of input file to be read from
     * @param fileToProcess filename
     * @param showContents true if detailed logging required, else false
     * @param recsRead number of records read so far
     * @return returns BSPRecord with contents of one line from input file
     */
    public static BSPRecord readRecord    (BufferedReader fileIn, boolean showContents, File fileToProcess, int recsRead )    {
        try {
            String oneLine = fileIn.readLine( );
            String transSeqString = "";

            if (oneLine != null ) {

                if (showContents) {
                    application.log.finest("File contents:" + recsRead + "," + oneLine);
                }
                if (oneLine.length() < 3) {
                    String mesg = "ERROR i/o error datafile :" + fileToProcess + "," + recsRead + "th record too short:" + oneLine ;
                    application.log.severe(mesg);


                }

                BSPRecord fileRec = new BSPRecord();
                fileRec.recordID = oneLine.substring(0,3);
                fileRec.recordText = oneLine;

                if (!fileRec.recordID.equals("BFH") && !fileRec.recordID.equals("BOH") && !fileRec.recordID.equals("BCH") && !fileRec.recordID.equals("BCT") && !fileRec.recordID.equals("BFT"))  {
                    // if not one of these record types then validate the record sequence
                    if (!NumberProcessor.validateStringAsNumber(oneLine.substring(3,11))) {
                        application.log.severe("ERROR datafile :" + fileToProcess + " record seq num non-numeric:"+ oneLine.substring(3,11) + " (record was:" + fileRec.recordText + ")");
                        System.out.println("ended with errors, see log file");
                        System.exit(0);
                    }
                    if (oneLine.length() > 24)  {
                        transSeqString = oneLine.substring(19,25);
                        if (!NumberProcessor.validateStringAsNumber(transSeqString)) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " record seq num non-numeric:"+ transSeqString + " (record was:" + fileRec.recordText + ")");
                            System.out.println("ended with errors, see log file");
                            System.exit(0);
                        }
                        fileRec.transSeq = Integer.parseInt(transSeqString);
                    }

                }

                recSequence = Integer.parseInt(oneLine.substring(3,11));
                if (recSequence != prevHighSequence + 1) {
                    application.log.severe("ERROR datafile :" + fileToProcess + " record seq num out of sequence:" + recSequence + "(record was:" + fileRec.recordText + ")");
                    System.out.println("ended with errors, see log file");
                    System.exit(0);
                }

                prevHighSequence = recSequence;


                if ( fileRec.recordID.equals("BKS") ) {
                    fileRec.bksType = oneLine.substring(11,13).trim();
                    transSeqString = oneLine.substring(28,38).trim();  // the ticket number
                    // bks/45 records can sometimes have blank related ticket/document numbers, so don't validate
                    // type 45's are not used anywhere anyway
                    if (!fileRec.bksType.equals("45")) {
                        if (!NumberProcessor.validateStringAsNumber(transSeqString)) {
                            application.log.severe("ERROR datafile :" + fileToProcess + " BKS tkt num non-numeric:"+ transSeqString + " (record was:" + fileRec.recordText + ")");
                            System.out.println("ended with errors, see log file");
                            System.exit(0);
                        }
                        fileRec.ticketNo = Long.parseLong(transSeqString);
                    }
                }
                if ( fileRec.recordID.equals("BKP") ) {
                    fileRec.bksType = oneLine.substring(11,13).trim();
                }


                return fileRec;
            }
            else {
                // must be null file read
                return null;
            } // end of check for null


        } // end try

        catch( IOException e )
        { System.out.println( e );
          application.log.severe("ERROR i/o error :" + fileToProcess );
          System.out.println("ended with errors, see log file");
          System.exit(0);
          return null;
        }


    } // end readRecord method


    /** checks sequence of one record of input file against business rules for this type of file
     * e.g. does it follow the order that BSP specification states
     * compares against last record type encountered
     * if not then fail
     * @param prevRecType the last record's record type - checked against
   current record type
     * @param currentRecType the current record's record type -- used to determine
   if in correct sequence
     * @param recordText the actual text of the input record to be validated
     * @param fileName filename string
     * @return true if valid sequence, false if out of sequence
     */
    public static boolean isRecordTypeSeqValid(String prevRecType, String currentRecType,
    String recordText, String fileName) {
        /** validates that the sequence of records in the file is OK.
         *returns true if valid sequence , false if not expected sequence
         **/

        String failMessage = "ERROR datafile :" + fileName + " invalid record sequence. Prev:" + prevRecType + " current:" + currentRecType + "(record was:" + recordText + ")";


        if (currentRecType.equals("BFT") ) {
            if (!prevRecType.equals("BCT") ) {
                return deliberateCrashOut(failMessage);
            }
        }
        else if (currentRecType.equals("BOH") ) {
            if (!prevRecType.equals("BCH") && !prevRecType.equals("BOT") ) {
                return deliberateCrashOut(failMessage);
            }
        }
        else if (currentRecType.equals("BKT") ) {
            if (!prevRecType.equals("BOH") && !prevRecType.equals("BKP") && !prevRecType.equals("BOT") ) {
                return deliberateCrashOut(failMessage);
            }
        }
        else if (currentRecType.equals("BKS") ) {
            if (!prevRecType.equals("BKT") && !prevRecType.equals("BKS") && !prevRecType.equals("BKF") && !prevRecType.equals("BKI") ) {
                return deliberateCrashOut(failMessage);
            }
        }
        else if (currentRecType.equals("BKI") ) {
            if (!prevRecType.equals("BKS") && !prevRecType.equals("BKI") ) {
                return deliberateCrashOut(failMessage);
            }
        }
        else if (currentRecType.equals("BAR") ) {
            // Added JR, 21/01/05
            // If current record type is BAR than Previous record type can also be BKS
            if (!prevRecType.equals("BKI") && !prevRecType.equals("BAR") && !prevRecType.equals("BKS")) {
                return deliberateCrashOut(failMessage);
            }
        }
        // Added JR, 21/01/05
        // If current record type is BMP than previous record type can be BAR or BMP
         else if (currentRecType.equals("BMP") ) {
            if (!prevRecType.equals("BAR") && !prevRecType.equals("BMP")) {
                return deliberateCrashOut(failMessage);
            }
        }

        else if (currentRecType.equals("BKF") ) {
            if (!prevRecType.equals("BAR") && !prevRecType.equals("BKF")) {
                return deliberateCrashOut(failMessage);
            }
        }

        else if (currentRecType.equals("BCT") ) {
            if (!prevRecType.equals("BOT") ) {
                return deliberateCrashOut(failMessage);
            }
        }
        else if (currentRecType.equals("BCH") ) {
            if (!prevRecType.equals("BFH")&& !prevRecType.equals("BCT")) {   // 06/12/05 added BCT as previous record to BCH , to load new formated BSP file
                return deliberateCrashOut(failMessage);
            }
        }


        // Added JR, 21/01/05
        // If current record type is BKP than added previous record type as BMP
        //LA 030907 need to allow BKF before BKP but also allow BAR before BKP i.e. BKF is optional
        // so allowed BAR to preceed a BKP

        else if (currentRecType.equals("BKP") ) {
            if (!prevRecType.equals("BKS") && !prevRecType.equals("BKI") &&
                !prevRecType.equals("BKP") && !prevRecType.equals("BKF") &&
                !prevRecType.equals("BMP") && !prevRecType.equals("BAR") )
                {
                return deliberateCrashOut(failMessage);
            }

        }
        else if (currentRecType.equals("BOT") ) {
            if (!prevRecType.equals("BKP") && !prevRecType.equals("BOT") ) {
                return deliberateCrashOut(failMessage);
            }
        }

        else if (prevRecType.equals("BOH")) {
            if (!currentRecType.equals("BKT") ) {
                return deliberateCrashOut(failMessage);
            }
        }




        else {
            // record ID has not been entered into this validation correctly
            failMessage = "rectype not validated properly, prev:" + prevRecType + " cur:" +
            currentRecType;
            return deliberateCrashOut(failMessage);
        }


        // otherwise to have gotten this far, must be valid
        return true;

    }   // end method isRecordTypeSeqValid






    /** cause program to crash deliberately because of business logic failure
     * inputs: string to display as severe to application log
     * @param messageString mandatory string of message used to display to
   console when a crash is forced
     * @return boolean - always returns false to show that this is a failure
     *
     **/

    public static boolean deliberateCrashOut(String messageString) {
        application.log.severe(messageString);
        System.out.println(messageString);
        System.out.println("ended with errors, see log file");
        System.exit(0);
        return false;

    }

    /** insert data into database
     *  @param conn the database connection which has already been initialised
     *  @param showContents - true if want detailed log to be output to console,
                          false if detail not wanted
     *  @return true if success, false if failure
     *
     **/
    public static boolean insertTransaction(Connection conn, boolean showContents)
    throws SQLException{



        // round off amts which can get calculated to many decimal places
        //application.log.info("old tax:" + recTaxTotal);
        recTaxTotal = (Math.round((recTaxTotal * 100)) / (double)100);
        recUBTax = (Math.round((recUBTax * 100)) / (double)100);
        recGBTax = (Math.round((recGBTax * 100)) / (double)100);
        recRemainingTax = (Math.round((recRemainingTax * 100)) / (double)100);
        recAirlinePenalty = (Math.round((recAirlinePenalty * 100)) / (double)100);

        if (wConjunctionInd.equals("CNJ")) {
            recConjunctionInd = "Y";
        }
        else {
            recConjunctionInd = "N";
        }

        stage = "about to insert data";
        application.log.finest("insert params are:" +
        " tkt:" + recTicketNo +
        " tran:" + recTransCode +
        "fl:" + recFileName +
        " dt:" + recDate +
        " crs:" + recCRSCode +
        " air:" + recTicketingAirline +
        " agt:" + recAgentID + // iata
        " commable:" + recCommissionableAmt +
        " comm:" + recCommissionAmt +
        " tax:" + recTaxTotal +
        " netfare:" + recNetFareAmt +
        " ccy:" + recCcyCode +
        " nr:" + recNetRemitInd +
        " cnj:" + recConjunctionInd +
        " airpen:" + recAirlinePenalty +
        " balpay:" + recBalancePayable +
        " ubtx:" + recUBTax +
        " gbtx:" + recGBTax +
        " remtx:" + recRemainingTax +
        "");

        stage = "calling stored proc";
        try {
            CallableStatement cstmt = conn.prepareCall(
            "{?=call p_stella_bsp_data.insert_transaction(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
            cstmt.registerOutParameter(1, Types.CHAR);
            cstmt.setLong(2, recTicketNo);
            cstmt.setString(3, recTransCode.trim());
            cstmt.setString(4, recFileName.trim());

            application.log.finest("Date:"+DateProcessor.parseDate(recDate,"yyMMdd"));
            cstmt.setDate(5, new java.sql.Date( DateProcessor.parseDate(recDate,"yyMMdd").getTime())); // file date
            cstmt.setString(6, recCRSCode.trim());
            cstmt.setString(7,  recTicketingAirline.trim());
            cstmt.setString(8, recAgentID.trim());
            cstmt.setDouble(9, recCommissionableAmt);
            cstmt.setDouble(10, recCommissionAmt);
            cstmt.setDouble(11, recTaxTotal);
            cstmt.setDouble(12, recNetFareAmt);
            cstmt.setString(13, recCcyCode.trim());
            cstmt.setString(14, recNetRemitInd.trim());
            cstmt.setString(15, recConjunctionInd.trim());
            cstmt.setDouble(16, recAirlinePenalty);  // only set non-zero on refunds
            cstmt.setDouble(17, recBalancePayable);  // only set non-zero on refunds
            cstmt.setDouble(18, recUBTax);
            cstmt.setDouble(19, recGBTax);
            cstmt.setDouble(20, recRemainingTax);



            if (updateDatabase) {
                cstmt.execute();

                // there is no commit in the stored procedure
                //commit is issued later in code

                if (showContents) {application.log.finest("Insert Result: "+cstmt.getString(1));}
                if (cstmt.getString(1) != null) {
                    // severe error in the stored procedure
                    deliberateCrashOut("Database insert failed:" + cstmt.getString(1) + ", moved to error area");
                    application.log.info("data rolled back");
                    conn.rollback();
                    return false;
                }
            }
            cstmt.close();


        }
        catch (SQLException sqlex) {
            sqlex.printStackTrace();
            application.log.info("data rolled back");
            conn.rollback();
            deliberateCrashOut("Database insert failed, sql exception raised");
            return false;

        }


        // store totals

        countInsertedTrans ++;
        totalNetFareAmt += recNetFareAmt;
        totalCommissionAmt += recCommissionAmt;
        totalEffectiveCommissionAmt += recEffectiveCommissionAmt;
        totalTaxTotal += recTaxTotal;
        totalCommissionableAmt += recCommissionableAmt;
        totalAirlinePenalty += recAirlinePenalty;
        totalBalancePayable += recBalancePayable;

        totalDocumentAmt    += recBalancePayable;

        // reset flags on record progress
        hadBKS24 = false;
        hadBKS39 = false;
        hadBKS30 = false;
        processedThisTrans = true;

        return true;
    }



}

