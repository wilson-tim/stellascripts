package uk.co.firstchoice.stella;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Vector;

import uk.co.firstchoice.util.FileUtils;
import uk.co.firstchoice.util.AccessMode;
import uk.co.firstchoice.util.Application;
import uk.co.firstchoice.util.DBManager;

import uk.co.firstchoice.util.Level;
import uk.co.firstchoice.util.PropertyNotFoundException;
import uk.co.firstchoice.util.StringUtils;
import uk.co.firstchoice.util.businessrules.DateProcessor;
import uk.co.firstchoice.util.businessrules.NumberProcessor;

/**
 * Class to load data from Amadeus flight ticketing File (AIR FILE) into Stella
 * system database
 *
 * Creation date: (10/08/04 10:58:07 pM) Contact Information: FirstChoice
 *
 * @version 1.00
 * @author Jyoti Renganathan
 *
 * @version 1.1
 * @author Jyoti
 * This release is to support AIR load program to work with HandJ tickets
 * Fixed some bugs :
 * 1) Exchange tickets were  not picking up additional collection amount from FPO line , as CASH word was missing.
 * 2) aria 19 Pax type  , Children were loaded as Adult when DOB information is there, this is fixed by changing search logic
 * 3) aria 20 Pseudo city code , start position corrected
 * 4) Fixed Critical error - when ticket type is blank , program should default it to
 *
 * @version 1.2
 * @author Jyoti Renganathan
 * Modified code to add string occurance of LONSH in MUC1A for Thomson office code ,  int pos1 = StringUtils.searchStringOccur(oneLine,"LONFC",1);
 *
 * @version 1.3
 * @author Jyoti Renganathan
 * Added logic to read travelink booking ref from RM ##D line , if it is not found in FPNONREF AGT line
 *
 * @version 1.4
 * @author Camila Kill
 * Modified code to add string occurrence of LONVM in MUC1A for Austravel office code ,  int pos1 = StringUtils.searchStringOccur(oneLine,"LONVM",1);
 * added code to cope with the millionth booking references in travelink
 *
 * @version 1.5
 * @author Tim Wilson
 * 18/07/2018
 * Revised logic to read group code and travelink booking ref from FP NONREFAGT and RM ##D lines
 * following record structure changes (DWSES-376) and brand code length changes (DWSES-343)
 *
 * @version 1.6
 * @author Tim Wilson
 * 19/07/2018
 * Revised logging details for clarity (DWSES-377)
 *
 * @version 1.7
 * @author Tim Wilson
 * 19/07/2018
 * New processing for taxAmt == "EXEMPT" (DWSES-372)
 *
 * @version 1.8
 * @author Tim Wilson
 * 19/07/2018
 * New processing for recCommPct when refund ticket (DWSES-371); also related fix for FP records of the form FPNONREFAGTA1159137/GBP119.91
 *
 * @version 1.9
 * @author Tim Wilson
 * 19/07/2018
 * Bug fix for collectionAmt assignment (DWSES-368)
 * RM line processing check whether group and recBookingRef are already assigned (DWSES-389)
 *
 * @version 1.10
 * @author Tim Wilson
 * 20/07/2018
 * Bug fix for exchange ticket processing (DWSES-373)
 * 01 Debugged during first round of testing
 * 02 Debugged during second round of testing
 *
 * @version 2.0
 * @author Tim Wilson
 * 14/08/2018
 * Processing for EMD (Electronic Miscellaneous Document) ticket records
 * 01 Use regex to match group and booking reference
 *
 * @version 2.1
 * @author Tim Wilson
 * 10/09/2018
 * Bug fix for TMCD record processing
 * 01 Processing for RFD (refund) record
 * 02 New processing for taxAmt == "EXEMPT" (MI-1444), revise error message from tax1 to tax2 for clarity
 *
 * @version 2.2
 * @author Tim Wilson
 * 09/11/2018
 * Bug fix for "Group Code is Blank in Air File" error
 *
 */


/*
 *
 *
 * Datawarehouse live server connection URL :
 * jdbc:oracle:thin:@dwlive_en1:1521:dwl
 *
 * Required inputs
 *
 * Usage Option -m run mode (L for Live or T for Test - so can get correct
 * parameters from app registry) if internal connection not to be used: -d
 * driverClass ( Driver Class for connecting to ORACLE/SYBASE ) -c connectionURL (
 * Connects to the database where
 * CRM_EXTRACT_SETUP,CRM_EXTRACT_LOG,CRM_EXTRACT_ERROR tables are kept) -u
 * userid ( User id for connecting to database ) -p passwd ( Password for
 * connecting to database ) -f single filename (run for a single file only
 * instead of all pending)
 *
 *
 */

public class StellaAIRLoad {

    // Class constants
    private static final String programName = "StellaAIRLoad";

    private static final String programShortName = "STELAIRL";

    private static final String programVersion = "2.1.1";

    private static int numRowsInserted = 0;

    // The application object. Provides logging, properties etc
    private static final Application application = new Application(programName,
        "Routines to import data " + " into data warehouse from Amadeus " +
        " flight ticketing systems (A IRS)");

    /**
     * main method , called from outside ie.command line. set up and then loop
     * through all files to be processed
     *
     * @param args
     *            arguments from command line
     */
    public static void main(String[] args) {

        String driverClass = "";
        String connectionURL = "";
        String dbUserID = "";
        String dbUserPwd = "";

        String runMode = "LIVE";
        String singleFileName = "none";

        // start processing

        System.out.println(programName + "v." + programVersion + " started");

        if (args.length == 0) {
            System.out.println("Error: main method. No runtime args specified");
            System.exit(1);
            return;

        }

        // show run-time params
        String argvalue = "";
        String argset = "";
        for (int i = 0; i < args.length; i++) {
            System.out.print(args[i] + " ");

            // If the argument is -e20 then -e is the exp_id and 20 is the
            // passed argument value

            argvalue = args[i];

            if ((argvalue.length() > 2) &&
                ((argvalue.substring(0, 1)).equalsIgnoreCase("-"))) {

                // split the argument into switch and value
                argset = argvalue.substring(0, 2);
                argvalue = argvalue.substring(2, argvalue.length());

                if (argset.equalsIgnoreCase("-m")) {
                    runMode = argvalue;
                    System.out.println("Runmode:" + runMode);
                } else if (argset.equalsIgnoreCase("-f")) {
                    // run for one file only, not all contents of directory
                    singleFileName = argvalue;
                } else if (argset.equalsIgnoreCase("-d")) {
                    // driverclass
                    driverClass = argvalue;

                } else if (argset.equalsIgnoreCase("-c")) {
                    // connection url
                    connectionURL = argvalue;
                } else if (argset.equalsIgnoreCase("-u")) {
                    dbUserID = argvalue;
                } else if (argset.equalsIgnoreCase("-p")) {
                    dbUserPwd = argvalue;
                }

            }
        }

        // actually run the method to do the processing
        String retCode = runLoad(driverClass, connectionURL, dbUserID,
            dbUserPwd, singleFileName, runMode);
        System.out.println("end, return was:" + retCode);
        if (retCode.substring(0, 2).equalsIgnoreCase("OK")) {
            System.exit(0);
        } else {
            System.exit(1);
        }

        return;

    }

    // end of main method

    /**
     * runLoad method to load data from air files into stella database this
     * method actually does the work
     *
     * @param driverClass
     *            jdbc driver class for database interaction
     * @param connectionURL
     *            database conn url
     * @param dbUserID
     *            database user id to use
     * @param dbUserPwd
     *            database user password
     * @param singleFileName
     *            if want to run in single file mode then specify a filename
     *            (without directory) here, otherwise pass ""
     * @param runMode
     *            Test or Live : determines if in debug mode
     * @return return code 1 for failure, 0 if success
     */
    public static String runLoad(String driverClass, String connectionURL,
        String dbUserID, String dbUserPwd, String singleFileName,
        String runMode) {

        String rowsetTag = null;

        String dirData, dirBackup, dirRecycle, logPath, ddlFileName, headerRecPrefix, dirError;
        String voidTicketInitials;


        boolean debugMode = false;
        int numFilesRead = 0, numFilesError = 0, numFilesSuccess = 0, numFilesRecycle = 0;

        String logFileName = "";
        String returnStr = "";
        String stage; // used to denote place in code where any exception
        // happened
        String logFileReturnValue = "";
        String logLevel; // min logging level to be logged
        String specialistPseudoList; // comma separated list of all the specialist pseudo city codes from registry

        // Create a instance of StellaAIRLoad class
        StellaAIRLoad f = new StellaAIRLoad();

        // create database connection

        // the dbmanager trys to connect to the internal oracle connection
        // first before using these dbconnection parameters
        // better to use internal connection if possible
        // but leave these params avail in case want to run from outside Oracle
        // java stored procedures.

        stage = "setup vars";
        //	DBManager dbManager = new DBManager(
        //                                   "jdbc:oracle:thin:@dwdev:1521:dwd",
        //                                   "oracle.jdbc.driver.OracleDriver",
        //                                   "jdbc.stella",
        //                                    "???????");
        System.out.println("Database:" + connectionURL + "," + driverClass +
            "," + dbUserID + "," + dbUserPwd);
        DBManager dbManager = new DBManager(connectionURL, driverClass,
            dbUserID, dbUserPwd);

        try {
            // set up the application object.
            application.setLogger(programName);
            application.setLoggerLevel(Level.ALL.toString()); // default to log
            // all levels
            application.setAccessMode(dbManager.connect());
            application.setRegistry(programShortName, runMode, "ALL", dbManager
                .getConnection());

            // find the log file path and create the logger
            // if running from client PC then use local paths
            // otherwise must be running on server

            try {
                // get the correct build path
                if (application.getAccessMode() == AccessMode.CLIENT) {
                    logPath = application
                        .getRegisteryProperty("LocalLogFilePath");
                    dirData = application.getRegisteryProperty("LocalFilePath");
                    dirBackup = application
                        .getRegisteryProperty("LocalBackupPath");
                    dirRecycle = application
                        .getRegisteryProperty("LocalRecyclePath");
                    dirError = application
                        .getRegisteryProperty("LocalErrorPath");
                    //  ddlFileName =
                    // application.getRegisteryProperty("LocalDDLFileName");

                    application.log.info("Database: " + connectionURL + "," +
                        driverClass + "," + dbUserID + "," + dbUserPwd);

                } else {
                    logPath = application
                        .getRegisteryProperty("ServerLogFilePath");
                    dirData = application
                        .getRegisteryProperty("ServerFilePath");
                    dirBackup = application
                        .getRegisteryProperty("ServerBackupPath");
                    dirRecycle = application
                        .getRegisteryProperty("ServerRecyclePath");
                    dirError = application
                        .getRegisteryProperty("ServerErrorPath");
                    //  ddlFileName =
                    // application.getRegisteryProperty("ServerDDLFileName");

                }
                //headerRecPrefix =
                // application.getRegisteryProperty("Headerrecprefix");

                logLevel = application.getRegisteryProperty("LogLevel");

                specialistPseudoList = application.getRegisteryProperty("SpecialistPseudoList");

                //headerLength =
                // Integer.parseInt(application.getRegisteryProperty("HeaderLength"));
                //tktPermittedSectors =
                // Integer.parseInt(application.getRegisteryProperty("TktPermittedSectors"));

                voidTicketInitials = application
                    .getRegisteryProperty("VoidTktInitials");

            } catch (PropertyNotFoundException ex) {

                application.log
                    .severe("Error: unable to find registry property. " +
                        ex.getMessage());

                return "Error: unable to find registry property. " +
                    ex.getMessage();
            }
            //fileUtils myFU = new fileUtils();
            application.setLoggerLevel(logLevel);

            application.setLogSysOut(false); // don't log to sysout as well as
            // to file
            logFileName = programShortName.toLowerCase() + "_" +
                (FileUtils.fileGetTimeStamp()) + ".log";
            application.log.config("Log level is: " + logLevel);
            application.log.config("Logfile is: " + logFileName);
            System.out.println("Log level is: " + logLevel);
            System.out.println("Logfile is: " + logFileName);
            application.log.setLoggerFile(new File(logPath, logFileName));

            logFileReturnValue = "|Lf#" + logPath + "/" + logFileName; // returned
            // at end
            // of
            // program
            // so
            // that
            // calling
            // program
            // knows
            // log
            // file
            // name

            //
            // Write out header to the log file
            //

            application.log.info(programName + " v." + programVersion);
            application.log.config("Runmode: " + runMode);
            application.log.info("START " +
                java.util.Calendar.getInstance().getTime());
            application.log
                .config("Access mode: " + application.getAccessMode());

            try {
                application.log.config("Name         => " +
                    application.getRegisteryProperty("Name"));
                application.log.config("Version      => " +
                    application.getRegisteryProperty("Version"));
                application.log.config("LogPath      => " + logPath);
                application.log.config("FilePath     => " + dirData);
                application.log.config("Backuppath   => " + dirBackup);
                application.log.config("Recyclepath  => " + dirRecycle);
                application.log.config("Errorpath    => " + dirError);
                //  application.log.config("Headerlength => " + headerLength);
                //  application.log.config("Headerrecprefix=> " +
                // headerRecPrefix);
                //  application.log.config("ddlfilename => " + ddlFileName);

            } catch (PropertyNotFoundException ex) {
                //return null;

                return "Error: unable to find registry property.(2nd attempt) " +
                    ex.getMessage() + logFileReturnValue;
            }

            if (application.getRegisteryProperty("DebugMode").equalsIgnoreCase(
                    "Y")) {
                application.log.info("Debug mode set");
                debugMode = true;
            } else {
                debugMode = false;
            }

            stage = "check dirs";

            // now check all file permissions and directories

            File fileDataDirectory = new File(dirData);
            File backupDataDirectory = new File(dirBackup);
            File recycleDataDirectory = new File(dirRecycle);
            File errorDataDirectory = new File(dirError);
            String newFileName = "";
            if (fileDataDirectory.isDirectory()) {
                application.log.info("data :" +
                    fileDataDirectory.getAbsolutePath() + " exists OK.");
            } else {
                returnStr = "ERROR data :" + dirData + " does not exist.";
                application.log.severe(returnStr);
                return returnStr + logFileReturnValue;
            }
            if (backupDataDirectory.isDirectory()) {
                application.log
                    .info("data :" + backupDataDirectory.getAbsolutePath() +
                        " exists OK.");
            } else {

                returnStr = "ERROR data :" + dirBackup + " does not exist.";
                application.log.severe(returnStr);
                return returnStr + logFileReturnValue;

            }
            if (recycleDataDirectory.isDirectory()) {
                application.log.info("data :" +
                    recycleDataDirectory.getAbsolutePath() +
                    " exists OK.");
            } else {
                returnStr = "ERROR data :" + dirRecycle + " does not exist.";
                application.log.severe(returnStr);
                return returnStr + logFileReturnValue;
            }
            if (errorDataDirectory.isDirectory()) {
                application.log.info("data :'" +
                    errorDataDirectory.getAbsolutePath() + "' exists OK."); // quotes so email won't catch error keyword
            } else {
                returnStr = "ERROR data :" + dirError + " does not exist.";
                application.log.severe(returnStr);
                return returnStr + logFileReturnValue;
            }

            // get contents of data directory
            stage = "list files";

            /*JR, commented to get files in sorted by time order
                         String[] contents = fileDataDirectory.list();
			application.log.info(contents.length + " files in data directory:");
			*/

            String[] contents = fileDataDirectory.list();
            java.util.Arrays.sort(contents);
            application.log.info(contents.length + " files in data directory.");

            // for (File file : files) {

            if (debugMode) {
                application.log.fine("List of files to be processed:");
                for (int i = 0; i < contents.length; i++) {
                    File indFile = new File(
                        fileDataDirectory.getAbsolutePath(), contents[i]);
                    application.log.info(contents[i] + "  " +
                        new Date(indFile.lastModified()));
                }
            }

            stage = "process files";
            File fileToProcess;

            for (int i = 0; i < contents.length; i++) {

                // now process each file
                if (singleFileName.equalsIgnoreCase("none")) {
                    // normal run mode, process ALL files in dir
                    fileToProcess = new File(fileDataDirectory
                        .getAbsolutePath(), contents[i]);
                } else {
                    // single file name passed from command line parameters,
                    // check it exists
                    application.log.info("Single file mode: " + singleFileName);
                    fileToProcess = new File(fileDataDirectory, singleFileName);
                }

                if (fileToProcess.exists()) {
                    // ok
                } else {

                    returnStr = "ERROR datafile " + fileToProcess +
                        " does not exist.";
                    application.log.severe(returnStr);
                    return returnStr + logFileReturnValue;
                }
                if (fileToProcess.canRead()) {
                    // ok
                } else {

                    returnStr = "ERROR datafile " + fileToProcess +
                        " cannot be read.";
                    application.log.severe(returnStr);
                    return returnStr + logFileReturnValue;

                }
                application.log.fine(""); // blank line or separate files in log
                application.log.info("File " + (i + 1) + " :" + fileToProcess +
                    " " + fileToProcess.length() + " bytes" + " at:" +
                    java.util.Calendar.getInstance().getTime());

                // now have valid filehandle

                stage = "about to processFile";

                int intReturn = processFile(fileToProcess, debugMode,
                    dbManager.getConnection(), voidTicketInitials, specialistPseudoList);

                stage = "eval return from processFile";

                // 0 for success
                // 1 for move to recycle so will be processed in next run
                // 2 for error

                switch (intReturn) {
                    case 0:
                        // success, move file to backup area
                        // check to see if it exists already, if so then add a suffix
                        {
                            File moveFile = new File(backupDataDirectory, fileToProcess
                                .getName());
                            if (moveFile.exists()) {
                                moveFile = new File(backupDataDirectory, fileToProcess
                                    .getName() +
                                    ".1");
                            }
                            if (fileToProcess.renameTo(moveFile)) {
                                // successful move

                            } else {
                                returnStr = "ERROR datafile " + fileToProcess +
                                    " cannot be moved to backup dir " + moveFile;
                                application.log.severe(returnStr);
                                return returnStr + logFileReturnValue;

                            }
                            numFilesSuccess++;
                            application.log.fine(fileToProcess + " processed OK");
                            break;
                        }
                    case 1:
                        // error, but non-critical so can be recycled
                        // e.g. data needs foreign key fulfilled before can be inserted
                        {
                            File moveFile = new File(recycleDataDirectory,
                                fileToProcess.getName());
                            if (fileToProcess.renameTo(moveFile)) {
                                // successful move
                                application.log.info(fileToProcess +
                                    " non-success. Moved to recycle");
                            } else {

                                returnStr = "ERROR datafile " + fileToProcess +
                                    " cannot be moved to recycle.";
                                application.log.severe(returnStr);
                                return returnStr + logFileReturnValue;

                            }
                            numFilesRecycle++;
                            break;
                        }
                    case 2:
                        // non-critical error , log and move on
                        // rename with error suffix and move to error directory
                        application.log.info("ERROR datafile " + fileToProcess +
                            " failed, renamed as error file");
                        if (fileToProcess.getName().length() > 2) {
                            if (!fileToProcess.getName().substring(0, 3)
                                .equalsIgnoreCase("err")) {
                                newFileName = "err_" + fileToProcess.getName();
                            } else {
                                newFileName = fileToProcess.getName();
                            }
                        } else {
                            newFileName = "err_" + fileToProcess.getName();
                        }

                        File moveFile = new File(errorDataDirectory, newFileName);
                        if (fileToProcess.renameTo(moveFile)) {
                            // successful move
                            application.log.info(fileToProcess +
                                " renamed as error, moved to error area");
                        } else {
                            returnStr = "ERROR datafile " + fileToProcess +
                                " cannot be renamed as error file";
                            application.log.severe(returnStr);
                            return returnStr + logFileReturnValue;

                        }
                        numFilesError++;
                        break;
                    case 3:
                        // critical error , log and stop run
                        numFilesError++;
                        returnStr = "CRITICAL ERROR datafile " + fileToProcess +
                            " failed, renamed as error file";
                        application.log.severe(returnStr);
                        return returnStr + logFileReturnValue;

                    default:
                        returnStr = "ERROR invalid return from processFile :" +
                            fileToProcess + intReturn;
                        application.log.severe(returnStr);
                        return returnStr + logFileReturnValue;

                } // end case

                if (!singleFileName.equalsIgnoreCase("none")) {
                    break;
                }

            } // end for loop through files

            numFilesRead = contents.length;

        } catch (Exception ex) {
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

        dbManager.shutDown();

        if (numFilesRead == 0) {
            application.log
                .warning("WARNING!!! No files were found to process");
        }

        application.log.info("");
        application.log.info("Files Read:" + numFilesRead);
        if (!singleFileName.equalsIgnoreCase("none")) {
            application.log.info("Was in SingleFileMode:" + singleFileName);
        }
        application.log.info("Files in success:" + numFilesSuccess);
        application.log.info("Files in recycle:" + numFilesRecycle);
        application.log.info("Files in error:" + numFilesError);
        application.log.info("Num tkts inserted:" + numRowsInserted);

        application.log.info("COMPLETE " +
            java.util.Calendar.getInstance().getTime());
        application.log.close();

        // end of class - report to console as well as log

        System.out.println("Files Read:" + numFilesRead);
        if (!singleFileName.equalsIgnoreCase("none")) {
            System.out.println("Was in SingleFileMode:" + singleFileName);
        }
        System.out.println("Files in success:" + numFilesSuccess);
        System.out.println("Files in error:" + numFilesError);
        System.out.println("Files in recycle:" + numFilesRecycle);
        System.out.println("Num tkts inserted:" + numRowsInserted);

        System.out.println(programName + " COMPLETE " +
            java.util.Calendar.getInstance().getTime());

        return "OK" + logFileReturnValue;

    }

    /**
     * read and process each individual file
     *
     * @param fileToProcess
     *            filename of an individual file to be processed
     * @param showContents
     *            true if debug on, false if not on
     * @param conn
     *            the connection object
     * @param voidTicketInitials
     *            default ticket agent id
     * @param specialistPseudoList
     *            comma separated list of all the specialist pseudo city codes from the registry
     * @throws SQLException
     *             sql error has occurred
     * @return 0 - success, 1 - data related error e.g foreign key problem, 2 -
     *         error in file layout, 3 - critical error
     */

    public static int processFile(File fileToProcess, boolean showContents,
        Connection conn, String voidTicketInitials, String specialistPseudoList) throws SQLException {
        /** read and process each individual file */

        String stage = "start of processFile";

        FileReader theFile, theFile2, theFile3;
        BufferedReader fileIn = null, fileIn2 = null, fileIn3 = null;
        String oneLine, lineData = "";
        Vector fileRecs = new Vector();
        AirRecord fileRec = new AirRecord();
        //int paxNo = 0, fareBasisNo = 0;
        int numPaxRecs = 0;
//        int recCounter = 0;
        String recID = "";

        Vector EMDRecs = new Vector();  // All EMD record(s) in a file
        Vector EMDTkts = new Vector();  // EMD record(s) for the passenger currently being processed (current I- record)
        EMDRecord EMDCurrentRec = new EMDRecord();

        String CheckBookingRef = "";
        boolean BookingRefInconsistent = false;

        try {
            // fields for output to database records
            String recPassengerName = "";
            String recETicketInd = "";
            String recBranchCode = "AAIR"; // initialised to constant value for
            // Air

            String bookingRef = "0";
            String recSeason = "";
            String recBookingRef = "1"; // 1 for a dummy ref where ref is not
            // available, but we still want to load
            // the record to database
            String recSourceInd = "M"; // Air sourced records
            String recPassengerType = "";
            String recTicketNo = "";
            String recCcyCode = "";
            String recPublishedFare = "0";
            String recSellingFare = "0";
            String recCommAmt = "0.0";
            String recCommPct = "0.0";
            String recGBTax = "0";
            String recRemainTax = "0";
            String recUBTax = "0";
            String recAirline = "";
            String recDeptDate = "";
            String recTicketDate = "";
            String recTicketAgent = "";
            String recIATANum = "";
            String recPNR = "";
            String recTourCode = "";
            //String recTourCodeType = "";
            String recInsertUpdateFlag = "I"; // initially an insert
            String recExchangeTicketNo = "";
            String recExchangeTicketNos = "";
            String recPseudoCityCode = "";
            String recFareBasisCode = "";
            String recPNRdate = "";
            String recOtherTaxes = ""; // long string of other (not GB or UB)
            // taxes separated by ;
            String recTicketType = ""; // E,K,U,P, A for ATB/OPATB ,T for TAT or
            // OPTAT ,E for Electronic ticket
            // (ATB/OPATB), O for TTPBTK

            String insPublishedFare = "";
            String insSellingFare = "";
            String insGBTax = "";
            String insRemainTax = "";
            String insUBTax = "";
            String insOtherTaxes = "";
            int startPos;
            int endPos;
            int slashPos;
            String EMDDocID;
            int idxEMDRecs;
            boolean idxEMDFound;

            AirRecord fileUBAmtRec = new AirRecord();
            boolean voidTkt = false;
            boolean exchangeFound = false;
            //  double workingGBTaxAmt = 0;
            //  double workingUBTaxAmt = 0;
            BigDecimal workingRemainTaxAmt = new BigDecimal("0.00");
            //     double workingXTTaxAmt = 0;
            String taxAmt = "";
            boolean foundUB = false;
            BigDecimal commAmt = new BigDecimal("0.00");
            BigDecimal commPct = new BigDecimal("0.00");
            BigDecimal publishedFareAmt = new BigDecimal("0.00");
            String bookRef = "";
            //        double conjTktsRequired = 0;
            //        double highestA04Sector =0;
            boolean firstDepDateFound = false;
            boolean publishedFareFound = false;
            boolean KFfound = false;
            String KFline = "";
            boolean KNFfound = false;
            String KNFline = "";
            boolean KIfound = false;
            String KIline = "";
            boolean KNIfound = false;
            String KNIline = "";
            String tktRecord = "";
            String mfpRecord = "";
            int noOfTkts = 0;
            int noOfConjTkts = 0;
            int noOfConjExchTkts = 0;
            boolean EMDfound = false;
            int insertedTickets = 0;
            int insertedEMDTickets = 0;
            Date finalDeptDate = null;
            //  String conjTktInd = "N" ;
            int dashPos = 0;
            int dashOccur = 0;
            int foLen = 0;
            BigDecimal totalAdditionalTax = new BigDecimal("0.00");
            int tktIssueMonth = 0;
            int tktIssueYear = 0;
            int depYear = 0;
            String maxConjTktNum = "";
            String maxConjExchTktNum = "";

            boolean taxFound = false;
            boolean UnpaidTaxFound = false;

            int lineLength;
            String collectionAmt = "0";
            String group = ""; // default to HandJ branch group , try again in stored procedure to find a group
            boolean inside_rm = false;
            boolean RFDfound = false;

            ResultSet rs;

            application.log.finest("F:" + fileToProcess.getName());

            // now actually process the file

            if (showContents) {
                try
                // display entire file contents
                {
                    stage = "reading file for display";
                    theFile2 = new FileReader(fileToProcess);
                    fileIn2 = new BufferedReader(theFile2);
                    while ((oneLine = fileIn2.readLine()) != null) {
                        // output file contents to console
                        application.log.finest(oneLine);
                    }
                } catch (IOException e) {
                    System.out.println(e);
                    application.log.severe("ERROR i/o error datafile " +
                        fileToProcess);
                    application.log.severe(String.valueOf(e));
                    return 2;
                } finally {
                    // Close the stream
                    try {
                        if (fileIn2 != null)
                            fileIn2.close();
                    } catch (IOException e) {
                        System.out.println(e);
                        application.log
                            .severe("ERROR i/o error closing datafile " +
                                fileToProcess);
                        return 3;
                    }
                }
            } // end if show contents

            try {

                // Now check if the file had END line at the last line if not
                // this is incomplete file so rename with error suffix and move
                // to error directory
                // non-critical error, log and move on to next air file

                stage = "Checking for END of file indicator";
                theFile = new FileReader(fileToProcess);
                fileIn = new BufferedReader(theFile);

                while ((oneLine = fileIn.readLine()) != null) {
                    lineData = oneLine;

                    // Check Here if it is a exchange ticket than get the
                    // collection amount if exists
                    if ((lineData.length() > 2) &&
                        (oneLine.substring(0, 2).equalsIgnoreCase("FO"))) {
                        exchangeFound = true;
                    }

                    /*
                     * This code is added here so that when reading through fare
                     * records (K-R, K-Y etc , check can be done to match on
                     * additional fare Form of Payment eg FPO/NONREF
                     * AGT+/CASH/GBP731.00 or FPO/NONREF
                     * AGT+/CASH/GBP100.00;S2-5;P1
                     * Ver 1.1 : FPO line received as FPO/NONREF AGT+/NONREF AGT/GBP50.00  , code could not read \\uFFFD50 as no /CASH in it.
                     * Fixed by searching for /GBP rather CASH/
                     */

                    if ((lineData.length() > 3) &&
                        (exchangeFound) &&
                        (oneLine.substring(0, 3).equalsIgnoreCase("FPO"))) {

                        //int slashPos = oneLine.indexOf("CASH/");
                        slashPos = oneLine.indexOf("/GBP"); // pos 26
                        endPos = oneLine.indexOf(";"); // either -1 for not fo

                        // The end position of Amount is either up to first
                        // semicolon or up to end of line length
                        if (slashPos > 0) {
                            if (endPos > 0) {
                                collectionAmt = oneLine.substring(slashPos + 4, endPos);
                            } else {
                                collectionAmt = oneLine.substring(slashPos + 4, oneLine.length());
                            }
                            application.log.fine("collectionAmt " + collectionAmt);
                        }

                        /*
                         * if (! recPublishedFare.equalsIgnoreCase("0") ) {
                         * recPublishedFare = collectionAmt ; } else if
                         * (!recSellingFare.equalsIgnoreCase("0") ) {
                         * recSellingFare = collectionAmt ; }
                         */
                    } // end of FPO record

                    // Check if this is a refund previously loaded from an EMD
                    if ((lineData.length() > 3) &&
                        (oneLine.substring(0, 3).equalsIgnoreCase("RFD"))) {
                        RFDfound = true;
                    }

                } // end of while

                // Here last line can be END or ENDX , 'X' is used in the last
                // of a stream of AIR records being transmitted
                if (!(lineData.equalsIgnoreCase("END") || lineData
                        .equalsIgnoreCase("ENDX"))) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ", missing END of file indicator ");
                    return 2;
                }

            } // end try
            catch (IOException e) {
                System.out.println(e);
                application.log.severe("ERROR i/o error datafile (2) " +
                    fileToProcess);
                application.log.severe(String.valueOf(e));
                return 3;
            } finally {
                // Close the stream
                try {
                    if (fileIn != null)
                        fileIn.close();
                } catch (IOException e) {
                    System.out.println(e);
                    application.log
                        .severe("ERROR i/o error closing datafile (2) " +
                            fileToProcess);
                    return 3;
                }
            } // end finally

            try {
                // Read the required records into vector so can be processed
                // later on

                stage = "Reading through Air File";
                theFile3 = new FileReader(fileToProcess);
                fileIn3 = new BufferedReader(theFile3);

                while ((oneLine = fileIn3.readLine()) != null) {

                    lineLength = oneLine.length();

                    stage = "build vector";

                    if (lineLength > 2) { // Line Length is checked if its not a
                        // blank line or a L- line
                        AirRecord fileRec1 = new AirRecord();
                        recID = oneLine.substring(0, 2);
                        fileRec1.recordID = recID;
                        fileRec1.recordText = oneLine;

                        fileRecs.addElement(fileRec1);

                    } //Line length more than 2 characters

                    // $$$$$$$$$ Code here to stop index out of bound error
                    if ((lineLength > 2) &&
                        (recID.equals("AI") || recID.equals("MU") ||
                            recID.equals("C-") || recID.equals("D-") ||
                            recID.equals("EM") || 
                            recID.equals("H-") || recID.equals("U-") || recID.equals("K-") ||
                            recID.equals("KN") || recID.equals("KF") ||
                            recID.equals("KS") || recID.equals("M-"))) {

                        // AIR- Record , get void tkt indicator (MA - short
                        // version , CA - long version)
                        // We are asked to receive short version so look for MA
                        // in this line
                        if ((recID.equalsIgnoreCase("AI")) &&
                            (oneLine.substring(0, 4)
                                .equalsIgnoreCase("AIR-"))) {
                            if ((oneLine.substring(11, 13))
                                .equalsIgnoreCase("MA")) {
                                application.log.fine("Voided Ticket");
                                voidTkt = true; // insert blanks for this type
                                // of ticket
                            }
                        } // AI record

                        // Get Header Information
                        // AMD record - get PNR number , IATA number , Pseudo
                        // city Code
                        if (oneLine.substring(0, 5).equalsIgnoreCase("MUC1A")) { // part
                            // of
                            // AMD
                            // record
                            stage = "processing AMD Header record";
                            recPNR = oneLine.substring(6, 12);
                            int pos = StringUtils.searchStringOccur(oneLine,
                                ";", 9);

                            application.log
                                .fine("pos of 9th semicolon in muc1a is " +
                                    pos);
                            // starts after 9th semicolon
                            recIATANum = oneLine.substring(pos + 1, pos + 9);
                            application.log.fine("iata num " + recIATANum);

                            // aria 20 , staring position is changed to pos1 + 6 (it was 5 before and  storing C3100 in db)
                            // starts after 2nd semicolon ,  not for Galileo booking , it comes after 4th semicolon , so rather searching for LONFC
                            int pos1 = StringUtils.searchStringOccur(oneLine, "LONFC", 1);

                            application.log.fine("pos1 for LONFC is " + pos1);

                            if (pos1 <= 0) { // look for LONSH
                                pos1 = StringUtils.searchStringOccur(oneLine, "LONSH", 1);
                                application.log.fine("pos1 for LONSH is " + pos1);
                            }

                            if (pos1 <= 0) { // look for LONVM
                                pos1 = StringUtils.searchStringOccur(oneLine, "LONVM", 1);
                                application.log.fine("pos1 for LONVM is " + pos1);
                            }

                            recPseudoCityCode = oneLine.substring(pos1 + 5, pos1 + 9);
                            application.log.fine("pseudocity code " +
                                recPseudoCityCode);
                        }

                        if (!voidTkt) {

                            // C- record - get ticketing agent inititals]
                            if (recID.equalsIgnoreCase("C-")) {
                                stage = "processing C- for agent initials";
                                recTicketAgent = oneLine.substring(21, 23);
                                application.log.fine("recTicketAgent " +
                                    recTicketAgent);
                            }
                            // D- record - get ticket issue date
                            else if (recID.equalsIgnoreCase("D-")) {

                                stage = "processing D- for ticket issue date";

                                recTicketDate = oneLine.substring(16, 22); // yymmdd
                                // format
                                recPNRdate = oneLine.substring(2, 8); //  yymmdd
                                // format

                                tktIssueMonth = Integer.parseInt(oneLine.substring(
                                    18, 20));

                                tktIssueYear = Integer.parseInt(oneLine.substring(16, 18));
                                application.log.fine("tktIssueYear " + tktIssueYear);
                                tktIssueYear = tktIssueYear + 2000; // get year in 2007 YYYY format
                                application.log.fine("tktIssueYear " + tktIssueYear);

                                application.log.fine("recTicketDate " +
                                    recTicketDate);
                                application.log.fine("recPNRdate " + recPNRdate);

                                if (!DateProcessor.checkDate(recTicketDate,
                                        "yymmdd") |
                                    !DateProcessor.checkDate(recPNRdate,
                                        "yymmdd")) { // validate date
                                    // format
                                    application.log.severe("F:" +
                                        fileToProcess.getName() + ",PNR Date:" +
                                        recPNRdate + " or Ticket Date:" +
                                        recTicketDate +
                                        " is invalid date format error");
                                    return 2;
                                }
                                //if (DateProcessor.parseDate(recTicketDate,
                                // "ddMMMyyhhmm") == null) {
                                if (DateProcessor.parseDate(recTicketDate,
                                        "yyMMdd") == null) {
                                    application.log.severe("F:" +
                                        fileToProcess.getName() +
                                        ",Ticket Date:" + recTicketDate +
                                        " is invalid date, error");
                                    return 2;
                                }
                                if (DateProcessor.parseDate(recPNRdate,
                                        "yyMMdd") == null) {
                                    application.log.severe("F:" +
                                        fileToProcess.getName() +
                                        ",PNR Date:" + recPNRdate +
                                        " is invalid date, error");
                                    return 2;
                                }


                            }
                            // H- record , get First Departure date, it will
                            // always be on first H- record, so ignore to read
                            // rest of H- records
                            // date is in the format of DDMON
                            else if ((recID.equalsIgnoreCase("H-") || recID.equalsIgnoreCase("U-")) &&
                                (!firstDepDateFound)) {

                                stage = "processing H- for First departure date";
                                int pos3 = StringUtils.searchStringOccur(
                                    oneLine, ";", 5);

                                recDeptDate = oneLine.substring(pos3 + 16,
                                    pos3 + 21); // DDMON format
                                firstDepDateFound = true;
                                application.log.fine("recDeptDate " +
                                    recDeptDate);

                                int deptMonth = DateProcessor.findMonthNumber(
                                    recDeptDate, "ddMMM"); // return month
                                // number
                                //application.log.fine(("recDeptDate month no is = " + deptMonth);

                                //Calendar cal = Calendar.getInstance();
                                //int thisYear = (cal.get(Calendar.YEAR)); // returns 2007

                                //int	 thisYr = Integer.parseInt((Integer.toString(thisYear)).substring(3,4)); // returns 07,17 etc   last two number of year



                                //application.log.fine("thisYr "	+ thisYr);

                                // Check if ticket issue date month is >
                                // departure date than departure is next year of
                                // issue date so increment year

                                if (((tktIssueMonth > 0) && (tktIssueMonth > deptMonth))) {
                                    depYear = tktIssueYear + 1;
                                } else {
                                    depYear = tktIssueYear;
                                }

                                application.log.fine("depYear " + depYear);


                                recDeptDate += (String.valueOf(depYear))
                                    .substring(2, 4);
                                //System.out.println("recDeptDate = " +
                                // recDeptDate);
                                application.log.fine("recDeptDate " + recDeptDate);

                                if (!DateProcessor.checkDate(recDeptDate,
                                        "ddMMMyyyy")) {
                                    application.log.severe("F:" +
                                        fileToProcess.getName() + ",PNR:" +
                                        recPNR + " tkt:" + recTicketNo +
                                        ", " + " Departure Date" +
                                        recDeptDate +
                                        " is invalid date format error");
                                    return 2;
                                }

                                finalDeptDate = DateProcessor.parseDate(
                                    recDeptDate + "0000", "ddMMMyyhhmm");
                                //finalDeptDate =
                                // DateProcessor.parseDate(recDeptDate ,
                                // "ddMMMyyyy");
                                //System.out.println("finalDeptDate = " +
                                // finalDeptDate );

                                if (finalDeptDate == null) {
                                    application.log.severe("F:" +
                                        fileToProcess.getName() + ",PNR:" +
                                        recPNR + " tkt:" + recTicketNo +
                                        ", " + " Departure Date" +
                                        recDeptDate +
                                        " is invalid date, error");
                                    return 2;
                                }

                                // determine season of departure e.g S03
                                // it will return eg Y06 --
                                recSeason = DateProcessor.calcSeason(
                                    new java.sql.Date(finalDeptDate
                                        .getTime()), 3, "Y");


                            }

                            /*
                             * K- record published fare In case of 1) Published
                             * fare than K-F amt = P.F = S.F calculate
                             * commission 2) Nett Remitt than K-F amt = P.F. and
                             * KN-F amt = S.F 3) BT/IT K.I or K-B amt = S.F and
                             * P.F = 0 , commission = 0 4) CAT 35 (only BT/IT)
                             * KN-I or KN-B amt = S.F , P.F = 0 , This type of
                             * fare follows BT/IT provided it does not include
                             * net remitt. For exchane tickets for Published
                             * Fare K-F comes as k-R (reissue) K-I comes as K-Y
                             * KN-F as KN-R K-B as K-W KN-I as KN-Y and KS-I as
                             * KS-Y
                             *
                             */
                            else if ((recID.equalsIgnoreCase("K-")) ||
                                (oneLine.substring(0, 3)
                                    .equalsIgnoreCase("KN-")) ||
                                (oneLine.substring(0, 3)
                                    .equalsIgnoreCase("KS-"))) {

                                //System.out.println("inside else if of K- and
                                // KN and KS");

                                stage = "processing Fare Records (K-F, KN-F, K-I, KN-I)";
                                int pos = StringUtils.searchStringOccur(
                                    oneLine, ";", 12);

                                if (pos <= 0) {
                                    application.log
                                        .severe("F:" +
                                            fileToProcess.getName() +
                                            ",PNR:" +
                                            recPNR +
                                            " tkt:" +
                                            recTicketNo +
                                            " Can not find Currency Code in Fare record . If is due to position of 12th  semicolon is zero ");
                                    return 2;

                                }


                                // get the Currency code , for that skip 12 ;
                                if (recID.equalsIgnoreCase("K-")) {
                                    recCcyCode = oneLine.substring(pos + 1,
                                        pos + 4);
                                }

                                // Published Fare
                                if ((oneLine.substring(0, 3))
                                    .equalsIgnoreCase("K-F")) { // get
                                    // Published
                                    // Fare

                                    recPublishedFare = populateFare(oneLine
                                        .substring(0, 3), oneLine, true);
                                    recSellingFare = recPublishedFare; // In
                                    // case
                                    // of P.F
                                    // S.F =
                                    // P.F
                                    application.log.finest("K-F recSellingFare " + recSellingFare);
                                } // end of K-F
                                else if (oneLine.substring(0, 4)
                                    .equalsIgnoreCase("KN-F")) { // Net
                                    // Remitt
                                    // type
                                    // fare
                                    recSellingFare = populateFare(oneLine
                                        .substring(0, 4), oneLine, true);
                                    application.log.finest("KN-F recSellingFare " + recSellingFare);
                                } else if ((oneLine.substring(0, 3)
                                        .equalsIgnoreCase("K-I")) ||
                                    (oneLine.substring(0, 3)
                                        .equalsIgnoreCase("K-B"))) { // BT
                                    // IT
                                    // type
                                    // fare
                                    recSellingFare = populateFare(oneLine
                                        .substring(0, 3), oneLine, true);
                                    application.log.finest("K-I or K-B recSellingFare " + recSellingFare);
                                    recPublishedFare = "0";
                                } else if ((oneLine.substring(0, 4)
                                        .equalsIgnoreCase("KN-I")) ||
                                    (oneLine.substring(0, 4)
                                        .equalsIgnoreCase("KN-B"))) { // CAT
                                    // 35 (
                                    // BT /
                                    // IT )
                                    // fare
                                    recSellingFare = populateFare(oneLine
                                        .substring(0, 4), oneLine, true);
                                    application.log.finest("KN-I or KN-B recSellingFare " + recSellingFare);
                                    recPublishedFare = "0";
                                }

                                // EXCHANGE / REISSUE TICKET FARE , WHEN THERE
                                // IS A ADDITIONAL COLLECTION
                                // IMPORTANT NOTES :
                                // Additional collection in FPO line and K- line
                                // are always represents same amount
                                // Additional collection in fare = K- fare amt (
                                // at the end of line ) - (sum of all tax
                                // changes in KFT- line )
                                // In other words , additional collection in
                                // fare line (K-) might represents only tax
                                // changes
                                // so find out if there is any tax changes in
                                // KFT- line than deduct that from fare , which
                                // is actual fare changes
                                else if (oneLine.substring(0, 3)
                                    .equalsIgnoreCase("K-R")) { // Published
                                    // Fare
                                    // Reissue


                                    recPublishedFare = oneLine.substring(
                                            pos + 4,
                                            StringUtils.searchStringOccur(
                                                oneLine, ";", 13) - 1)
                                        .trim();

                                    // if there is additional collection than it
                                    // will be same as collection amt at FPO
                                    // record
                                    application.log.finest("K-R recSellingFare before " + recSellingFare);
                                    application.log.finest("K-R collectionAmt " + collectionAmt);
                                    if (!recPublishedFare.equals("")) {
                                        if ((new BigDecimal(recPublishedFare)).compareTo(new BigDecimal(collectionAmt)) != 0) {  // recPublishedFare != collectionAmt
                                            recPublishedFare = "0";
                                        }
                                    }


                                    recSellingFare = recPublishedFare;
                                    application.log.finest("K-R recSellingFare after " + recSellingFare);
                                    //System.out.println("in K- R
                                    // recPublishedFare" + recPublishedFare);
                                } else if (oneLine.substring(0, 4)
                                    .equalsIgnoreCase("KN-R")) { // Nett
                                    // Remit
                                    // Reissue
                                    recSellingFare = oneLine.substring(
                                            pos + 4,
                                            StringUtils.searchStringOccur(
                                                oneLine, ";", 13) - 1)
                                        .trim();
                                    application.log.finest("KN-R recSellingFare before " + recSellingFare);
                                    application.log.finest("KN-R collectionAmt " + collectionAmt);
                                    //System.out.println("in KN- R
                                    // recSellingFare" + recSellingFare);
                                    // if there is additional collection than it
                                    // will be same as collection amt at FPO
                                    // record
                                    if ((new BigDecimal(recSellingFare)).compareTo(new BigDecimal(collectionAmt)) != 0) {  // recSellingFare != collectionAmt
                                        recSellingFare = "0";
                                    }
                                    application.log.finest("KN-R recSellingFare after " + recSellingFare);
                                } else if ((oneLine.substring(0, 3)
                                        .equalsIgnoreCase("K-Y")) ||
                                    (oneLine.substring(0, 3)
                                        .equalsIgnoreCase("K-W"))) { // IT
                                    // BT
                                    // Reissue
                                    recPublishedFare = "0";
                                    recSellingFare = oneLine.substring(
                                            pos + 4,
                                            StringUtils.searchStringOccur(
                                                oneLine, ";", 13) - 1)
                                        .trim();
                                    application.log.finest("K-Y or K-W recSellingFare before " + recSellingFare);
                                    application.log.finest("K-Y or K-W collectionAmt " + collectionAmt);
                                    //System.out.println("in K- Y
                                    // recSellingFare" + recSellingFare);
                                    if ((new BigDecimal(recSellingFare)).compareTo(new BigDecimal(collectionAmt)) != 0) {  // recSellingFare != collectionAmt
                                        recSellingFare = "0";
                                    }
                                    application.log.finest("K-Y or K-W recSellingFare after " + recSellingFare);
                                } else if ((oneLine.substring(0, 4)
                                        .equalsIgnoreCase("KN-Y")) ||
                                    (oneLine.substring(0, 4)
                                        .equalsIgnoreCase("KN-W"))) { // CAT
                                    // 35
                                    // Reissue
                                    recPublishedFare = "0";
                                    recSellingFare = oneLine.substring(
                                            pos + 4,
                                            StringUtils.searchStringOccur(
                                                oneLine, ";", 13) - 1)
                                        .trim();
                                    application.log.finest("KN-Y or KN-W recSellingFare before " + recSellingFare);
                                    application.log.finest("KN-Y or KN-W collectionAmt " + collectionAmt);
                                    //System.out.println("in KN- Y
                                    // recSellingFare" + recSellingFare);
                                    if ((new BigDecimal(recSellingFare)).compareTo(new BigDecimal(collectionAmt)) != 0) {  // recSellingFare != collectionAmt
                                        recSellingFare = "0";
                                    }
                                    application.log.finest("KN-Y or KN-W recSellingFare after " + recSellingFare);
                                }
                                // selling fare is only read for
                                // Exchange/Reissue tickets
                                else if ((oneLine.substring(0, 4)
                                        .equalsIgnoreCase("KS-Y")) ||
                                    (oneLine.substring(0, 4)
                                        .equalsIgnoreCase("KS-W"))) { // CAT
                                    // 35
                                    // Reissue
                                    recPublishedFare = "0";
                                    recSellingFare = oneLine.substring(
                                            pos + 4,
                                            StringUtils.searchStringOccur(
                                                oneLine, ";", 13) - 1)
                                        .trim();
                                    application.log.finest("KS-Y or KS-W recSellingFare before " + recSellingFare);
                                    application.log.finest("KS-Y or KS-W collectionAmt " + collectionAmt);
                                    //System.out.println("in KS- Y
                                    // recSellingFare" + recSellingFare);
                                    if ((new BigDecimal(recSellingFare)).compareTo(new BigDecimal(collectionAmt)) != 0) {  // recSellingFare != collectionAmt
                                        recSellingFare = "0";
                                    }
                                    application.log.finest("KS-Y or KS-W recSellingFare after " + recSellingFare);
                                }

                                //else if (recID.equalsIgnoreCase("K-")) {
                                //recPublishedFare = "";
                                //String recSellingFare = "";
                                //String recCommAmt = "";
                                //String recCommPct = ""

                                application.log.fine("recPublishedFare " +
                                    recPublishedFare);
                                application.log.fine("recSellingFare " +
                                    recSellingFare);

                            } // end of else if
                            else if ((oneLine.substring(0, 3)
                                    .equalsIgnoreCase("KFT")) ||
                                (oneLine.substring(0, 3)
                                    .equalsIgnoreCase("KNT"))) { // tax
                                // info
                                // ||
                                // (oneLine.substring(0,3).equalsIgnoreCase("KST"))
                                // // no need to read KST tax line

                                if (!exchangeFound) {

                                    application.log.fine("exchange and tax both not found");

                                    if (!taxFound) {

                                        taxFound = true; // already read KFT
                                        // lines of tax so no
                                        // need to go through
                                        // KNT line

                                        // Store each tax info (between ; 's)
                                        int pos  = oneLine.indexOf(";");
                                        int pos1 = oneLine.indexOf(";", pos + 1);

                                        while (pos > 0 && pos1 > 0) { // till
                                            // semicolon
                                            // is
                                            // found

                                            //KFTB; GBP12.20 QX AP; GBP5.00 GB
                                            // AD; GBP 10.40 UB AS; GBP5.00 YQ
                                            // TI; GBP4.60 YQ AS; GBP8.00 FR SE;
                                            // GBP10.80 FR TI; GBP5.00 YQ VO;
                                            // GBP2.70 YC AE; GBP7.50 US AP;
                                            // GBP7.50 US AS; GBP1.70 XA CO;
                                            // GBP3.80 XY CR; GBP4.60 YQ DP;
                                            // GBP1.40 AY SE;;;;;;;;;;;;;;;

                                            String chunk = oneLine.substring(
                                                pos + 1, pos1); // chunk
                                            // will be
                                            // "GBP12.20
                                            // QX AP"

                                            if (chunk.length() > 0) {

                                                // aira 26 fix , GB and U are not always coming as GB AD and UB AS

                                                //int gbPos = chunk.indexOf("GB AD");
                                                int gbPos = chunk.indexOf("GB ");
                                                //int ubPos = chunk.indexOf("UB AS");
                                                int ubPos = chunk.indexOf("UB ");

                                                taxAmt = (chunk.substring(4,
                                                        chunk.length() - 5))
                                                    .trim();

                                                taxAmt = taxAmt.replaceAll("EXEMPT", "0.00");

                                                if (!NumberProcessor
                                                    .validateStringAsNumber(taxAmt)) {
                                                    // invalid number
                                                    application.log
                                                        .severe("F:" +
                                                            fileToProcess
                                                            .getName() +
                                                            ",PNR:" +
                                                            recPNR +
                                                            " tkt:" +
                                                            recTicketNo +
                                                            ", tax1:" +
                                                            taxAmt +
                                                            " is not numeric error");
                                                    return 2;
                                                }

                                                if (gbPos > 0) {
                                                    recGBTax = taxAmt;
                                                }
                                                if (ubPos > 0) {
                                                    recUBTax = taxAmt;
                                                }
                                                if (gbPos <= 0 && ubPos <= 0) {

                                                    workingRemainTaxAmt = workingRemainTaxAmt.add(new BigDecimal(taxAmt));
                                                    recOtherTaxes += chunk +
                                                        ";";
                                                }

                                            } // chunk length is > 0

                                            pos = pos1; // Here for next chunk
                                            // end position becomes
                                            // start position
                                            pos1 = oneLine
                                                .indexOf(";", pos + 1);

                                        } // end of pos > 0

                                        recRemainTax = String
                                            .valueOf(workingRemainTaxAmt);

                                    } // tax found

                                } // Not a exchange Ticket
                                else { // It is a exchange ticket

                                    //  System.out.println("exchange found");

                                    if (!UnpaidTaxFound) { // Go through next
                                        // tax line KNT only
                                        // if nothing found
                                        // in KFT tax line
                                        // (Nett Fare fix)

                                        //  System.out.println("inside tax lines
                                        // ");
                                        int pos  = oneLine.indexOf(";");
                                        int pos1 = oneLine.indexOf(";", pos + 1);

                                        while (pos > 0 && pos1 > 0) { // till
                                            // semicolon
                                            // is
                                            // found

                                            // eg. KNTR;OGBP40.00 GB
                                            // AD;OGBP10.40 UB AS;OGBP2.80 YC
                                            // AE;OGBP7.60 US AP;OGBP7.60 US
                                            // AS;OGBP1.70 XA CO;OGBP3.90 XY
                                            // CR;OGBP1.40 AY SE;OGBP1.70 XF
                                            // ;;;;;;;;;;;;;;;;;;;;;

                                            String chunk = oneLine.substring(
                                                pos + 1, pos1); // chunk
                                            // will be
                                            // "OGBP12.20
                                            // QX AP" O
                                            // for
                                            // original
                                            // tax , for
                                            // new tax
                                            // there
                                            // will be
                                            // blank in
                                            // place of
                                            // O

                                            if ((chunk.length() > 0) &&
                                                (!chunk.substring(0, 1)
                                                    .equalsIgnoreCase(
                                                        "O"))) {

                                                UnpaidTaxFound = true;

                                                //System.out.println("first
                                                // letter in chunk " + chunk +
                                                // "is" + chunk.substring(1,2));

                                                //if
                                                // (!chunk.substring(1,2).equalsIgnoreCase("O"))
                                                // {
                                                // There is change in tax for
                                                // this exchange so load this
                                                // for exchange tickets

                                                // aira 26 fix , GB and U are not always coming as GB AD and UB AS

                                                //int gbPos = chunk.indexOf("GB AD");
                                                int gbPos = chunk.indexOf("GB ");
                                                //int ubPos = chunk.indexOf("UB AS");
                                                int ubPos = chunk.indexOf("UB ");

                                                taxAmt = (chunk.substring(4,
                                                        chunk.length() - 5))
                                                    .trim();

                                                taxAmt = taxAmt.replaceAll("EXEMPT", "0.00");

                                                if (!NumberProcessor
                                                    .validateStringAsNumber(taxAmt)) {
                                                    // invalid number
                                                    application.log
                                                        .severe("F:" +
                                                            fileToProcess
                                                            .getName() +
                                                            ",PNR:" +
                                                            recPNR +
                                                            " tkt:" +
                                                            recTicketNo +
                                                            ", tax2:" +
                                                            taxAmt +
                                                            " is not numeric error");
                                                    return 2;
                                                }

                                                totalAdditionalTax = totalAdditionalTax.add(new BigDecimal(taxAmt));

                                                if (gbPos > 0) {
                                                    recGBTax = taxAmt;
                                                }
                                                if (ubPos > 0) {
                                                    recUBTax = taxAmt;
                                                }
                                                if (gbPos <= 0 && ubPos <= 0) {
                                                    workingRemainTaxAmt = workingRemainTaxAmt.add(new BigDecimal(taxAmt));
                                                    recOtherTaxes += chunk +
                                                        ";";
                                                }

                                            } // chunk length is > 0

                                            pos = pos1; // Here for next chunk
                                            // end position becomes
                                            // start position
                                            pos1 = oneLine
                                                .indexOf(";", pos + 1);

                                        } // end of while loop

                                        recRemainTax = String
                                            .valueOf(workingRemainTaxAmt);

                                        // System.out.println("totalAdditionalTax
                                        // " +totalAdditionalTax);

                                        // Deduct total additional tax from fare
                                        // amount as , fare with K- record is
                                        // (additional fare + additional tax)
                                        // which is same as FPO amt
                                        if ((new BigDecimal(recPublishedFare)).compareTo(new BigDecimal("0")) > 0) { // recPublishedFare > 0
                                            recPublishedFare = (new BigDecimal(recPublishedFare)).subtract(totalAdditionalTax).toString();
                                        }

                                        application.log.finest("not exchange and not unpaid tax recSellingFare before " + recSellingFare);
                                        application.log.finest("not exchange and not unpaid tax totalAdditionalTax " + totalAdditionalTax);
                                        if ((new BigDecimal(recSellingFare)).compareTo(new BigDecimal("0")) > 0) { // recSellingFare > 0
                                            recSellingFare = (new BigDecimal(recSellingFare)).subtract(totalAdditionalTax).toString();
                                        }
                                        application.log.finest("not exchange and not unpaid tax recSellingFare after " + recSellingFare);
                                    } // UnpaidTax found

                                } // End of Exchange Ticket Tax data
                                application.log.fine("recGBtax " + recGBTax);
                                application.log.fine("recUBtax " + recUBTax);
                                application.log.fine("recRemainTax " +
                                    recRemainTax);
                                application.log.fine("recOtherTaxes " +
                                    recOtherTaxes);

                            } // end of KFT/KNT tax data
                            else if (recID.equalsIgnoreCase("M-")) { // fare
                                // basis
                                // code

                                int pos = oneLine.indexOf(';');
                                if (pos > 0) {
                                    recFareBasisCode = oneLine
                                        .substring(2, pos).trim();
                                } else {
                                    recFareBasisCode = oneLine.substring(2,
                                        oneLine.length()).trim();
                                }

                            } // end of M- record

                        } // not a void tkt

                        if ((recID.equalsIgnoreCase("EM")) &&
                            (oneLine.substring(0, 3).equalsIgnoreCase("EMD"))) {
                            stage = "processing EMD Record";
                            // Store EMD record
                            if (!EMDfound) {
                                EMDfound = true;
                                application.log.fine("EMDfound");
                            }
                            
                            EMDRecord EMDRec = new EMDRecord();
                            
                            // TSM (Transitional Stored Miscellaneous) document identifier
                            startPos = StringUtils.searchStringOccur(oneLine,
                                ";", 5);
                            endPos = StringUtils.searchStringOccur(oneLine,
                                ";", 6);
//                            application.log.finest("startPos and endPos " + startPos + " " + endPos);
                            if ((startPos > 0) && (endPos > 0) && (endPos > startPos)) {
                                EMDRec.docID = oneLine.substring(startPos +1, endPos);

                                // Total base amount
                                startPos = StringUtils.searchStringOccur(oneLine,
                                    ";", 28);
                                endPos = StringUtils.searchStringOccur(oneLine,
                                    ";", 29);
//                                application.log.finest("startPos and endPos " + startPos + " " + endPos);
                                if ((startPos > 0) && (endPos > 0) && (endPos > startPos)) {
                                    EMDRec.sellingFareAmount = oneLine.substring(startPos + 1, endPos)
                                            .replaceAll("[A-Z]", "").replaceAll("\\s+", "");
                                } else {
                                    EMDRec.sellingFareAmount = "";
                                }
                                if (EMDRec.sellingFareAmount.length() == 0) {
                                    EMDRec.sellingFareAmount = "0";
                                }

                                // Tax
                                startPos = StringUtils.searchStringOccur(oneLine,
                                    ";", 33);
                                endPos = StringUtils.searchStringOccur(oneLine,
                                    ";", 34);
//                                application.log.finest("startPos and endPos " + startPos + " " + endPos);
                                if ((startPos > 0) && (endPos > 0) && (endPos > startPos)) {
                                    EMDRec.remainingTax = oneLine.substring(startPos + 1, endPos)
                                            .replaceAll("T\\-", "").replaceAll("[A-Z]", "").replaceAll("\\s+", "");
                                } else {
                                    EMDRec.remainingTax = "";
                                }
                                if (EMDRec.remainingTax.length() == 0) {
                                    EMDRec.remainingTax = "0";
                                }
                                
                                EMDRecs.addElement(EMDRec);
                                application.log.fine("EMD record created " + EMDRec.docID + " " + EMDRec.sellingFareAmount + " " + EMDRec.remainingTax );
                            }
                        }
                    } // not within the list
                } // end of while

            } // end try
            catch (IOException e) {
                System.out.println(e);
                application.log.severe("ERROR i/o error datafile (3) " +
                    fileToProcess);
                application.log.severe(String.valueOf(e));
                return 3;
            } finally {
                // Close the stream
                try {
                    if (fileIn3 != null)
                        fileIn3.close();
                } catch (IOException e) {
                    System.out.println(e);
                    application.log
                        .severe("ERROR i/o error closing datafile (3) " +
                            fileToProcess);
                    return 3;
                }
            } // end finally

            // now process that vector of records we have built up
            // this is done so we can cope with repeating record types e.g
            // passengers I- records
            // Conjunctive type tickets etc.
            //while (recData.hasNext()) {

//            recCounter = 0;
            boolean firstTime = true;

            for (Enumeration recData = fileRecs.elements(); recData
                .hasMoreElements();) {

                if (firstTime) {
                    fileRec = (AirRecord) recData.nextElement();
                    firstTime = false;
                }

                //  recCounter ++; // store position in iterator

                //  Search for I- record and set pointer to it.
                while (!fileRec.recordID.equals("I-")) {
                    fileRec = (AirRecord) recData.nextElement();
                }

                if (fileRec.recordID.equals("I-")) {
                    //paxNo ++;
                    numPaxRecs++; // Counter for number of pax records

                    EMDTkts.clear();
                    
                    // find out Passenger Type if supplied else default it to
                    // Adult
                    //YT - Youth Tkt SN - Senior citizeN CO- Companion Ticket

                    int pos = StringUtils.searchStringOccur(fileRec.recordText,
                        ";", 2); // find second semi colon

                    if (pos > 0) { // This will fix StringIndexOutOfBoundsException at line 1714 , 28/04/2010
                        recPassengerName = fileRec.recordText.substring(8, pos);
                    }


                    // V1.1 , fixed bug so now if dob is supplied it should still find pax type correctly

                    if (fileRec.recordText.indexOf("(ADT)") > 0) {
                        recPassengerType = "AD";
                    } else if (fileRec.recordText.indexOf("(CHD)") > 0) {
                        recPassengerType = "CH";
                    } else if (fileRec.recordText.indexOf("(INF)") > 0) {
                        recPassengerType = "IN";
                    } else if (fileRec.recordText.indexOf("(SN") > 0) { // senior citizen
                        recPassengerType = "SN";
                    } else if (fileRec.recordText.indexOf("(CO") > 0) { // Companion Ticket
                        recPassengerType = "CO";
                    } else {
                        recPassengerType = "AD";
                    }

                    //}



                    /*
                    					if (fileRec.recordText.substring(pos - 5, pos - 4).equals(
                    							"(")) {
                    						String paxType = fileRec.recordText.substring(pos - 4,
                    								pos - 2);
                    						if (paxType.equalsIgnoreCase("AD")
                    								|| paxType.equalsIgnoreCase("CH")
                    								|| paxType.equalsIgnoreCase("IN")
                    								|| paxType.equalsIgnoreCase("YT")
                    								|| paxType.equalsIgnoreCase("SN")
                    								|| paxType.equalsIgnoreCase("CO")) {
                    							recPassengerType = paxType;
                    						} else {
                    							recPassengerType = "AD";
                    						}
                    					} else {
                    						recPassengerType = "AD";
                    					} */

                    fileRec = (AirRecord) recData.nextElement();
                } // end of while

                //else if (fileRec.recordID.equals( "T-") )

                //for (int i = recCounter + 1; i < fileRecs.size() &&
                // !fileRec.recordID.equals( "I-") ; i++)

                while (recData.hasMoreElements() &&
                    !fileRec.recordID.equals("I-")) {
                    if (fileRec.recordID.equals("T-")) {

                        // Recover all fare values which was set to 0 for
                        // conjunctive dummies
                        System.out.println("inside T- record published Fare" +
                            recPublishedFare);
                        application.log.finest("inside T-");
                        application.log.finest("T- line : " + fileRec.recordText);

                        insPublishedFare = recPublishedFare;
                        insSellingFare = recSellingFare;
                        insGBTax = recGBTax;
                        insUBTax = recUBTax;
                        insRemainTax = recRemainTax;
                        insOtherTaxes = recOtherTaxes;

                        stage = "processing T- for ticket no"; // T-A618-9600018513-14
                        noOfConjTkts = 0;
                        tktRecord = fileRec.recordText;

                        recTicketNo = fileRec.recordText.substring(7, 17);
                        System.out.println("recTicketNo " + recTicketNo);
                        application.log.fine("recTicketNo " + recTicketNo);

                        // if (lineLength > 17 ) { conjTktInd = "Y" ; } // i.e
                        // it is conjunctive/linked ticket

                        dashOccur = 3;
                        dashPos = StringUtils.searchStringOccur(
                            fileRec.recordText, "-", dashOccur);

                        // commented as always one dash and there will be range
                        // e.g. T-A117-123456789-99 , need to cover all missing
                        // number in between
                        /*
                         * while (dashPos > 0) { // find out how many
                         * linked/conjunctive tickets, ++noOfConjTkts ; dashPos =
                         * StringUtils.searchStringOccur(fileRec.recordText,"-",dashOccur +
                         * 1); }
                         */

                        maxConjTktNum = recTicketNo;
                        if (dashPos > 0) {
                            long tempTktNo = Long.parseLong(recTicketNo);
                            String lastTktNo = fileRec.recordText.substring(dashPos + 1, dashPos + 3);
                            // Up to 10 conjunctive tickets possible
                            for (int i = 1; i <= 10; i++) {
                                tempTktNo += 1;
                                if (Long.toString(tempTktNo).substring(Long.toString(tempTktNo).length()- 2).equals(lastTktNo)) {
                                    maxConjTktNum = Long.toString(tempTktNo);
                                    break;
                                }
                            }
                        } // If no conjunctive ticket it will be defaulted to
                        // ticket number

                        application.log.fine("maxConjTktNum " + maxConjTktNum);

                        noOfConjTkts = (int)(Long.parseLong(maxConjTktNum) -
                            Long.parseLong(recTicketNo) + 1);

                        application.log.fine("noOfConjTkts " + noOfConjTkts);

                        // get ticket Type
                        recTicketType = fileRec.recordText.substring(2, 3);

                        // get e ticket indicator
                        if (recTicketType.equalsIgnoreCase("E") ||
                            recTicketType.equalsIgnoreCase("K") ||
                            recTicketType.equalsIgnoreCase("U") ||
                            recTicketType.equalsIgnoreCase("P")) {
                            recETicketInd = "Y";
                        } else {
                            recETicketInd = "N";
                        }

                        // Get Airline Number
                        recAirline = fileRec.recordText.substring(3, 6);

                    } // end of T- record

                    // Get commission Amt / Pct . If letter before ; is A than
                    // amount else pct,FM*C*1;S3-4,6-7;P1-2 OR FM*M*8
                    else if ((fileRec.recordID.equals("FM")) &&
                        (!((fileRec.recordText.substring(0, 3))
                            .equalsIgnoreCase("FMB")))) {

                        stage = "processing FM for commission amount";

                        startPos = StringUtils.searchStringOccur(
                            fileRec.recordText, "*", 2); // start position
                        // for amount / pct

                        if (startPos < 0) {
                            // Assuming recordText starts with "FM" so not startPos = 0
                            startPos = 1;
                        }

                        int fmEndPos = fileRec.recordText.indexOf(";"); // returns -1
                        // if not
                        // found
                        if (fmEndPos < 0) {
                            fmEndPos = fileRec.recordText.length();
                        } // Here if no semicolon means it is manual commission
                        // eg FM*M*8

                        if (fileRec.recordText.substring(fmEndPos - 1, fmEndPos)
                            .equalsIgnoreCase("P")) { // "P" commission pct
                            recCommPct = fileRec.recordText.substring(
                                startPos + 1, fmEndPos - 1);
                        } else if (fileRec.recordText.substring(fmEndPos - 1, fmEndPos)
                            .equalsIgnoreCase("A")) { // "A" commission amt
                            recCommAmt = fileRec.recordText.substring(
                                startPos + 1, fmEndPos - 1);
                        } else { // other
                            recCommAmt = fileRec.recordText.substring(
                                startPos + 1, fmEndPos);
                        }
                    } // end of FM

                    // Tour Code Can start with FTIT (BT / IT) or FTNR( Net
                    // remitt),
                    // Tour Code can be very long so make sure you get maximum
                    // of 15 chars
                    // look for ;S if found than take up to that or else
                    // look for ;P if found than take up to that or else till end
                    // of line.

                    // If Tour Code is bigger than 15 chars than truncate it
                    else if (fileRec.recordID.equals("FT")) {
                        stage = "processing FT for tour code";
                        int spos = fileRec.recordText.indexOf(";S");
                        if (spos <= 0) {
                            spos = fileRec.recordText.indexOf(";P");
                        }
                        if (spos <= 0) {
                            spos = fileRec.recordText.length();
                        }

                        recTourCode = fileRec.recordText.substring(2, spos);
                        if (recTourCode.length() > 15) {
                            recTourCode = recTourCode.substring(0, 15);
                        }

                    }

                    // Original Issue / In Exchange For
                    else if (fileRec.recordID.equals("FO")) {
                        // Example FO1252570513764LON15MAY1891278401
                        //         the airline code is 125 (always 3 digits) and the exchange ticket no is 2570513764
                        stage = "processing FO for exchange ticket no";
                        exchangeFound = true;

                        String foRecord = "";
                        dashOccur = 1;
                        dashPos = StringUtils.searchStringOccur(
                            fileRec.recordText, "-", dashOccur);
                        if (dashPos == 5) {
                            // e.g. FO006-2570769969-70LON24MAY18/91212295/006-25707699694E1234*I;S4-8;P3
                            foRecord = fileRec.recordText.substring(6);
                        } else {
                            // e,g, :FO1572570111863LON30APR1891278401
                            foRecord = fileRec.recordText.substring(5);
                        }

                        foLen = foRecord.length();

                        // Loop to the end of the exchange ticket number
                        if (foLen > 10) {
                            int foEndPos = 0;
                            while ((foEndPos < foLen) && (recExchangeTicketNos.equals(""))) {
                                if ((foRecord.charAt(foEndPos) >= 'A') && (foRecord.charAt(foEndPos) <= 'Z')) {
                                    recExchangeTicketNos = foRecord.substring(0, foEndPos);
                                } else {
                                    foEndPos = foEndPos + 1;
                                }
                            }
                        }

                        System.out.println("recExchangeTicketNos " + recExchangeTicketNos);
                        application.log.fine("recExchangeTicketNos " + recExchangeTicketNos);


                        // commented as always one dash and there will be range
                        // e.g. FO006-123456789-99 , need to cover all missing
                        // number in between
                        /*
                         * while (dashPos > 0) { // find out how many
                         * linked/conjunctive tickets, ++noOfConjTkts ; dashPos =
                         * StringUtils.searchStringOccur(fileRec.recordText,"-",dashOccur +
                         * 1); }
                         */
                        // Check for exchange ticket number range
                        dashOccur = 1;
                        dashPos = StringUtils.searchStringOccur(
                            recExchangeTicketNos, "-", dashOccur);
                        if (dashPos > 0) {
                            recExchangeTicketNo = recExchangeTicketNos.substring(0, dashPos);
                            maxConjExchTktNum = recExchangeTicketNo;
                            long tempTktNo = Long.parseLong(recExchangeTicketNo);
                            String lastTktNo = recExchangeTicketNos.substring(dashPos + 1, dashPos + 3);
                            // Up to 10 conjunctive tickets possible
                            for (int i = 1; i <= 10; i++) {
                                tempTktNo += 1;
                                if (Long.toString(tempTktNo).substring(Long.toString(tempTktNo).length()- 2).equals(lastTktNo)) {
                                    maxConjExchTktNum = Long.toString(tempTktNo);
                                    break;
                                }
                            }
                        } else {
                            recExchangeTicketNo = recExchangeTicketNos;
                            maxConjExchTktNum = recExchangeTicketNo;
                        } // If no conjunctive ticket it will be defaulted to
                          // ticket number

                        application.log.fine("maxConjExchTktNum " + maxConjExchTktNum);

                        noOfConjExchTkts = (int)(Long.parseLong(maxConjTktNum) -
                            Long.parseLong(recTicketNo) + 1);

                        application.log.fine("noOfConjExchTkts " + noOfConjExchTkts);

                    }
                    // FPO record is already read at the beginning to get
                    // collection amt

                    // ver 1.1 Read Travel booking ref number from FP (Form of Payment ) record, itour always defaults to 1
                    // ver 1.2 new logic , if tlink ref is not found in FP NONref line 
                    //         look for RM ##D where booking ref is letter followed by numbers
                    // ver 1.5 "NONREF AGT " is now "NONREFAGT"; group codes may be either 1 or 2 characters


                    // JR added length > 10 to ignore processing for record like eg : FPNONREF, FPNONREFAGT
                    else if ((fileRec.recordID.equals("FP") && (specialistPseudoList.indexOf(recPseudoCityCode) >= 0) && (fileRec.recordText.length() > 14)) ||
                        (fileRec.recordID.equals("RM") && (fileRec.recordText.startsWith("RM ##D") || fileRec.recordText.startsWith("RM #D")))) {

                        application.log.finest("inside FP or RM record " + fileRec.recordID);
                        application.log.finest("first 6 char: " + fileRec.recordText.substring(0, 6));

                        if (fileRec.recordID.equals("FP") && (StringUtils.searchStringOccur(fileRec.recordText, "NONREFAGT", 1) > 0)) {
                            stage = "processing FP for Group and Booking Ref for Specialist";
                            application.log.finest("inside FP");
                            application.log.finest("FP line : " + fileRec.recordText);

                            // locate NONREFAGT
                            String patternString = "NONREFAGT[A-Z]{1,3}[0-9]{1,10}";

                            Pattern pattern = Pattern.compile(patternString);
                            Matcher matcher = pattern.matcher(fileRec.recordText);

                            if (matcher.find()) {
                                String fprefs = fileRec.recordText.substring(matcher.start() + 9, matcher.end());
                                group = fprefs.replaceAll("[0-9]", "");
                                recBookingRef = fprefs.replaceAll("[A-Z]", "");

                                if (EMDfound) {
                                    if ((CheckBookingRef.equals("")) && (!recBookingRef.equals(""))) {
                                        CheckBookingRef = recBookingRef;
                                    } else {
                                        BookingRefInconsistent = (!CheckBookingRef.equals(recBookingRef));
                                    }
                                }

                                application.log.fine(" FP group is " + group);
                                application.log.fine(" FP booking ref is " + recBookingRef);
                            }
                            
                        } else if (fileRec.recordID.equals("FP") && (StringUtils.searchStringOccur(fileRec.recordText, "NONREF", 1) > 0)) {
                            // version 2.2  09/11/2018
                            stage = "processing FP for Group and Booking Ref for Specialist";
                            application.log.finest("inside FP");
                            application.log.finest("FP line : " + fileRec.recordText);

                            // locate NONREF
                            String patternString = "NONREF[A-Z]{1,3}[0-9]{1,10}";

                            Pattern pattern = Pattern.compile(patternString);
                            Matcher matcher = pattern.matcher(fileRec.recordText);

                            if (matcher.find()) {
                                String fprefs = fileRec.recordText.substring(matcher.start() + 6, matcher.end());
                                group = fprefs.replaceAll("[0-9]", "");
                                recBookingRef = fprefs.replaceAll("[A-Z]", "");

                                if (EMDfound) {
                                    if ((CheckBookingRef.equals("")) && (!recBookingRef.equals(""))) {
                                        CheckBookingRef = recBookingRef;
                                    } else {
                                        BookingRefInconsistent = (!CheckBookingRef.equals(recBookingRef));
                                    }
                                }

                                application.log.fine(" FP group is " + group);
                                application.log.fine(" FP booking ref is " + recBookingRef);
                            }
                            
                            // eg. RM ##DX352331 or RM ##D X352331 or RM #DL1189738
                        } else if (fileRec.recordID.equals("RM") && (fileRec.recordText.substring(0, 6).equals("RM ##D") || fileRec.recordText.substring(0, 5).equals("RM #D"))) {
                            stage = "processing RM for Group and Booking Ref for Specialist";
                            application.log.finest("inside RM");
                            application.log.finest("RM line : " + fileRec.recordText);

                            inside_rm = true;

                            // locate #D
                            String patternString = "#D[A-Z]{1,3}[0-9]{1,10}";

                            Pattern pattern = Pattern.compile(patternString);
                            Matcher matcher = pattern.matcher(fileRec.recordText);

                            if (matcher.find()) {
                                String rmrefs = fileRec.recordText.substring(matcher.start() + 2, matcher.end());
                                if (group.equals("")) {
                                    group = rmrefs.replaceAll("[0-9]", "");
                                }
                                if ((recBookingRef.equals("")) || (recBookingRef.equals("1"))) {
                                    recBookingRef = rmrefs.replaceAll("[A-Z]", "");
                                }

                                if (EMDfound) {
                                    if ((CheckBookingRef.equals("")) && (!recBookingRef.equals(""))) {
                                        CheckBookingRef = recBookingRef;
                                    } else {
                                        BookingRefInconsistent = (!CheckBookingRef.equals(recBookingRef));
                                    }
                                }

                                application.log.fine(" RM group is " + group);
                                application.log.fine(" RM booking ref is " + recBookingRef);
                            }
                            
                            // Added for version 2, to get tlink ref from remark line
                        } else if (inside_rm && group.equals("")) { // Cannot find NONREFAGT OR RM ##D
                            application.log.severe("F:" + fileToProcess.getName() + ",PNR:" + recPNR + "/" + " tkt:" + recTicketNo +
                                " Cannot read Travelink Ref in either FP line or RM line " + fileRec.recordText);
                            return 2;
                        }

                    } // end of else if for FP and RM
                    
                    else if ((fileRec.recordID.equals("TM")) && fileRec.recordText.startsWith("TMCD")) {
                        stage = "processing TMCD record";
                        application.log.finest("inside TMCD");
                        application.log.finest("TMCD line : " + fileRec.recordText);
                        
                        tktRecord = fileRec.recordText.substring(4);

                        // Extract airline code and ticket number
                        dashPos = StringUtils.searchStringOccur(
                            tktRecord, "-", 1);
                        endPos = StringUtils.searchStringOccur(
                            tktRecord, ";", 1);
                        if (endPos < 0) {
                            endPos = tktRecord.length();
                        }
                        if ((dashPos > 0) && (endPos > dashPos + 1)) {
                            String tmpAirline  = tktRecord.substring(0, dashPos);
                            String tmpTicketNo = tktRecord.substring(dashPos + 1, endPos);

                            recTicketType = "E";
                            recETicketInd = "Y";
                            
                            // Update associated EMD record in EMDTkts
                            // first get the document id
                            startPos = StringUtils.searchStringOccur(
                                tktRecord, ";", 2);
                            if (startPos > 0) {
                                EMDDocID = tktRecord.substring(startPos + 1);

                                // then look for that document id in the EMDRecs vector
                                idxEMDRecs = 0;
                                idxEMDFound = false;
                                for (Enumeration EMDData = EMDRecs.elements(); EMDData
                                    .hasMoreElements();) {

                                    EMDCurrentRec = (EMDRecord) EMDData.nextElement();
                                    if (EMDCurrentRec.docID.equals(EMDDocID)) {
                                        idxEMDFound = true;
                                        application.log.finest("EMD and ticket docID is " + EMDDocID);
                                        application.log.finest("Ticket airline is " + tmpAirline);
                                        application.log.finest("Ticket number is " + tmpTicketNo);
                                        EMDCurrentRec.airline = tmpAirline;
                                        EMDCurrentRec.ticketNo = tmpTicketNo;
                                        break;
                                    }
                                    idxEMDRecs ++;
                                }
                                // finally if found then update the appropriate record in the EMDRecs vector
                                // and add the record to the EMDTkts vector
                                if (idxEMDFound) {
                                    EMDRecs.set(idxEMDRecs, EMDCurrentRec);
                                    EMDTkts.addElement(EMDCurrentRec);
                                    application.log.fine("EMD record updated " + EMDCurrentRec.docID + " " + EMDCurrentRec.sellingFareAmount + " " + EMDCurrentRec.remainingTax + " " + EMDCurrentRec.airline + " " + EMDCurrentRec.ticketNo );
                                }
                            } else {
                                if (RFDfound) {
                                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:" + recPNR + "/" + " tkt:" + recTicketNo + " tmpAirline: " + tmpAirline + " tmpTicketNo: " + tmpTicketNo +
                                        " RFD (Refund) record found, please check the details and input manually " + fileRec.recordText);
                                    return 2;
                                } else {
                                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:" + recPNR + "/" + " tkt:" + recTicketNo + " tmpAirline: " + tmpAirline + " tmpTicketNo: " + tmpTicketNo +
                                        " Cannot find EMD doc ID in TMCD line " + fileRec.recordText);
                                    return 2;
                                }
                            }
                        } else {
                            application.log.severe("F:" + fileToProcess.getName() + ",PNR:" + recPNR + "/" + " tkt:" + recTicketNo +
                                " Cannot read TMCD line " + fileRec.recordText);
                            return 2;
                        }
                    }

                    else if ((fileRec.recordID.equals("MF")) && fileRec.recordText.startsWith("MFPNONREFAGT")) {
                        stage = "processing MFP record";
                        application.log.finest("inside MFP");
                        application.log.finest("MFP line : " + fileRec.recordText);
                        
                        // locate NONREFAGT
                        String patternString = "NONREFAGT[A-Z]{1,3}[0-9]{1,10}";

                        Pattern pattern = Pattern.compile(patternString);
                        Matcher matcher = pattern.matcher(fileRec.recordText);

                        if (matcher.find()) {
                            mfpRecord = fileRec.recordText.substring(matcher.start() + 9);

                            String tmpRecord = fileRec.recordText.substring(matcher.start() + 9, matcher.end());
                            String tmpGroup = tmpRecord.replaceAll("[0-9]", "");
                            String tmpBookingRef = tmpRecord.replaceAll("[A-Z]", "");

                            // Update associated EMD record in EMDTkts
                            // first get the document id
                            startPos = StringUtils.searchStringOccur(
                                mfpRecord, ";", 3);
                            EMDDocID = mfpRecord.substring(startPos + 1);

                            if ((CheckBookingRef.equals("")) && (!tmpBookingRef.equals(""))) {
                                CheckBookingRef = tmpBookingRef;
                            } else {
                                BookingRefInconsistent = (!CheckBookingRef.equals(tmpBookingRef));
                            }

                            // then look for that document id in the EMDRecs vector
                            idxEMDRecs = 0;
                            idxEMDFound = false;
                            for (Enumeration EMDData = EMDRecs.elements(); EMDData
                                .hasMoreElements();) {

                                EMDCurrentRec = (EMDRecord) EMDData.nextElement();
                                if (EMDCurrentRec.docID.equals(EMDDocID)) {
                                    idxEMDFound = true;
                                    application.log.finest("EMD and ticket docID is " + EMDDocID);
                                    application.log.finest("Ticket number is " + EMDCurrentRec.ticketNo);
                                    application.log.finest("Ticket group is " + tmpGroup);
                                    application.log.finest("Ticket booking ref is " + tmpBookingRef);
                                    EMDCurrentRec.group = tmpGroup;
                                    EMDCurrentRec.bookingRef = tmpBookingRef;
                                    break;
                                }
                                idxEMDRecs ++;
                            }
                            // finally if found then update the appropriate record in the EMDRecs vector
                            if (idxEMDFound) {
                                EMDRecs.set(idxEMDRecs, EMDCurrentRec);
                                application.log.fine("EMD record updated " + EMDCurrentRec.docID + " " + EMDCurrentRec.sellingFareAmount + " " + EMDCurrentRec.remainingTax + " " + EMDCurrentRec.airline + " " + EMDCurrentRec.ticketNo + " " + EMDCurrentRec.group + " " + EMDCurrentRec.bookingRef );
                            }

                            // then look for that document id in the EMDTkts vector
                            idxEMDRecs = 0;
                            idxEMDFound = false;
                            for (Enumeration EMDData = EMDTkts.elements(); EMDData
                                .hasMoreElements();) {

                                EMDCurrentRec = (EMDRecord) EMDData.nextElement();
                                if (EMDCurrentRec.docID.equals(EMDDocID)) {
                                    idxEMDFound = true;
                                    application.log.finest("EMD and ticket docID is " + EMDDocID);
                                    application.log.finest("Ticket number is " + EMDCurrentRec.ticketNo);
                                    application.log.finest("Ticket group is " + tmpGroup);
                                    application.log.finest("Ticket booking ref is " + tmpBookingRef);
                                    EMDCurrentRec.group = tmpGroup;
                                    EMDCurrentRec.bookingRef = tmpBookingRef;
                                    break;
                                }
                                idxEMDRecs ++;
                            }
                            // finally if found then update the appropriate record in the EMDTkts vector
                            // assuming that TMCD records always precede their associated MFP records in the data file
                            // so EMDTkts already populated
                            if (idxEMDFound) {
                                EMDTkts.set(idxEMDRecs, EMDCurrentRec);
                                application.log.fine("EMD ticket record updated " + EMDCurrentRec.docID + " " + EMDCurrentRec.sellingFareAmount + " " + EMDCurrentRec.remainingTax + " " + EMDCurrentRec.airline + " " + EMDCurrentRec.ticketNo + " " + EMDCurrentRec.group + " " + EMDCurrentRec.bookingRef );
                            }
                        }
                    }
                    fileRec = (AirRecord) recData.nextElement();

                } // end of while loop , going through records followed after I-
                // pax record till next I- record found
                
                // There may be EMD data present but not necessarily relating to the current passenger
                if ((EMDfound) && (EMDTkts.size() > 0)) {
                    // Use data from first EMD record for validation checks
                    EMDCurrentRec = (EMDRecord) EMDTkts.get(0);
                    if (recTicketNo.equals("")) {
                        recTicketNo = EMDCurrentRec.ticketNo;
                    }
                    if (recAirline.equals("")) {
                        recAirline = EMDCurrentRec.airline;
                    }
                    if (group.equals("")) {
                        group = EMDCurrentRec.group;
                    }
                    if (recBookingRef.equals("")) {
                        recBookingRef = EMDCurrentRec.bookingRef;
                    }
                }

                if (showContents) {
                    application.log.finest("pax:" + numPaxRecs + " " +
                        recPassengerName + " tkt:" + recTicketNo);
                }

                /*
                 * commented because it was adding season 04 for each pax record
                 * I- if (!voidTkt) { Calendar cal = Calendar.getInstance(); int
                 * thisYear = (cal.get(Calendar.YEAR));
                 *
                 * recDeptDate += (String.valueOf(thisYear)).substring(2,4) ;
                 * System.out.println("recDEptDate = " + recDeptDate);
                 *
                 * if (!DateProcessor.checkDate(recDeptDate, "ddMMMyyyy" )) {
                 * application.log.severe("F:" + fileToProcess.getName() +
                 * ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", " + " Departure
                 * Date" + recDeptDate + " is invalid date format error");
                 * return 2;}
                 *
                 * finalDeptDate = DateProcessor.parseDate(recDeptDate + "0000",
                 * "ddMMMyyhhmm"); if (finalDeptDate == null) {
                 * application.log.severe("F:" + fileToProcess.getName() +
                 * ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", " + " Departure
                 * Date" + recDeptDate + " is invalid date, error"); return 2;}
                 *
                 *  // determine season of departure e.g S03 // if between 1st
                 * May and 31-Oct then Summer // else Winter recSeason =
                 * DateProcessor.calcSeason(new
                 * java.sql.Date(finalDeptDate.getTime()), 3);
                 * System.out.println("recSeason = " + recSeason);
                 *  } // not a void
                 *
                 */
                // now validate all numbers
                stage = "validating all extracted data";
                if (!NumberProcessor.validateStringAsNumber(recTicketNo)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + " tkt:" + recTicketNo +
                        " is not numeric (or missing) error");
                    return 2;
                }

                if (exchangeFound &&
                    !NumberProcessor
                    .validateStringAsNumber(recExchangeTicketNo)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", exchange tkt:" +
                        recExchangeTicketNo +
                        " is not numeric (or missing) error");
                    return 2;
                }

                if (!NumberProcessor.validateStringAsNumber(recAirline)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + " tkt:" + recTicketNo +
                        ", airline:" + recAirline +
                        " is not numeric error");
                    return 2;
                }

                application.log.fine("Booking Ref no before calling tlink is " + recBookingRef);
                application.log.fine("group before calling tlink is " + group);
                application.log.fine("pnr " + recPNR);
                application.log.fine("pnrdate " + recPNRdate);
                application.log.fine("recPseudoCityCode " + recPseudoCityCode);
                application.log.fine("specialistPseudoList " + specialistPseudoList);

                if (specialistPseudoList.indexOf(recPseudoCityCode) >= 0) {
                    // if (recPseudoCityCode.equals("38HJ")) {
                    application.log.fine("inside HJ");
                    if (recBookingRef.equals("") || recBookingRef.equals("0")) {

                        // V1.1 for HandJ populate  booking ref and branch code from Travel Link
                        application.log.fine("calling stored proc to populate booking ref from travelink view");

                        stage = "calling stored proc to populate booking ref from travelink view";
                        CallableStatement cstmt_tl = conn.prepareCall("{?=call p_stella_get_data.sp_populate_travelink_ref(?,?)}");

                        application.log.fine("pnr " + recPNR);
                        application.log.fine("pnrdate " + recPNRdate);

                        cstmt_tl.registerOutParameter(1, Types.CHAR);
                        //oracle.jdbc.driver.OracleTypes.CURSOR);



                        cstmt_tl.setString(2, recPNR); // pnr no
                        cstmt_tl.setString(3, recPNRdate); // pnr creation date

                        cstmt_tl.execute();

                        if (cstmt_tl.getString(1) != null) {
                            System.out.println("Output of tLink Execute statement :" + cstmt_tl.getString(1));
                            application.log.fine("Output of tLink Execute statement :" + cstmt_tl.getString(1));

                            if (cstmt_tl.getString(1).startsWith("Error")) {
                                application.log.severe("F:" +
                                    fileToProcess.getName() + ",PNR:" + recPNR +
                                    "/" + recPNRdate + ", failed to retrieve Travelink Ref:" +
                                    cstmt_tl.getString(1) + ", fix and retry next run");
                                // return 1 so moved to recycle and will try again
                                // until foriegn key satisfied
                                //return 1;
                            } else if (cstmt_tl.getString(1).equals("0")) { // can not find ref in Travel Link view
                                application.log.warning("F:" +
                                    fileToProcess.getName() + ",PNR:" + recPNR +
                                    "/" + recPNRdate + ", failed to retrieve Travelink Ref:   Booking Ref. used : 0  ,  Branch Code used : HAJS ");
                            } else {
                                bookingRef = cstmt_tl.getString(1);
                            }
                        }

                        /*if (!bookingRef.equals("0") ){ // does not exists in travel link
                        System.out.println("recBookingRef inside if " + bookingRef);
                        group = bookingRef.substring(0,1);
                        recBookingRef = bookingRef.substring(1,bookingRef.length()) ;
                        System.out.println("recBookingRef inside if " + recBookingRef);
                            }*/


                        if (!bookingRef.equals("0")) { // does  exists in travelink

                            recBookingRef = bookingRef.substring(1, bookingRef.length());
                            group = bookingRef.substring(0, 1);
                            application.log.fine("group from travelink " + group);
                            application.log.fine("recBookingRef from travelink " + recBookingRef);
                        } else {
                            group = "L"; //default is HAJS group
                            application.log.warning("F:" + " Please Correct Branch Code, defaulted to: " + group);
                        }
                        /*
                        stage = "calling stored proc to retrieve branch code";
                        CallableStatement cstmt1 = conn.prepareCall("{?=call p_stella_get_data.get_specialist_branch(?)}");//group_code
                        cstmt1.registerOutParameter(1,oracle.jdbc.driver.OracleTypes.CURSOR );
                        cstmt1.setString(2, group);  // eg :  A,L,S,M,C

                        cstmt1.execute();

                        rs =  (ResultSet)cstmt1.getObject(1);
                        rs.next();   // There is always one branch code for this group
                        recBranchCode =   rs.getString("branch_code"); // this will have a values in (HAJS,UACS,SOVE,CITA,MEON)
                        */
                    }

                } else { // for First Choice AIRS leave it blank

                    if (recBookingRef.equals("") || recBookingRef.equals("0")) {
                        // use a dummy booking ref to indicate it is missing
                        recBookingRef = "0";
                        // give warning message about dummy values which have been
                        // used
                        // raise an error message, but allow processing to continue
                        application.log.warning("F:" + fileToProcess.getName() +
                            ",PNR:" + recPNR + "/" + recPseudoCityCode +
                            " tkt:" + recTicketNo +
                            ", no booking ref. Used 0 ");
                    }
                }

                if (!NumberProcessor.validateStringAsNumber(recBookingRef)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", bkg:" + recBookingRef +
                        " is not numeric error");
                    return 2;
                }

                if (!NumberProcessor.validateStringAsNumber(recIATANum)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", iata:" + recIATANum +
                        " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recGBTax)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", gbtax:" + recGBTax +
                        " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recPublishedFare)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", pub fare:" +
                        recPublishedFare + " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recSellingFare)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", selling fare:" +
                        recSellingFare + " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recUBTax)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", ubtax:" + recUBTax +
                        " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recRemainTax)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", remtax:" +
                        recRemainTax + " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recCommAmt)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", commission amt:" +
                        recCommAmt + " is not numeric error");
                    return 2;
                }
                if (!NumberProcessor.validateStringAsNumber(recCommPct)) {
                    application.log.severe("F:" + fileToProcess.getName() +
                        ",PNR:" + recPNR + "/" + recPseudoCityCode +
                        " tkt:" + recTicketNo + ", commission pct:" +
                        recCommPct + " is not numeric error");
                    return 2;
                }

                /*
                 * if (recBranchCode.equals("DUMM")) {
                 * application.log.severe("F:" + fileToProcess.getName() +
                 * ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" +
                 * recTicketNo+ ", no branch code found. Loaded as DUMM"); //
                 * this error allow processing to continue so do not return
                 * //return 2; }
                 */

                if ((!voidTkt) && (!exchangeFound)) { // also if not Exchange
                    // tickets ???
                    // now derive commission amt or commission pct in case both
                    // are not supplied
                    // convert to numbers so can test contents and work out pct
                    // if pct is not supplied,
                    // and work out amt if amt is not supplied
                    application.log.fine("commamt debug 1 " + recCommAmt);
                    application.log.fine("commpct debug 1 " + recCommPct);

                    commAmt = new BigDecimal(recCommAmt);
                    commPct = new BigDecimal(recCommPct);
                    application.log.fine("commamt debug 2 " + commAmt);
                    application.log.fine("commpct debug 2 " + commPct);

                    publishedFareAmt = (new BigDecimal(recPublishedFare));
                    if (publishedFareAmt.compareTo(new BigDecimal("0")) == 0) {  // publishedFareAmt == 0
                        // prevent divide by zero error
                        // comm amt and pct must be 0
                        recCommAmt = String.valueOf(0);
                        recCommPct = String.valueOf(0);
                    } else {
                        if (commAmt.compareTo(new BigDecimal("0")) == 0 && commPct.compareTo(new BigDecimal("0")) != 0) {
                            // calc amt from pct * published fare
                            commAmt = (publishedFareAmt.multiply(commPct)).divide(new BigDecimal("100"), 2, 4);
                            recCommAmt = String.valueOf(commAmt);
                        } else if (commAmt.compareTo(new BigDecimal("0")) != 0 && commPct.compareTo(new BigDecimal("0")) == 0) {
                            // calc pct from amt / published fare
                            commPct = (commAmt.divide(publishedFareAmt, 2, 4)).multiply(new BigDecimal("100"));
                            recCommPct = String.valueOf(commPct);

                        }
                    }
                } else if (voidTkt) {
                    // for Void Ticket
                    // recPublishedFare = "0.00";
                    // recSellingFare = "0.00";
                    insPublishedFare = "0";
                    insSellingFare = "0";
                    recCommPct = "0";
                    recCommAmt = "0";
                    recGBTax = "0";
                    recUBTax = "0";
                    recRemainTax = "0";
                    insOtherTaxes = "";

                    finalDeptDate = null;
                    recPassengerName = "VOID";
                    // v1.10 Leave recBookingRef assigned
                    // recBookingRef = "";
                    recSeason = "";
                    recBranchCode = "";
                    recTicketAgent = voidTicketInitials; //  This id default
                    // hardcoded
                } else if (exchangeFound) {
                    recCommPct = "0";
                    recCommAmt = "0";
                    /*
                     * insPublishedFare = "0"; insSellingFare = "0"; insGBTax =
                     * "0"; insUBTax = "0"; insRemainTax = "0"; insOtherTaxes =
                     * "";
                     */
                }

                // if this is first ticket in file then pass I for insert
                // else pass U for update
                if (numPaxRecs == 1) { // replaces paxNo by numPaxRecs
                    recInsertUpdateFlag = "I";
                } else {
                    recInsertUpdateFlag = "U";
                }

                // usually only have to insert one ticket, BUT sometimes, it may
                // be a conjunctive AIR
                // this means you have to insert the main ticket with all values
                // gotten so far
                // but then also insert some dummy tickets with zero amounts but
                // on same pnr
                // This is done because sometimes there are too many sectors to
                // fit onto one ticket stub,
                // Conjunctive ticket number are the one followed by - , first 8
                // digit will be same as main tkt only last two digits gets
                // changed

                stage = "inserting tickets";
                String insTktNum = "";
                String conjTktInd = "N";

                // if (conjTktInd.equals("Y")) {

                System.out.println("noOfConjTkts" + noOfConjTkts);
                System.out.println("noOfEMDTkts" + EMDTkts.size());

                insTktNum = recTicketNo; // This is original ticket for i= 0
                // insert

                if ((EMDfound) && (EMDTkts.size() > 0)) {
                    noOfTkts = EMDTkts.size();
                } else {
                    noOfTkts = noOfConjTkts;
                }
                application.log.finest("EMDfound:" + EMDfound);
                application.log.finest("noOfTkts:" + noOfTkts);
                for (int i = 1; i <= noOfTkts; i++) {

                    // i =1 does insert for main ticket

                    // loop through each conjunctive ticket required and insert
                    // a row for each one
                    // ensure there is always at one least one ticket inserted
                    // (the main one)

                    if ((i > 1) && (!((EMDfound) && (EMDTkts.size() > 0)))) {
                        // we are now processing the conjunctive "dummy" tickets
                        // so reset amounts to 0
                        application.log.finest("conjloop:" + i);
                        insPublishedFare = "0";
                        insSellingFare = "0";
                        recCommAmt = "0";
                        recCommPct = "0";
                        insGBTax = "0";
                        insRemainTax = "0";
                        insUBTax = "0";
                        insOtherTaxes = "0";
                        conjTktInd = "Y";

                        insTktNum = Long
                            .toString(Long.parseLong(insTktNum) + 1);

                        System.out.println("	insTktNum in Conjunctive tkt");
                        System.out.println("	tktRecord" + tktRecord);
                        System.out.println("	conj tkt no" + insTktNum);

                        if (exchangeFound) {
                            // Get the next Exchange ticket number if this is a
                            // exchange of conjunction ticket
                            recExchangeTicketNo = Long.toString(Long
                                .parseLong(recExchangeTicketNo) + 1);
                        }

                    }
                    if ((EMDfound) && (EMDTkts.size() > 0)) {
                        // we are now processing the EMD tickets
                        application.log.finest("EMDloop:" + i);
                        EMDCurrentRec = (EMDRecord) EMDTkts.get(i - 1);
                        if (!EMDCurrentRec.ticketNo.equals("")) {
                            recTicketNo = EMDCurrentRec.ticketNo;
                            insTktNum   = EMDCurrentRec.ticketNo;
                        }
                        if (!EMDCurrentRec.airline.equals("")) {
                            recAirline = EMDCurrentRec.airline;
                        }
                        insPublishedFare = "0";
                        insSellingFare = EMDCurrentRec.sellingFareAmount;
                        recCommAmt = "0";
                        recCommPct = "0";
                        insGBTax = "0";
                        insRemainTax = EMDCurrentRec.remainingTax;
                        insUBTax = "0";
                        insOtherTaxes = "0";
//                        conjTktInd = "Y";
                        if (!EMDCurrentRec.group.equals("")) {
                            group = EMDCurrentRec.group;
                        }
                        if (!EMDCurrentRec.bookingRef.equals("")) {
                            recBookingRef = EMDCurrentRec.bookingRef;
                        }

                        System.out.println("	insTktNum in EMD tkt");
                        System.out.println("	tktRecord" + tktRecord);
                        System.out.println("	EMD tkt no" + insTktNum);

//                        if (exchangeFound) {
//                            // Get the next Exchange ticket number if this is a
//                            // exchange of conjunction ticket
//                            recExchangeTicketNo = Long.toString(Long
//                                .parseLong(recExchangeTicketNo) + 1);
//                        }

                    }

                    application.log.fine("recTicketDate " + recTicketDate);
                    if (showContents) {
                        application.log.finest("ins rec, pnr:" +
                            recPNR +
                            " dep:" +
                            finalDeptDate +
                            " origtkt:" +
                            recTicketNo + // the original/main ticket number
                            " tkt:" +
                            insTktNum + // the actual ticket number (may be different
                            // in case of conjunctive)
                            " air:" +
                            recAirline +
                            " br:" +
                            recBranchCode +
                            " bkg:" +
                            recBookingRef +
                            " seas:" +
                            recSeason +
                            " etkt:" +
                            recETicketInd +
                            " ag:" +
                            recTicketAgent +
                            " tour:" +
                            recTourCode +
                            " pubamt:" +
                            insPublishedFare +
                            " selamt:" +
                            insSellingFare +
                            " cmamt:" +
                            recCommAmt +
                            " cmpct:" +
                            recCommPct +
                            " IATA:" +
                            recIATANum +
                            " tktdt:" +
                            DateProcessor.parseDate(recTicketDate,
                                "yyMMdd") +
                            //DateProcessor.parseDate(recTicketDate +
                            // "0000","yymmddhhmm") +
                            //" numpax:" + String.valueOf(numPaxRecs) +
                            " numpax:" + "1" + " gbtax:" + insGBTax +
                            " remtax:" + insRemainTax + " ubtax:" +
                            insUBTax + " ccy:" + recCcyCode + //ccy code
                            " city:" + recPseudoCityCode + " paxtype:" +
                            recPassengerType + " pax:" +
                            recPassengerName.trim() + " i/u:" +
                            recInsertUpdateFlag + " exc:" +
                            recExchangeTicketNo + " fb:" +
                            recFareBasisCode + " cj:" + conjTktInd + // should
                            // be Y
                            // if
                            // not
                            // the
                            // main
                            // ticket,
                            // but
                            // a
                            // "child"
                            // of
                            // the
                            // main
                            // ticket
                            // i.e.
                            // created
                            // because
                            // it
                            // is a
                            // conjunctive
                            // tkt
                            " tickettype:" + recTicketType + " othertaxes:" +
                            insOtherTaxes + " grp:" + group);
                    }

                    // finally call the stored procedure to do the insert into
                    // database
                    stage = "calling stored proc p_stella_get_data.insert_ticket()";

                    CallableStatement cstmt = conn
                        .prepareCall("{?=call p_stella_get_data.insert_ticket(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
                    cstmt.registerOutParameter(1, Types.CHAR);
                    cstmt.setString(2, "AL");
                    cstmt.setString(3, recPNR.trim()); //pnr
                    if (voidTkt || (finalDeptDate == null)) { // if H- record missing and finalDeptDate is null it is currently failing
                        // with NullPointerException, at line 2444 and therefore added (finalDeptDate == null) , 28/04/2010
                        // cstmt.setDate(4, null); //finalDeptDate); is null
                        cstmt.setNull(4, java.sql.Types.DATE);
                    } else {
                        cstmt.setDate(4, new java.sql.Date(finalDeptDate
                            .getTime()));
                    }
                    cstmt.setString(5, insTktNum.trim()); //tkt num
                    cstmt.setString(6, recAirline.trim()); // airline
                    cstmt.setString(7, recBranchCode.trim()); //branch
                    cstmt.setString(8, recBookingRef.trim()); //booking ref

                    cstmt.setString(9, recSeason.trim()); //season , for void
                    // this will be blank

                    cstmt.setString(10, recETicketInd.trim()); //eticketind
                    cstmt.setString(11, recTicketAgent.trim()); //ticketagntinitials
                    cstmt.setString(12, recCommAmt.trim()); // commamt
                    cstmt.setString(13, recCommPct.trim()); //commpct
                    cstmt.setString(14, insSellingFare.trim()); // selling fare
                    cstmt.setString(15, insPublishedFare.trim()); // published
                    // fare
                    cstmt.setString(16, recIATANum.trim()); // iata num
                    cstmt.setString(17, "AL"); // entry user id
                    if (voidTkt) {
                        // We don't receive Ticket Issue date in D- record for
                        // void ticket
                        cstmt.setNull(18, java.sql.Types.DATE);
                    } else {
                        cstmt.setDate(18, new java.sql.Date(DateProcessor
                            .parseDate(recTicketDate, "yyMMdd").getTime()));
                    }
                    cstmt.setString(19, "1"); // hard code in 1 - always one for
                    // MIR files
                    cstmt.setString(20, recPassengerName.trim());
                    cstmt.setString(21, insGBTax.trim());
                    cstmt.setString(22, insRemainTax.trim());
                    cstmt.setString(23, insUBTax.trim());

                    if (recTicketNo.equals(insTktNum)) {
                        // if it's the main ticket as opposed to a conjunctive
                        // one, insert null
                        cstmt.setNull(24, java.sql.Types.VARCHAR); // linked
                        // conjunctive
                        // ticket
                    } else {
                        cstmt.setString(24, recTicketNo); // linked conjunctive
                        // ticket
                    }

                    cstmt.setString(25, recCcyCode.trim()); //ccy code
                    cstmt.setString(26, recPseudoCityCode); //city code
                    cstmt.setString(27, recPassengerType.trim());
                    cstmt.setString(28, "AMA"); // galileo ticket doc type
                    cstmt.setString(29, recInsertUpdateFlag); // I = insert of
                    // pnr (first
                    // ticket in file)
                    // or U = an
                    // update (second
                    // ticket in file)
                    cstmt.setString(30, recExchangeTicketNo);
                    cstmt.setNull(31, java.sql.Types.INTEGER);
                    cstmt.setString(32, recTourCode);
                    cstmt.setString(33, recFareBasisCode);

                    cstmt.setString(34, i == 0 ? "N" : conjTktInd); // conj tkt
                    // ind

                    // This is added for AirLoad, In MirLoad this won't be
                    // passed
                    if (voidTkt) {
                        // We don't receive PNR Creation Date in D- record for
                        // void ticket
                        cstmt.setNull(35, java.sql.Types.DATE);
                    } else {
                        cstmt.setDate(35, new java.sql.Date(DateProcessor
                            .parseDate(recPNRdate, "yyMMdd").getTime()));
                    }

                    cstmt.setString(36, insOtherTaxes);
                    cstmt.setString(37, recTicketType);
                    cstmt.setString(38, group); // This field is only used for Specialist branch group code eg :  A,L,S,M,C

                    cstmt.execute();

                    // there is no commit in the stored procedure
                    if (cstmt.getString(1) != null) {
                        System.out.println("Output of SQL execute statement :" +cstmt.getString(1) );
                        application.log.fine("Output of SQL execute statement :" +cstmt.getString(1) );

                        if (cstmt.getString(1).startsWith("Error, (fk)")) {
                            application.log.warning("F:"
                                            + fileToProcess.getName() + ",PNR:" + recPNR
                                            + "/" + recPseudoCityCode + " tkt:"
                                            + recTicketNo + ", failed:"
                                            + cstmt.getString(1) + ", retry next run");
                            // return 1 so moved to recycle and will try again
                            // until foreign key satisfied
                            return 1;
                        } else if (cstmt.getString(1).startsWith("Error")) {
                            // severe error in the stored procedure
                            application.log.severe("F:"
                                            + fileToProcess.getName() + ",PNR:"
                                            + recPNR + "/" + recPseudoCityCode
                                            + " tkt:" + recTicketNo + ", failed:"
                                            + cstmt.getString(1)
                                            + ", moved to error area");
                            // return 2 so moved to error and will not try again
                            // next run
                            return 2;
                        }
                    } else {
                            numRowsInserted++;
                        if (i == 1) {
                            // the original ticket insert, not any conjunctive
                            // ones
                            insertedTickets++;
                        }
                        if ((EMDfound) && (EMDTkts.size() > 0)) {
                            insertedEMDTickets++;
                        }
                    }
                    cstmt.close();

                } // end for loop through conj / EMD tickets

            } // end of vector for loop

            //}

            stage = "finished processing file";
            
            if ((EMDfound) && (EMDTkts.size() > 0)) {
                if (insertedEMDTickets != EMDRecs.size()) {
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:" +
                        recPNR + "/" + recPseudoCityCode + " num EMD records (" +
                        EMDRecs.size() + ") <> num inserts (" + insertedEMDTickets +
                        "), please check");
                    return 2;
                }
                if (BookingRefInconsistent) {
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:" +
                        recPNR + "/" + recPseudoCityCode + " booking references in file are inconsistent, please check");
                    return 2;
                }
            }
            
            if (insertedTickets != numPaxRecs) {
                application.log.severe("F:" + fileToProcess.getName() + ",PNR:" +
                    recPNR + "/" + recPseudoCityCode + " num pax (" +
                    numPaxRecs + ") <> num inserts (" + insertedTickets +
                    "), error");
                return 2;
            }
            
            // no errors , processing OK, return OK return code
            conn.commit();
            return 0;

        } catch (Exception ex) {
            String returnStr = "SYSTEM FAILURE F:" + fileToProcess.getName() +
                " failed while " + stage + ". [Exception " +
                ex.getClass() + " " + ex.getMessage() + "]";
            application.log.severe(returnStr);
            //application.log.severe( ex.fillInStackTrace().toString());

            ex.printStackTrace();
            ByteArrayOutputStream ostr = new ByteArrayOutputStream();
            ex.printStackTrace(new PrintStream(ostr));
            application.log.severe(ostr.toString());
            return 3;
        }

    }

    /**
     * Process fare record , this is modularise to avoid repeating equivalent
     * currency logic
     *
     * @param recIdToProcess
     *            record id will be K-F, KN-F, K-I, or KN-I
     * @param oneLine
     *            this is a record line
     * @param showContents
     *            true if debug on, false if not on
     * @return fareAmt , this will be either Published Fare or Selling Fare
     *         depending on record Id
     */

    public static String populateFare(String recIdToProcess, String oneLine,
        boolean showContents) {

        /** read and process each individual file */

        int startPos = 0;
        String stage = "start of processing Fare Record";
        String fareAmt = "0";
        boolean fareFound = false;

        if ((recIdToProcess.length()) == 3) { // K-F and K- I,K-B
            startPos = 3;
        } else if (recIdToProcess.length() == 4) { // KN-F and KN-I,KN-B
            startPos = 4;
        }

        // get the position of first and second delimiter (;) if it is
        // equivalent currency case
        int pos1 = oneLine.indexOf(";");
        int pos2 = StringUtils.searchStringOccur(oneLine, ";", 2);

        if (oneLine.substring(startPos, startPos + 3).equalsIgnoreCase("GBP")) {
            fareAmt = oneLine.substring(startPos + 3, pos1);
            fareFound = true;
        }
        if (!fareFound) { // In case of Equivalent currency
            if (oneLine.substring(pos1 + 1, pos1 + 4).equalsIgnoreCase("GBP")) {
                fareAmt = oneLine.substring(pos1 + 4, pos2);
            }
        }
        //					 else if
        //					     "Error"

        return fareAmt.trim();

    } // end of populateFare function

}

