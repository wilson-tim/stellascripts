package uk.co.firstchoice.stella;

import java.io.*;
import java.sql.*;
import java.util.Date;
import java.util.Enumeration;
import java.util.Vector;

import uk.co.firstchoice.util.*;
//import uk.co.firstchoice.util.logging.*;
import uk.co.firstchoice.util.businessrules.DateProcessor;
import uk.co.firstchoice.util.businessrules.NumberProcessor;
import uk.co.firstchoice.util.ddlparser.Ddl;
import uk.co.firstchoice.util.ddlparser.DdlField;



/**
 * Class to load flat files from MIR flight tickets system into Stella system database
 *
 * Creation date: (28/11/02 16:38:07 pM)
  * Contact Information: FirstChoice
  * @version 1.05
  * @author: Leigh Ashton
 */

/*


	Datawarehouse live server connection URL : jdbc:oracle:thin:@dwlive_en1:1521:dwl

	Required inputs

	Usage Option
		-m run mode   (L for Live or T for Test - so can get correct parameters from app registry)
		if internal connection not to be used:
		-d driverClass   ( Driver Class for connecting to ORACLE/SYBASE )
		-c connectionURL ( Connects to the database where CRM_EXTRACT_SETUP,CRM_EXTRACT_LOG,CRM_EXTRACT_ERROR
						   tables are kept)
		-u userid        ( User id  for connecting to database )
		-p passwd		 ( Password for connecting to database )
		-f single filename (run for a single file only instead of all pending)


*/
public class StellaMIRLoad {


    // Class constants
    private static final String programName = "StellaMIRLoad";
    private static final String programShortName = "STELMIRL";
    private static final String programVersion = "1.05";
    private static int numRowsInserted = 0;

	    // The application object. Provides logging, properties etc
    private static final Application application = new Application(
	                                    programName,
	                                    "Routines to import data "
	                                    + " into data warehouse from Galileo "
	                                    + " flight ticketing systems (MIRS)");









   /** main method ,
    *   called from outside ie.command line.
    *   set up and then loop through all files to be processed
    * @param args arguments from command line
    */
    public static void main(String[] args) {

        String   driverClass    = "";
        String   connectionURL  = "";
        String   dbUserID	    = "";
        String   dbUserPwd      = "";

        String runMode = "LIVE";
        String singleFileName = "none";


        // start processing



        System.out.println(programName + "v." + programVersion  +" started");

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

            }
        }

        // actually run the method to do the processing
        String retCode = runLoad( driverClass, connectionURL, dbUserID, dbUserPwd, singleFileName, runMode);
        System.out.println("end, return was:" +retCode);
        if (retCode.substring(0,2).equals("OK") ) { System.exit(0);}
        else { System.exit(1);}

        return;

    }

    // end of main method

    /** runLoad method to load data from mir files into stella database
     *  this method actually does the work
     * @param driverClass jdbc driver class for database interaction
     * @param connectionURL database conn url
     * @param dbUserID database user id to use
     * @param dbUserPwd database user password
     * @param singleFileName if want to run in singlefile mode then specify a filename (without directory)
     * here, otherwise pass ""
     * @param runMode Test or Live : determines if in debug mode
     * @return return code 1 for failure, 0 if success
     */
    public static String runLoad(String driverClass,String  connectionURL,String  dbUserID,String  dbUserPwd,String  singleFileName,String runMode)
    {

        String rowsetTag =  null;

        Ddl ddl ;
        DdlField[] ddlFields = null;


        String dirData, dirBackup, dirRecycle, logPath, ddlFileName, headerRecPrefix, dirError;
        int headerLength = 0, tktPermittedSectors= 0;

        boolean debugMode = false;
        int numFilesRead = 0, numFilesError = 0, numFilesSuccess = 0, numFilesRecycle =0;

        String logFileName = "";
        String returnStr = "";
        String stage; // used to denote place in code where any exception happened
        String logFileReturnValue = "";
        String logLevel; // min logging level to be logged

        // Create a instance of StellaMIRLoad class
	 StellaMIRLoad   f =  new StellaMIRLoad();




	// create database connection

        // the dbmanager trys to connect to the internal oracle connection
        // first before using these dbconnection parameters
        // better to use internal connection if possible
        // but leave these params avail in case want to run from outside Oracle java stored procedures.

	stage = "setup vars";
//	DBManager dbManager = new DBManager(
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
        application.setRegistry(programShortName,runMode,"ALL",dbManager.getConnection());

        // find the log file path and create the logger
        // if running from client PC then use local paths
        // otherwise must be running on server

        try {
            // get the correct build path
            if (application.getAccessMode() == AccessMode.CLIENT) {
                logPath   = application.getRegisteryProperty("LocalLogFilePath");
                dirData   = application.getRegisteryProperty("LocalFilePath");
                dirBackup = application.getRegisteryProperty("LocalBackupPath");
                dirRecycle= application.getRegisteryProperty("LocalRecyclePath");
                dirError  = application.getRegisteryProperty("LocalErrorPath");
                ddlFileName = application.getRegisteryProperty("LocalDDLFileName");

                application.log.info("Database:" + connectionURL +","+ driverClass +","+ dbUserID +","+ dbUserPwd);

            } else {
                logPath   = application.getRegisteryProperty("ServerLogFilePath");
                dirData   = application.getRegisteryProperty("ServerFilePath");
        	dirBackup = application.getRegisteryProperty("ServerBackupPath");
		dirRecycle= application.getRegisteryProperty("ServerRecyclePath");
                dirError  = application.getRegisteryProperty("ServerErrorPath");
                ddlFileName = application.getRegisteryProperty("ServerDDLFileName");

            }
            headerRecPrefix = application.getRegisteryProperty("Headerrecprefix");

            logLevel = application.getRegisteryProperty("LogLevel");

            headerLength    = Integer.parseInt(application.getRegisteryProperty("HeaderLength"));
            tktPermittedSectors = Integer.parseInt(application.getRegisteryProperty("TktPermittedSectors"));

        } catch (PropertyNotFoundException ex) {

            application.log.severe( "Error: unable to find registry property. " + ex.getMessage() );

            return "Error: unable to find registry property. " + ex.getMessage();
        }
        //fileUtils myFU = new fileUtils();
        application.setLoggerLevel(logLevel);

        application.setLogSysOut(false); // don't log to sysout as well as to file
        logFileName =  programShortName.toLowerCase() + "_" +  ( FileUtils.fileGetTimeStamp()) + ".log";
        application.log.config("Log level is:" + logLevel);
        application.log.config("Logfile is: " + logFileName);
        System.out.println("Logfile is: " + logFileName);
        application.log.setLoggerFile(new File(logPath, logFileName));


        logFileReturnValue = "|Lf#" + logPath +"/" +  logFileName; // returned at end of program so that calling program knows log file name

        //
        // Write out header to the log filI
        //

        application.log.info(programName+ " v." + programVersion);
        application.log.config("Runmode:" + runMode);
        application.log.info("START " + java.util.Calendar.getInstance().getTime());
        application.log.config("Access mode:" + application.getAccessMode());

        try {
            application.log.config("Name         => " + application.getRegisteryProperty("Name"));
            application.log.config("Version      => " + application.getRegisteryProperty("Version"));
            application.log.config("LogPath      => " + logPath );
            application.log.config("FilePath     => " + dirData );
            application.log.config("Backuppath   => " + dirBackup );
            application.log.config("Recyclepath  => " + dirRecycle );
            application.log.config("Errorpath    => " + dirError );
            application.log.config("Headerlength => " + headerLength);
            application.log.config("Headerrecprefix=> " + headerRecPrefix);
            application.log.config("ddlfilename => " + ddlFileName);


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
		File recycleDataDirectory = new File(dirRecycle);
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
		if (recycleDataDirectory.isDirectory()) {
			application.log.info("data :" + recycleDataDirectory.getAbsolutePath() +
					" exists OK.");
		}
		else {
                        returnStr = "ERROR data :" + dirRecycle + " does not exist.";
			application.log.severe(returnStr);
                        return returnStr+logFileReturnValue;
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
                            File indFile = new File (fileDataDirectory.getAbsolutePath(), contents[i]);
                            application.log.info(contents[i] + "  " + new Date(indFile.lastModified()));
                        }
		}

		// now read in ddl file which is used to define record layout of input file header


                // read in ddl file into memory
                  int reclen;
                  // Check the ddl filename
                  File ddlFile = new File(ddlFileName);
                  if (!ddlFile.isFile()) {
                        returnStr = "ERROR data :" + ddlFileName + " does not exist.";
			application.log.severe(returnStr);
                        return returnStr + logFileReturnValue;

                  }
                  application.log.info("ddl F:" + ddlFileName + " exists");
                  ddl = new Ddl(ddlFileName);

                  // Get all the fields after reading DDL file and converting each field into
                  // Ddlfield object array
                  ddlFields = ddl.getDdlFields();

                  // Fix len record
                  reclen = ddl.getReclen();

                  rowsetTag = ddl.getRowSetTag ();
                  if (debugMode) {application.log.finest(rowsetTag + "_" + reclen);}
                  if ((rowsetTag == null) || (reclen == 0) || !rowsetTag.equals("mirheader") ) {
                       System.out.println("Hdr Record tag is not defined" );

                       returnStr = "ERROR hdr record tag not defined";
                    	application.log.severe(returnStr);
                        return returnStr + logFileReturnValue;

                  }
                  if (debugMode) {
                    for (int i = 0; i < ddlFields.length ; i++) {
						 application.log.finest(ddlFields[i].name + " length:" + ddlFields[i].length + " Start:" + ddlFields[i].start);

                    }
                  }



		stage = "process files";
                File fileToProcess;
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
				fileToProcess = new File (fileDataDirectory, singleFileName);
			}

			if (fileToProcess.exists()) {
				// ok
			}
			else {

                                returnStr = "ERROR datafile " + fileToProcess + " does not exist.";
                                application.log.severe(returnStr);
                                return returnStr + logFileReturnValue;
			}
			if (fileToProcess.canRead()) {
				// ok
			}
			else {

                                returnStr = "ERROR datafile " + fileToProcess +
									" cannot be read.";
                                application.log.severe(returnStr);
                                return returnStr + logFileReturnValue;

			}
                        application.log.fine(""); // blank line ot separate files in log
			application.log.info("File " + (i+1) + " :" + fileToProcess + " "
				+ fileToProcess.length() + " bytes" + " at:"   + java.util.Calendar.getInstance().getTime());


			// now have valid filehandle

			stage = "about to processFile";

			int intReturn = f.processFile(fileToProcess, debugMode, ddlFields, headerRecPrefix, headerLength, dbManager.getConnection(), tktPermittedSectors);

			stage = "eval return from processFile";

			// 0 for success
			// 1 for move to recycle so will be processed in next run
			// 2 for error
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
					returnStr = "ERROR datafile " + fileToProcess +" cannot be moved to backup.";
                                        application.log.severe(returnStr);
                                        return returnStr + logFileReturnValue;

				}
				numFilesSuccess ++;
                                application.log.fine(fileToProcess + " processed OK");
				break;
                        }
			case 1:
				// error, but non-critical so can be recycled
                                // e.g. data needs foreign key fulfilled before can be inserted
                        {
                                File moveFile = new File (recycleDataDirectory, fileToProcess.getName());
				if (fileToProcess.renameTo(moveFile )) {
					// successful move
					application.log.info(fileToProcess + " non-success. Moved to recycle");
				} else {

					returnStr = "ERROR datafile " + fileToProcess +" cannot be moved to recycle.";
                                        application.log.severe(returnStr);
                                        return returnStr + logFileReturnValue;

				}
				numFilesRecycle ++;
				break;
                        }
			case 2:
				// non-critical error , log and move on
				// rename with error suffix and move to error directory
				application.log.info("ERROR datafile " + fileToProcess + " failed, renamed as error file");
                                if (fileToProcess.getName().length() > 2 ) {
                                    if ( !fileToProcess.getName().substring(0,3).equals("err") ) {
                                        newFileName = "err_" + fileToProcess.getName();
                                    }
                                    else {
                                        newFileName = fileToProcess.getName();
                                    }
                                }
                                else {
                                    newFileName = "err_" + fileToProcess.getName();
                                }

				File moveFile = new File (errorDataDirectory, newFileName);
				if (fileToProcess.renameTo(moveFile )) {
					// successful move
					application.log.info(fileToProcess + " renamed as error, moved to error area");
				} else {
        				returnStr = "ERROR datafile " + fileToProcess +" cannot be renamed as error file";
                                        application.log.severe(returnStr);
                                        return returnStr + logFileReturnValue;

				}
				numFilesError ++;
				break;
			case 3:
				// critical error , log and stop run
				numFilesError ++;
				returnStr = "CRITICAL ERROR datafile " + fileToProcess + " failed, renamed as error file";
                                application.log.severe(returnStr);
                                return returnStr + logFileReturnValue;

			default:
				returnStr = "ERROR invalid return from processFile :" + fileToProcess +	intReturn;
                                application.log.severe(returnStr);
                                return returnStr + logFileReturnValue;

			} // end case


			if (!singleFileName.equals("none")) {break;
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
            application.log.warning("WARNING!!! No files were found to process");
        }

        application.log.info("");
        application.log.info("Files Read:" + numFilesRead);
        if (!singleFileName.equals("none")) {
            application.log.info("Was in SingleFileMode:" + singleFileName);
			}
        application.log.info("Files in success:" + numFilesSuccess);
	application.log.info("Files in recycle:" + numFilesRecycle);
	application.log.info("Files in error:" + numFilesError);
        application.log.info("Num tkts inserted:" + numRowsInserted );



        application.log.info("COMPLETE " + java.util.Calendar.getInstance().getTime());
        application.log.close();


        // end of class - report to console as well as log

        System.out.println("Files Read:" + numFilesRead);
        System.out.println("Files in success:" + numFilesSuccess);
        System.out.println("Files in error:" + numFilesError);
        System.out.println("Files in recycle:" + numFilesRecycle);
        System.out.println("Num tkts inserted:" + numRowsInserted );

        System.out.println(programName + " ended");

        return "OK"  + logFileReturnValue;


    }






    /** read and process each individual file
     * @param fileToProcess filename of an individual file to be processed
     * @param showContents true if debug on, false if not on
     * @param ddlFields vector of position of field definition to be used to map data from input files
     * to output fields
     * @param headerRecPrefix what string the header record should begin with
     * @param headerLength length in bytes of the header record in the file
     * @param conn the connection object
     * @throws SQLException sql error has occurred
     * @return 0 - sucsess, 1 - data related error e.g foreign key problem, 2 - error in file layout, 3 - critical error
     */

    public static int processFile(File fileToProcess, boolean showContents, DdlField[] ddlFields, String headerRecPrefix, int headerLength, Connection conn, int tktPermittedSectors) throws SQLException
    {
		/** read and process each individual file */


       String stage = "start of processFile";

       try {


        FileReader theFile, theFile2;
        BufferedReader fileIn = null, fileIn2 = null;
        String oneLine;
        Vector fileRecs = new Vector();
        MirRecord fileRec = new MirRecord();
        MirRecord fileRec2 = new MirRecord();
        int paxNo = 0, fareBasisNo = 0;
        int numPaxRecs =0, numA04Sectors =0;
        int recCounter = 0;

        // fields for output to database records
        String recPassengerName = "";
        String recETicketInd = "";
        String recBranchCode = "DUMM"; // initialised to dummy value
        String recSeason = "";
        String recBookingRef = "0"; // 0 for a dummy ref where ref is not available, but we still want to load the record to database
        String recSourceInd = "M"; // Mir sourced records
        String recPassengerType = "";
        String recTicketNo = "";
        String recCcyCode  = "";
        String recPublishedFare = "";
        String recSellingFare = "";
        String recCommAmt = "";
        String recCommPct = "";
        String recGBTax = "";
        String recRemainTax = "";
        String recUBTax = "";
        String recAirline = "";
        String recDeptDate= "";
        String recTicketDate= "";
        Date   finalDeptDate;
        String recTicketAgent = "";
        String recIATANum = "";
        String recPNR = "";
        String recTourCode = "";
        String recTourCodeType = "";
        String recInsertUpdateFlag = "I"; // initially an insert
        String recExchangeTicketNo = "";
        String recPseudoCityCode = "";
        String recFareBasisCode = "";
        String recPNRCreationDate = "";


        MirRecord fileUBAmtRec = new MirRecord();
        boolean recNetRemit = false;
        double workingGBTaxAmt = 0;
        double workingUBTaxAmt = 0;
        double workingRemainTaxAmt = 0;
        double workingXTTaxAmt = 0;
        String taxAmt = "";
        boolean foundUB = false;
        double commAmt = 0;
        double commPct = 0;
        double publishedFareAmt = 0;
        String bookRef = "";
        double conjTktsRequired = 0;
        double highestA04Sector =0;
        int sectorNo = 0;
        application.log.finest( "F: " + fileToProcess.getName() );

	// now actually process the file

        if (showContents) {
			try
                        // display entire file contents
			{
                            stage = "reading file for display";
				theFile2 = new FileReader( fileToProcess );
				fileIn2  = new BufferedReader( theFile2 );
				while( ( oneLine = fileIn2.readLine( ) ) != null )
					{
					// output file contents to console
					application.log.finest( oneLine );
				}
			}
			catch( IOException e )
			  {  System.out.println( e );
                             application.log.severe("ERROR i/o error datafile " + fileToProcess );
                             application.log.severe(String.valueOf(e));
                             return 2;
                           }
                             finally
			{
				// Close the stream
				try
				{
					if(fileIn2 != null )
						fileIn2.close( );
				}
				catch( IOException e )
				  {System.out.println( e );
                                   application.log.severe("ERROR i/o error closing datafile " + fileToProcess );
                                   return 3; }
			}
	} // end if show contents




        try
        {
                // the header record is x bytes long, of fixed length and contains some fields we need
                // it has a number of line feeds in it, but we ignore these and just treat as extra characters
                // format of the header record (fields we need from it) is defined in ddl file in case it changes
                stage = "setting up file to process";
                theFile = new FileReader( fileToProcess );
                fileIn  = new BufferedReader( theFile );

                char charArray[] = new char[20000];
                String prevRecType = "";


                int charsRead = fileIn.read(charArray,0, headerLength);

                if (showContents && charArray != null) {
                    application.log.finest("File contents:" + String.copyValueOf(charArray));
                }
                if (headerLength != charsRead  || charsRead == -1) {
                    application.log.severe("f: " + fileToProcess.getName() + ", insufficient hdr chars (" + charsRead + "), error");
                    return 2;
                }

                // first line is header record
                // check the header identifier to see if it is a valid file
                int colIndex = startElement("recheader", ddlFields);
                //System.out.println("hdr colindex:" + colIndex);
                // get corresponding text from file record
                // ddl file starts record at position 0 in same way as string is held in buffer

                // am using this ddl technique to allow easy changes if record format changes in future
                String headerText = String.copyValueOf(charArray);
                String colText = headerText.substring(ddlFields[colIndex].start  ,ddlFields[colIndex].start  + ddlFields[colIndex].length );
                if (showContents) {application.log.finest("hdr:" + colText);}
                if (!colText.equals(headerRecPrefix)) {
                    application.log.severe("F:" + fileToProcess.getName() + ", header wrong format, error");
                    return 2;
                }


               // now go through each element of ddl and get corresponding data
               // then use to form insert statement
                stage = "forming ddlfields";
               for (int i = 0; i < ddlFields.length ; i++) {
                   // store actual value from file alongside each element in ddl
                   ddlFields[i].value = headerText.substring(ddlFields[i].start  ,ddlFields[i].start  + ddlFields[i].length ).trim();
                   if (showContents) {application.log.finest(ddlFields[i].name + " length:" + ddlFields[i].length + " Start:" + ddlFields[i].start + " Value:" + ddlFields[i].value);}
               }
               // now get non-header records of different record types which can repeat a number of times
               // read one line at a time and interpret each record type according to it's contents
               String recID = "";
               String recType = "";
               int vectorCount = 0;

               recPNR = ddlFields[6].value;

               recTourCode = ddlFields[11].value; // FROM 10 TO 11

               if (recTourCode.length() > 1 ) {
                recTourCodeType = recTourCode.substring(0,1);
               }
               else {
                recTourCodeType = "";
               }

               // read all of remaining file into an array so can be processed later
               // this is done so that is easier to deal with multiple records e.g. multiple
               // passengers
               stage = "about to read file";
               while( ( oneLine = fileIn.readLine( ) ) != null ) {
                   // all record headers begin with A
                   // skip any blanklines that don't begin with A
                   // BUT ALSO grab TI: line which is part of A10 record
                   // and also grab the ssecond line from the A07 so can be used in certain tax circumstances

                   if (prevRecType.equals("A07")) {
                       // capture this second a07 amount type line in it's own record for use later
                       fileUBAmtRec.recordID="UB";
                       fileUBAmtRec.recordText=oneLine;
                   }

                   if (oneLine.length() > 0){
                       //if (showContents) {System.out.println( oneLine );}
                       if ((oneLine.length() > 3 && oneLine.substring(0,1).equals("A"))
                       || (oneLine.length() > 2 && oneLine.substring(0,3).equals("TI:"))
                       || (oneLine.length() > 10 && prevRecType.equals("A07")) ){
                            stage = "build vector";
                            MirRecord fileRec3 = new MirRecord();
                            recID = oneLine.substring(0,3);
                            fileRec3.recordID = recID;
                            fileRec3.recordText = oneLine;

                            fileRecs.addElement(fileRec3);

                            //System.out.println("recid:" + recID);
                            //while reading through sequentially, grab the fields
                            // that occur only once in file that we need to populate the
                            // pnr database record
                            if (recID.equals("A02")) {
                                numPaxRecs ++; // used as a check later
                            }
                            if (recID.equals("A04")) {
                                numA04Sectors ++; // used as a check later
                                  if (oneLine.length() > 4){
                                      sectorNo = Integer.parseInt(oneLine.substring(3,5));
                                      if (sectorNo > highestA04Sector) {
                                        highestA04Sector = sectorNo;
                                      }
                                  }

                            }

                            if (recID.equals("A14")) {
                                if (oneLine.length() > 8) {
                                    recType = oneLine.substring(3,9);
                                    // sometimes you get a FT-CD that contains a number (booking ref), exclude these
                                    if (recType.equals("FT-CD/") ) {
                                        // there should only be one branch code per file, but sometimes two or more
                                        // so take the last branch code encountered
                                        if (oneLine.substring(9).trim().length() > 3 ) {
                                            if (!NumberProcessor.validateStringAsNumber(oneLine.substring(9,13).trim())) {
                                                recBranchCode = oneLine.substring(9,13).trim();
                                            }

                                        } // end if on length
                                    } // if ft-cd
                                    else if (recType.equals("FT-TA/") ) {
                                        if (oneLine.length() > 10 ) {
                                            // get the ticketing agent initials
                                            recTicketAgent = oneLine.substring(9,11).trim();
                                        }
                                    } // ft-ta


                                    // sometimes the branch code is from FT-xxxx
                                    else if (!recType.equals("FT-INV") && oneLine.substring(3,6).equals("FT-") ) {
                                        if (oneLine.length() > 9 ) {
                                            if (oneLine.length() == 10) {
                                                if (!NumberProcessor.validateStringAsNumber(oneLine.substring(6,10).trim())
                                                && oneLine.substring(6,10).trim().indexOf("/") == -1 ) {
                                                    recBranchCode = oneLine.substring(6,10).trim();
                                                }
                                            }
                                        }
                                    }


                                    else if (recType.equals("FT-INV")) {
                                        // get booking ref info, but sometimes get 2 or more FT-INV records
                                        // if two or more, take the last one which is numeric
                                        if (oneLine.length() > 9 ) {
                                            bookRef = oneLine.substring(10).trim();
                                            if (NumberProcessor.validateStringAsNumber(bookRef)) {
                                                recBookingRef = bookRef;
                                            }
                                            else {
                                                if (bookRef.toUpperCase().equals("FLEX")) {
                                                    // use a dummy booking ref to indicate it is a flex schools bookingg
                                                    recBookingRef = "9999999999";
                                                }

                                            }
                                        }


                                    } // end if record type ft-inv
                                } // end if length whole record
                            } // if rec type a14

                            prevRecType = recID;

                        } // if A
                   } // if length > 0
		} // end while readfile

        } // end try


        catch( IOException e )
          {  System.out.println( e );
             application.log.severe("ERROR i/o error datafile " + fileToProcess );
             application.log.severe(String.valueOf(e));
             return 3;
           }
        finally
        {
                // Close the stream
                try
                {
                        if(fileIn != null )
                                fileIn.close( );
                }
                catch( IOException e )
                  { System.out.println( e );
                    application.log.severe("ERROR i/o error closing datafile (2) :" + fileToProcess );
                    return 3;
                }
        } // end finally


        // now process that vector of records we have built up
        // this is done so we can cope with repeating record types e.g passengers A02 records


        // loop through each A02 record , and for each one, get ticket level info
        //Iterator recData = fileRecs.iterator();
        stage = "processing vector";
        recCounter = 0;
        paxNo = 0;
        recInsertUpdateFlag = "I"; // insert of new pnr,not an update yet
        boolean fareBasisMatch = false;
        int countA07, countA08 =0;
        int insertedTickets  =0;
        boolean exchangeFound = false;
        String conjTktInd = "N";
        String insTktNum = "";

        //while (recData.hasNext()) {
        for (Enumeration recData = fileRecs.elements(); recData.hasMoreElements();) {
            fileRec = (MirRecord)recData.nextElement();
            recCounter ++; // store position in iterator
            //if (showContents) {System.out.println(fileRec.recordID + "," + fileRec.recordText);}
            // get first passenger
            if (fileRec.recordID.equals( "A02") ){
                // for each a02, there is one passenger and therefore one ticket

                paxNo ++;
                //passenger record
                recPassengerName = fileRec.recordText.substring(3,36);
                recTicketNo      = fileRec.recordText.substring(48,58);

                // paxNo is used to index other passenger level info later in vector
                // this number may be one if only one fare basis exists for all
                // passengers
                fareBasisNo      = Integer.parseInt(fileRec.recordText.substring(75,77));
                recPassengerType = fileRec.recordText.substring(69,71);

                if (showContents) {application.log.finest("PNR:"+ recPNR + " Pax:" + paxNo + " " + recPassengerName + " t:" + recTicketNo +
                                   " Fare:" + fareBasisNo + " Type:" + recPassengerType);}
                // get other ticket-level values from other record types by readng through rest of vector

                // no point in reading from beginning, so read from last loop position (recCounter+1)

                countA07 = 0;
                fareBasisMatch = false;
                for (int i = recCounter + 1; i < fileRecs.size(); i++) {
                    fileRec2 = (MirRecord)fileRecs.elementAt(i);

                    // take airline from headr, not from A04 itinary record
                    //if (fileRec2.recordID.equals("A04")) {
                    //    // a04 is airline info, but this really should come from header, so comment this code out
                    //    countA04++;
                    //    if (paxNo == Integer.parseInt(fileRec2.recordText.substring(3,5))) {

                    //        recAirline = fileRec2.recordText.substring(7,10);
                    //    }
                    //}


                    if (fileRec2.recordID.equals("A07")) {
                        // fare value info
                        countA07++;
                        stage = "processing A07";
                        if (fareBasisNo == Integer.parseInt(fileRec2.recordText.substring(3,5))) {
                            // this is the correct A07 record for this passenger iteration
                            fareBasisMatch = true;
                            recCcyCode = fileRec2.recordText.substring(5,8);
                            if (!recCcyCode.equals("GBP") && fileRec2.recordText.substring(35,38).equals("GBP") ) {
                                // non-gbp currency, but amt is available later in record converted to GBP
                                recSellingFare = fileRec2.recordText.substring(38,50).trim();
                            }
                            else {
                                recSellingFare = fileRec2.recordText.substring(8,20).trim(); // but can be overriden if A21 record later in code

                            }

                            // fare can sometimes be all blanks - this means 0
                            if (recSellingFare.equals("")) {
                                recSellingFare = "0";
                            }
                            recPublishedFare = recSellingFare; // start with the two equal

                            if (recTourCodeType.equals("I") || recTourCodeType.equals("B") ) {
                               // inclusive tour or bulk tour
                               // overwrite the published fare with 0
                               recPublishedFare = "0";
                            }



                            // now get tax
                            // sometimes the file does not contain any tax info, so record is short
                            // file does not always have a gb or ub tax amount
                            // remaining tax is built up by adding together all taxes
                            stage = "processing tax";
                            workingGBTaxAmt = 0;
                            workingUBTaxAmt = 0;
                            workingRemainTaxAmt = 0;
                            workingXTTaxAmt = 0;
                            foundUB = false;
                            //if (fileRec2.recordText.length() > 52 ) {

                                //if (!recCcyCode.equals(fileRec2.recordText.substring(20,23)) |
                                //    !recCcyCode.equals(fileRec2.recordText.substring(50,53))   ) {
                                //    // mixture of currencies, system does not allow this, so fail
                                //    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + ", (a07) mix of currencies in fare info, error");
                                //    return 2;
                                //}
                            //}

                            // tax 1 fields:
                            if (fileRec2.recordText.length() >= 64 ) {
                                taxAmt = fileRec2.recordText.substring(56,64).trim();
                                if (fileRec2.recordText.substring(53,56).equals("T1:")) {
                                        if (!NumberProcessor.validateStringAsNumber(taxAmt)) {
                                            // invalid number
                                            application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", tax1:" + taxAmt + " is not numeric error");
                                            return 2;
                                        }

                                    if (fileRec2.recordText.substring(64,66).equals("GB")) {
                                        workingGBTaxAmt = workingGBTaxAmt +  Double.parseDouble(taxAmt);
                                    }
                                    else {
                                        if (fileRec2.recordText.substring(64,66).equals("UB")) {
                                            foundUB = true;
                                            workingUBTaxAmt = workingUBTaxAmt + Double.parseDouble(taxAmt);
                                        }
                                        else { // some other tax code, so just add it to the remaining tax amt field
                                            workingRemainTaxAmt = workingRemainTaxAmt + Double.parseDouble(taxAmt);
                                        }
                                    }

                                    //else if (fileRec2.recordText.substring(64,66).equals("XT")) {
                                    //    foundXT = true;
                                    //    workingXTTaxAmt = workingXTTaxAmt + Double.parseDouble(taxAmt);
                                    //}

                                }
                           } // end t1

                            // tax 2 fields:
                            if (fileRec2.recordText.length() >= 79 ) {
                                taxAmt = fileRec2.recordText.substring(69,77).trim();
                                if (fileRec2.recordText.substring(66,69).equals("T2:")) {
                                        if (!NumberProcessor.validateStringAsNumber(taxAmt)) {
                                            // invalid number
                                            application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", tax2:" + taxAmt + " is not numeric error");
                                            return 2;
                                        }

                                    if (fileRec2.recordText.substring(77,79).equals("GB")) {
                                        workingGBTaxAmt = workingGBTaxAmt +  Double.parseDouble(taxAmt);
                                    }
                                    else {
                                        if (fileRec2.recordText.substring(77,79).equals("UB")) {
                                            foundUB = true;
                                            workingUBTaxAmt = workingUBTaxAmt + Double.parseDouble(taxAmt);
                                        }
                                        else {
                                            workingRemainTaxAmt = workingRemainTaxAmt + Double.parseDouble(taxAmt);
                                        }
                                    }
                                    // anything else goes in remaining tax column
                                    //else if (fileRec2.recordText.substring(77,79).equals("XT")) {
                                    //    foundXT = true;
                                    //    workingXTTaxAmt = workingXTTaxAmt + Double.parseDouble(taxAmt);
                                    //}

                                }
                            } // end t2

                            // tax 3 fields:
                            if (fileRec2.recordText.length() >= 90 ) {
                                taxAmt = fileRec2.recordText.substring(82,90).trim();
                                if (fileRec2.recordText.substring(79,82).equals("T3:")) {
                                        if (!NumberProcessor.validateStringAsNumber(taxAmt)) {
                                            // invalid number
                                            application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo+ ", tax3:" + taxAmt + " is not numeric error");
                                            return 2;
                                        }

                                    if (fileRec2.recordText.substring(90,92).equals("GB")) {
                                        workingGBTaxAmt = workingGBTaxAmt +  Double.parseDouble(taxAmt);
                                    }
                                    else {
                                        if (fileRec2.recordText.substring(90,92).equals("UB")) {
                                            foundUB = true;
                                            workingUBTaxAmt = workingUBTaxAmt + Double.parseDouble(taxAmt);
                                        }
                                        else {
                                            workingRemainTaxAmt = workingRemainTaxAmt + Double.parseDouble(taxAmt);
                                        }
                                    // anything else goes in remaining tax column
                                    //else if (fileRec2.recordText.substring(90,92).equals("XT")) {
                                    //    foundXT = true;
                                    //    workingXTTaxAmt = workingXTTaxAmt + Double.parseDouble(taxAmt);
                                    }

                                }
                            } // end t3

                            // end of tax processing

                            if (!foundUB) {
                                // now have to look in the second line of the A07 record
                                // this has previously been stored just for this purpose
                                // when we first read in the input file
                                if (fileUBAmtRec.recordID.equals("UB")
                                && fileUBAmtRec.recordText.length() > 10) {
                                    // now look through this second amount line and
                                    // see if we can see any ub tax amounts

                                    application.log.finest("extra ub:" + fileUBAmtRec.recordText );

                                    int UBPos = fileUBAmtRec.recordText.indexOf("UB");
                                    if (UBPos > -1) {
                                      taxAmt = fileUBAmtRec.recordText.substring(UBPos - 8,UBPos).trim();
                                      application.log.finest("extra ub, ubpos:" + UBPos + "," + taxAmt );
                                      //validate that amt
                                      if (!NumberProcessor.validateStringAsNumber(taxAmt)) {
                                            application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo+ ", 2nd row ub tax:" + taxAmt + " is not numeric error");
                                            return 2;
                                      }
                                      // add it to the ub tax amt so far, BUT reduce the remaining tax accordingly
                                      workingUBTaxAmt     = workingUBTaxAmt + Double.parseDouble(taxAmt);
                                      workingRemainTaxAmt = workingRemainTaxAmt - Double.parseDouble(taxAmt);
                                    } // end if ubpos > 1


                                 }
                                else {
                                    application.log.finest("extra ub NOT FOUND:" + fileUBAmtRec.recordText );
                                }

                            } // end if not found a ub amt

                            recGBTax = String.valueOf(workingGBTaxAmt);
                            recUBTax = String.valueOf(workingUBTaxAmt);

                            recRemainTax = String.valueOf(workingRemainTaxAmt);

                            if (showContents) {application.log.finest("PNR:"+ recPNR + "GBTax:" + recGBTax + " UBTax:" + recUBTax + " rem:" + recRemainTax);}
                        }


                    } // end if a07



                    if (fileRec2.recordID.equals("A08")) {
                        // fare value info
                        countA08++;
                        stage = "processing A08";
                        if (fileRec2.recordText.length() > 15) {
                            if (fareBasisNo == Integer.parseInt(fileRec2.recordText.substring(3,5))) {
                                // this is the correct A08 record for this passenger iteration

                                recFareBasisCode = fileRec2.recordText.substring(7,15);
                            }
                        }
                    }


                    if (fileRec2.recordID.equals("A10")) {
                         // this is an exchange ticket
                        stage = "processing A10";

                        if (paxNo == Integer.parseInt(fileRec2.recordText.substring(3,5))) {
                            // this is the correct A10 record for this passenger iteration
                            //System.out.println("pax:"  + paxNo  + "true");
                            exchangeFound = true;
                        }
                        else {
                            exchangeFound = false;
                            recExchangeTicketNo = "";
                        }
                    } // end a10

                    if (exchangeFound && fileRec2.recordID.equals("TI:")) {
                        // exchnage ticket details
                        recExchangeTicketNo = fileRec2.recordText.substring(6,16);
                        application.log.finest("exch:" + recExchangeTicketNo);

                    }


                    if (fileRec2.recordID.equals("A21")) {
                        // net remit ticket type
                        recNetRemit = true;
                        recSellingFare = fileRec2.recordText.substring(6,18);
                        if (recSellingFare.equals("") ) {
                            recSellingFare = "0";
                        }
                        if (recTourCodeType.equals("I") || recTourCodeType.equals("B") ) {
                                // Inclusive tour or bulk tour
                                // should not be an a21 record for these tour type tickets
                               application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", inclusive/bulk tour but has A21 record error");
                               return 2;
                        }
                    }

                } // for next through rest of vector



                // check we have sufficient values
                stage = "validating data";
                if (countA07 < 1) {
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", no A07 record for fare info error");
                    return 2;}
                if (!fareBasisMatch) {
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", no matching A07 record for fare info error");
                    return 2;}

                recDeptDate = ddlFields[3].value;
                recTicketDate = ddlFields[1].value;
    System.out.println(" recDeptDate" + recDeptDate);
                if (!DateProcessor.checkDate(recDeptDate, "ddMMMyyyy" )
                    | !DateProcessor.checkDate(recTicketDate.substring(0,7),"ddMMMyyyy" )) {
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " tkt:" + recTicketNo + ", " + recDeptDate + " or " + recTicketDate + " is invalid date format error");
                    return 2;}
                finalDeptDate = DateProcessor.parseDate(recDeptDate + "0000", "ddMMMyyhhmm");
                if (finalDeptDate == null
                   | DateProcessor.parseDate(recTicketDate, "ddMMMyyhhmm") == null){
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + ", " + recDeptDate + " or " + recTicketDate + " is invalid date, error");
                        return 2;}

                application.log.finest("tkt dt:" + recTicketDate + ","+ DateProcessor.parseDate(recTicketDate,"ddMMMyyhhmm") + "high sector:" + highestA04Sector);
                // determine season of departure e.g S03
                // if between 1st May and 31-Oct then Summer
                // else Winter
                recSeason = DateProcessor.calcSeason(new java.sql.Date(finalDeptDate.getTime()), 3);


                // Added by JR as part of Air Load changes , new field pnr_creation_date
				recPNRCreationDate = ddlFields[8].value;  // Rest of the fields after this position (124 are re numbered in array
System.out.println("recPNRCreationDate"+ recPNRCreationDate);
				if (!DateProcessor.checkDate(recPNRCreationDate  , "ddMMMyyyy" )) {
				  application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + " PNR Creation Date:"  + recPNRCreationDate + " is invalid date format error");
				  return 2;}
				if (DateProcessor.parseDate(recPNRCreationDate + "0000", "ddMMMyyhhmm") == null){
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + ", " + " PNR Creation Date: " + recPNRCreationDate + " is invalid date, error");
                    return 2;}


               // End of addition by Jyoti

                if (ddlFields[14].value.equals("E") //eticketind  // FROM 13 TO 14
                || ddlFields[14].value.equals("L")   // FROM 13 TO 14
                || ddlFields[12].value.equals("5"))  //atb ind  // FROM 11 TO 12
                {
                    recETicketInd = "Y";
                } else { recETicketInd = "N";}


                if (recTicketAgent.equals("")) {
                    recTicketAgent = "??";  // insert a dummy value
                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", no ticketing agent. Used ?? ");
                }

                // recTicketAgent = ddlFields[7].value;  --removed LA , no longer gotten from header but from A14 instead
                recAirline = ddlFields[2].value;
                recPseudoCityCode = ddlFields[4].value.trim();
                recIATANum = ddlFields[5].value;
                recCommAmt = ddlFields[9].value;  // FROM 8 TO 9
                recCommPct = ddlFields[10].value;  // FROM 9 TO 10
                // comm pct is in file as 4 digits, but we have to assume it is to 2 decimal places
                // so need to "divide" by 100:
                if (recCommPct.indexOf(".") == -1  ) {
                    recCommPct = recCommPct.substring(0,2) + "." + recCommPct.substring(2,4);
                }
                if (recCommAmt.indexOf(".") == -1  ) {
                    recCommAmt = recCommAmt.substring(0,6) + "." + recCommAmt.substring(6,8);
                }

                conjTktInd = ddlFields[13].value; // conjunction ticket ind // FROM 12 TO 13


                // now validate all numbers
                if (!NumberProcessor.validateStringAsNumber(recTicketNo)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode +  " tkt:" + recTicketNo + " is not numeric (or missing) error");
                        return 2;}

                if (exchangeFound && !NumberProcessor.validateStringAsNumber(recExchangeTicketNo)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", exchange tkt:" + recTicketNo + " is not numeric (or missing) error");
                        return 2;}

                if (!NumberProcessor.validateStringAsNumber(recAirline)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", airline:" + recAirline + " is not numeric error");
                        return 2;}
                if (recBookingRef.equals("") || recBookingRef.equals("0")) {
                        // use a dummy booking ref to indicate it is missing
                        recBookingRef = "0";
                        // give warning message about dummy values which have been used
                        // raise an error message, but allow processing to continue
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", no booking ref. Used 0 ");
                }

                if (!NumberProcessor.validateStringAsNumber(recBookingRef)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", bkg:" + recBookingRef + " is not numeric error");
                        return 2;}

                if (!NumberProcessor.validateStringAsNumber(recIATANum)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", iata:" + recIATANum + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recGBTax)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", gbtax:" + recGBTax + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recPublishedFare)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", pub fare:" + recPublishedFare + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recSellingFare)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", selling fare:" + recSellingFare + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recUBTax)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo + ", ubtax:" + recUBTax + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recRemainTax)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo+ ", remtax:" + recRemainTax + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recCommAmt)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo+ ", commission amt:" + recCommAmt + " is not numeric error");
                        return 2;}
                if (!NumberProcessor.validateStringAsNumber(recCommPct)) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo+ ", commission pct:" + recCommPct + " is not numeric error");
                        return 2;}


                if (recBranchCode.equals("DUMM")) {
                        application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo+ ", no branch code found. Loaded as DUMM");
                        // this error allow processing to continue so do not return
                        //return 2;
                }


                if (!exchangeFound) {
                    // now derive commission amt or commission pct in case both are not supplied
                    // convert to numbers so can test contents and work out pct if pct is not supplied,
                    // and work out amt if amt is not supplied
                    commAmt = Double.parseDouble(recCommAmt);
                    commPct = Double.parseDouble(recCommPct);
                    publishedFareAmt = Double.parseDouble(recPublishedFare);
                     if (publishedFareAmt == 0 ) {
                            // prevent divide by zero error
                            // comm amt and pct must be 0
                            recCommPct = String.valueOf(0);
                            recCommAmt = String.valueOf(0);
                     }
                     else {
                        if (commAmt == 0 && commPct != 0) {
                            // calc amt from pct * published fare
                            commAmt = publishedFareAmt * commPct / 100;
                            recCommAmt = String.valueOf(commAmt);
                        }
                        else if (commAmt != 0 && commPct == 0) {
                            // calc pct from amt / published fare
                            commPct = commAmt / publishedFareAmt * 100;
                            recCommPct = String.valueOf(commPct);

                        }
                    }
                }
                else {
                    // exchange ticket resets all amounts to 0
                    recCommPct = "0";
                    recCommAmt = "0";
                    recPublishedFare = "0";
                    recSellingFare = "0";
                    recGBTax = "0";
                    recUBTax = "0";
                    recRemainTax = "0";

                }


                // if this is first ticket in firle then pass I for insert
                // else pass U for update
                if (paxNo == 1) {
                    recInsertUpdateFlag = "I";
                }
                else {
                    recInsertUpdateFlag = "U";
                }

                // usually only have to insert one ticket, BUT sometimes, it may be a conjunction MIR
                // this means you have to insert the main ticket with all values gotten so far
                // but then also insert some dummy tickets with zero amounts but on same pnr
                // This is done because sometimes there are too many sectors to fit onto one ticket stub,
                // so the list of sectors is carried onto the next ticket stub
                // and we have to record both in stella
                // there is a spec document for this conjunction ticket stuff in the business spec area
                // called "Processing a Conjunction Ticket MIR"

                if (conjTktInd.equals("Y")) {

                    double y = highestA04Sector / tktPermittedSectors;
                    application.log.finest("conj tkt" + highestA04Sector + "," + tktPermittedSectors + "," + y);
                    conjTktsRequired = Math.ceil(highestA04Sector / tktPermittedSectors);
                }
                else {
                    conjTktsRequired = 1; // don't do any conjunction "dummies", just do the main ticket
                }// end of if conj ind Y
                application.log.finest("num of tkts plus conj to insert:" + conjTktsRequired);

                for (int i = 0; i < conjTktsRequired; i++)  {
                    // loop through each conjunction ticket required and insert a row for each one
                    // ensure there is always at one least one ticket inserted (the main one)


                    long x = Long.parseLong(recTicketNo);
                    insTktNum = String.valueOf( x + i);

                    if (i > 0) {
                        // we are now processing the conjunction "dummy" tickets
                        // so reset amounts to 0
                            application.log.finest("conjloop:" + i);
                            recPublishedFare = "0";
                            recSellingFare = "0";
                            recCommAmt = "0";
                            recCommPct = "0";
                            recGBTax = "0";
                            recRemainTax = "0";
                            recUBTax = "0";
                    }



                    if (showContents) {
                        application.log.finest("ins rec, pnr:" + recPNR +
                            " dep:" + finalDeptDate +
                            "origtkt:" + recTicketNo + // the original/main ticket number
                            " tkt:" + insTktNum +  // the actual ticket number (may be different in case of conjunction
                            " air:" + recAirline +
                            " br:" + recBranchCode +
                            " bkg:" + recBookingRef +
                            " seas:" + recSeason +
                            " etkt:" + recETicketInd +
                            " ag:" + recTicketAgent +
                            " tour:" + recTourCode +
                            " netrmt:" + recNetRemit +
                            " pubamt:" + recPublishedFare +
                            " selamt:" + recSellingFare +
                            " cmamt:" + recCommAmt +
                            " cmpct:" + recCommPct +
                            " IATA:" + recIATANum +
                            " tktdt:" + DateProcessor.parseDate(recTicketDate,"ddMMMyyhhmm") +
                            //" numpax:" + String.valueOf(numPaxRecs) +
                            " numpax:" + "1" +
                            " gbtax:" + recGBTax+
                            " Remtax:" + recRemainTax +
                            " ubtax:" + recUBTax+
                            " ccy:" +recCcyCode + 		//ccy code
                            " city:" + recPseudoCityCode +
                            " paxtype:" + recPassengerType +
                            " pax:" + recPassengerName.trim() +
                            " i/u:" + recInsertUpdateFlag +
                            " exc:" + recExchangeTicketNo +
                            " fb:" + recFareBasisCode +
                            " cj:" + conjTktInd +   // should be Y if not the main ticket, but a "child" of the main ticket i.e. created because it is a conjunction tkt
                            "pnr Creationdt:" + DateProcessor.parseDate(recPNRCreationDate ,"ddMMMyyhhmm") +
                            //" numpax:" + String.valueOf(numPaxRecs) +
                            "" );}


                    // finally call the stored procedure to do the insert into database
                    stage = "calling stored proc";
                    CallableStatement cstmt = conn.prepareCall(
                            "{?=call p_stella_get_data.insert_ticket(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
                            cstmt.registerOutParameter(1, Types.CHAR);
                            cstmt.setString(2, "ML");
                            cstmt.setString(3, recPNR.trim()); //pnr
                            cstmt.setDate(4, new java.sql.Date( finalDeptDate.getTime())); //dep date
                            cstmt.setString(5, insTktNum.trim()); //tkt num
                            cstmt.setString(6, recAirline.trim()); // airline
                            cstmt.setString(7, recBranchCode.trim()); //branch
                            cstmt.setString(8, recBookingRef.trim());	//booking ref
                            cstmt.setString(9, recSeason.trim()); //season
                            cstmt.setString(10, recETicketInd.trim()); //eticketind
                            cstmt.setString(11, recTicketAgent.trim()); //ticketagntinitials
                            cstmt.setString(12, recCommAmt.trim()); // commamt
                            cstmt.setString(13, recCommPct.trim()); //commpct
                            cstmt.setString(14, recSellingFare.trim()); // selling fare
                            cstmt.setString(15, recPublishedFare.trim());  // published fare
                            cstmt.setString(16, recIATANum.trim()); // iata num
                            cstmt.setString(17, "ML"); // entry user id
                            cstmt.setDate  (18, new java.sql.Date( DateProcessor.parseDate(recTicketDate,"ddMMMyyhhmm").getTime()));
                            cstmt.setString(19, "1" ); // hard code in 1 - always one for MIR files
                            cstmt.setString(20, recPassengerName.trim());
                            cstmt.setString(21, recGBTax.trim());
                            cstmt.setString(22, recRemainTax.trim());
                            cstmt.setString(23, recUBTax.trim());
                            if (recTicketNo.equals(insTktNum) ) {
                                // if it's the main ticket as opposed to a conjunction one, insert null
                                cstmt.setNull(24, java.sql.Types.VARCHAR); // linked conjunction ticket
                            }
                            else {
                                cstmt.setString(24, recTicketNo); // linked conjunction ticket
                            }

                            cstmt.setString(25, recCcyCode.trim());		//ccy code
                            cstmt.setString(26, recPseudoCityCode);		//city code
                            cstmt.setString(27, recPassengerType.trim());
                            cstmt.setString(28, "GAL"); // galileo ticket doc type
                            cstmt.setString(29, recInsertUpdateFlag); // I = insert of pnr (first ticket in file) or U = an update (second ticket in file)
                            cstmt.setString(30, recExchangeTicketNo);
                            cstmt.setNull(31, java.sql.Types.INTEGER);
                            cstmt.setString(32, recTourCode);
                            cstmt.setString(33, recFareBasisCode);

                            cstmt.setString(34, i==0?"N":conjTktInd);        // conj tkt ind

							cstmt.setDate  (35,  new java.sql.Date(DateProcessor.parseDate(recPNRCreationDate+ "0000","ddMMMyyhhmm").getTime())); //Pnr Creation date added by Jyoti

                            cstmt.execute();
                            // there is no commit in the stored procedure
                            //commit is issued later in code

                            //if (showContents) {application.log.info("PNR:"+ recPNR + ". Insert Result: "+cstmt.getString(1));}
                            if (cstmt.getString(1) != null) {
                                if (cstmt.getString(1).startsWith("Error, (fk)")) {
                                    application.log.warning("F:" + fileToProcess.getName() + ",P:"+ recPNR + "/" + recPseudoCityCode + " T:" + recTicketNo + ", failed:" + cstmt.getString(1) + ", retry next run");
                                    // return 1 so moved to recycle and will try again until foriegn key satisfied
                                    return 1;
                                }
                                else if (cstmt.getString(1).startsWith("Error")) {
                                    // severe error in the stored procedure
                                    application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " tkt:" + recTicketNo+ ", failed:" + cstmt.getString(1) + ", moved to error area");
                                    // return 2 so moved to error and will not try again next run
                                    return 2;
                                }
                            }
                            else {
                               numRowsInserted ++;
                               if (i == 0) {
                                   // the original ticket insert, not any conjunction ones
                                   insertedTickets ++;
                               }
                            }
                            cstmt.close();

                } // end for loop through conj tickets

            }    // end while
        }

        stage = "finished with file";
        if (insertedTickets != numPaxRecs) {
             application.log.severe("F:" + fileToProcess.getName() + ",PNR:"+ recPNR + "/" + recPseudoCityCode + " num pax <> num inserts, error");
             return 2;
        }

        // no errors , processing OK, return OK return code
        conn.commit();
        return 0;


  } catch (Exception ex) {
       String returnStr = "SYSTEM FAILURE F:" + fileToProcess.getName() + " failed while " + stage + ". [Exception " +
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





  /** determine position in map of specified field
   * @param qName name of field to determine position of
   * @param ddlFields map name
   * @return integer position in map
   */
    public static int startElement(String qName, DdlField[] ddlFields) {

      int columnIndex = -1;

      for (int i = 0; i < ddlFields.length; i++) {
                if ( qName.equalsIgnoreCase(ddlFields[i].name))
                {
                        columnIndex = i;
                        break;

                }
        }
       if (columnIndex == -1 ) {
           // no match found in ddl - error
           System.out.println(qName + " " + columnIndex);
           application.log.severe("ERROR ddl file col missing: " + qName  + " " + columnIndex);
           System.exit(0);
       }
       return columnIndex;
  }



}




