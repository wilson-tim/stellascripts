package uk.co.firstchoice.util.ddlparser;

/** read copybook.java
 * class to read data definition file
 **/

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * ddl manipulator - allows you to map a flat file to fields defined in a data
 * definition file
 */
public class Ddl {
	private Hashtable ddlFields = new Hashtable(100);

	private String ddlFile = null;

	private int recLen = -1;

	private String rowsetTag = null;

	public Ddl(String ddlFile) {
		// pass the file location of the data definition file
		this.ddlFile = ddlFile;
		loadDdlFile();
	}

	private void loadDdlFile() {
		BufferedReader br = null;

		//
		//  Open input parm file and check success
		//
		try {
			br = new BufferedReader(new FileReader(ddlFile));
		} catch (FileNotFoundException enofile) {
			System.err.println("Unable to open Ddl file " + ddlFile);
			System.exit(1);
		}
		//
		//  Now read each parm line and store the parm key and parm values into
		// the
		//  list area supplied by the user
		//
		try {
			String line = null;
			String field = null;
			String type = null;
			String starts = null;
			String length = null;
			String defvalue = null;
			boolean redefine = false;

			while ((line = br.readLine()) != null) {
				SuperStringTokenizer str = new SuperStringTokenizer(line);

				String[] stringarea = new String[0];

				try {
					stringarea = str.getQuotedStringList(false);
				} catch (QuotedStringException qse) {
					System.err.println("Quouted String error: "
							+ qse.getMessage());
					System.exit(1);
				}

				if (stringarea.length > 0) {
					//
					// look for 'Dictionary' statement and convert RECORD, and
					// FIELD
					//
					if (stringarea[0].equalsIgnoreCase("RECORD")
							|| stringarea[0].equalsIgnoreCase("FIELD")
							|| stringarea[0].equalsIgnoreCase("//REDEFINE")
							|| stringarea[0].equalsIgnoreCase("//ANCHOR")) {
						if (field != null) {
							if (type == null || length == null) {
								if (stringarea[0]
										.equalsIgnoreCase("//REDEFINE")
										|| stringarea[0]
												.equalsIgnoreCase("//ANCHOR")) {
									redefine = true;
									continue;
								}
								System.err.println("Invalid data in Ddl "
										+ ddlFile + " - line is " + line);
								System.exit(1);
							}
							ddlFields.put(field.toUpperCase(), new DdlField(
									field, type, starts, length, defvalue,
									redefine));
							redefine = false;
						}
						if (stringarea[0].equalsIgnoreCase("//REDEFINE")
								|| stringarea[0].equalsIgnoreCase("//ANCHOR")) {
							redefine = true;
							field = null;
						} else if (stringarea[0].equalsIgnoreCase("RECORD")) {
							field = "Record Descriptor";
							// DSB : ADD to check the RecordTAG
							if (stringarea[1].equalsIgnoreCase("IS")) {
								rowsetTag = stringarea[2];
							}
						} else if (stringarea[1].equalsIgnoreCase("IS")) {
							field = stringarea[2];
						} else {
							field = stringarea[1];
						}
						type = null;
						starts = "0";
						length = null;
						defvalue = null;
					}
					//
					// Test for user comments - skip straight out
					//
					else if (stringarea[0].startsWith("//")) {
						continue;
					} else if (stringarea[0].equalsIgnoreCase("TYPE")) {
						int start = 1;
						type = " ";
						if (stringarea[1].equalsIgnoreCase("IS")) {
							start = 2;
						}
						for (int j = start; j < stringarea.length; j++) {
							type = type + stringarea[j] + "_";
						}
					} else if (stringarea[0].equalsIgnoreCase("STARTS")) {
						starts = stringarea[stringarea.length - 1];
					} else if (stringarea[0].equalsIgnoreCase("LENGTH")) {
						length = stringarea[stringarea.length - 1];
					} else if (stringarea[0].equalsIgnoreCase("DEFAULT")) {
						defvalue = stringarea[stringarea.length - 1];
					}
				}
			}
			br.close();
			if (field != null) {
				if (type == null || length == null) {
					System.err.println("Invalid data in Ddl " + ddlFile
							+ " - line is " + line);
					System.exit(1);
				}
				ddlFields.put(field.toUpperCase(), new DdlField(field, type,
						starts, length, defvalue, redefine));
			}
		} catch (IOException eio) {
			System.err.println("I/O exception found during read of Ddl file");
			System.exit(1);
		}
		DdlField rec = (DdlField) ddlFields.get("Record Descriptor"
				.toUpperCase());
		// DSB : Change to check if it is null
		if (rec != null) {
			recLen = rec.length;
			ddlFields.remove("Record Descriptor".toUpperCase());
		}
	}

	public int getReclen() {
		//System.err.println("Record length is "+recLen);
		return recLen;
	}

	public DdlField[] getLevelKeys(String[] keyList) {
		DdlField[] ddlList = new DdlField[keyList.length];
		for (int i = 0; i < keyList.length; i++) {
			if (keyList[i] == null) {
				System.err.println("The level" + (i + 1) + "Key supplied on "
						+ "open for Ddl file " + ddlFile
						+ " was null or invalid");
				Thread.dumpStack();
				System.exit(1);
			}
			ddlList[i] = (DdlField) ddlFields.get(keyList[i].toUpperCase());
			if (ddlList[i] == null) {
				System.err.println("The level" + (i + 1) + "Key '" + keyList[i]
						+ "' supplied on " + "open for Ddl file " + ddlFile
						+ " was not found");
				Thread.dumpStack();
				System.exit(1);
			}
		}
		return ddlList;
	}

	public String getKey(DdlField[] levelKeys, byte[] buffer) {
		String res = "";

		for (int i = 0; i < levelKeys.length; i++) {
			res += new String(buffer, levelKeys[i].start, levelKeys[i].length);
		}

		return res;
	}

	public byte[] createPadRecord() {
		String key;
		byte[] padRecord = new byte[recLen];
		byte[] sourceData = new byte[0];
		DdlField target;

		Enumeration enumber = ddlFields.keys();
		while (enumber.hasMoreElements()) {
			key = (String) enumber.nextElement();
			target = (DdlField) ddlFields.get(key);
			if (target.redefine == false) {
				DdlField tempDdl = new DdlField("dummy", "ASCII_CHARACTER_",
						"0", "0", null, false);
				tempDdl.dataCharSet = target.dataCharSet;
				tempDdl.dataClass = target.dataClass;
				DdlField.copyField(sourceData, tempDdl, padRecord, target);
			}
		}
		return padRecord;
	}

	public Enumeration getDdlEnumerator() {
		return ddlFields.keys();
	}

	public DdlField getDdlField(String fieldName) {
		return getDdlField(fieldName, true);
	}

	public String getRowSetTag() {
		return rowsetTag;
	}

	public DdlField getDdlField(String fieldName, boolean fail) {
		DdlField temp = (DdlField) ddlFields.get(fieldName.toUpperCase());
		if (temp == null && fail == true) {
			System.err.println("DDlField for field named '" + fieldName
					+ "' not found in ddl file " + ddlFile);
			System.exit(1);
		}
		//System.err.println("Temp is "+temp.toString());
		return temp;
	}

	public void dumpDdl() {
		System.err.println("Ddl dump of file " + ddlFile + " follows:\n");
		Enumeration enumber = ddlFields.keys();
		while (enumber.hasMoreElements()) {
			String key = (String) enumber.nextElement();
			DdlField target = (DdlField) ddlFields.get(key);
			System.err.println("DumpTarget=" + target.toString());
		}
	}

	public DdlField[] getDdlFields() {
		Vector v = new Vector(100, 10);
		Enumeration enumber = ddlFields.keys();
		while (enumber.hasMoreElements()) {
			String key = (String) enumber.nextElement();
			v.addElement(ddlFields.get(key));
		}

		DdlField[] ret = new DdlField[v.size()];
		v.copyInto(ret);

		boolean swap = true;

		do {
			swap = false;

			for (int i = 0; i < ret.length - 1; i++) {
				if (ret[i].start < ret[i + 1].start) {
					continue;
				}

				if (ret[i].start == ret[i + 1].start
						&& ret[i].length <= ret[i + 1].length) {
					continue;
				}

				DdlField temp = ret[i];
				ret[i] = ret[i + 1];
				ret[i + 1] = temp;
				swap = true;
			}
		} while (swap);

		return ret;
	}

	public static void main(String[] args) {
		Ddl ddl = new Ddl(args[0]); // filename of ddl is passed
		System.err.println("Reclen for Ddl is " + ddl.getReclen());
		DdlField[] ddlFields = ddl.getDdlFields();
		System.err.println("There are " + ddlFields.length + " fields in Ddl");
		for (int i = 0; i < ddlFields.length; i++) {
			System.err.println("Field " + i + " = " + ddlFields[i].toString());
		}
	}
}

