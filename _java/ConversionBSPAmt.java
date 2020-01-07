package uk.co.firstchoice.stella;

import uk.co.firstchoice.util.businessrules.NumberProcessor;

/**
 * ConversionBSPAmt.java
 * 
 * Created on 21 May 2003, 11:59 leigh ashton convert a BSP amt from string to a
 * decimal monetary amount BSP file contains numbers in format where last digit
 * is ascii char, not numeric e.g. 0000001157{ so need to convert to it's true
 * decimal value
 * 
 * @author Lashton june 2003
 * @version 1.0
 */
public class ConversionBSPAmt {

	/**
	 * convert the bsp amt to a normal decimal amt last digit is "overpunched"
	 * so needs hardcoding of conversion values as supplied by BSP directly
	 * 
	 * @return double of the actual amount
	 * @param inString
	 *            string representing the bsp amount taken from bsp input file -
	 *            has overpunched last char which needs to be decifered
	 * @throws BSPAmtException
	 *             there is an invalid amount
	 */

	public static double parseLastCharAmt(String inString)
			throws BSPAmtException {

		/**
		 * On the DISH HOT, for all numeric value amounts, the last digit
		 * contains an 'overpunched' sign, which doubles up as an identifier for
		 * both the value of the last digit, and also whether the amount is a
		 * credit or debit (as there are no separate credit/debit identifiers on
		 * the HOT).
		 * 
		 * Credit is defined as monies passed from Agent to Airline (i.e.
		 * document amount of a Sale, commission on a Refund), Debit is defined
		 * as monies passed from Airline to Agent (i.e. document amount of a
		 * Refund, commission on a Sale).
		 * 
		 * Overpunch values are as follows
		 * 
		 * Credit { = 0 A = 1 B = 2 C = 3 D = 4 E = 5 F = 6 G = 7 H = 8 I = 9
		 * 
		 * Debit } = 0 J = 1 K = 2 L = 3 M = 4 N = 5 O = 6 P = 7 Q = 8 R = 9
		 * 
		 * examples: 1000{ = 100.00 credit, 1214N = 121.45 debit
		 *  
		 */

		String lastChar = inString.substring(inString.length() - 1);
		String firstSection = inString.substring(0, inString.length() - 1);
		String lastDigit = "";
		int amtSign = 1; // set to -1 if amt should be negative

		if (lastChar.equals("{")) {
			lastDigit = "0";
		} else if (lastChar.equals("A")) {
			lastDigit = "1";
		} else if (lastChar.equals("B")) {
			lastDigit = "2";
		} else if (lastChar.equals("C")) {
			lastDigit = "3";
		} else if (lastChar.equals("D")) {
			lastDigit = "4";
		} else if (lastChar.equals("E")) {
			lastDigit = "5";
		} else if (lastChar.equals("F")) {
			lastDigit = "6";
		} else if (lastChar.equals("G")) {
			lastDigit = "7";
		} else if (lastChar.equals("H")) {
			lastDigit = "8";
		} else if (lastChar.equals("I")) {
			lastDigit = "9";
		}
		// now negative amounts
		else if (lastChar.equals("}")) {
			lastDigit = "0";
			amtSign = -1;
		} else if (lastChar.equals("J")) {
			lastDigit = "1";
			amtSign = -1;
		} else if (lastChar.equals("K")) {
			lastDigit = "2";
			amtSign = -1;
		} else if (lastChar.equals("L")) {
			lastDigit = "3";
			amtSign = -1;
		} else if (lastChar.equals("M")) {
			lastDigit = "4";
			amtSign = -1;
		} else if (lastChar.equals("N")) {
			lastDigit = "5";
			amtSign = -1;
		} else if (lastChar.equals("O")) {
			lastDigit = "6";
			amtSign = -1;
		} else if (lastChar.equals("P")) {
			lastDigit = "7";
			amtSign = -1;
		} else if (lastChar.equals("Q")) {
			lastDigit = "8";
			amtSign = -1;
		} else if (lastChar.equals("R")) {
			lastDigit = "9";
			amtSign = -1;
		} else {
			// unknown end digit so fail
			throw new BSPAmtException("Invalid last char in BSP amt:"
					+ inString + ", last char was:" + lastChar);

		}

		// reform the string with new ending character
		String finalString = firstSection + lastDigit;
		// now validate the number and then convert to negative if applicable

		if (!NumberProcessor.validateStringAsNumber(finalString)) {
			throw new BSPAmtException("BSP Amt after conversion not numeric"
					+ finalString);
		}

		// amounts in file assumed to be 2 decimal places, so divide by 100
		double finalAmt = Double.parseDouble(finalString) * amtSign / 100;

		return finalAmt;
	}

}

