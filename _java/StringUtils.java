
/*
*@author Jyoti Renganathan
*@version 1.0
*@parm  mainStr - main string to search into
*@param subStr -  string to search for
*@param whichOccur - nth occurance of substrinto mainStr
*@returns  Position of nth occurance of a substring
*/
/* Simple utility to find the nth occurance of a substring in  a string */

package uk.co.firstchoice.util;

import java.io.*;

public class StringUtils  {


public static int searchStringOccur(String mainStr, String subStr,int whichOccur) {

int colonPos = 0, counter = 0 ;
    colonPos = mainStr.indexOf(subStr,colonPos) ; // start from oth position

    if (colonPos != -1) {
		counter++;

     while (counter  < whichOccur)  {
	    colonPos = mainStr.indexOf(subStr,colonPos + 1);

	    if (colonPos == -1) {
	//	  application.log.severe(" Can not find " + whichOccur + "th occurance of " + subStr + " into " + mainStr);
	      break ;
	    }

	    counter++;
     } // end of while

    } // end of if


     return colonPos ;

    // } // end of try

}
}









