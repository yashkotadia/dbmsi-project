import BigT.*;
import global.*;
import iterator.*;

import java.io.*;
import java.lang.*;
import java.util.Arrays;

public class getCounts implements GlobalConst {

    public static String[] bigtableNames;

    public static void main(String[] args) {
        //Parsing arguments:
        if (args.length != 2 || args[0] == "-h") {
            System.out.println("Enter correct arguments: \ngetCounts BIGTABLENAME NUMBUF\n");
            return;
        }

        String bigtableName = args[0];
        int numbuffs = Integer.parseInt(args[1]);

        String dbpath = "/tmp/" + System.getProperty("user.name") + "database";
        SystemDefs sysdef = new SystemDefs(dbpath, 0, numbuffs, "Clock");

        bigtableNames = new String[5];
        for(int i=0; i<5; i++) bigtableNames[i] = bigtableName + "_" + (i+1);

        try {
            System.out.println("Map    Count: " +getMapCnt());
            System.out.println("Row    Count: " +getRowCnt());
            System.out.println("Column Count: " +getColumnCnt());
            sysdef.close();
            
        }
        catch (Exception e) {
            System.err.println("*** error in retrieving bigTable ***");
            e.printStackTrace();
        }


    }

    /** Sums the result of getMapCnt() on all bigTables
    */
    private static int getMapCnt(){

        int mapCount = 0;
        try{
            bigT[]      bigTable = new bigT[5];

            // Initialize all the BigTables and Streams
            for(int i=0; i<5; i++){
                bigTable[i] = new bigT(bigtableNames[i]);
                mapCount += bigTable[i].getMapCnt();
            }
        } catch(Exception e){
            System.err.println("*** error in Calculating Map Count ***");
            e.printStackTrace();
        }

        return mapCount;
    }

    /** Calls BigStream with orderType=1 RCT
    */
    private static int getRowCnt(){

        int rowCnt = 0;
        try{
            BigStream s = new BigStream(bigtableNames, 1, "*", "*", "*");
            String prevRow = "";

            while(true){

                Map out = s.getNext();
                if(out == null) break;

                // Check if the Map's row matches with last row
                if(!out.getRowLabel().equals(prevRow)){
                    prevRow = out.getRowLabel();
                    rowCnt++;
                }
            }

            s.close();

        } catch(Exception e){
            System.err.println("*** error in Calculating Distinct Row Count ***");
            e.printStackTrace();
        }

        return rowCnt;

    }

    /** Calls BigStream with orderType=2 CRT
    */
    private static int getColumnCnt(){

        int colCnt = 0;
        try{
            BigStream s = new BigStream(bigtableNames, 2, "*", "*", "*");
            String prevCol = "";

            while(true){

                Map out = s.getNext();
                if(out == null) break;

                // Check if the Map's column matches with last map's column
                if(!out.getColumnLabel().equals(prevCol)){
                    prevCol = out.getColumnLabel();
                    colCnt++;
                }
            }

            s.close();

        } catch(Exception e){
            System.err.println("*** error in Calculating Distinct Column Count ***");
            e.printStackTrace();
        }

        return colCnt;

    }

}
