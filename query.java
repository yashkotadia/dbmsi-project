import BigT.*;
import global.*;
import iterator.*;

import java.io.*;
import java.lang.*;
import java.util.Arrays;

public class query implements GlobalConst {

    public static void main(String[] args) {
        //Parsing arguments:
        if (args.length != 6 || args[0] == "-h") {
            System.out.println("Enter correct arguments: \nquery BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF\n");
            return;
        }

        String bigtableName = args[0], rowfilter = args[2], columnfilter = args[3], vfilter = args[4];
        int numbuffs = Integer.parseInt(args[5]), orderType = Integer.parseInt(args[1]);

        String dbpath = "/tmp/" + System.getProperty("user.name") + bigtableName;
        SystemDefs sysdef = new SystemDefs(dbpath, 0, numbuffs, "Clock");


        try {

            bigT[]      bigTable = new bigT[5];
            Stream[]    stream = new Stream[5];
            Map[]       rMap = new Map[5];
            boolean[]   done = new boolean[5];

            // Initialize all the BigTables and Streams
            for(int i=0; i<5; i++){
                bigTable[i] = new bigT(bigtableName+"_"+(i+1));
                stream[i] = bigTable[i].openStream(orderType,rowfilter,columnfilter,vfilter);
                rMap[i] = stream[i].getNext();
                if(rMap[i]==null) done[i] = true;
            }
            
            while(true){
                // Initialize the first null Map as minimum for further comparison
                int minInd = 0; 
                for(int i=0; i<5; i++){
                    if(rMap[i]!=null){
                        minInd = i;
                        break;
                    }
                }

                // Find minimum out of all streams
                for(int i=0; i<5; i++){
                    if( rMap[i]!= null && MapUtils.CompareMapWithMap(rMap[minInd], rMap[i], orderType)==1 ){
                        minInd = i;
                    }
                }

                // Print the minimum valued Map
                Map out = rMap[minInd];
                System.out.println(out.getRowLabel() + " " + out.getColumnLabel() + " " + out.getValue() + " " + out.getTimeStamp());

                // Call stream.getNext() on the stream that we used
                rMap[minInd] = stream[minInd].getNext();
                if(rMap[minInd]==null) done[minInd] = true;

                // Check Breaking condition
                if(areAllTrue(done)) break;

            }


            for(int i=0; i<5; i++) stream[i].close();
            sysdef.close();
        }
        catch (Exception e) {
            System.err.println("*** error in retrieving bigTable ***");
            e.printStackTrace();
        }


    }

    private static boolean areAllTrue(boolean[] array)
    {
        for(boolean b : array) if(!b) return false;
        return true;
    }

}
