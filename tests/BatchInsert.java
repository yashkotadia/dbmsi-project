package tests;

import BigT.InvalidMapSizeException;
import BigT.Map;
import BigT.bigT;
import global.GlobalConst;
import global.SystemDefs;
import heap.Heapfile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class BatchInsert implements GlobalConst {
    public static void main(String[] args) {

        //Parsing arguments:
        if (args.length != 3 || args[0] == "-h")
        {
            System.out.println("Enter correct arguments: \nbatchinsert DATAFILENAME TYPE BIGTABLENAME");
            return;
        }

        String dataFileName = args[0], databaseType = args[1], bigtableName = args[2];
        SystemDefs sysdef = new SystemDefs( bigtableName, 1000, NUMBUF, "Clock" );
        //Reading input csv file:
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName));

            String line = br.readLine();

            bigT bigTable = null;
            try {
                bigTable = new bigT(bigtableName+"_"+databaseType);
            }
            catch (Exception e) {
                System.err.println("*** error in creating bigT ***");
                e.printStackTrace();
            }

            Map m = new Map();
            while (line != null) {

                String[ ] attributes = line.split(",");
                m.setRowLabel(attributes[0]);
                m.setColumnLabel(attributes[1]);
                m.setTimeStamp(Integer.parseInt(attributes[2]));
                m.setValue(attributes[3]);

                try {
                    bigTable.insertMap(m.returnMapByteArray());
                }catch (Exception e) {
                    System.err.println("*** error in bigT.insertMap() ***");
                    e.printStackTrace();
                }


                line = br.readLine();
            }

        }catch (IOException e) {
            e.printStackTrace();
        }catch (InvalidMapSizeException e){
            System.err.println("*** InvalidMapSize ***");
            e.printStackTrace();}
}

}
