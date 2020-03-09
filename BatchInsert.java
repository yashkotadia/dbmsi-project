import global.*;
import index.*;
import BigT.*;
import java.io.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

public class BatchInsert implements GlobalConst {
    public static void main(String[] args) {

        //Parsing arguments:
        if (args.length != 4 || args[0] == "-h")
        {
            System.out.println("Enter correct arguments: \nbatchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUFF");
            return;
        }

        String dataFileName = args[0], databaseType = args[1], bigtableName = args[2];
        int numbuffs = Integer.parseInt(args[3]);
        String dbpath = "/tmp/"+System.getProperty("user.name")+bigtableName; 
        SystemDefs sysdef = new SystemDefs( dbpath, 10000, numbuffs, "Clock",  Integer.parseInt(databaseType));
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

            while (line != null) {

                Map m = new Map();
                String[ ] attributes = line.split(",");
                m.setValue(attributes[3]);
                m = new Map(m.size());
                m.setRowLabel(attributes[0]);
                m.setColumnLabel(attributes[1]);
                m.setTimeStamp(Integer.parseInt(attributes[2]));
                m.setValue(attributes[3]);

                try {
                    bigTable.insertMap(m.returnMapByteArray());
                    System.out.println("Inserted map value: " + attributes[3]);
                }catch (Exception e) {
                    System.err.println("*** error in bigT.insertMap() ***");
                    e.printStackTrace();
                }


                line = br.readLine();
            }
            
            Scan scan = bigTable.openScan();
            MID mid = new MID();
            Map map1 = new Map();
            while(true) {
                if((map1 =  scan.getNext(mid)) == null) 
                    break;
                System.out.println(map1.getValue());
            }


        }catch (IOException e) {
            e.printStackTrace();
        }catch (InvalidMapSizeException e){
            System.err.println("*** InvalidMapSize ***");
            e.printStackTrace();
        }catch (InvalidTupleSizeException e){
            System.err.println("*** InvalidTupleSize ***");
            e.printStackTrace();
        }
}

}
