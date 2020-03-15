import BigT.*;
import global.*;
import iterator.FileScan;
import iterator.Sort;

import java.io.*;
import java.lang.*;


public class query implements GlobalConst {

    private static void InsertMaps(bigT bigTable)
    {
        try {
            BufferedReader br = new BufferedReader(new FileReader("/home/sanika/Sanika_DBMSI/dbmsiPhase2Debug/project2_testdata.csv"));

            String line = br.readLine();

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
                    //System.out.println("Inserted map value: " + m.getValue());
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
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int   SORTPGNUM = 12;
        //Parsing arguments:
        if (args.length != 7 || args[0] == "-h") {
            System.out.println("Enter correct arguments: \nquery BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF\n");
            return;
        }

        String bigtableName = args[0], databaseType = args[1], rowfilter = args[3], columnfilter = args[4], vfilter = args[5];
        int numbuffs = Integer.parseInt(args[6]), orderType = Integer.parseInt(args[2]);

        String dbpath = "/tmp/" + System.getProperty("user.name") + bigtableName;
        SystemDefs sysdef = new SystemDefs(dbpath, 10000, numbuffs, "Clock", Integer.parseInt(databaseType));


        bigT bigTable = null;
        try {
            bigTable = new bigT(bigtableName+"_"+databaseType);
            InsertMaps(bigTable);

            Stream stream = bigTable.openStream(orderType,rowfilter,columnfilter,vfilter);
            
            MID mid = new MID();
            Map rMap;
            while(true){
                if((rMap   = stream.getNext(mid)) == null){
                    break;
                }
                System.out.println(rMap.getRowLabel() + " " + rMap.getColumnLabel() + " " + rMap.getTimeStamp() + " " + rMap.getValue());
            }


            stream.closestream();
        }
        catch (Exception e) {
            System.err.println("*** error in retrieving bigTable ***");
            e.printStackTrace();
        }


    }

}
