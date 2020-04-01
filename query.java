import BigT.*;
import global.*;
import iterator.FileScan;
import iterator.Sort;

import java.io.*;
import java.lang.*;


public class query implements GlobalConst {

    public static void main(String[] args) {
        //Parsing arguments:
        if (args.length != 7 || args[0] == "-h") {
            System.out.println("Enter correct arguments: \nquery BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF\n");
            return;
        }

        String bigtableName = args[0], databaseType = args[1], rowfilter = args[3], columnfilter = args[4], vfilter = args[5];
        int numbuffs = Integer.parseInt(args[6]), orderType = Integer.parseInt(args[2]);

        String dbpath = "/tmp/" + System.getProperty("user.name") + bigtableName + "_" + databaseType;
        SystemDefs sysdef = new SystemDefs(dbpath, 0, numbuffs, "Clock", Integer.parseInt(databaseType));


        bigT bigTable = null;
        try {
            bigTable = new bigT(bigtableName+"_"+databaseType);

            Stream stream = bigTable.openStream(orderType,rowfilter,columnfilter,vfilter);
            
            MID mid = new MID();
            Map rMap;
            while(true){
                if((rMap   = stream.getNext(mid)) == null){
                    break;
                }
                System.out.println(rMap.getRowLabel() + " " + rMap.getColumnLabel() + " " + rMap.getValue() + " " + rMap.getTimeStamp());
            }


            stream.close();
            sysdef.close();
        }
        catch (Exception e) {
            System.err.println("*** error in retrieving bigTable ***");
            e.printStackTrace();
        }


    }

}
