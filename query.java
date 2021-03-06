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
            System.out.println("Enter correct arguments: \nquery BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF\n");
            return;
        }

        String bigtableName = args[0], rowfilter = args[2], columnfilter = args[3], vfilter = args[4];
        int numbuffs = Integer.parseInt(args[5]), orderType = Integer.parseInt(args[1]);

        String dbpath = "/tmp/" + System.getProperty("user.name") + "database";
        SystemDefs sysdef = new SystemDefs(dbpath, 0, numbuffs, "Clock");

        String[] bigtableNames = new String[5];
        for(int i=0; i<5; i++) bigtableNames[i] = bigtableName + "_" + (i+1);

        try {
            BigStream s = new BigStream(bigtableNames, orderType, 0, rowfilter, columnfilter, vfilter);

            while(true){

                Map out = s.getNext();
                if(out == null) break;

                out.print();
            }

            s.close();
            sysdef.close();
        }
        catch (Exception e) {
            System.err.println("*** error in retrieving bigTable ***");
            e.printStackTrace();
        }


    }

}
