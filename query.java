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
            BigStream s = new BigStream(bigtableName, orderType, rowfilter, columnfilter, vfilter);

            while(true){

                Map out = s.getNext();
                if(out == null) break;

                System.out.println(out.getRowLabel() + " " + out.getColumnLabel() + " " + out.getValue() + " " + out.getTimeStamp());
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
