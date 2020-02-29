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
        if (args.length != 3 || args[0] == "-h")
        {
            System.out.println("Enter correct arguments: \nbatchinsert DATAFILENAME TYPE BIGTABLENAME");
            return;
        }

        String dataFileName = args[0], databaseType = args[1], bigtableName = args[2];
        String dbpath = "/tmp/"+System.getProperty("user.name")+bigtableName; 
        SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock", 1 );
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
