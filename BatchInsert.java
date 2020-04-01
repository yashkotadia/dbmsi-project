import global.*;
import index.*;
import BigT.*;
import java.io.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;
import iterator.*;

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
        SystemDefs sysdef = new SystemDefs( dbpath, 20000, numbuffs, "Clock");
        //Reading input csv file:
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName));

            String line = br.readLine();

            bigT bigTable = null;
            bigT tempbt = null;
            
            try {
                bigTable = new bigT(bigtableName+"_"+databaseType);
                tempbt = new bigT(null);
            }
            catch (Exception e) {
                System.err.println("*** Error in creating/opening bigT ***");
                e.printStackTrace();
            }

            while (line != null) {

                Map m = new Map();
                String[ ] attributes = line.split(",");
                m.setValue(attributes[2]);
                m = new Map(m.size());
                m.setRowLabel(attributes[0]);
                m.setColumnLabel(attributes[1]);
                m.setTimeStamp(Integer.parseInt(attributes[3]));
                m.setValue(attributes[2]);

                try {
                    tempbt.insertMap(m.returnMapByteArray(), false);
                    System.out.println("Added map timestamp to temporary: " + attributes[3]);
                }catch (Exception e) {
                    System.err.println("*** error in bigT.insertMap() ***");
                    e.printStackTrace();
                }


                line = br.readLine();
            }

            if(bigTable.getMapCnt()!=0){
                Scan scan = bigTable.openScan();
                MID mid = new MID();
                while(true) {
                    Map map1 = new Map();
                    if((map1 =  scan.getNext(mid)) == null) 
                        break;
                    System.out.println("Added Map TS from old to temp: " + map1.getTimeStamp());
                    tempbt.insertMap(map1.returnMapByteArray(), true);
                }
                scan.closescan();

                bigTable.deletebigT();
                bigTable = new bigT(bigtableName+"_"+databaseType);
            }

            Stream stream = tempbt.openStream(6, "*", "*", "*");
            Map prevMap = null;
            Map rMap;
            int prevCount = 0;
            while(true){
                if((rMap   = stream.getNext()) == null){
                    break;
                }
                if(prevMap!=null && prevMap.getRowLabel().equals(rMap.getRowLabel()) && prevMap.getColumnLabel().equals(rMap.getColumnLabel()) ){
                    prevCount++;
                } else {
                    prevCount = 1;
                    prevMap = new Map(rMap);
                }
                if(prevCount<=3){
                    System.out.println("Added Map TS from temp to new: " + rMap.getTimeStamp());
                    bigTable.insertMap(rMap.returnMapByteArray(), true);
                }
            }
            stream.close();
            tempbt.deletebigT();


            sysdef.close();

        }catch (IOException e) {
            e.printStackTrace();
        }catch (InvalidMapSizeException e){
            System.err.println("*** InvalidMapSize ***");
            e.printStackTrace();
        }catch(Exception e){
            System.err.println("*** error ***");
            e.printStackTrace();
        }

}

}
