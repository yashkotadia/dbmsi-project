import global.*;
import BigT.*;

import java.io.IOException;
import java.lang.*;


public class MapInsert implements GlobalConst {

    public static void main(String[] args) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {

        //Parsing arguments:
        if (args.length != 7 || args[0] == "-h")
        {
            System.out.println("Enter correct arguments: \nmapinsert RL CL VAL TS TYPE BIGTABLENAME NUMBUF\n");
            return;
        }

        String bigtableName = args[5], row = args[0], column = args[1], val = args[2], ts = args[3];
        int numbuffs = Integer.parseInt(args[6]), storageType = Integer.parseInt(args[4]);

        String dbpath = "/tmp/"+System.getProperty("user.name")+"database";
        SystemDefs sysdef = new SystemDefs( dbpath, 20000, numbuffs, "Clock");

        String[] bigtableNames = new String[5];
        for(int i=0; i<5; i++) bigtableNames[i] = bigtableName + "_" + (i+1);

        //Creating/Opening BigT
        for(int i=0; i<5; i++){
            bigT b = new bigT(bigtableNames[i]);
            b.createIndex();
        }

        try {
            //Create the map to be inserted:
            Map m = new Map();
            m.setValue(val);
            m = new Map(m.size());
            m.setRowLabel(row);
            m.setColumnLabel(column);
            m.setTimeStamp(Integer.parseInt(ts));
            m.setValue(val);

            // Initialize the stream
            BigStream s = new BigStream(bigtableNames, 6, row, column, "*", false);

            // Delete the map with smallest timestamp
            MID resmid = null;
            Map resmap = null;
            int resCnt = 0;
            Map map;
            MID mid = new MID();

            System.out.println("Matching maps:");
            while ((map = s.getNext(mid)) != null)
            {
                resCnt++;
                //Checking if map is a duplicate of the new map
                if (map.getValue().equals(m.getValue()))
                    {
                        System.out.println("Trying to insert a duplicate map");
                        return;
                    }

                if (resmap == null || map.getTimeStamp() <= resmap.getTimeStamp())
                {
                    resmap = new Map(map);
                    resmid = new MID(mid.pageNo, mid.slotNo);
                }

                System.out.println(map.getRowLabel() + " " + map.getColumnLabel() + " " + map.getValue() + " " + map.getTimeStamp() + " storageType=" + (s.outInd+1));

            }

            if (resmap != null && resCnt >= 3)
            {
                //Delete this map from the bigT
                bigT Bt = new bigT(bigtableNames[s.outInd]);

                //resmap = Bt.getMap(resmid);
                System.out.println("Deleted Map: " + resmap.getRowLabel() + " " + resmap.getColumnLabel() + " " + resmap.getValue() + " " + resmap.getTimeStamp());

                Bt.deleteMap(resmid);
            }
            s.close();

            //Insert the map:
            String btName = bigtableName +  "_" + storageType;
            bigT existingBt = new bigT(btName);
            existingBt.insertMap(m.getMapByteArray());

            int ordertype = 6;
            switch(storageType){
                    case 2: ordertype = 6; break;
                    case 3: ordertype = 2; break;
                    case 4: ordertype = 2; break;
                    case 5: ordertype = 7; break;
            }

            Stream newStream = new Stream(existingBt, ordertype, "*", "*", "*");

            String renamedBT = "old_" + storageType;
            SystemDefs.JavabaseDB.rename_file_entry(btName, renamedBT);
            if (storageType != 1)
                SystemDefs.JavabaseDB.rename_file_entry(btName + "_index", renamedBT + "_index");

            existingBt = new bigT(renamedBT);
            existingBt.deletebigT();


            bigT bt = new bigT(btName);
            bt.createIndex();
            System.out.println("Maps Inserted:");
            while(true){

                Map out = newStream.getNext();
                if(out == null) break;

                bt.insertMap(out.getMapByteArray());

                System.out.println(out.getRowLabel() + " " + out.getColumnLabel() + " " + out.getValue() + " " + out.getTimeStamp());
            }

            newStream.close();
            sysdef.close();

        } catch (InvalidMapSizeException e) {
            System.err.println("*** InvalidMapSize ***");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
