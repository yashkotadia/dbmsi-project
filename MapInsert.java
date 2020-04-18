import global.*;
import BigT.*;
import iterator.*;

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
            BigStream s = new BigStream(bigtableName, 6, 0, row, column, "*", false);

            // Delete the map with smallest timestamp
            MID resmid = null;
            Map resmap = null;
            int resCnt = 0;
            int resInd = 0;
            Map map;
            MID mid = new MID();

            System.out.println("Matching maps:");
            while ((map = s.getNext(mid)) != null)
            {
                resCnt++;
                //Checking if map is a duplicate of the new map
                if (map.getTimeStamp() == m.getTimeStamp())
                    {
                        System.out.println("Trying to insert a duplicate map");
                        return;
                    }

                if (resmap == null || map.getTimeStamp() <= resmap.getTimeStamp())
                {
                    resmap = map;
                    resmid = new MID(mid.pageNo, mid.slotNo);
                    resInd = s.outInd;
                }

                System.out.println(map.getRowLabel() + ", " + map.getColumnLabel() + ", " + map.getValue() + ", " + map.getTimeStamp() + " | storageType=" + (s.outInd+1));

            }

            if (resmap != null && resCnt >= 3)
            {
                //Delete this map from the bigT
                bigT Bt = new bigT(bigtableNames[resInd]);

                //resmap = Bt.getMap(resmid);
                System.out.print("Deleted Map: ");
                resmap.print();

                Bt.deleteMap(resmid);
            }
            s.close();

            //Insert the map:
            String btName = bigtableName +  "_" + storageType;
            //bigT existingBt = new bigT(btName);
            //existingBt.insertMap(m.getMapByteArray());

            int ordertype = 6;
            switch(storageType){
                    case 2: ordertype = 6; break;
                    case 3: ordertype = 2; break;
                    case 4: ordertype = 2; break;
                    case 5: ordertype = 7; break;
            }

            String renamedBT = "old_" + storageType;
            SystemDefs.JavabaseDB.rename_file_entry(btName, renamedBT);
            if (storageType != 1)
                SystemDefs.JavabaseDB.rename_file_entry(btName + "_index", renamedBT + "_index");

            bigT existingBt = new bigT(renamedBT);
            FileScan fs = new FileScan(renamedBT, "*", "*", "*");

            bigT bt = new bigT(btName);
            bt.createIndex();

            boolean inserted = false;

            while(true){

                Map out = fs.get_next();
                if(out == null){
                    if(!inserted){
                        bt.insertMap(m.getMapByteArray());
                        m.print();
                        inserted = true;
                    }
                    break;
                }

                if(MapUtils.CompareMapWithMap(out, m, ordertype)==1 && !inserted){
                    bt.insertMap(m.getMapByteArray());
                    m.print();
                    inserted = true;
                }

                bt.insertMap(out.getMapByteArray());

                out.print();
            }

            if(!inserted) System.out.println("New Map not inserted");

            fs.close();
            existingBt.deletebigT();
            sysdef.close();

        } catch (InvalidMapSizeException e) {
            System.err.println("*** InvalidMapSize ***");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
