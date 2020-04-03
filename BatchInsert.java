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

        String dataFileName = args[0], bigtableName = args[2];
        int databaseType = Integer.parseInt(args[1]);
        int numbuffs = Integer.parseInt(args[3]);
        String dbpath = "/tmp/"+System.getProperty("user.name")+bigtableName; 
        SystemDefs sysdef = new SystemDefs( dbpath, 20000, numbuffs, "Clock");

        String[] streamBTNames = new String[6];
        for(int i=0; i<5; i++) streamBTNames[i] = "old_"+(i+1);

        //Reading input csv file:
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataFileName));

            String line = br.readLine();

            bigT tempbt = new bigT(null);
            streamBTNames[5] = tempbt.get_fileName();

            // Put the new data into temporary bigT
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
                    tempbt.insertMap(m.returnMapByteArray());
                    System.out.println("Added map timestamp to temporary: " + attributes[3]);
                }catch (Exception e) {
                    System.err.println("*** error in bigT.insertMap() ***");
                    e.printStackTrace();
                }


                line = br.readLine();
            }

            // Ensure that all bigTables are there before attempting renaming
            for(int i=1; i<=5; i++) new bigT(bigtableName +"_" +i);

            // Rename all the bigTable file entries to something else
            for(int i=0; i<5; i++)
                SystemDefs.JavabaseDB.rename_file_entry(bigtableName +"_" +(i+1), streamBTNames[i] );

            // Create new Big Tables
            bigT[] newBTs = new bigT[5];
            for(int i=0; i<5; i++)
                newBTs[i] = new bigT(bigtableName +"_" +(i+1));

            // Initialize the stream
            BigStream stream = new BigStream(streamBTNames, 6, "*", "*", "*");

            // Reinitialize indexes
            for(int i=1; i<=5; i++){
                SystemDefs.JavabaseDB.initIndex(i);
                SystemDefs.JavabaseDB.deleteIndex(i);
                SystemDefs.JavabaseDB.initIndex(i);
                SystemDefs.JavabaseDB.closeIndex(i);
            }

            // Delete the old big tables
            for(int i=0; i<5; i++) {
                bigT b = new bigT(streamBTNames[i]);
                b.deletebigT();
            }
            
            // Distribute the maps to respective big tables
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
                    if(stream.outInd==5) newBTs[databaseType-1].insertMap(rMap.returnMapByteArray());
                    else newBTs[stream.outInd].insertMap(rMap.returnMapByteArray());
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
