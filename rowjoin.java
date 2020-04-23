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

public class rowjoin implements GlobalConst{
	private static boolean OK = true;
    private static boolean FAIL = false;
    public static void main(String args[])
        throws JoinNewFailed ,
       JoinLowMemory,
       SortException,
       HFException,
       HFBufMgrException,
       HFDiskMgrException,
       IOException,
       InvalidMapSizeException,
       InvalidSlotNumberException,
       InvalidTupleSizeException,
       MapUtilsException,
       SpaceNotAvailableException,
       FileScanException,
       IndexException,
       JoinsException,
       Exception
    {
		boolean status = OK;
        if (args.length != 5 || args[0] == "-h")
        {
            System.out.println("Enter correct arguments: \nrowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF");
            return;
        }
        String bigTable1Name = args[0]; String bigTable2Name = args[1];
        String outBigTableName = args[2]; 
        String colFilter = args[3];
        int numbuffs = Integer.parseInt(args[4]);

        String dbpath = "/tmp/" + System.getProperty("user.name") + "database";
        SystemDefs sysdef = new SystemDefs(dbpath, 0, numbuffs, "Clock");

        String[] bigtable1Names = new String[5];
        for(int i=0; i<5; i++) 
            bigtable1Names[i] = bigTable1Name + "_" + (i+1);

        String[] bigtable2Names = new String[5];
        for(int i=0; i<5; i++) 
            bigtable2Names[i] = bigTable2Name + "_" + (i+1);



        //Initialize two bigstreams, one on each of the two bigbigTables.
        BigStream bs1 = new BigStream(bigtable1Names, 6, 0, "*", colFilter, "*"); // orderType = 6
        BigStream bs2 = new BigStream(bigtable2Names, 6, 0, "*", colFilter, "*"); // orderType = 6

        //Initialize two bigTs to contain only most recent maps 
        bigT outerbt = new bigT(null);
        bigT innerbt = new bigT(null);

        //Get only the most recents maps and output them to a temp bigT
        Map prevMap = null;
        Map rMap;
        while(true){
            if((rMap = bs1.getNext()) == null){
                break;
            }
            if(prevMap == null || !(prevMap.getRowLabel().equals(rMap.getRowLabel()) && prevMap.getColumnLabel().equals(rMap.getColumnLabel()))){
                //Output this map to bigT
                // System.out.print("Inserted map in outerbt: ");
                // rMap.print();
                outerbt.insertMap(rMap.getMapByteArray());
            }
            prevMap = new Map(rMap);
             
        }
        
        prevMap = null;
        while(true){
            if((rMap = bs2.getNext()) == null){
                break;
            }
            if(prevMap == null || !(prevMap.getRowLabel().equals(rMap.getRowLabel()) && prevMap.getColumnLabel().equals(rMap.getColumnLabel()))){
                //Output this map to the temp bigT
                // System.out.print("Inserted map in innerbt: ");
                // rMap.print();
                innerbt.insertMap(rMap.getMapByteArray());
            }
            prevMap = new Map(rMap);
            
        }
        //Create streams on the new temp bigTs, ordered on values
        Stream outerStream = new Stream(outerbt, 8, 0, "*", "*", "*");
        Stream innerStream = new Stream(innerbt, 8, 0, "*", "*", "*");

        BigSortMerge sm = null;
        try{
            sm = new BigSortMerge(bigtable1Names, bigtable2Names, outerStream, innerStream, outBigTableName);
            System.out.println("Initialed sortmerge constructor");
        }
        catch(Exception e){
            System.err.println("*** join error in SortMerge constructor ***"); 
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        try{
            sm.performJoin();
             System.out.println("Performed sortmerge join");
        }
        catch(Exception e){
            System.err.println (""+e);
            e.printStackTrace();
            status = FAIL;
        }
        
        if (status != OK){
            //bail out
            System.err.println ("*** Error in performing join");
            Runtime.getRuntime().exit(1);
        }
        try {
            sm.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println ("\n"); 
        
        if (status != OK) {
            //bail out
            System.err.println ("*** Error in closing ");
            Runtime.getRuntime().exit(1);
        }
        try{
            bs1.close();
            bs2.close();
        }
        catch(Exception e){
            System.err.println (""+e);
            e.printStackTrace();
        }
        sysdef.close();
	}
}