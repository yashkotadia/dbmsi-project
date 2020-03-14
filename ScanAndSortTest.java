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

public class ScanAndSortTest implements GlobalConst {
    private static int   SORTPGNUM = 100; 
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
        SystemDefs sysdef = new SystemDefs( dbpath, 12500, numbuffs, "Clock",  Integer.parseInt(databaseType));
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
                    System.out.println("Inserted map value: " + attributes[3]);
                }catch (Exception e) {
                    System.err.println("*** error in bigT.insertMap() ***");
                    e.printStackTrace();
                }


                line = br.readLine();
            }
            
            //Scan Test

            Scan scan = bigTable.openScan();
            MID mid = new MID();
            Map map1 = new Map();
            while(true) {
                if((map1 =  scan.getNext(mid)) == null) 
                    break;
                System.out.println(map1.getValue());
            }

            //for(int i=0; i<30; i++)
            //    SystemDefs.JavabaseDB.add_file_entry("check"+i, new PageId(0));

            // FileScanTest 

            FileScan fscan = new FileScan(bigtableName+"_"+databaseType,"*", "*", "*");
            /*
            while(true) {
                if((map1 =  fscan.get_next()) == null){
                    System.out.println("Breaking out");
                    break;
                } 
                    
                System.out.println(map1.getValue());
            }*/

            // Sort Test
            // set up an identity selection
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.string = "Singapore";
            expr[0].next = null;
            expr[1] = null;
            IndexScan iscan = new IndexScan ( new IndexType(IndexType.B_Index), bigtableName+"_"+databaseType,
               "row_index", "*", "*", "*", null);

            TupleOrder[] order = new TupleOrder[2];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            order[1] = new TupleOrder(TupleOrder.Descending);
            Sort sort = new Sort(order[0], fscan, SORTPGNUM, 1, 80);
            while(true){
                if((map1 = sort.get_next()) == null){
                    break;
                }
                System.out.println(map1.getValue());
            }


        }catch (IOException e) {
            e.printStackTrace();
        }catch (InvalidMapSizeException e){
            System.err.println("*** InvalidMapSize ***");
            e.printStackTrace();
        }catch (InvalidTupleSizeException e){
            System.err.println("*** InvalidTupleSize ***");
            e.printStackTrace();
        }catch (FileScanException e){
            System.err.println("*** Invalid file scan ***");
            e.printStackTrace();
        }catch (MapUtilsException e){
            System.err.println("*** MapUtils error ***");
            e.printStackTrace();
        }catch (PageNotReadException e){
            System.err.println("*** Page not read error ***");
            e.printStackTrace();
        }catch (WrongPermat e){
            System.err.println("*** Wrong permat error ***");
            e.printStackTrace();
        }catch(Exception e){
            System.err.println("*** error ***");
            e.printStackTrace();
        }

}

}
