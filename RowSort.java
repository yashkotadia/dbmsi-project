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


public class RowSort{

	public static void main(String[] args){

		// Parsing arguments:
		if(args.length != 4){
			System.out.println("Enter correct arguments: \nrowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF");
			return;
		}

		String inputBigTable = args[0];
		String outputBigTable = args[1];
		String columnName = args[2];
		int numBuffers = Integer.parseInt(args[3]);

		try{
			// Open the database
			String dbpath = "/tmp/"+System.getProperty("user.name")+"database"; 
	        SystemDefs sysdef = new SystemDefs( dbpath, 20000, numBuffers, "Clock");

			// Create output BigTable
			bigT newBT = new bigT(outputBigTable +"_" +(1));
			newBT.createIndex();

			// Create temp heap file
			bigT tempbt = new bigT(null);
	  
			// Initialize two Big Streams:
			BigStream stream1 = new BigStream(inputBigTable, 1, "*", columnName, "*", true);
			BigStream stream2 = new BigStream(inputBigTable, 1, "*", "*", "*",true);

			Map map1 = stream1.getNext();
			Map map2 = stream2.getNext();
			Map map3 = null;
			String prevRow1 = "";
			String currRow1 = "";
			String prevRow2 = "";
			String currRow2 = "";

			System.out.println("Maps inserted in the following order:");
			// get entry from stream1 using getNext
			// use this entry to iterate over stream2
			// if entry exists, put all the maps with the corresponding name in the new Bigtable
			// else put it in the temporary heap file.

			while( map1!=null && map2!=null ){
				
				// First stream is exhausted, put maps of 2nd stream to temporary BT
				if(map1==null){
					tempbt.insertMap(map2.getMapByteArray());
					while( (map2=stream2.getNext()) != null ){
						tempbt.insertMap(map2.getMapByteArray());
					}

				}

				// The maps at head of both the stream belong to the same row
				// Thus, insert all the maps of that row to output BT
				// Reposition Stream 1 to next row
				else if( map1.getRowLabel().equals(map2.getRowLabel()) ){
					newBT.insertMap(map2.getMapByteArray());
					map2.print();
					prevRow2 = map2.getRowLabel();

					// Insert all the maps having this specific rowlabel
					while(((map2 = stream2.getNext())!=null ) && prevRow2.equals(map2.getRowLabel())){
						newBT.insertMap(map2.getMapByteArray());
						map2.print();
						prevRow2 = map2.getRowLabel();
					}
					
					// Jump in stream1 to the next row
					prevRow1 = map1.getRowLabel();
					while(((map1 = stream1.getNext())!=null ) && prevRow1.equals(map2.getRowLabel())){
						prevRow1 = map2.getRowLabel();
					}
				}
				
				// Encountered a row in stream 2 that does not contain the desired column
				// Output the entire row to temporary BT
				else{
					prevRow2 = map2.getRowLabel();
					tempbt.insertMap(map2.getMapByteArray());

					while( ((map2 = stream2.getNext())!=null ) && prevRow2.equals(map2.getRowLabel()) ){
					    tempbt.insertMap(map2.getMapByteArray());
						prevRow2 = map2.getRowLabel();
					}

				}
	 
			}
			
			stream1.close();
			stream2.close();

			FileScan fs = new FileScan(tempbt.get_fileName(), "*", "*", "*");

			while( (map3 = fs.get_next()) !=null){
				newBT.insertMap(map3.getMapByteArray());
				map3.print();
			}

			fs.close();
			tempbt.deletebigT();
		
			sysdef.close();
		} catch(Exception e){
			System.out.println("Error in RowSort");
		}
	}

}
