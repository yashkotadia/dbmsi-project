package iterator;

import BigT.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import index.*;
import java.io.*;
import java.util.Collections;
import java.util.ArrayList;

/**
 * This file contains the interface for the sort_merg joins.
 * This file contains an implementation of the sort merge join for two bigTables
 * It makes use of the external sorting utility to generate runs,
 * and then uses the iterator interface to
 * get successive tuples for the final merge.
 */
public class BigSortMerge extends Iterator implements GlobalConst{
	private Stream stream1;
	private Stream stream2;
	bigT outputBT;
	private Map map1, map2;
	private Map TempMap1, TempMap2;
	private boolean done; 
	private boolean get_from_s1, get_from_s2;
	private boolean process_next_block;
	private int _n_pages;
	private  byte _bufs1[][], _bufs2[][];
	private  IoBuf io_buf1,  io_buf2;
	private bigT temp_file_fd1, temp_file_fd2;
	private String[] bigtable1Names = new String[5];
	private String[] bigtable2Names = new String[5];
	private int size = 4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4+20+2;

	public BigSortMerge(String[] bnames1, String[] bnames2, Stream s1, Stream s2, String outBigTableName)
	throws JoinNewFailed ,
	   JoinLowMemory,
	   SortException,
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException{

		bigtable1Names = bnames1;
		bigtable2Names = bnames2;
		stream1 = s1;
		stream2 = s2;
		outputBT = new bigT(outBigTableName + "_1");

		get_from_s1 = true;
      	get_from_s2 = true;

		// open io_bufs
	    io_buf1 = new IoBuf();
	    io_buf2 = new IoBuf();

	    //value size = 20 (Same as that set in stream)
	    TempMap1 = new Map(size);  
	    TempMap2 = new Map(size);
	    map1 = new Map(size);
	    map2 = new Map(size);

	    if (io_buf1  == null || io_buf2  == null || TempMap1 == null || TempMap2==null
	  			|| map1 ==  null || map1 ==null)
			throw new JoinNewFailed ("SortMerge.java: allocate failed");
	

	  	process_next_block = true;
	  	done = false;
	  	// Two buffer pages to store equivalence classes
      	// NOTE -- THESE PAGES ARE NOT OBTAINED FROM THE BUFFER POOL
      	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      	_n_pages = 1;
      	_bufs1 = new byte [_n_pages][MINIBASE_PAGESIZE];
      	_bufs2 = new byte [_n_pages][MINIBASE_PAGESIZE];

      	temp_file_fd1 = null;
      	temp_file_fd2 = null;
      	try{
      		temp_file_fd1 = new bigT(null);
      		temp_file_fd2 = new bigT(null);
      	}
      	catch(Exception e){
      		throw new SortException (e, "Create heap file failed");
      	}

	}


	/**
   *  The map is returned
   * All this function has to do is to get 1 map from one of the Streams
   * (from both initially), use the sorting order to determine which one
   * gets sent up.)
   * Hmmm it seems that some thing more has to be done in order to account
   * for duplicates.... => I am following Raghu's 564 notes in order to
   * obtain an algorithm for this merging. Some funda about 
   *"equivalence classes"
   *@return the joined map is returned
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class
   *@exception InvalidMapSizeException invalid tuple size
   *@exception PageNotReadException exception from lower layer
   *@exception MapUtilsException exception from using tuple utilities
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception Exception other exceptions
   */
	public void performJoin() throws IOException,
	   IndexException,
	   InvalidMapSizeException,
	   PageNotReadException,
	   MapUtilsException,
	   SortException,
	   LowMemException,
	   JoinsException,
	   Exception{

	   	int comp_res;
      	Map _map1, _map2;
      	int map1_size, map2_size;

      	if (done) return;
      	//System.out.println("Starting sortmerge while loop");
      	while(true){
      		if(process_next_block){
      			//System.out.println("Entered next block");
      			process_next_block = false;
      			if(get_from_s1)
      				if((map1 = stream1.getNext()) == null){
      					done = true;
      					//System.out.println("stream1.getNext() returned null");
      					return;
      				}
      			if(get_from_s2)
      				if((map2 = stream2.getNext()) == null){
      					done = true;
      					//System.out.println("stream2.getNext() returned null");
      					return;
      				}
      			get_from_s1 = get_from_s2 = false;

      			//compare the maps on value (orderType = 8)
      			comp_res = MapUtils.CompareMapWithMap(map1, map2, 8);

				//Assuming ascending order
				//Iterate on outer 
      			while(comp_res < 0){
      				//map1 value < map2 value
      				//System.out.println("Compare result < 0");
      				if((map1 = stream1.getNext()) == null){
      					done = true;
      					return;
      				}
      				comp_res = MapUtils.CompareMapWithMap(map1, map2, 8);
      			}

      			comp_res = MapUtils.CompareMapWithMap(map1, map2, 8);
      			
      			//Iterate over inner
      			while(comp_res > 0){
      				//map1 value > map2 value
      				//System.out.println("Compare result > 0");
      				if((map2 = stream2.getNext()) == null){
      					done = true;
      					return;
      				}
      				comp_res = MapUtils.CompareMapWithMap(map1, map2, 8);
      			}

      			if(comp_res != 0){
      				process_next_block = true;
      				continue;
      			}

      			//com_res = 0 

      			TempMap1.mapCopy(map1);
      			TempMap2.mapCopy(map2);

      			io_buf1.init(_bufs1, 1, size, temp_file_fd1);
      			io_buf2.init(_bufs2, 1, size, temp_file_fd2);

      			while(MapUtils.CompareMapWithMap(map1, TempMap1, 8) == 0){
      				//Insert map1 into io_buf1
      				try {
		    			io_buf1.Put(map1);
		    			//System.out.print("Putting map in io_buf1 ");
		    			map1.print();
		  			}
		  			catch (Exception e){
		    			throw new JoinsException(e,"IoBuf error in sortmerge");
		  			}
		  			if((map1 = stream1.getNext()) == null){
		  				get_from_s1 = true;
		  				break;
		  			}
      			}

      			while(MapUtils.CompareMapWithMap(map2, TempMap2, 8) == 0){
      				//Insert map2 into io_buf2
      				try {
		    			io_buf2.Put(map2);
		    			//System.out.print("Putting map in io_buf2 ");
		    			map2.print();
		  			}
		  			catch (Exception e){
		    			throw new JoinsException(e,"IoBuf error in sortmerge");
		  			}
		  			if((map2 = stream2.getNext()) == null){
		  				get_from_s2 = true;
		  				break;
		  			}
      			}

      			// map1 and map2 contain the next map to be processed after this set.
	      		// Now perform a join of the maps in io_buf1 and io_buf2.
	      		// This is going to be a simple nested loops join with no frills. I guess,
	      		// it can be made more efficient.
			    // Another optimization that can be made is to choose the inner and outer
			    // by checking the number of tuples in each equivalence class.

			    if ((_map1=io_buf1.Get(TempMap1)) == null)                // Should not occur
					System.out.println( "Equiv. class 1 in sort-merge has no maps");
      		}

      		if ((_map2 = io_buf2.Get(TempMap2)) == null){
	      		if ((_map1 = io_buf1.Get(TempMap1)) == null){
		  			process_next_block = true;
		  			continue;                                // Process next equivalence class
				}
	      		else{
	      			io_buf2.i_buf.close();
		  			io_buf2.reread();
		  			_map2= io_buf2.Get(TempMap2);
				}
	    	}

	    	//Join the two maps
	    	if(MapUtils.CompareMapWithMap(TempMap1, TempMap2, 8) == 0){
	    		String row1 = TempMap1.getRowLabel();
	    		String row2 = TempMap2.getRowLabel();
	    		String newRow = row1 + ":" + row2;
	    		String newCol = TempMap1.getColumnLabel();
	    		String value = TempMap1.getValue();
	    		//int minTimeStamp = Math.min(TempMap1.getTimeStamp(), TempMap2.getTimeStamp());
	    		System.out.println("Joining two maps");
				
				//Create two temp bigStreams to get all the columns for the matched row(in joined map) in each bigT
	    		//Ordered on row labels
	    		BigStream tempbs1 = new BigStream(bigtable1Names, 9, 0, row1, "*", "*", false);
				BigStream tempbs2 = new BigStream(bigtable2Names, 9, 0, row2, "*", "*", false);

				//Arraylist to store all the versions of two matched maps (Max size = 6)
				ArrayList<Map> versions = new ArrayList<Map>();
				Map nextMap;
				//iterate over temp bigstream1 and get all the columns for matched row
				while(true){
					if((nextMap = tempbs1.getNext()) == null){
						//System.out.println("Getting all columns for matched row in tempbs1");
						break;
					}
					if(nextMap.getColumnLabel().equals(newCol)){
						versions.add(nextMap);
					}
					else{
						Map temp = new Map(nextMap);
						temp.setRowLabel(newRow);
						String tempCol = temp.getColumnLabel();
						temp.setColumnLabel("left_" + tempCol);
						System.out.print("Inserting ");
						temp.print();
						outputBT.insertMap(temp.getMapByteArray());
					}
				}

				//iterate over temp bigstream1 and get all the columns for matched row
				while(true){
					if((nextMap = tempbs2.getNext()) == null){
						break;
					}
					if(nextMap.getColumnLabel().equals(newCol)){
						//if(!(nextMap.getTimeStamp() == minTimeStamp))
							versions.add(nextMap);
					}
					else{
						Map temp = new Map(nextMap);
						temp.setRowLabel(newRow);
						String tempCol = temp.getColumnLabel();
						temp.setColumnLabel("right_" + tempCol);
						System.out.print("Inserting ");
						temp.print();
						outputBT.insertMap(temp.getMapByteArray());
					}
				}
				try{
					tempbs1.close();
					tempbs2.close();
				}
				catch(Exception e){
					throw new JoinsException(e, "SortMerge.java: error in closing temp bigstreams");
				}
				
				/*Till here,the matching maps have been output. Now we need to output
				the 3 most recent versions of the joined maps*/

				ArrayList<Integer> timestamps = new ArrayList<Integer>();
				for(int i = 0; i<versions.size(); i++)
					timestamps.add(versions.get(i).getTimeStamp());

				int counter = 0;
				while(!versions.isEmpty()){
					int maxIndex = timestamps.indexOf(Collections.max(timestamps));
					counter++;
					if(counter<=3){
						Map toinsert = versions.get(maxIndex);
						toinsert.setRowLabel(newRow);
						System.out.print("Inserting ");
						toinsert.print();
						outputBT.insertMap(toinsert.getMapByteArray());
						versions.remove(maxIndex);
						timestamps.remove(maxIndex);
					}
					else
						break;
				}
				
	    	}
      	}
	}
	public void close()
		throws JoinsException,
		IOException,
		IndexException{

		if(!closeFlag){
			try{
				stream1.close();
				stream2.close();
				//System.out.println("Closed two streams");
			}
			catch (Exception e) {
	  			throw new JoinsException(e, "SortMerge.java: error in closing streams.");
			}
			
			
			// try{
			// 	io_buf1.close();
			// 	io_buf2.close();
			// }
			// catch(Exception e){
			// 	throw new JoinsException(e, "SortMerge.java: error in closing IoBuf");
			// }

			if (temp_file_fd1 != null) {
				try {
			    	temp_file_fd1.deletebigT();
			  	}
			  	catch (Exception e) {
			    	throw new JoinsException(e, "SortMerge.java: delete file failed");
			  	}
	   			temp_file_fd1 = null; 
			}
			if (temp_file_fd2 != null) {
	  			try {
	    			temp_file_fd2.deletebigT();
	  			}
				catch (Exception e) {
					throw new JoinsException(e, "SortMerge.java: delete file failed");
				}
	  			temp_file_fd2 = null; 
			}
			closeFlag = true;
		}

	}

	/*	Empty get_next() method so that error is not thrown since 
		get_next() is abstract method of class Iterator
	*/
	public Map get_next()
		throws IOException,
		   JoinsException ,
		   IndexException,
		   InvalidTupleSizeException,
		   InvalidTypeException, 
		   PageNotReadException,
		   TupleUtilsException, 
		   PredEvalException,
		   SortException,
		   LowMemException,
		   UnknowAttrType,
		   UnknownKeyTypeException,
		   Exception{

		return null;
	}
}