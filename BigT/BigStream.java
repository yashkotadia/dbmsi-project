package BigT;

import global.*;
import iterator.*;

import java.io.*;
import java.lang.*;
import java.util.Arrays;


/** 
 * A BigStream object provides the similar getNext interface like Stream
 * However, it combines 5 sorted-streams of different BigTable Maps stored in different index types
 */
public class BigStream{

	bigT[]      bigTable = new bigT[5];
    Stream[]    stream = new Stream[5];
    Map[]       rMap = new Map[5];
    boolean[]   done = new boolean[5];
    int 		orderType;

    /** Opens the bigtables and their streams
    */
	public BigStream(String bigtableName, int orderType, String rowFilter, String columnFilter, String valueFilter)
		throws HFException, InvalidTupleSizeException,
				InvalidMapSizeException, IOException,
				HFBufMgrException, HFDiskMgrException{

		this.orderType = orderType;

		// Initialize all the BigTables and Streams
        for(int i=0; i<5; i++){
            bigTable[i] = new bigT(bigtableName+"_"+(i+1));
            stream[i] = bigTable[i].openStream(orderType,rowFilter,columnFilter,valueFilter);
            rMap[i] = stream[i].getNext();
            if(rMap[i]==null) done[i] = true;
        }

	}

    /**
    * Uses the K sorted arrays algorithm for creating a sorted stream from 5 sorted streams
    */
	public Map getNext() 
		throws InvalidMapSizeException, IOException, MapUtilsException
	{

		// Check Breaking condition
        if(areAllTrue(done)) return null;

		// Initialize the first null Map as minimum for further comparison
        int minInd = 0; 
        for(int i=0; i<5; i++){
            if(rMap[i]!=null){
                minInd = i;
                break;
            }
        }

        // Find minimum out of all streams
        for(int i=0; i<5; i++){
            if( rMap[i]!= null && MapUtils.CompareMapWithMap(rMap[minInd], rMap[i], orderType)==1 ){
                minInd = i;
            }
        }

        // Print the minimum valued Map
        Map out = new Map(rMap[minInd]);

        // Call stream.getNext() on the stream that we used
        rMap[minInd] = stream[minInd].getNext();
        if(rMap[minInd]==null) done[minInd] = true;

        return out;

	}

    /** Closes all the 5 streams
    */
	public void close(){
		for(int i=0; i<5; i++) stream[i].close();
	}
    
    /** Checks if all elements of a boolean array are true
    */
	private boolean areAllTrue(boolean[] array)
    {
        for(boolean b : array) if(!b) return false;
        return true;
    }
}