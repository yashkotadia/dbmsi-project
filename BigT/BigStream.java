package BigT;

import global.*;
import iterator.*;

import java.io.*;
import java.lang.*;
import java.util.Arrays;


/** 
 * A BigStream object provides the similar getNext interface like Stream
 * However, it combines n sorted-streams of different BigTables
 * Mainly it is used for combining streams from 5 index types
 */
public class BigStream{

	bigT[]      bigTable;
    Stream[]    stream;
    Map[]       rMap;
    boolean[]   done;
    int 		orderType;
    public int   outInd = 0;
    int         nStreams;

    /** Opens the bigtables and their streams
    */

    public BigStream(String[] bigtableNames, int orderType, String rowFilter, String columnFilter, String valueFilter)
            throws HFException, InvalidTupleSizeException,
            InvalidMapSizeException, IOException,
            HFBufMgrException, HFDiskMgrException {

        this(bigtableNames, orderType, rowFilter, columnFilter, valueFilter, true);
    }


	public BigStream(String[] bigtableNames, int orderType, String rowFilter, String columnFilter, String valueFilter, boolean useSort)
		throws HFException, InvalidTupleSizeException,
				InvalidMapSizeException, IOException,
				HFBufMgrException, HFDiskMgrException{

		this.orderType = orderType;
        nStreams = bigtableNames.length;

        bigTable = new bigT[nStreams];
        stream = new Stream[nStreams];
        rMap = new Map[nStreams];
        done = new boolean[nStreams];

		// Initialize all the BigTables and Streams

        for(int i=0; i<nStreams; i++){
            bigTable[i] = new bigT(bigtableNames[i]);
            stream[i] = bigTable[i].openStream(orderType,rowFilter,columnFilter,valueFilter, useSort);
            rMap[i] = stream[i].getNext();
            if(rMap[i]==null) done[i] = true;
        }

	}

    /**
    * Uses the K sorted arrays algorithm for creating a sorted stream from 5 sorted streams
    */
    public Map getNext()
            throws InvalidMapSizeException, IOException, MapUtilsException {

        MID mid = new MID();
        return this.getNext(mid);
    }


	public Map getNext(MID mid)
		throws InvalidMapSizeException, IOException, MapUtilsException
	{

		// Check Breaking condition
        if(areAllTrue(done)) return null;

		// Initialize the first null Map as minimum for further comparison
        int minInd = 0; 
        for(int i=0; i<nStreams; i++){
            if(rMap[i]!=null){
                minInd = i;
                break;
            }
        }

        // Find minimum out of all streams
        for(int i=0; i<nStreams; i++){
            if( rMap[i]!= null && MapUtils.CompareMapWithMap(rMap[minInd], rMap[i], orderType)==1 ){
                minInd = i;
            }
        }

        // Store the Index to which the minimum valued map belongs
        outInd = minInd;

        // Create a new Map from the minimum valued map
        Map out = new Map(rMap[minInd]);

        mid.pageNo.pid = stream[minInd].scan.outmid.pageNo.pid;
        mid.slotNo = stream[minInd].scan.outmid.slotNo;

        // Call stream.getNext() on the stream that we used
        rMap[minInd] = stream[minInd].getNext();

        if(rMap[minInd]==null) done[minInd] = true;

        return out;

	}

    /** Closes all the streams
    */
	public void close(){
		for(int i=0; i<nStreams; i++) stream[i].close();
	}
    
    /** Checks if all elements of a boolean array are true
    */
	private boolean areAllTrue(boolean[] array)
    {
        for(boolean b : array) if(!b) return false;
        return true;
    }
}