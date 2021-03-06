 package iterator;

import java.io.*; 
import global.*;
import bufmgr.*;
import diskmgr.*;
//import heap.*;
import index.*;
import chainexception.*;
import BigT.*;
/**
 * The Sort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatedly call <code>get_next()</code> to get maps in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class Sort extends Iterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 100;
  
  /*private AttrType[]  _in;         
  private short       n_cols;
  private short[]     str_lens;
  private Iterator    _am;
  private int         _sort_fld;
  private TupleOrder  order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         Nruns;
  private int         max_elems_in_heap;
  private int         sortFldLen;
  private int         tuple_size;
  */
  private RowOrder  order;
  private Iterator    _am;
  private int         _n_pages;
  private byte[][]    bufs;
  private int         Nruns;
  private int         max_elems_in_heap;
  private int         map_size;
  private int         orderType;
  
  private pnodeSplayPQ Q;
  private bigT[]   temp_files; 
  private int          n_tempfiles;
  private Map        output_map;  
  private int[]        n_maps;
  private int          n_runs;
  private Map        op_buf;
  private OBuf         o_buf;
  private SpoofIbuf[]  i_buf;
  private PageId[]     bufs_pids;
  private boolean useBM = true; // flag for whether to use buffer manager
  
  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get 
   * the minimum of all runs.
   * @param map_size size (in bytes) of each map
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to 
   *                 sort in two passes (a subclass of SortException).
   * @exception SortException something went wrong in the lower layer. 
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int map_size, int n_R_runs)
    throws IOException, 
	   LowMemException, 
	   SortException,
	   Exception
  {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages) 
      throw new LowMemException("Sort.java: Not enough memory to sort in two passes."); 

    int i;
    pnode cur_node;  // need pq_defs.java
    
    i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
    for (int j=0; j<n_R_runs; j++) i_buf[j] = new SpoofIbuf();
    
    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98
    
    for (i=0; i<n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      i_buf[i].init(temp_files[i], apage, 1, map_size, n_maps[i]);

      cur_node = new pnode();
      cur_node.run_num = i;
      
      // may need change depending on whether Get() returns the original
      // or makes a copy of the map, need io_bufs.java ???
      Map temp_map = new Map(map_size);

      /*try {
	temp_tuple.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
	throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
      }*/
      
      temp_map =i_buf[i].Get(temp_map);  // need io_bufs.java
            
      if (temp_map != null) {
	/*
	System.out.print("Get tuple from run " + i);
	temp_tuple.print(_in);
	*/
	cur_node.map = temp_map; // no copy needed
	try {
	  Q.enq(cur_node);
	}
	catch (UnknowAttrType e) {
	  throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	}
	catch (MapUtilsException e) {
	  throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
	}

      }
    }
    return; 
  }
  
  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   * @exception TupleUtilsException error while comparing tuples
   * @exception JoinsException from <code>Iterator.get_next()</code>
   * @exception Exception other exceptions.
   */
  private int generate_runs(int max_elems/*AttrType sortFldType, int sortFldLen*/) 
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   TupleUtilsException,
	   JoinsException,
	   Exception
  {
    Map map; 
    pnode cur_node;
    pnodeSplayPQ Q1 = new pnodeSplayPQ(order, orderType);
    pnodeSplayPQ Q2 = new pnodeSplayPQ(order, orderType);
    pnodeSplayPQ pcurr_Q = Q1;
    pnodeSplayPQ pother_Q = Q2; 
    Map lastElem = new Map(map_size);  // need tuple.java
    /*try {
      lastElem.setHdr(n_cols, _in, str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: setHdr() failed");
    }*/
    
    int run_num = 0;  // keeps track of the number of runs

    // number of elements in Q
    //    int nelems_Q1 = 0;
    //    int nelems_Q2 = 0;
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    
    // set the lastElem to be the minimum value for the sort field
    if(order.rowOrder == RowOrder.Ascending) {
      try {
	MIN_VAL(lastElem);
      } catch (UnknowAttrType e) {
	throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
      } catch (Exception e) {
	throw new SortException(e, "MIN_VAL failed");
      } 
    }
    else {
      try {
	MAX_VAL(lastElem);
      } catch (UnknowAttrType e) {
	throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
      } catch (Exception e) {
	throw new SortException(e, "MIN_VAL failed");
      } 
    }
    
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
	map = _am.get_next();  // according to Iterator.java
      } catch (Exception e) {
	e.printStackTrace(); 
	throw new SortException(e, "Sort.java: get_next() failed");
      } 
      
      if (map == null) {
	break;
      }
      cur_node = new pnode();
      cur_node.map = new Map(map); 

      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;
    }
    
    // now the queue is full, starting writing to file while keep trying
    // to add new map to the queue. The ones that do not fit are put on
    // the other queue temporarily
    while (true) {
      cur_node = pcurr_Q.deq();
      if (cur_node == null) break; 
      p_elems_curr_Q --;
      
      comp_res = MapUtils.CompareMapWithMap(cur_node.map, lastElem, orderType);  // need map_utils.java
      
      if ((comp_res < 0 && order.rowOrder == RowOrder.Ascending) || (comp_res > 0 && order.rowOrder == RowOrder.Descending)) {
	// doesn't fit in current run, put into the other queue
	try {
	  pother_Q.enq(cur_node);
	}
	catch (UnknowAttrType e) {
	  throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	}
	p_elems_other_Q ++;
      }
      else {
	// set lastElem to have the value of the current tuple,
	// need tuple_utils.java
	//TupleUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, sortFldType);

    // set lastElem to have the row label, column label and
    // time stamp of the current map
  lastElem.setRowLabel(cur_node.map.getRowLabel());
  lastElem.setColumnLabel(cur_node.map.getColumnLabel());
  lastElem.setTimeStamp(cur_node.map.getTimeStamp());

	// write tuple to output file, need io_bufs.java, type cast???
	//	System.out.println("Putting tuple into run " + (run_num + 1)); 
	//	cur_node.tuple.print(_in);
	
    // write map to output buffer
	o_buf.Put(cur_node.map);
      }
      
      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
	// close current run and start next run
	n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
	run_num ++;

	// check to see whether need to expand the array
	if (run_num == n_tempfiles) {
	  bigT[] temp1 = new bigT[2*n_tempfiles];
	  for (int i=0; i<n_tempfiles; i++) {
	    temp1[i] = temp_files[i];
	  }
	  temp_files = temp1; 
	  n_tempfiles *= 2; 

	  int[] temp2 = new int[2*n_runs];
	  for(int j=0; j<n_runs; j++) {
	    temp2[j] = n_maps[j];
	  }
	  n_maps = temp2;
	  n_runs *=2; 
	}
	
	try {
	    temp_files[run_num] = new bigT(null);
	}
	catch (Exception e) {
	  throw new SortException(e, "Sort.java: create Heapfile failed");
	}
	
	// need io_bufs.java
	o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);
	
	if(order.rowOrder == RowOrder.Ascending) {
		// set the last Elem to be the minimum value for the sort field
	  try {
	    MIN_VAL(lastElem);
	  } catch (UnknowAttrType e) {
	    throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
	  } catch (Exception e) {
	    throw new SortException(e, "MIN_VAL failed");
	  } 
	}
	else {
		// set the last Elem to be the maximum value for the sort field
	  try {
	    MAX_VAL(lastElem);
	  } catch (UnknowAttrType e) {
	    throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
	  } catch (Exception e) {
	    throw new SortException(e, "MIN_VAL failed");
	  } 
	}
    
	// switch the current heap and the other heap
	pnodeSplayPQ tempQ = pcurr_Q;
	pcurr_Q = pother_Q;
	pother_Q = tempQ;
	int tempelems = p_elems_curr_Q;
	p_elems_curr_Q = p_elems_other_Q;
	p_elems_other_Q = tempelems;
      }
      
      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
	while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
	  try {
	    map = _am.get_next();  // according to Iterator.java
	  } catch (Exception e) {
	    throw new SortException(e, "get_next() failed");
	  } 
	  
	  if (map == null) {
	    break;
	  }
	  cur_node = new pnode();
	  cur_node.map = new Map(map); // map copy needed 

	  try {
	    pcurr_Q.enq(cur_node);
	  }
	  catch (UnknowAttrType e) {
	    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	  }
	  p_elems_curr_Q ++;
	}
      }
      
      // Check if we are done
      if (p_elems_curr_Q == 0) {
	// current queue empty despite our attempts to fill in
	// indicating no more maps from input
	if (p_elems_other_Q == 0) {
	  // other queue is also empty, no more maps to write out, done
	  break; // of the while(true) loop
	}
	else {
	  // generate one more run for all maps in the other queue
	  // close current run and start next run
	  n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
	  run_num ++;
	  
	  // check to see whether need to expand the array
	  if (run_num == n_tempfiles) {
	    bigT[] temp1 = new bigT[2*n_tempfiles];
	    for (int i=0; i<n_tempfiles; i++) {
	      temp1[i] = temp_files[i];
	    }
	    temp_files = temp1; 
	    n_tempfiles *= 2; 
	    
	    int[] temp2 = new int[2*n_runs];
	    for(int j=0; j<n_runs; j++) {
	      temp2[j] = n_maps[j];
	    }
	    n_maps = temp2;
	    n_runs *=2; 
	  }

	  try {
	    temp_files[run_num] = new bigT(null); 
	  }
	  catch (Exception e) {
	    throw new SortException(e, "Sort.java: create Heapfile failed");
	  }
	  
	  // need io_bufs.java
	  o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);
	  
	  // set the last Elem to be the minimum value for the sort field
	  if(order.rowOrder == RowOrder.Ascending) {
	    try {
	      MIN_VAL(lastElem);
	    } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
	    } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
	    } 
	  }
	  else {
	    try {
	      MAX_VAL(lastElem);
	    } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
	    } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
	    } 
	  }
	
	  // switch the current heap and the other heap
	  pnodeSplayPQ tempQ = pcurr_Q;
	  pcurr_Q = pother_Q;
	  pother_Q = tempQ;
	  int tempelems = p_elems_curr_Q;
	  p_elems_curr_Q = p_elems_other_Q;
	  p_elems_other_Q = tempelems;
	}
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_maps[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }
  
  /**
   * Remove the minimum value among all the runs.
   * @return the minimum map removed
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception Exception other exceptions.
   */
  private Map delete_min() 
    throws IOException, 
	   SortException,
	   Exception
  {
    pnode cur_node;                // needs pq_defs.java  
    Map new_map, old_map;  

    cur_node = Q.deq();
    old_map = cur_node.map;
    /*
    System.out.print("Get ");
    old_tuple.print(_in);
    */
    // we just removed one map from one run, now we need to put another
    // map of the same run into the queue
    if (i_buf[cur_node.run_num].empty() != true) { 
      // run not exhausted 
      new_map = new Map(map_size); // need Map.java

      /*try {
	new_tuple.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
	throw new SortException(e, "Sort.java: setHdr() failed");
      }*/
      
      new_map = i_buf[cur_node.run_num].Get(new_map);  
      if (new_map != null) {
	/*
	System.out.print(" fill in from run " + cur_node.run_num);
	new_tuple.print(_in);
	*/
	cur_node.map = new_map;  // no copy needed -- I think Bingjie 4/22/98
	try {
	  Q.enq(cur_node);
	} catch (UnknowAttrType e) {
	  throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	} catch (MapUtilsException e) {
	  throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
	} 
      }
      else {
	throw new SortException("********** Wait a minute, I thought input is not empty ***************");
      }
      
    }

    // changed to return Tuple instead of return char array ????
    
    // return map
    return old_map; 
  }
  
  /**
   * Set lastElem to be the minimum value of the appropriate type
   * @param lastElem the map
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MIN_VAL(Map lastElem) 
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MIN_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    /*
    switch (sortFldType.attrType) {
    case AttrType.attrInteger: 
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
      break;
    case AttrType.attrReal:
      //      lastElem.setHdr(fld-no, junk, null);
      lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
      break;
    case AttrType.attrString:
      //      lastElem.setHdr(fld_no, junk, s_size);
      lastElem.setStrFld(_sort_fld, s);
      break;
    default:
      // don't know how to handle attrSymbol, attrNull
      //System.err.println("error in sort.java");
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    */
    lastElem.setRowLabel(s);
    lastElem.setColumnLabel(s);
    lastElem.setTimeStamp(Integer.MIN_VALUE);

    return;
  }

  /**
   * Set lastElem to be the maximum value of the appropriate type
   * @param lastElem the map
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MAX_VAL(Map lastElem) 
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MAX_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    /*
    switch (sortFldType.attrType) {
    case AttrType.attrInteger: 
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
      break;
    case AttrType.attrReal:
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
      break;
    case AttrType.attrString:
      //      lastElem.setHdr(fld_no, junk, s_size);
      lastElem.setStrFld(_sort_fld, s);
      break;
    default:
      // don't know how to handle attrSymbol, attrNull
      //System.err.println("error in sort.java");
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    */
    lastElem.setRowLabel(s);
    lastElem.setColumnLabel(s);
    lastElem.setTimeStamp(Integer.MAX_VALUE);
    return;
  }
  
  /** 
   * Class constructor, take information about the maps, and set up 
   * the sorting
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param am an iterator for accessing the maps
   * @param n_pages amount of memory (in pages) available for sorting
   * @param oType the order type
   * @param valueSize arbitrary size of the map (set to 80)
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public Sort(/*AttrType[] in,         
	      short      len_in,             
	      short[]    str_sizes,
	      Iterator   am,                 
	      int        sort_fld,          
	      TupleOrder sort_order,     
	      int        sort_fld_len,*/
        RowOrder sort_order,
        Iterator am,
	      int n_pages,
        int oType,
        int valueSize 

	      ) throws IOException, SortException,
          SortException, UnknowAttrType, LowMemException, 
          JoinsException, Exception
  {
    /*_in = new AttrType[len_in];
    n_cols = len_in;
    int n_strs = 0;

    for (int i=0; i<len_in; i++) {
      _in[i] = new AttrType(in[i].attrType);
      if (in[i].attrType == AttrType.attrString) {
	n_strs ++;
      } 
    }
    
    str_lens = new short[n_strs];
    
    n_strs = 0;
    for (int i=0; i<len_in; i++) {
      if (_in[i].attrType == AttrType.attrString) {
	str_lens[n_strs] = str_sizes[n_strs];
	n_strs ++;
      }
    }
    */
    orderType = oType;
    
    //Tuple t = new Tuple(); // need Tuple.java
    Map m = new Map();
    /*try {
      t.setHdr(len_in, _in, str_sizes);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: t.setHdr() failed");
    }
    tuple_size = t.size();
    */
    

    map_size = valueSize;

    _am = am;
    //_sort_fld = sort_fld;
    order = sort_order;
    _n_pages = n_pages;
    
    // this may need change, bufs ???  need io_bufs.java
    //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
	get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
	throw new SortException(e, "Sort.java: BUFmgr error");
      }
    }
    else {
      for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
    }
        
    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new bigT[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_maps = new int[ARBIT_RUNS]; 
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new bigT(null);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: Heapfile error");
    }
    
    o_buf = new OBuf();
    
    o_buf.init(bufs, _n_pages, map_size, temp_files[0], false);
    //    output_tuple = null;
    
    max_elems_in_heap = 500;
    //sortFldLen = sort_fld_len;
    
    Q = new pnodeSplayPQ(order, oType);

    op_buf = new Map(map_size);   // need Tuple.java
    /*try {
      op_buf.setHdr(n_cols, _in, str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
    }*/
    
    // generate runs
    Nruns = generate_runs(max_elems_in_heap);
    //      System.out.println("Generated " + Nruns + " runs");
    
    // setup state to perform merge of runs. 
    // Open input buffers for all the input file
    setup_for_merge(map_size, Nruns);
    try {
        _am.close();
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: error in closing iterator.");
    }
  }
  
  /**
   * Returns the next map in sorted order.
   * Note: You need to copy out the content of the map, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next map, null if all maps exhausted
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception UnknowAttrType attribute type unknown
   * @exception LowMemException memory low exception
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception Exception other exceptions
   */
  public Map get_next() 
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   LowMemException, 
	   JoinsException,
	   Exception
  {

    if (Q.empty()) {  
      // no more maps available
      return null;
    }
    
    output_map = delete_min();
    if (output_map != null){
      op_buf.mapCopy(output_map);
      return op_buf; 
    }
    else 
      return null; 
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public void close() throws SortException, IOException
  {
    // clean up
    if (!closeFlag) {

      if (useBM) {
	try {
	  free_buffer_pages(_n_pages, bufs_pids);
	} 
	catch (Exception e) {
	  throw new SortException(e, "Sort.java: BUFmgr error");
	}
	for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
      }

    for(SpoofIbuf spoofIbuf : i_buf){
      spoofIbuf.close();
      //System.out.println("Closed I Buf");
    }
      
      for (int i = 0; i<temp_files.length; i++) {
	if (temp_files[i] != null) {
	  try {
	    temp_files[i].deletebigT();
	  }
	  catch (Exception e) {
	    throw new SortException(e, "Sort.java: Heapfile error");
	  }
	  temp_files[i] = null; 
	}
      }
      closeFlag = true;
    } 
  } 

}



