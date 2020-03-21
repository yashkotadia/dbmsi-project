package index;
import global.*;
import bufmgr.*;
import diskmgr.*; 
import btree.*;
import iterator.*; 
import java.io.*;
import BigT.*;


/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class IndexScan extends Iterator {

  /**
   * class constructor. set up the index scan.
   * @param index type of the index (B_Index, Hash)
   * @param relName name of the input relation
   * @param indName name of the input index
   * @param types array of types in this relation
   * @param str_sizes array of string sizes (for attributes that are string)
   * @param noInFlds number of fields in input tuple
   * @param noOutFlds number of fields in output tuple
   * @param outFlds fields to project
   * @param selects conditions to apply, first one is primary
   * @param fldNum field number of the indexed field
   * @param indexOnly whether the answer requires only the key or the tuple
   * @exception IndexException error from the lower layer
   * @exception InvalidTypeException tuple type not valid
   * @exception InvalidTupleSizeException tuple size not valid
   * @exception UnknownIndexTypeException index type unknown
   * @exception IOException from the lower layer
   */
  public IndexScan(
	    IndexType     index,        
	    final String  relName,  
	    final String  indName,  
	    String rFilter, 
      String cFilter,
      String vFilter,
      CondExpr selects[]
	   ) 
    throws IndexException, 
	   InvalidTypeException,
	   InvalidTupleSizeException,
	   UnknownIndexTypeException,
	   IOException
  {
    rowFilter = rFilter;
    columnFilter = cFilter;
    valueFilter = vFilter;
    map1 = new Map();
    
    m1_size = map1.size();
    
    try {
      bgt = new bigT(relName);
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Big Table not created");
    }
    
    switch(index.indexType) {
      // linear hashing is not yet implemented
    case IndexType.B_Index:
      // error check the select condition
      // must be of the type: value op symbol || symbol op value
      // but not symbol op symbol || value op value
      try {
	indFile = new BTreeFile(indName); 
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
      }
      
      try {
	indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
      }
      
      break;
    case IndexType.None:
    default:
      throw new UnknownIndexTypeException("Only BTree index is supported so far");
      
    }
    
  }
  
  /**
   * returns the next tuple.
   * if <code>index_only</code>, only returns the key value 
   * (as the first field in a tuple)
   * otherwise, retrive the tuple and returns the whole tuple
   * @return the tuple
   * @exception IndexException error from the lower layer
   * @exception UnknownKeyTypeException key type unknown
   * @exception IOException from the lower layer
   */
  public Map get_next() 
    throws IndexException, 
	   UnknownKeyTypeException,
	   IOException
  {
    MID mid;
    int unused;
    KeyDataEntry nextentry = null;

    try {
      nextentry = indScan.get_next();
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: BTree error");
    }	  
    
    while(nextentry != null) {
            
      // not index_only, need to return the whole tuple
      mid = new MID(((LeafData)nextentry.data).getData());
      outmid = new MID(mid.pageNo, mid.slotNo);

      try {
	map1 = bgt.getMap(mid);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: getRecord failed");
      }

      //System.out.println(map1==null);
      
      try{
        if(checkFilters(map1.getRowLabel(), rowFilter) && checkFilters(map1.getColumnLabel(), columnFilter) && checkFilters(map1.getValue(), valueFilter)){
          //System.out.println("Here");
          return map1;
        }
      } catch(NullPointerException e){
        System.out.println(map1==null);
      }     

      try {
	nextentry = indScan.get_next();
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTree error");
      }	  
    }
    
    return null; 
  }
  
  /**
   * Cleaning up the index scan, does not remove either the original
   * relation or the index from the database.
   * @exception IndexException error from the lower layer
   * @exception IOException from the lower layer
   */
  public void close() throws IOException, IndexException
  {
    if (!closeFlag) {
      if (indScan instanceof BTFileScan) {
	try {
	  ((BTFileScan)indScan).DestroyBTreeFileScan();
    ((BTreeFile)indFile).close();
	}
	catch(Exception e) {
	  throw new IndexException(e, "BTree error in destroying index scan.");
	}
      }
      
      closeFlag = true; 
    }
  }

  // For range filter, we assume that there is no space, eg. [A,Y]
  private boolean checkFilters(String mapLabel, String filter){
      if(filter.equals("") || filter.equals("*") || (mapLabel.compareTo(filter)==0) )
        return true;
      else if(filter.charAt(0) == '['){
        //System.out.println(filter);
        String startRange = filter.substring(1, filter.indexOf(','));
        String endRange = filter.substring(filter.indexOf(',')+1, filter.indexOf(']'));
        if(mapLabel.compareToIgnoreCase(startRange) >= 0 && mapLabel.compareToIgnoreCase(endRange) <= 0){
          return true;
        }
      }
      return false; 
  }
  
  private IndexFile     indFile;
  private IndexFileScan indScan;
  private bigT          bgt;
  private Map           map1;
  private int           m1_size;
  private CondExpr[]    _selects;
  private String        rowFilter;
  private String        columnFilter;
  private String        valueFilter; 
}

