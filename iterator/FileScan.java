package iterator;
   

//import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import BigT.*;

import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class FileScan extends  Iterator
{
  /*
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private Heapfile f;
  private Scan scan;
  private Tuple     tuple1;
  private Tuple    Jtuple;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;
  */
  private bigT bgt;
  private Scan scan;
  private String rowFilter;
  private String columnFilter;
  private String valueFilter;
  private Map map1;
  private int m1_size; 

  /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are. 
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions
   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception MapUtilsException exception from this class
   */
  public  FileScan (String  file_name,
        String rFilter, 
        String cFilter,
        String vFilter       		    
		    )
    throws IOException,
	   FileScanException,
	   MapUtilsException 
    {
      /*
      _in1 = in1; 
      in1_len = len_in1;
      s_sizes = s1_sizes;
      
      Jtuple =  new Tuple();
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    ts_size;
      ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);
      
      OutputFilter = outFilter;
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
      */
      rowFilter = rFilter;
      columnFilter = cFilter;
      valueFilter = vFilter;
      map1 =  new Map();

      /*
      try {
	tuple1.setHdr(in1_len, _in1, s1_sizes);
      }catch (Exception e){
	throw new FileScanException(e, "setHdr() failed");
      }
      */
      m1_size = map1.size();
      
      try {
	bgt = new bigT(file_name);
	
      }
      catch(Exception e) {
	throw new FileScanException(e, "Create new heapfile failed");
      }
      
      try {
	scan = bgt.openScan();
      }
      catch(Exception e){
	throw new FileScanException(e, "openScan() failed");
      }
    }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  /*
  public FldSpec[] show()
    {
      return perm_mat;
    }
    */
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public Map get_next()
    throws 
	   IOException,
	   InvalidMapSizeException,
     InvalidTupleSizeException,
	   PageNotReadException, 
	   WrongPermat
    {     
      MID mid = new MID();
      
      while(true) {
      	if((map1 =  scan.getNext(mid)) == null)
      	  return null;

        outmid = new MID(scan.outmid.pageNo, scan.outmid.slotNo);
	    
	
  /*
	tuple1.setHdr(in1_len, _in1, s_sizes);
	if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
	  Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
	  return  Jtuple;
	}
  */ 

   /* if(rowFilter == "" || rowFilter == "*" || map1.getRowLabel().equals(rowFilter)){
      if(columnFilter == "" || columnFilter == "*"|| map1.getColumnLabel().equals(columnFilter)){
        if(valueFilter == "" || valueFilter == "*" || map1.getValue().equals(valueFilter)){
          return map1;
        }
      }
    }*/  
    //System.out.println("Here");
    //System.out.print(map1.getRowLabel() + " ");
    //System.out.print(map1.getColumnLabel()+ " ");
    //System.out.print(map1.getValue());
    
    //System.out.print(map1.getRowLabel().length()+ " " + rowFilter);
    //System.out.println();
        if(checkFilters(map1.getRowLabel(), rowFilter) && checkFilters(map1.getColumnLabel(), columnFilter) && checkFilters(map1.getValue(), valueFilter)){
              //System.out.println("Here");
              return map1;
        }
      }     
    
  }


  // For range filter, we assume that there is no space, eg. [A,Y]
  public boolean checkFilters(String mapLabel, String filter){
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

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */

  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}


