package BigT;

/** JAVA */
/**
 * Stream.java-  class Stream
 *
 */

import java.io.*;
import global.*;
import index.IndexScan;
import iterator.*;


/**	
 * A Stream object is created ONLY through the function openStream
 * of a bigtable. It supports the getNext interface which will
 * simply retrieve the next map in the bigtable in the specified order.

 */
public class Stream implements GlobalConst{

    private static int SORTPGNUM = 100;
    private Sort sort;
    public Iterator scan=null;
    private int type;
    private String bt_name;

    public boolean UseSort = true;

    /**
     *
     *        *  orderType is
     *        * · 1, then results are ﬁrst ordered in row label, then column label, then time stamp
     *        * · 2, then results are ﬁrst ordered in column label, then row label, then time stamp
     *        * · 3, then results are ﬁrst ordered in row label, then time stamp
     *        * · 4, then results are ﬁrst ordered in column label, then time stamp
     *        * · 5, then results are ordered in time stamp
     *
     *  The constructor
     * @exception InvalidMapSizeException Invalid map size
     * @exception IOException I/O errors
     *
     * @param bt A BigT object
     */

    public Stream(bigT bt, int orderType, String rowFilter, String columnFilter, String valueFilter)
            throws InvalidMapSizeException,
            IOException
    {
        this(bt, orderType, rowFilter, columnFilter, valueFilter, true);
    }


  public Stream(bigT bt, int orderType, String rowFilter, String columnFilter, String valueFilter, boolean useSort)
          throws InvalidMapSizeException,
          IOException
  {

      UseSort = useSort;
      int mapSize = 4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4+20+2; //Taking value length to be 20 arbitrarily
      TupleOrder[] order = new TupleOrder[2];
      order[0] = new TupleOrder(TupleOrder.Ascending);
      order[1] = new TupleOrder(TupleOrder.Descending);
      type = bt._itype;
      bt_name = bt.get_fileName();

      boolean rowAll = rowFilter.equals("*");
      boolean columnAll = columnFilter.equals("*");
      boolean valueAll = valueFilter.equals("*");

      CondExpr[] expr;
      try{
        if(type==2 && !rowAll){

        expr = getCondExpr(rowFilter);
        scan = new IndexScan ( new IndexType(IndexType.B_Index), bt_name, bt_name+"_index", rowFilter, columnFilter, valueFilter, expr);

      } else if(type==3 && !columnAll){

        expr = getCondExpr(columnFilter);
        scan = new IndexScan ( new IndexType(IndexType.B_Index), bt_name, bt_name+"_index", rowFilter, columnFilter, valueFilter, expr);

      } else if(type==4 && !columnAll){

        if(columnFilter.charAt(0)=='[' && !rowAll){
          expr = getCondExpr(columnFilter, "*");
          scan = new IndexScan ( new IndexType(IndexType.B_Index), bt_name, bt_name+"_index", rowFilter, columnFilter, valueFilter, expr);
        } else {
          expr = getCondExpr(columnFilter, rowFilter);
          scan = new IndexScan ( new IndexType(IndexType.B_Index), bt_name, bt_name+"_index", rowFilter, columnFilter, valueFilter, expr);
        }

      } else if(type==5 && !rowAll){

        if(rowFilter.charAt(0)=='[' && !valueAll){
          expr = getCondExpr(rowFilter, "*");
          scan = new IndexScan ( new IndexType(IndexType.B_Index), bt_name, bt_name+"_index", rowFilter, columnFilter, valueFilter, expr);
        } else {
          expr = getCondExpr(rowFilter, valueFilter);
          scan = new IndexScan ( new IndexType(IndexType.B_Index), bt_name, bt_name+"_index", rowFilter, columnFilter, valueFilter, expr);
        }

      } else {

        scan = new FileScan(bt.get_fileName(), rowFilter, columnFilter, valueFilter);

      }

        if (useSort)
            sort = new Sort(order[0], scan, SORTPGNUM, orderType, mapSize);

    } catch(Exception e){
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
      
  }

  /**
   * Creates the condition expression required for index scan
   * Generates appropriate expression based on equality search or range search
   * @param filter based on which the condition expression is to be created
   */
  private CondExpr[] getCondExpr(String filter) {
    CondExpr[] expr = new CondExpr[2];

    if (filter.charAt(0) == '[')
    {
      // RangeSearch
      String[] fAttributes = filter.split(",");
      expr = new CondExpr[3];
      expr[0] = new CondExpr();
      expr[0].op = new AttrOperator(AttrOperator.aopGE);
      expr[0].type1 = new AttrType(AttrType.attrSymbol);
      expr[0].type2 = new AttrType(AttrType.attrString);
      expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
      expr[0].operand2.string = fAttributes[0].substring(1);
      expr[0].next = null;
      expr[1] = new CondExpr();
      expr[1].op = new AttrOperator(AttrOperator.aopLE);
      expr[1].type1 = new AttrType(AttrType.attrSymbol);
      expr[1].type2 = new AttrType(AttrType.attrString);
      expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
      expr[1].operand2.string = fAttributes[1].substring(0,fAttributes[1].length() - 1);
      expr[1].next = null;
      expr[2] = null;
    }
    else //Equality Search
    {
      expr = new CondExpr[2];
      expr[0] = new CondExpr();
      expr[0].op = new AttrOperator(AttrOperator.aopEQ);
      expr[0].type1 = new AttrType(AttrType.attrSymbol);
      expr[0].type2 = new AttrType(AttrType.attrString);
      expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
      expr[0].operand2.string = filter;
      expr[0].next = null;
      expr[1] = null;
    }
    return expr;
  }

  /**
   * Creates the condition expression required for index scan
   * @param filter1 based on which the condition expression is to be created (range/equality)
   * @param filter2 based on which the condition expression is to be created (range/equality/*)
   */
  private CondExpr[] getCondExpr(String filter1, String filter2) {
    CondExpr[] expr = new CondExpr[3];
    String[] ge = new String[2];
    String[] le = new String[2];

    // If filter 1 is range and filter 2 is *
    if(filter1.charAt(0) == '['){

      if(filter2.equals("*")){
      String[] fAttributes = filter1.split(",");
      
      char[] c = new char[1];
      c[0] = Character.MIN_VALUE;
      ge[0] = fAttributes[0].substring(1); 
      ge[1] = new String(c);
      
      
      char[] d = new char[1];
      d[0] = Character.MAX_VALUE;
      le[0] = fAttributes[1].substring(0,fAttributes[1].length() - 1); 
      le[1] = new String(d);
      }
      
    } 
    // If filter 1 is equality then multiple cases are possible
    else{

      // Filter 2 is *
      if(filter2.equals("*")){
        char[] c = new char[1];
        c[0] = Character.MIN_VALUE;
        ge[0] = filter1;
        ge[1] = new String(c);
        
        char[] d = new char[1];
        d[0] = Character.MAX_VALUE;
        le[0] = filter1; 
        le[1] = new String(d);
      }

      // filter 2 is range
      else if(filter2.charAt(0) == '['){
        String[] fAttributes = filter2.split(",");

        ge[0] = filter1;
        ge[1] = fAttributes[0].substring(1);
        
        le[0] = filter1; 
        le[1] = fAttributes[1].substring(0,fAttributes[1].length() - 1); 
      }

      //filter 2 is equality
      else{
        ge[0] = filter1;
        ge[1] = filter2;
        
        le[0] = filter1; 
        le[1] = filter2;
      }
    }

    expr = new CondExpr[3];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopGE);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrStringString);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[0].operand2.stringstring = ge;
    expr[0].next = null;
    expr[1] = new CondExpr();
    expr[1].op = new AttrOperator(AttrOperator.aopLE);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrStringString);
    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[1].operand2.stringstring = le;
    expr[1].next = null;
    expr[2] = null;

    return expr;
  }

    /** Retrieve the next map in a sequential Stream
   *
   * @exception InvalidMapSizeException Invalid map size
   * @exception IOException I/O errors
   *
   * @return the Map of the retrieved map.
   */

  public Map getNext()
    throws InvalidMapSizeException,
	   IOException
  {
      Map rMap = new Map();
      try {

          if (UseSort)
              rMap = sort.get_next();
          else
              rMap = scan.get_next();

      }catch(Exception e){
          e.printStackTrace();
      }

      return rMap;
  }


  /**
   *  Safely closes a stream object by closing the sort object
   */
  public void close()
  {
      try
      {
          if (UseSort)
              sort.close();
          else
              scan.close();
      }
      catch (Exception e)
      {
          System.err.println("*** Error in closing stream ***");
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
      }

  }
 }
