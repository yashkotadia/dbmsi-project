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

    private static int SORTPGNUM = 12;
    private Sort sort;

    private void tempFuncPrint() {
        try
        {
            Map rMap;
            while (true) {
                if ((rMap = sort.get_next()) == null)
                    break;

                System.out.println(rMap.getRowLabel() + " " + rMap.getColumnLabel() + " " + rMap.getTimeStamp() + " " + rMap.getValue());
            }
        }
        catch(Exception e)
        {
            System.err.println("*** Error in Stream get_Next ***");
            e.printStackTrace();
        }
    }

    /**
     *
     *        *  orderType is
     *        * · 1, then results are ﬁrst ordered in row label, then column label, then time stamp
     *        * · 2, then results are ﬁrst ordered in column label, then row label, then time stamp
     *        * · 3, then results are ﬁrst ordered in row label, then time stamp
     *        * · 4, then results are ﬁrst ordered in column label, then time stamp
     *        * · 6, then results are ordered in time stamp
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

      int mapSize = 4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4+20+2; //Taking value length to be 20 arbitrarily
      TupleOrder[] order = new TupleOrder[2];
      order[0] = new TupleOrder(TupleOrder.Ascending);
      order[1] = new TupleOrder(TupleOrder.Descending);

      switch(SystemDefs.JavabaseDB.type)
      {

          case 1: //No Index
          {
              try{
                  FileScan fscan = new FileScan(bt.get_fileName(), rowFilter, columnFilter, valueFilter);

                  sort = new Sort(order[0], fscan, SORTPGNUM, orderType, mapSize);

              }catch(Exception e){
                  System.err.println("*** Error in Stream Case 1 ***");
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
              }

          }
          break;

          case 2: //index row labels
          {
              try {
                  CondExpr[] expr;
                  switch (orderType)
                  {
                      case 1:
                      case 3:
                      {
                          expr = GetConditionalExpression(rowFilter, 2);

                          IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), bt.get_fileName(), "row_index", rowFilter, columnFilter, valueFilter, expr);
                          sort = new Sort(order[0], iscan, SORTPGNUM, orderType, mapSize);

                          break;
                      }

                      case 2:
                      case 4:
                      case 5:
                      {
                          //************* WILL Using INDEX Scan help in improving perf? ************
                          FileScan fscan = new FileScan(bt.get_fileName(), rowFilter, columnFilter, valueFilter);
                          sort = new Sort(order[0], fscan, SORTPGNUM, orderType, mapSize);
                          break;
                      }
                  }
              }
              catch(Exception e){
                  System.err.println("*** Error in Stream Case 2 ***");
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
              }
          }
          break;

          case 3: //index column labels
          {
              try {
                  CondExpr[] expr;
                  switch (orderType)
                  {
                      case 2:
                      case 4:
                      {
                          expr = GetConditionalExpression(columnFilter, 3);

                          IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), bt.get_fileName(), "column_index", rowFilter, columnFilter, valueFilter, expr);
                          sort = new Sort(order[0], iscan, SORTPGNUM, orderType, mapSize);

                          break;
                      }

                      case 1:
                      case 3:
                      case 5:
                      {
                          FileScan fscan = new FileScan(bt.get_fileName(), rowFilter, columnFilter, valueFilter);
                          sort = new Sort(order[0], fscan, SORTPGNUM, orderType, mapSize);
                          break;
                      }
                  }
              }
              catch(Exception e){
                  System.err.println("*** Error in Stream Case 3 ***");
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
              }

          }
          break;

          case 4: //index column+row labels and  index timestamps
          {
              try
              {
                  CondExpr[] expr;
                  switch (orderType)
                  {
                      case 1:
                      case 3:
                      {
                          FileScan fscan = new FileScan(bt.get_fileName(), rowFilter, columnFilter, valueFilter);
                          sort = new Sort(order[0], fscan, SORTPGNUM, orderType, mapSize);
                          break;
                      }
                      case 2:
                      case 4:
                      {
                          expr = GetConditionalExpression(columnFilter, 4);

                          IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), bt.get_fileName(), "column_row_index", rowFilter, columnFilter, valueFilter, expr);
                          sort = new Sort(order[0], iscan, SORTPGNUM, orderType, mapSize);

                          break;
                      }
                      case 5:
                      {
                          IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), bt.get_fileName(), "timestamp_index", rowFilter, columnFilter, valueFilter, null);
                          sort = new Sort(order[0], iscan, SORTPGNUM, orderType, mapSize);

                          break;
                      }
                  }
              }
              catch(Exception e){
                  System.err.println("*** Error in Stream Case 4 ***");
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
              }
          }
          break;


          case 5: // index row+value labels and index timestamps
          {
              try
              {
                  CondExpr[] expr;
                  switch (orderType)
                  {
                      case 1:
                      case 3:
                      {
                          //This might become expensive depending on the dataset.
                          expr = GetConditionalExpression(rowFilter, 5);

                          IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), bt.get_fileName(), "row_value_index", rowFilter, columnFilter, valueFilter, expr);
                          sort = new Sort(order[0], iscan, SORTPGNUM, orderType, mapSize);

                          break;
                      }
                      case 2:
                      case 4:
                      {
                          FileScan fscan = new FileScan(bt.get_fileName(), rowFilter, columnFilter, valueFilter);
                          sort = new Sort(order[0], fscan, SORTPGNUM, orderType, mapSize);
                          break;
                      }

                      case 5:
                      {
                          IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), bt.get_fileName(), "timestamp_index", rowFilter, columnFilter, valueFilter, null);
                          sort = new Sort(order[0], iscan, SORTPGNUM, orderType, mapSize);

                          break;
                      }
                  }
              }
              catch(Exception e){
                  System.err.println("*** Error in Stream Case 5 ***");
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
              }
          }
          break;

      }

  }


    private CondExpr[] GetConditionalExpression(String filter, int dbType) {

        CondExpr[] expr = null;
        if (filter.equals("*"))
            expr = null;
        else if (filter.charAt(0) == '[')
        {
            // RangeSearch
            String[ ] fAttributes = filter.split(",");
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

            if (dbType == 4 || dbType == 5)
            {
                expr[0].type2 = new AttrType(AttrType.attrStringString);
                expr[1].type2 = new AttrType(AttrType.attrStringString);
            }
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

            if (dbType == 4 || dbType == 5)
                expr[0].type2 = new AttrType(AttrType.attrStringString);

        }
        return expr;
    }




    /** Retrieve the next map in a sequential Stream
   *
   * @exception InvalidMapSizeException Invalid map size
   * @exception IOException I/O errors
   *
   * @param mid Map ID of the map
   * @return the Map of the retrieved map.
   */

  public Map getNext(MID mid)
    throws InvalidMapSizeException,
	   IOException
  {
      Map rMap = new Map();
      try {
          rMap = sort.get_next();
      }catch(Exception e){
          e.printStackTrace();
      }

      return rMap;
  }



    public void closestream()
    {
        try
        {
            sort.close();
        }
        catch (Exception e)
        {
            System.err.println("*** Error in closing stream ***");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }
 }
