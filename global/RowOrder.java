package global;

/** 
 * Enumeration class for RowOrder
 * 
 */

public class RowOrder {

  public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;

  public int rowOrder;

  /** 
   * RowOrder Constructor
   * <br>
   * A row ordering can be defined as 
   * <ul>
   * <li>   RowOrder rowOrder = new RowOrder(RowOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (rowOrder.rowOrder == RowOrder.Random) ....
   * </ul>
   *
   * @param _rowOrder The possible ordering of the rows 
   */

  public RowOrder (int _rowOrder) {
    rowOrder = _rowOrder;
  }

  public String toString() {
    
    switch (rowOrder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected RowOrder " + rowOrder);
  }

}
