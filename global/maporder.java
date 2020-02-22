package global;

/** 
 * Enumeration class for maporder
 * 
 */

public class maporder {

  public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;

  public int maporder;

  /** 
   * maporder Constructor
   * <br>
   * A map ordering can be defined as 
   * <ul>
   * <li>   maporder maporder = new maporder(maporder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (maporder.maporder == maporder.Random) ....
   * </ul>
   *
   * @param _maporder The possible ordering of the tuples 
   */

  public maporder (int _maporder) {
    maporder = _maporder;
  }

  public String toString() {
    
    switch (maporder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected maporder " + maporder);
  }

}
