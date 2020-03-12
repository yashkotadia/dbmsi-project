/*  File MID.java */  

package global;

import java.io.*;

/** class MID
 */

public class MID extends RID{
  
  /**
   * default constructor of class
   */
  public MID () { }
  
  /**
   *  constructor of class
   */
  public MID (PageId pageno, int slotno)
    {
      super(pageno, slotno);
    }

  /**
   *  constructor of class
   */
  public MID (RID rid)
    {
      super(rid.pageNo, rid.slotNo);
    }
  
}
