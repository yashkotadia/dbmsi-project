package btree;

/**  StringKey: It extends the KeyClass.
 *   It defines the string Key.
 */ 
public class StringStringKey extends KeyClass {

  private String key1;
  private String key2;

  public String toString(){
     return key1+key2;
  }

  /** Class constructor
   *  @param     s   the value of the string key to be set 
   */
  public StringStringKey(String s1, String s2) { 
    key1 = new String(s1);
    key2 = new String(s2);
  }

  /** get a copy of the istring key
  *  @return the reference of the copy 
  */ 
  public String[] getKey() {
    String[] ret = new String[2];
    ret[0] = new String(key1);
    ret[1] = new String(key2);
    return ret;
  }

  /** set the string key value
   */ 
  public void setKey(String s1, String s2) { 
    key1=new String(s1);
    key2=new String(s2);
  }
}
