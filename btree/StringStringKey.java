package btree;

/**  StringKey: It extends the KeyClass.
 *   It defines the composite (String,String) Key.
 */ 
public class StringStringKey extends KeyClass {

  private String key1;
  private String key2;

  public String toString(){
     return key1+key2;
  }

  /** Class constructor
   *  @param     s1   the value of the first string key to be set
   *  @param     s2   the value of the second string key to be set
   */
  public StringStringKey(String s1, String s2) { 
    key1 = new String(s1);
    key2 = new String(s2);
  }

  /** get a copy of the string,string key
  *  @return the reference of the array copy 
  */ 
  public String[] getKey() {
    String[] ret = new String[2];
    ret[0] = new String(key1);
    ret[1] = new String(key2);
    return ret;
  }

  /** set the string,string key value
   *  @param     s1   the value of the first string key to be set
   *  @param     s2   the value of the second string key to be set
   */ 
  public void setKey(String s1, String s2) { 
    key1=new String(s1);
    key2=new String(s2);
  }
}
