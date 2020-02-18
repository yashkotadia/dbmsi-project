/* File Map.java */

package BigT;

import java.io.*;
import java.lang.*;
import global.*;


public class Map implements GlobalConst{


 /** 
  * Maximum size of any Map
  */
  public static final int max_size = MINIBASE_PAGESIZE;

 /** 
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this map in data[]
   */
  private int map_offset;

  /**
   * length of this map
   */
  private int map_length;

   /**
    * Class constructor
    * Creat a new map with length = max_size,map offset = 0.
    */

  public  Map()
  {
       // Creat a new map
       data = new byte[max_size];
       map_offset = 0;
       map_length = max_size;
  }
   
   /** Constructor
    * @param amap a byte array which contains the map
    * @param offset the offset of the map in the byte array
    */

   public Map(byte [] amap, int offset)
   {
      data = amap;
      map_offset = offset;
   }
   
   /** Constructor(used as map copy)
    * @param fromMap   a byte array which contains the map
    * 
    */
   public Map(Map fromMap)
   {
       data = fromMap.getMapByteArray();
       map_length = fromMap.getLength();
       map_offset = 0;
   }

   /**  
    * Class constructor
    * Creat a new map with length = size,map offset = 0.
    */
 
  public  Map(int size)
  {
       // Creat a new map
       data = new byte[size];
       map_offset = 0;
       map_length = size;     
  }
   
   /** Copy a map to the current map position
    *  you must make sure the map lengths must be equal
    * @param fromMap the map being copied
    */
   public void mapCopy(Map fromMap)
   {
       byte [] temparray = fromMap.getMapByteArray();
       System.arraycopy(temparray, 0, data, map_offset, map_length);   
//       fldCnt = fromMap.noOfFlds(); 
//       fldOffset = fromMap.copyFldOffset(); 
   }

   /** This is used when you don't want to use the constructor
    * @param amap  a byte array which contains the map
    * @param offset the offset of the map in the byte array
    */

   public void mapInit(byte [] amap, int offset)
   {
      data = amap;
      map_offset = offset;
   }

 /**
  * Set a map with the given map length and offset
  * @param	record	a byte array contains the map
  * @param	offset  the offset of the map ( =0 by default)
  */
 public void mapSet(byte [] record, int offset)  
  {
      System.arraycopy(record, offset, data, 0, length);
      map_offset = 0;
  }

/** get the length of a map
  * @return     size of this map in bytes
  */
  public short size()
   {
      return map_length;
   }
 
   /** get the offset of a map
    *  @return offset of the map in byte array
    */   
   public int getOffset()
   {
      return map_offset;
   }   
   
   /** Copy the map byte array out
    *  @return  byte[], a byte array contains the map
    *		the length of byte[] = length of the map
    */
    
   public byte [] getMapByteArray() 
   {
       byte [] mapcopy = new byte [map_length];
       System.arraycopy(data, map_offset, mapcopy, 0, map_length);
       return mapcopy;
   }
   
   /** return the data byte array 
    *  @return  data byte array 		
    */
    
   public byte [] returnMapByteArray()
   {
       return data;
   }

   /** return row label
    *  @return String row label
    */
   public String getRowLabel(){
      return Convert.getStrValue(map_offset+4, data, ROW_LABEL_SIZE);
   }

   /** return column label
    *  @return String column label
    */
   public String getColumnLabel(){
      return Convert.getStrValue(map_offset+4+ROW_LABEL_SIZE, data, COLUMN_LABEL_SIZE);
   }

   /** return time stamp
    *  @return int time stamp
    */
   public String getTimeStamp(){
      return Convert.getIntValue(map_offset+4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE, data);
   }

   /** return value
    *  @return String value
    */
   public String getValue(){
      return Convert.getStrValue(map_offset+4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4, data, map_length-8-ROW_LABEL_SIZE-COLUMN_LABEL_SIZE);
   }

   /** set row label
   *   @param val the string value to be set as row label
   *   @exception IOException I/O exceptions
   */
   public Map setRowLabel(String val){

    Convert.setStrValue(val, map_offset+4, data);
    return this;
   }

   /** set column label
   *   @param val the string value to be set as column label
   *   @exception IOException I/O exceptions
   */
   public Map setColumnLabel(String val){

    Convert.setStrValue(val, map_offset+4+ROW_LABEL_SIZE, data);
    return this;
   }

   /** set time stamp
   *   @param val the int value to be set as column label
   *   @exception IOException I/O exceptions
   */
   public Map setTimeStamp(int val){

    Convert.setIntValue(val, map_offset+4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE, data);
    return this;
   }

   /** set the map value
   *   @param val the string value to be set as map value
   *   @exception IOException I/O exceptions
   */
   public Map setValue(String val){
    map_length = 4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4+val.length();
    Convert.setStrValue(val, map_offset+4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4, data);
    return this;
   }

  
 /**
  * Print out the map
  * @exception IOException I/O exception
  */
 public void print()
    throws IOException 
 {
  int i, val;
  String sval;

  System.out.print("[");

  sval = Convert.getStrValue(map_offset+4, data, ROW_LABEL_SIZE);
  System.out.print(sval+", ");

  sval = Convert.getStrValue(map_offset+4+ROW_LABEL_SIZE, data, COLUMN_LABEL_SIZE);
  System.out.print(sval+", ");

  val = Convert.getIntValue(map_offset+4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE, data);
  System.out.print(val+", ");

  sval = Convert.getStrValue(map_offset+4+ROW_LABEL_SIZE+COLUMN_LABEL_SIZE+4, data, map_length-8-ROW_LABEL_SIZE-COLUMN_LABEL_SIZE);
  System.out.print(sval+"]");
 }

}

