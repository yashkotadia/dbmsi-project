package iterator;


import heap.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing map
 */
public class maputils
{
  
  /**
   * This function compares a map with another map in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    m1        one map.
   *@param    m2        another map.
   *@param    m1_fld_no the field numbers in the map to be compared.
   *@param    m2_fld_no the field numbers in the map to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the map is greater,
   *         -1        if the map is smaller,                              
   */
  public static int ComparemapWithmap(AttrType fldType,
					  map  m1, int m1_fld_no,
					  map m2, int  m2_fld_no)
    throws IOException,
	   UnknowAttrType,
	   maputilsException
    {
      int   m1_i,  m2_i;
      float m1_r,  m2_r;
      String m1_s, m2_s;
      
      switch (fldType.attrType) 
	{
	case AttrType.attrInteger:                // Compare two integers.
	  try {
	    m1_i = m1.getIntFld(m1_fld_no);
	    m2_i = m2.getIntFld(m2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new maputilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
	  }
	  if (m1_i == m2_i) return  0;
	  if (m1_i <  m2_i) return -1;
	  if (m1_i >  m2_i) return  1;
	  
	case AttrType.attrReal:                // Compare two floats
	  try {
	    m1_r = m1.getFloFld(m1_fld_no);
	    m2_r = m2.getFloFld(m2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new maputilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
	  }
	  if (m1_r == m2_r) return  0;
	  if (m1_r <  m2_r) return -1;
	  if (m1_r >  m2_r) return  1;
	  
	case AttrType.attrString:                // Compare two strings
	  try {
	    m1_s = m1.getStrFld(m1_fld_no);
	    m2_s = m2.getStrFld(m2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new maputilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
	  }
	  
	  // Now handle the special case that is posed by the max_values for strings...
	  if(m1_s.compareTo( m2_s)>0)return 1;
	  if (m1_s.compareTo( m2_s)<0)return -1;
	  return 0;
	default:
	  
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
    }
  
  
  
  /**
   * This function  compares  map1 with another map2 whose
   * field number is same as the map1
   *
   *@param    fldType   the type of the field being compared.
   *@param    m1        one map
   *@param    value     another map.
   *@param    m1_fld_no the field numbers in the map to be compared.  
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class   
   */            
  public static int ComparemapWithValue(AttrType fldType,
					  map m1, int t1_fld_no,
					  map  value)
    throws IOException,
	   UnknowAttrType,
	   maputilsException
    {
      return ComparemapWithmap(fldType, m1, m1_fld_no, value, m1_fld_no);
    }
  
  /**
   *This function Compares two map in all fields 
   * @param m1 the first map
   * @param m2 the second map
   * @param type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class
   */            
  
  public static boolean Equal(map m1, map m2, AttrType types[], int len)
    throws IOException,UnknowAttrType,maputilsException
    {
      int i;
      
      for (i = 1; i <= len; i++)
	if (ComparemapWithmap(types[i-1], m1, i, m2, i) != 0)
	  return false;
      return true;
    }
  
  /**
   *get the string specified by the field number
   *@param map the map 
   *@param fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class
   */
  public static String Value(map map, int fldno)
    throws IOException,
	   maputilsException
    {
      String temp;
      try{
	temp = map.getStrFld(fldno);
      }catch (FieldNumberOutOfBoundException e){
	throw new maputilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
      }
      return temp;
    }
  
 
  /**
   *set up a map in specified field from a map
   *@param value the map to be set 
   *@param map the given map
   *@param fld_no the field number
   *@param fldType the map attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class
   */  
  public static void SetValue(map value, map  map, int fld_no, AttrType fldType)
    throws IOException,
	   UnknowAttrType,
	   maputilsException
    {
      
      switch (fldType.attrType)
	{
	case AttrType.attrInteger:
	  try {
	    value.setIntFld(fld_no, map.getIntFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new maputilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
	  }
	  break;
	case AttrType.attrReal:
	  try {
	    value.setFloFld(fld_no, map.getFloFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new maputilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
	  }
	  break;
	case AttrType.attrString:
	  try {
	    value.setStrFld(fld_no, map.getStrFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by maputils.java");
	  }
	  break;
	default:
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
      
      return;
    }
  
  
  /**
   *set up the Jmap's attrtype, string size,field number for using join
   *@param Jmap  reference to an actual map  - no memory has been malloced
   *@param res_attrs  attributes type of result map
   *@param in1  array of the attributes of the map (ok)
   *@param len_in1  num of attributes of in1
   *@param in2  array of the attributes of the map (ok)
   *@param len_in2  num of attributes of in2
   *@param m1_str_sizes shows the length of the string fields in S
   *@param m2_str_sizes shows the length of the string fields in R
   *@param proj_list shows what input fields go where in the output map
   *@param nOutFlds number of outer relation fields
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class
   */
  public static short[] setup_op_map(map Jmap, AttrType[] res_attrs,
				       AttrType in1[], int len_in1, AttrType in2[], 
				       int len_in2, short m1_str_sizes[], 
				       short m2_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   maputilsException
    {
      short [] sizesM1 = new short [len_in1];
      short [] sizesM2 = new short [len_in2];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesM1[i] = t1_str_sizes[count++];
      
      for (count = 0, i = 0; i < len_in2; i++)
	if (in2[i].attrType == AttrType.attrString)
	  sizesM2[i] = t2_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer)
	    res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  else if (proj_list[i].relation.key == RelSpec.innerRel)
	    res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
	}
      try {
	Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new maputilsException(e,"setHdr() failed");
      }
      return res_str_sizes;
    }
  
 
   /**
   *set up the Jmap's attrtype, string size,field number for using project
   *@param Jmap reference to an actual map  - no memory has been malloced
   *@param res_attrs  attributes type of result map
   *@param in1  array of the attributes of the map(ok)
   *@param len_in1  num of attributes of in1
   *@param m1_str_sizes shows the length of the string fields in S
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fields
   *@exception IOException some I/O fault
   *@exception maputilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */

  public static short[] setup_op_map(map Jmap, AttrType res_attrs[],
				       AttrType in1[], int len_in1,
				       short m1_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   maputilsException, 
	   InvalidRelation
    {
      short [] sizesT1 = new short [len_in1];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesM1[i] = t1_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer) 
            res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  
	  else throw new InvalidRelation("Invalid relation -innerRel");
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer
	      && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
	    n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++) {
	if (proj_list[i].relation.key ==RelSpec.outer
	    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
	  res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
      }
     
      try {
	Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new maputilsException(e,"setHdr() failed");
      } 
      return res_str_sizes;
    }
}
