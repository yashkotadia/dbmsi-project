package iterator;


import BigT.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing map
 */
public class MapUtils
{

	/**
	 * This function compares a map with another map on different
	 * combinations of fields according to the order type, and
	 *  returns:
	 *
	 *    0        if the two are equal,
	 *    1        if the map is greater,
	 *   -1        if the map is smaller,
	 *
	 *@param    m1        one map.
	 *@param    m2        another map.
	 *@exception IOException some I/O fault
	 *@exception MapUtilsException exception from this class
	 *@return   0        if the two are equal,
	 *          1        if the map is greater,
	 *         -1        if the map is smaller,
	 */


	public static int CompareMapWithMap(Map  m1, Map m2, int  orderType)
			throws IOException, MapUtilsException
	{
		int   m1_i,  m2_i;
		
		String m1_s, m2_s;

		switch (orderType)
		{

			case 1:
				// order type: row label, column label, time stamp
				
				// Compare two strings
				
				m1_s = m1.getRowLabel();
				m2_s = m2.getRowLabel();

				if(m1_s.compareTo( m2_s)>0)return 1;
				if(m1_s.compareTo( m2_s)<0)return -1;
				
				//Till here, row label has matched
				
				m1_s = m1.getColumnLabel();
				m2_s = m2.getColumnLabel();

				if(m1_s.compareTo( m2_s)>0)return 1;
				if(m1_s.compareTo( m2_s)<0)return -1;
				
				//Till here, column label has matched
				
				m1_i = m1.getTimeStamp();
				m2_i = m2.getTimeStamp();

				if (m1_i <  m2_i) return -1;
				if (m1_i >  m2_i) return  1;
				
				return 0;
				

			case 2: 
				// order type: column label, row label, time stamp
				
				// Compare two strings
				
				m1_s = m1.getColumnLabel();
				m2_s = m2.getColumnLabel();

				if(m1_s.compareTo( m2_s)>0)return 1;
				if(m1_s.compareTo( m2_s)<0)return -1;
				
				//Till here, column label has matched
				
				m1_s = m1.getRowLabel();
				m2_s = m2.getRowLabel();

				if(m1_s.compareTo( m2_s)>0)return 1;
				if(m1_s.compareTo( m2_s)<0)return -1;
				
				//Till here, row label has matched
				
				m1_i = m1.getTimeStamp();
				m2_i = m2.getTimeStamp();

				if (m1_i <  m2_i) return -1;
				if (m1_i >  m2_i) return  1;
				
				return 0;

			case 3:
				// order type: row label, time stamp
				
				// Compare two strings
				
				m1_s = m1.getRowLabel();
				m2_s = m2.getRowLabel();

				if(m1_s.compareTo( m2_s)>0)return 1;
				if(m1_s.compareTo( m2_s)<0)return -1;
				
				//Till here, row label has matched
				
				m1_i = m1.getTimeStamp();
				m2_i = m2.getTimeStamp();
	
				if (m1_i <  m2_i) return -1;
				if (m1_i >  m2_i) return  1;
				
				return 0;

			case 4:
				// order type: column label, time stamp
				
				// Compare two strings
				
				m1_s = m1.getColumnLabel();
				m2_s = m2.getColumnLabel();

				if(m1_s.compareTo( m2_s)>0)return 1;
				if(m1_s.compareTo( m2_s)<0)return -1;
				
				//Till here, column label has matched
				
				m1_i = m1.getTimeStamp();
				m2_i = m2.getTimeStamp();

				if (m1_i <  m2_i) return -1;
				if (m1_i >  m2_i) return  1;
								
				return 0;
				
			case 5: 
				// order type: time stamp
				
				m1_i = m1.getTimeStamp();
				m2_i = m2.getTimeStamp();

				if (m1_i <  m2_i) return -1;
				if (m1_i >  m2_i) return  1;
								
				return 0;

			default:
				throw new MapUtilsException(null, "orderType < 1 or > 5 is caught by MapUtils.java");
		}
	}



	/**
	 * This function  compares  map1 with another map2 whose
	 * field number is same as the map1
	 *
	 *@param    m1        one map
	 *@param    value     another map.
	 *@return   0        if the two are equal,
	 *          1        if the tuple is greater,
	 *         -1        if the tuple is smaller,
	 *@exception IOException some I/O fault
	 *@exception MapUtilsException exception from this class
	 */
	public static int CompareMapWithValue(
			Map m1, int map_fld_no,
			Map  value)
			throws IOException,
			MapUtilsException
	{
		return CompareMapWithMap( m1, value, map_fld_no);
	}

	/**
	 *This function Compares two map in all fields
	 * @param m1 the first map
	 * @param m2 the second map
	 * @return  0        if the two are not equal,
	 *          1        if the two are equal,
	 *@exception IOException some I/O fault
	 *@exception MapUtilsException exception from this class
	 */

	public static boolean Equal(Map m1, Map m2)
			throws IOException, MapUtilsException
	{
		int i;
		int len = 4;
		for (i = 1; i <= len; i++)
			if (CompareMapWithMap( m1, m2, i) != 0)
				return false;
		return true;
	}

	/**
	 *get the string specified by the field number
	 *@param map the map
	 *@return the content of the field number
	 *@exception IOException some I/O fault
	 *@exception MapUtilsException exception from this class
	 */

	// dint understand the implementation so using this right now,
	// Will expand the functionality as required.
	///row label-fld no 1 , column label -fld no 2, timestamp-fld no 3, value - fld no 4
	public static String Value(Map map, int map_fld_no)
			throws IOException, MapUtilsException
	{
		String temp;
		switch(map_fld_no) {
			case 1:
				temp = map.getRowLabel();
				break;

			case 2:
				temp = map.getColumnLabel();
				break;

			case 4:
				temp = map.getValue();
				break;

			default:
				throw new MapUtilsException(null, "FieldNumber != 1 or != 2 is caught by MapUtils.java");

		}
		return temp;
	}


	/**
	 *set up a map in specified field from a map
	 *@param value the map to be set
	 *@param map the given map
	 *@exception IOException some I/O fault
	 *@exception MapUtilsException exception from this class
	 */
	///row label-fld no 1 , column label -fld no 2, timestamp-fld no 3, value - fld no 4
	public static void SetValue(Map value, Map map, int map_fld_no)
			throws IOException,
			MapUtilsException
	{

		switch (map_fld_no)
		{
			case 1:
				value.setRowLabel(map.getRowLabel());
				break;

			case 2:
				value.setColumnLabel(map.getColumnLabel());
				break;

			case 3:
				value.setTimeStamp(map.getTimeStamp());
				break;

			case 4:
				try {
					value.setValue(map.getValue());
				}catch (InvalidMapSizeException e) {
					throw new MapUtilsException(e, "InvalidMapSizeException is caught by MapUtils.java");
				}
				break;

			default:
				throw new MapUtilsException(null, "FieldNumber < 1 or > 4 is caught by MapUtils.java");

		}
	}
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
 *@exception MaputilsException exception from this class
 */
//public static short[] setup_op_map(Map Jmap,
//								   int len_in1,
//								   short m1_str_sizes[],
//								   short m2_str_sizes[],
//								   FldSpec proj_list[],
//								   int nOutFlds)
//		throws IOException,
//		MapUtilsException
//{
//	short [] sizesM1 = new short [4];
//	short [] sizesM2 = new short [4];
//	int i, count = 0;

//	for (i = 0; i < ; i++)
//		if (in1[i].map_fld_no ==i)
//			sizesM1[i] = m1_str_sizes[count++];

//for (count = 0, i = 0; i < ; i++)
//	if (in2[i].map_fld_no ==i)
//		sizesM2[i] = m2_str_sizes[count++];

//int n_strs = 0;
//	for (i = 0; i < ; i++)
//	{
//		if (proj_list[i].relation.key == RelSpec.outer)
//			map_fld_no[i] = new  Map(in1[proj_list[i].offset-1].attrType);
//		else if (proj_list[i].relation.key == RelSpec.innerRel)
//			map_fld_no[i] = new Map(in2[proj_list[i].offset-1].attrType);
//	}

//		// Now construct the res_str_sizes array.
//		for (i = 0; i < nOutFlds; i++)
//		{
//			if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
//				n_strs++;
//			else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
//				n_strs++;
//		}

//		short[] res_str_sizes = new short [n_strs];
//		count         = 0;
//		for (i = 0; i < nOutFlds; i++)
//		{
//			if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
//				res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
//			else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
//				res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
//		}
//		try {
//			Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
//		}catch (Exception e){
//			throw new MapUtilsException(e,"setHdr() failed");
//		}
//		return res_str_sizes;
//	}


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
 *@exception MaputilsException exception from this class
 *@exception InvalidRelation invalid relation
 */

//  public static short[] setup_op_map(Map Jmap, AttrType res_attrs[],
//				       AttrType in1[], int len_in1,
//				       short m1_str_sizes[],
//				       FldSpec proj_list[], int nOutFlds)
//  throws IOException,
//   MaputilsException,
// InvalidRelation
//{
//short [] sizesT1 = new short [len_in1];
//int i, count = 0;

//for (i = 0; i < len_in1; i++)
//if (in1[i].attrType == AttrType.attrString)
//sizesM1[i] = t1_str_sizes[count++];

//int n_strs = 0;
//for (i = 0; i < nOutFlds; i++)
//{
//  if (proj_list[i].relation.key == RelSpec.outer)
//          res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);

//  else throw new InvalidRelation("Invalid relation -innerRel");
//	}

// Now construct the res_str_sizes array.
//    for (i = 0; i < nOutFlds; i++)
//{
//if (proj_list[i].relation.key == RelSpec.outer
//  && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
// n_strs++;
//}

//short[] res_str_sizes = new short [n_strs];
//count         = 0;
//for (i = 0; i < nOutFlds; i++) {
//if (proj_list[i].relation.key ==RelSpec.outer
//  && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
//res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
//}

// try {
//Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
//}catch (Exception e){
//throw new MapUtilsException(e,"setHdr() failed");
//}
///return res_str_sizes;
//}
