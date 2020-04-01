package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;
import java.io.File;

public class SystemDefs {
  public static BufMgr	JavabaseBM;
  public static bigDB	JavabaseDB;
  public static Catalog	JavabaseCatalog;
  
  public static String  JavabaseDBName;
  public static String  JavabaseLogName;
  public static boolean MINIBASE_RESTART_FLAG = false;
  public static String	MINIBASE_DBNAME;
  
  public SystemDefs (){};
  
  public SystemDefs(String dbname, int num_pgs, int bufpoolsize,
		    String replacement_policy)
    {
      int logsize;
      
      String real_logname = new String(dbname); 
      String real_dbname = new String(dbname);
      
      if (num_pgs == 0) {
	logsize = 500;
      }
      else {
	logsize = 3*num_pgs;
      }
      
      if (replacement_policy == null) {
	replacement_policy = new String("Clock");
      }
      
      init(real_dbname,real_logname, num_pgs, logsize,
	   bufpoolsize, replacement_policy);
    }
  
  
  public void init( String dbname, String logname,
		    int num_pgs, int maxlogsize,
		    int bufpoolsize, String replacement_policy)
    {
      
      boolean status = true;
      JavabaseBM = null;
      JavabaseDB = null;
      JavabaseDBName = null;
      JavabaseLogName = null;
      JavabaseCatalog = null;
      
      try {
	JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
	JavabaseDB = new bigDB();
/*
	JavabaseCatalog = new Catalog(); 
*/
      }
      catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
      
      JavabaseDBName = new String(dbname);
      JavabaseLogName = new String(logname);
      MINIBASE_DBNAME = new String(JavabaseDBName);
      
      // create or open the DB
      File tempFile = new File(dbname);
      
      if ((MINIBASE_RESTART_FLAG)||(num_pgs == 0)||tempFile.exists()){//open an existing database
	try {
    System.out.println("Opening: " +dbname);
	  JavabaseDB.openDB(dbname);
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      } 
      else {
	try {
    System.out.println("Creating: " +dbname);
	  JavabaseDB.openDB(dbname, num_pgs);
	  //JavabaseBM.flushAllPages(); Commented out to keep the BTreeFile header page pinned
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      }
    }

  public void close(){
    try{
      JavabaseBM.flushAllPages();
      JavabaseDB.closeDB();
    } catch(Exception e){
      System.err.println ("Error Closing the Database");
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
  }

}
