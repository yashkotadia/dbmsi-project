package BigT;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.Tuple;
import btree.*;

/**  This bigT implementation is directory-based. We maintain a
 *  directory of info about the data pages (which are of type HFPage
 *  when loaded into memory).  The directory itself is also composed
 *  of HFPages, with each record being of type DataPageInfo
 *  as defined below.
 *
 *  The first directory page is a header page for the entire database
 *  (it is the one to which our filename is mapped by the DB).
 *  All directory pages are in a doubly-linked list of pages, each
 *  directory entry points to a single data page, which contains
 *  the actual records.
 *
 *  The bigT data pages are implemented as slotted pages, with
 *  the slots at the front and the records in the back, both growing
 *  into the free space in the middle of the page.
 *
 *  We can store roughly pagesize/sizeof(DataPageInfo) records per
 *  directory page; for any given HeapFile insertion, it is likely
 *  that at least one of those referenced data pages will have
 *  enough free space to satisfy the request.
 */


/** DataPageInfo class : the type of records stored on a directory page.
*
* April 9, 1998
*/


interface  Filetype {
  int TEMP = 0;
  int ORDINARY = 1;
  
} // end of Filetype

public class bigT implements Filetype,  GlobalConst {
  
  
  PageId      _firstDirPageId;   // page number of header page
  int         _ftype;
  private     boolean     _file_deleted;
  private     String 	 _fileName;
  private static int tempfilecount = 0;
  
  
  
  /* get a new datapage from the buffer manager and initialize dpinfo
     @param dpinfop the information in the new HFPage
  */
  private HFPage _newDatapage(DataPageInfo dpinfo)
    throws HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException
    {
      Page apage = new Page();
      PageId pageId = new PageId();
      pageId = newPage(apage, 1);
      
      if(pageId == null)
	throw new HFException(null, "can't new pae");
      
      // initialize internal values of the new page:
      
      HFPage hfpage = new HFPage();
      hfpage.init(pageId, apage);
      
      dpinfo.pageId.pid = pageId.pid;
      dpinfo.mapct = 0;
      dpinfo.availspace = hfpage.available_space();
      dpinfo.rowct = 0;
      dpinfo.colct = 0;

      return hfpage;
      
    } // end of _newDatapage
  
  /* Internal HeapFile function (used in getMap and updateMap):
     returns pinned directory page and pinned data page of the specified 
     user map(mid) and true if map is found.
     If the user map cannot be found, return false.
  */
  private boolean  _findDataPage( MID mid,
				  PageId dirPageId, HFPage dirpage,
				  PageId dataPageId, HFPage datapage,
				  RID rpDataPageRid) 
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   Exception
    {
      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
      
      HFPage currentDirPage = new HFPage();
      HFPage currentDataPage = new HFPage();
      RID currentDataPageRid = new RID();
      PageId nextDirPageId = new PageId();
      // datapageId is stored in dpinfo.pageId 
      
      
      pinPage(currentDirPageId, currentDirPage, false/*read disk*/);
      
      Tuple atuple = new Tuple();
      Map amap = new Map();
      
      while (currentDirPageId.pid != INVALID_PAGE)
	{// Start While01
	  // ASSERTIONS:
	  //  currentDirPage, currentDirPageId valid and pinned and Locked.
	  
	  for( currentDataPageRid = currentDirPage.firstRecord();
	       currentDataPageRid != null;
	       currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid))
	    {
	      try{
		atuple = currentDirPage.getRecord(currentDataPageRid);
	      }
	      catch (InvalidSlotNumberException e)// check error! return false(done) 
		{
		  return false;
		}
	      
	      DataPageInfo dpinfo = new DataPageInfo(atuple);
	      try{
		pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);
		
		
		//check error;need unpin currentDirPage
	      }catch (Exception e)
		{
		  unpinPage(currentDirPageId, false/*undirty*/);
		  dirpage = null;
		  datapage = null;
		  throw e;
		}
	      
	      
	      
	      // ASSERTIONS:
	      // - currentDataPage, currentDataPageRid, dpinfo valid
	      // - currentDataPage pinned
	      
	      if(dpinfo.pageId.pid==mid.pageNo.pid)
		{
		  amap = currentDataPage.returnMap(mid);
		  // found user's record on the current datapage which itself
		  // is indexed on the current dirpage.  Return both of these.
		  
		  dirpage.setpage(currentDirPage.getpage());
		  dirPageId.pid = currentDirPageId.pid;
		  
		  datapage.setpage(currentDataPage.getpage());
		  dataPageId.pid = dpinfo.pageId.pid;
		  
		  rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
		  rpDataPageRid.slotNo = currentDataPageRid.slotNo;
		  return true;
		}
	      else
		{
		  // user record not found on this datapage; unpin it
		  // and try the next one
		  unpinPage(dpinfo.pageId, false /*undirty*/);
		  
		}
	      
	    }
	  
	  // if we would have found the correct datapage on the current
	  // directory page we would have already returned.
	  // therefore:
	  // read in next directory page:
	  
	  nextDirPageId = currentDirPage.getNextPage();
	  try{
	    unpinPage(currentDirPageId, false /*undirty*/);
	  }
	  catch(Exception e) {
	    throw new HFException (e, "heapfile,_find,unpinpage failed");
	  }
	  
	  currentDirPageId.pid = nextDirPageId.pid;
	  if(currentDirPageId.pid != INVALID_PAGE)
	    {
	      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
	      if(currentDirPage == null)
		throw new HFException(null, "pinPage return null page");  
	    }
	  
	  
	} // end of While01
      // checked all dir pages and all data pages; user record not found:(
      
      dirPageId.pid = dataPageId.pid = INVALID_PAGE;
      
      return false;   
      
      
    } // end of _findDatapage		     
  
  /** Initialize.  A null name produces a temporary heapfile which will be
   * deleted by the destructor.  If the name already denotes a file, the
   * file is opened; otherwise, a new empty file is created.
   *
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   */
  public  bigT(String name) 
    throws HFException, 
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException
	   
    {
      // Give us a prayer of destructing cleanly if construction fails.
      _file_deleted = true;
      _fileName = null;
      
      if(name == null) 
	{
	  // If the name is NULL, allocate a temporary name
	  // and no logging is required.
	  _fileName = "tempBigT";
	  String useId = new String("user.name");
	  String userAccName;
	  userAccName = System.getProperty(useId);
	  _fileName = _fileName + userAccName;
	  
	  String filenum = Integer.toString(tempfilecount);
	  _fileName = _fileName + filenum; 
	  _ftype = TEMP;
	  tempfilecount ++;
	  
	}
      else
	{
	  _fileName = name;
	  _ftype = ORDINARY;    
	}
      
      // The constructor gets run in two different cases.
      // In the first case, the file is new and the header page
      // must be initialized.  This case is detected via a failure
      // in the db->get_file_entry() call.  In the second case, the
      // file already exists and all that must be done is to fetch
      // the header page into the buffer pool
      
      // try to open the file
      
      Page apage = new Page();
      _firstDirPageId = null;
      if (_ftype == ORDINARY)
	_firstDirPageId = get_file_entry(_fileName);
      
      if(_firstDirPageId==null)
	{
	  // file doesn't exist. First create it.
	  _firstDirPageId = newPage(apage, 1);
	  // check error
	  if(_firstDirPageId == null)
	    throw new HFException(null, "can't new page");
	  
	  add_file_entry(_fileName, _firstDirPageId);
	  // check error(new exception: Could not add file entry
	  
	  HFPage firstDirPage = new HFPage();
	  firstDirPage.init(_firstDirPageId, apage);
	  PageId pageId = new PageId(INVALID_PAGE);
	  
	  firstDirPage.setNextPage(pageId);
	  firstDirPage.setPrevPage(pageId);
	  unpinPage(_firstDirPageId, true /*dirty*/ );
	  
	  
	}
      _file_deleted = false;
      // ASSERTIONS:
      // - ALL private data members of class Heapfile are valid:
      //
      //  - _firstDirPageId valid
      //  - _fileName valid
      //  - no datapage pinned yet    
      
    } // end of constructor 
  
  /** Return number of maps in bigT.
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   */
  public int getMapCnt() 
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   IOException
	   
    {
      int answer = 0;
      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
      
      PageId nextDirPageId = new PageId(0);
      
      HFPage currentDirPage = new HFPage();
      Page pageinbuffer = new Page();
      
      while(currentDirPageId.pid != INVALID_PAGE)
	{
	   pinPage(currentDirPageId, currentDirPage, false);
	   
	   RID rid = new RID();
	   Tuple atuple;
	   for (rid = currentDirPage.firstRecord();
	        rid != null;	// rid==NULL means no more record
	        rid = currentDirPage.nextRecord(rid))
	     {
	       atuple = currentDirPage.getRecord(rid);
	       DataPageInfo dpinfo = new DataPageInfo(atuple);
	       
	       answer += dpinfo.mapct;
	     }
	   
	   // ASSERTIONS: no more record
           // - we have read all datapage records on
           //   the current directory page.
	   
	   nextDirPageId = currentDirPage.getNextPage();
	   unpinPage(currentDirPageId, false /*undirty*/);
	   currentDirPageId.pid = nextDirPageId.pid;
	}
      
      // ASSERTIONS:
      // - if error, exceptions
      // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
      // - if not yet end of heapfile: currentDirPageId valid
      
      
      return answer;
    } // end of getMapCnt
  
  /** Insert map into file, return its Mid.
   *
   * @param mapPtr pointer of the record
   * @param mapLen the length of the record
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception SpaceNotAvailableException no space left
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   *
   * @return the mid of the map
   */
  public MID insertMap(byte[] mapPtr) 
    throws InvalidSlotNumberException,  
	   InvalidTupleSizeException,
	   SpaceNotAvailableException,
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException
    {
      int dpinfoLen = 0;	
      int mapLen = mapPtr.length;
      boolean found;
      RID currentDataPageRid = new RID();
      Page pageinbuffer = new Page();
      HFPage currentDirPage = new HFPage();
      HFPage currentDataPage = new HFPage();
      
      HFPage nextDirPage = new HFPage(); 
      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
      PageId nextDirPageId = new PageId();  // OK
      
      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
      
      found = false;
      Tuple atuple;
      DataPageInfo dpinfo = new DataPageInfo();
      while (found == false)
	{ //Start While01
	  // look for suitable dpinfo-struct
	  for (currentDataPageRid = currentDirPage.firstRecord();
	       currentDataPageRid != null;
	       currentDataPageRid = 
		 currentDirPage.nextRecord(currentDataPageRid))
	    {
	      atuple = currentDirPage.getRecord(currentDataPageRid);
	      
	      dpinfo = new DataPageInfo(atuple);
	      
	      // need check the map length == DataPageInfo'slength
	      
	       if(mapLen <= dpinfo.availspace)
		 {
		   found = true;
		   break;
		 }  
	    }
	  
	  // two cases:
	  // (1) found == true:
	  //     currentDirPage has a datapagerecord which can accomodate
	  //     the map which we have to insert
	  // (2) found == false:
	  //     there is no datapagerecord on the current directory page
	  //     whose corresponding datapage has enough space free
	  //     several subcases: see below
	  if(found == false)
	    { //Start IF01
	      // case (2)
	      
	      //System.out.println("no datapagerecord on the current directory is OK");
	      //System.out.println("dirpage availspace "+currentDirPage.available_space());
	      
	      // on the current directory page is no datapagerecord which has
	      // enough free space
	      //
	      // two cases:
	      //
	      // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
	      //         if there is enough space on the current directory page
	      //         to accomodate a new datapagerecord (type DataPageInfo),
	      //         then insert a new DataPageInfo on the current directory
	      //         page
	      // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
	      //         look at the next directory page, if necessary, create it.
	      
	      if(currentDirPage.available_space() >= dpinfo.size)
		{ 
		  //Start IF02
		  // case (2.1) : add a new data page record into the
		  //              current directory page
		  currentDataPage = _newDatapage(dpinfo); 
		  // currentDataPage is pinned! and dpinfo->pageId is also locked
		  // in the exclusive mode  
		  
		  // didn't check if currentDataPage==NULL, auto exception
		  
		  
		  // currentDataPage is pinned: insert its record in directory
		  // calling a HFPage function
		  
		  
		  
		  atuple = dpinfo.convertToTuple();
		  
		  byte [] tmpData = atuple.getTupleByteArray();
		  currentDataPageRid = currentDirPage.insertRecord(tmpData);
		  
		  RID tmprid = currentDirPage.firstRecord();
		  
		  
		  // need catch error here!
		  if(currentDataPageRid == null)
		    throw new HFException(null, "no space to insert rec.");  
		  
		  // end the loop, because a new datapage with its record
		  // in the current directorypage was created and inserted into
		  // the heapfile; the new datapage has enough space for the
		  // map which the user wants to insert
		  
		  found = true;
		  
		} //end of IF02
	      else
		{  //Start else 02
		  // case (2.2)
		  nextDirPageId = currentDirPage.getNextPage();
		  // two sub-cases:
		  //
		  // (2.2.1) nextDirPageId != INVALID_PAGE:
		  //         get the next directory page from the buffer manager
		  //         and do another look
		  // (2.2.2) nextDirPageId == INVALID_PAGE:
		  //         append a new directory page at the end of the current
		  //         page and then do another loop
		    
		  if (nextDirPageId.pid != INVALID_PAGE) 
		    { //Start IF03
		      // case (2.2.1): there is another directory page:
		      unpinPage(currentDirPageId, false);
		      
		      currentDirPageId.pid = nextDirPageId.pid;
		      
		      pinPage(currentDirPageId,
						    currentDirPage, false);
		      
		      
		      
		      // now go back to the beginning of the outer while-loop and
		      // search on the current directory page for a suitable datapage
		    } //End of IF03
		  else
		    {  //Start Else03
		      // case (2.2): append a new directory page after currentDirPage
		      //             since it is the last directory page
		      nextDirPageId = newPage(pageinbuffer, 1);
		      // need check error!
		      if(nextDirPageId == null)
			throw new HFException(null, "can't new pae");
		      
		      // initialize new directory page
		      nextDirPage.init(nextDirPageId, pageinbuffer);
		      PageId temppid = new PageId(INVALID_PAGE);
		      nextDirPage.setNextPage(temppid);
		      nextDirPage.setPrevPage(currentDirPageId);
		      
		      // update current directory page and unpin it
		      // currentDirPage is already locked in the Exclusive mode
		      currentDirPage.setNextPage(nextDirPageId);
		      unpinPage(currentDirPageId, true/*dirty*/);
		      
		      currentDirPageId.pid = nextDirPageId.pid;
		      currentDirPage = new HFPage(nextDirPage);
		      
		      // remark that MINIBASE_BM->newPage already
		      // pinned the new directory page!
		      // Now back to the beginning of the while-loop, using the
		      // newly created directory page.
		      
		    } //End of else03
		} // End of else02
	      // ASSERTIONS:
	      // - if found == true: search will end and see assertions below
	      // - if found == false: currentDirPage, currentDirPageId
	      //   valid and pinned
	      
	    }//end IF01
	  else
	    { //Start else01
	      // found == true:
	      // we have found a datapage with enough space,
	      // but we have not yet pinned the datapage:
	      
	      // ASSERTIONS:
	      // - dpinfo valid
	      
	      // System.out.println("find the dirpagerecord on current page");
	      
	      pinPage(dpinfo.pageId, currentDataPage, false);
	      //currentDataPage.openHFpage(pageinbuffer);
	      
	      
	    }//End else01
	} //end of While01
      
      // ASSERTIONS:
      // - currentDirPageId, currentDirPage valid and pinned
      // - dpinfo.pageId, currentDataPageRid valid
      // - currentDataPage is pinned!
      
      if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
	throw new HFException(null, "invalid PageId");
      
      if (!(currentDataPage.available_space() >= mapLen))
	throw new SpaceNotAvailableException(null, "no available space");
      
      if (currentDataPage == null)
	throw new HFException(null, "can't find Data page");
      
      
      MID mid;
      mid = currentDataPage.insertMap(mapPtr);
      
      dpinfo.mapct++;
      dpinfo.rowct++;
      dpinfo.colct++;
      dpinfo.availspace = currentDataPage.available_space();
      
      
      unpinPage(dpinfo.pageId, true /* = DIRTY */);
      
      // DataPage is now released
      atuple = currentDirPage.returnRecord(currentDataPageRid);
      DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
      
      
      dpinfo_ondirpage.availspace = dpinfo.availspace;
      dpinfo_ondirpage.mapct = dpinfo.mapct;
      dpinfo_ondirpage.rowct = dpinfo.rowct;
      dpinfo_ondirpage.colct = dpinfo.colct;
      dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
      dpinfo_ondirpage.flushToTuple();
      
      
      unpinPage(currentDirPageId, true /* = DIRTY */);

      insertMapIndex(mid, mapPtr); // Update the index      
      
      return mid;
      
    }
  
  /** Delete map from file with given mid.
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception Exception other exception
   *
   * @return true map deleted  false:map not found
   */
  public boolean deleteMap(MID mid)  
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFException, 
	   HFBufMgrException,
	   HFDiskMgrException,
	   Exception
  
    {
      boolean status;
      HFPage currentDirPage = new HFPage();
      PageId currentDirPageId = new PageId();
      HFPage currentDataPage = new HFPage();
      PageId currentDataPageId = new PageId();
      RID currentDataPageRid = new RID();
      
      status = _findDataPage(mid,
			     currentDirPageId, currentDirPage, 
			     currentDataPageId, currentDataPage,
			     currentDataPageRid);
      
      if(status != true) return status;	// map not found
      
      // ASSERTIONS:
      // - currentDirPage, currentDirPageId valid and pinned
      // - currentDataPage, currentDataPageid valid and pinned
      
      // get datapageinfo from the current directory page:
      Tuple atuple;	
      
      atuple = currentDirPage.returnRecord(currentDataPageRid);
      DataPageInfo pdpinfo = new DataPageInfo(atuple);
      
      // delete the map on the datapage
      currentDataPage.deleteMap(mid);
      
      pdpinfo.mapct--;
      pdpinfo.rowct--;
      pdpinfo.colct--;
      pdpinfo.flushToTuple();	//Write to the buffer pool
      if (pdpinfo.mapct >= 1) 
	{
	  // more maps remain on datapage so it still hangs around.  
	  // we just need to modify its directory entry
	  
	  pdpinfo.availspace = currentDataPage.available_space();
	  pdpinfo.flushToTuple();
	  unpinPage(currentDataPageId, true /* = DIRTY*/);
	  
	  unpinPage(currentDirPageId, true /* = DIRTY */);
	  
	  
	}
      else
	{
	  // the map is already deleted:
	  // we're removing the last map on datapage so free datapage
	  // also, free the directory page if 
	  //   a) it's not the first directory page, and 
	  //   b) we've removed the last DataPageInfo record on it.
	  
	  // delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
	  unpinPage(currentDataPageId, false /*undirty*/);
	  
	  freePage(currentDataPageId);
	  
	  // delete corresponding DataPageInfo-entry on the directory page:
	  // currentDataPageRid points to datapage (from for loop above)
	  
	  currentDirPage.deleteRecord(currentDataPageRid);
	  
	  
	  // ASSERTIONS:
	  // - currentDataPage, currentDataPageId invalid
	  // - empty datapage unpinned and deleted
	  
	  // now check whether the directory page is empty:
	  
	  currentDataPageRid = currentDirPage.firstRecord();
	  
	  // st == OK: we still found a datapageinfo record on this directory page
	  PageId pageId;
	  pageId = currentDirPage.getPrevPage();
	  if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE))
	    {
	      // the directory-page is not the first directory page and it is empty:
	      // delete it
	      
	      // point previous page around deleted page:
	      
	      HFPage prevDirPage = new HFPage();
	      pinPage(pageId, prevDirPage, false);

	      pageId = currentDirPage.getNextPage();
	      prevDirPage.setNextPage(pageId);
	      pageId = currentDirPage.getPrevPage();
	      unpinPage(pageId, true /* = DIRTY */);
	      
	      
	      // set prevPage-pointer of next Page
	      pageId = currentDirPage.getNextPage();
	      if(pageId.pid != INVALID_PAGE)
		{
		  HFPage nextDirPage = new HFPage();
		  pageId = currentDirPage.getNextPage();
		  pinPage(pageId, nextDirPage, false);
		  
		  //nextDirPage.openHFpage(apage);
		  
		  pageId = currentDirPage.getPrevPage();
		  nextDirPage.setPrevPage(pageId);
		  pageId = currentDirPage.getNextPage();
		  unpinPage(pageId, true /* = DIRTY */);
		  
		}
	      
	      // delete empty directory page: (automatically unpinned?)
	      unpinPage(currentDirPageId, false/*undirty*/);
	      freePage(currentDirPageId);
	      
	      
	    }
	  else
	    {
	      // either (the directory page has at least one more datapagerecord
	      // entry) or (it is the first directory page):
	      // in both cases we do not delete it, but we have to unpin it:
	      
	      unpinPage(currentDirPageId, true /* == DIRTY */);
	      
	      
	    }
	}
      return true;
    }
  
  
  /** Updates the specified map in the heapfile.
   * @param mid: the map which needs update
   * @param newmap: the new content of the map
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidUpdateException invalid update on record
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception Exception other exception
   * @return ture:update success   false: can't find the map
   */
  public boolean updateMap(MID mid, Map newmap) 
    throws InvalidSlotNumberException, 
	   InvalidUpdateException, 
	   InvalidTupleSizeException,
	   HFException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   Exception
    {
      boolean status;
      HFPage dirPage = new HFPage();
      PageId currentDirPageId = new PageId();
      HFPage dataPage = new HFPage();
      PageId currentDataPageId = new PageId();
      RID currentDataPageRid = new RID();
      
      status = _findDataPage(mid,
			     currentDirPageId, dirPage, 
			     currentDataPageId, dataPage,
			     currentDataPageRid);
      
      if(status != true) return status;	// map not found
      Map amap = new Map();
      amap = dataPage.returnMap(mid);
      
      // Assume update a map with a map whose length is equal to
      // the original map
      
      if(newmap.size() != amap.size())
	{
	  unpinPage(currentDataPageId, false /*undirty*/);
	  unpinPage(currentDirPageId, false /*undirty*/);
	  
	  throw new InvalidUpdateException(null, "invalid map update");
	  
	}

      // new copy of this record fits in old space;
      amap.mapCopy(newmap);
      unpinPage(currentDataPageId, true /* = DIRTY */);
      
      unpinPage(currentDirPageId, false /*undirty*/);
      
      
      return true;
    }
  
  
  /** Read record from file, returning pointer and length.
   * @param mid Map ID
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception SpaceNotAvailableException no space left
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception Exception other exception
   *
   * @return a Map. if Map==null, no more tuple
   */
  public  Map getMap(MID mid) 
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   Exception
    {
      boolean status;
      HFPage dirPage = new HFPage();
      PageId currentDirPageId = new PageId();
      HFPage dataPage = new HFPage();
      PageId currentDataPageId = new PageId();
      RID currentDataPageRid = new RID();
      
      status = _findDataPage(mid,
			     currentDirPageId, dirPage, 
			     currentDataPageId, dataPage,
			     currentDataPageRid);
      
      if(status != true) return null; // map not found 
      
      Map amap = new Map();
      amap = dataPage.getMap(mid);
      
      /*
       * getMap has copied the contents of mid into mapPtr and fixed up
       * mapLen also.  We simply have to unpin dirpage and datapage which
       * were originally pinned by _findDataPage.
       */    
      
      unpinPage(currentDataPageId,false /*undirty*/);
      
      unpinPage(currentDirPageId,false /*undirty*/);
      
      
      return  amap;  //(true?)OK, but the caller need check if amap==NULL
      
    }
  
  
  /** Initiate a sequential scan.
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   *
   */
  public Scan openScan() 
    throws InvalidTupleSizeException,
	   IOException
    {
      Scan newscan = new Scan(this);
      return newscan;
    }
  
  
  /** Delete the file from the database.
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception FileAlreadyDeletedException file is deleted already
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   */
  public void deleteBigT()  
    throws InvalidSlotNumberException, 
	   FileAlreadyDeletedException, 
	   InvalidTupleSizeException, 
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException
    {
      if(_file_deleted ) 
   	throw new FileAlreadyDeletedException(null, "file alread deleted");
      
      
      // Mark the deleted flag (even if it doesn't get all the way done).
      _file_deleted = true;
      
      // Deallocate all data pages
      PageId currentDirPageId = new PageId();
      currentDirPageId.pid = _firstDirPageId.pid;
      PageId nextDirPageId = new PageId();
      nextDirPageId.pid = 0;
      Page pageinbuffer = new Page();
      HFPage currentDirPage =  new HFPage();
      Tuple atuple;
      
      pinPage(currentDirPageId, currentDirPage, false);
      //currentDirPage.openHFpage(pageinbuffer);
      
      RID rid = new RID();
      while(currentDirPageId.pid != INVALID_PAGE)
	{      
	  for(rid = currentDirPage.firstRecord();
	      rid != null;
	      rid = currentDirPage.nextRecord(rid))
	    {
	      atuple = currentDirPage.getRecord(rid);
	      DataPageInfo dpinfo = new DataPageInfo( atuple);
	      //int dpinfoLen = arecord.length;
	      
	      freePage(dpinfo.pageId);
	      
	    }
	  // ASSERTIONS:
	  // - we have freePage()'d all data pages referenced by
	  // the current directory page.
	  
	  nextDirPageId = currentDirPage.getNextPage();
	  freePage(currentDirPageId);
	  
	  currentDirPageId.pid = nextDirPageId.pid;
	  if (nextDirPageId.pid != INVALID_PAGE) 
	    {
	      pinPage(currentDirPageId, currentDirPage, false);
	      //currentDirPage.openHFpage(pageinbuffer);
	    }
	}
      
      delete_file_entry( _fileName );
    }
  
  /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws HFBufMgrException {
    
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
    }
    
  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
    }

  } // end of unpinPage

  private void freePage(PageId pageno)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: freePage() failed");
    }

  } // end of freePage

  private PageId newPage(Page page, int num)
    throws HFBufMgrException {

    PageId tmpId = new PageId();

    try {
      tmpId = SystemDefs.JavabaseBM.newPage(page,num);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
    }

    return tmpId;

  } // end of newPage

  private PageId get_file_entry(String filename)
    throws HFDiskMgrException {

    PageId tmpId = new PageId();

    try {
      tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
    }
    catch (Exception e) {
      throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
    }

    return tmpId;

  } // end of get_file_entry

  private void add_file_entry(String filename, PageId pageno)
    throws HFDiskMgrException {

    try {
      SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
    }
    catch (Exception e) {
      throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
    }

  } // end of add_file_entry

  private void delete_file_entry(String filename)
    throws HFDiskMgrException {

    try {
      SystemDefs.JavabaseDB.delete_file_entry(filename);
    }
    catch (Exception e) {
      throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
    }

  } // end of delete_file_entry

  /** Called in insertMap, used to update the BTreeFile 
  *  @param mid Map ID of the map whose key is being inserted
  *  @param  mapPtr Map array
  */
  private void insertMapIndex(MID mid, byte[] mapPtr){

  	Map m = new Map(mapPtr, 0, mapPtr.length);

  	try{
	  	switch(SystemDefs.JavabaseDB.type){
	  		default: break;

	  		case 2:
	  				SystemDefs.JavabaseDB.indices[0].insert(new StringKey(m.getRowLabel()), mid);
	  				break;
	  		case 3:
	  				SystemDefs.JavabaseDB.indices[0].insert(new StringKey(m.getColumnLabel()), mid);
	  				break;
	  	}

	}catch (Exception e) {
		System.err.println("***** Error while inserting Map into index insertMapIndex() *******");
      	e.printStackTrace();
      	Runtime.getRuntime().exit(1);
    }

  }


  
}// End of bigT 
