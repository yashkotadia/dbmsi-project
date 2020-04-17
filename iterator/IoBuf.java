
package iterator;
import BigT.*;
import global.*;
import diskmgr.*;
import bufmgr.*;

import java.io.*;

public class IoBuf implements GlobalConst{
  /**
   * Constructor - use init to initialize.
   */
  public void IoBuf(){}             
  
  /**
   *Initialize some necessary inormation, call Iobuf to create the
   *object, and call init to finish instantiation
   *@param bufs[][] the I/O buffer
   *@param n_pages the numbers of page of this buffer
   *@param tSize the page size
   *@param temp_fd the reference to a Heapfile
   */ 
  public void init(byte bufs[][], int n_pages, int mSize, bigT temp_fd)
    {
      _bufs    = bufs;
      _n_pages = n_pages;
      m_size   = mSize;
      _temp_fd = temp_fd;
      
      dirty       = false;
      m_per_pg    = MINIBASE_PAGESIZE / m_size;
      m_in_buf    = n_pages * m_per_pg;
      m_wr_to_pg  = 0;
      m_wr_to_buf = 0;
      m_written   = 0L;
      curr_page   = 0;
      flushed     = false;
      mode        = WRITE_BUFFER;
      i_buf       = new SpoofIbuf();
      done        = false;
    }
  
  
  /**
   * Writes a tuple to the output buffer
   *@param buf the tuple written to buffer
   *@exception NoOutputBuffer the buffer is a input bufer now
   *@exception IOException  some I/O fault
   *@exception Exception  other exceptions
   */
  public void Put(Map buf)
    throws NoOutputBuffer,
	   IOException,
	   Exception
    {
      if (mode != WRITE_BUFFER)
	throw new NoOutputBuffer("IoBuf:Trying to write to io buffer when it is acting as a input buffer");
      
      byte[] copybuf;
      copybuf = buf.getMapByteArray();
      System.arraycopy(copybuf,0,_bufs[curr_page],m_wr_to_pg*m_size,m_size); 
      
      m_written++; m_wr_to_pg++; m_wr_to_buf++; dirty = true;
      
      if (m_wr_to_buf == m_in_buf)                // Buffer full?
	{
	  flush();                                // Flush it
	  m_wr_to_pg = 0; m_wr_to_buf = 0;        // Initialize page info
	  curr_page  = 0;
	}
      else if (m_wr_to_pg == m_per_pg)
	{
	  m_wr_to_pg = 0;
	  curr_page++;
	}      
      return;
    }

  /**
   *get a tuple from current buffer,pass reference buf to this method
   *usage:temp_tuple = tuple.Get(buf); 
   *@param buf write the result to buf
   *@return the result tuple
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public Map Get(Map  buf)
    throws IOException,
	   Exception
    {
      Map tempmap;
      if (done){
	buf =null;
	return null;
      }
      if (mode == WRITE_BUFFER)     // Switching from writing to reading?
	reread();
      
      if (flushed)
	{
	  // get maps from 
	  if ((tempmap= i_buf.Get(buf)) == null)
	    {
	      done = true;
	      return null;
	    }
	}
      else
	{
	  // just reading tuples from the buffer pages.
	  if ((curr_page * m_per_pg + m_rd_from_pg) == m_written)
	    {
	      done = true;
	      buf = null;
	      return null;
	    }
	  buf.mapSet(_bufs[curr_page],m_rd_from_pg*m_size,m_size);      
	  
	  // Setup for next read
	  m_rd_from_pg++;
	  if (m_rd_from_pg == m_per_pg)
	    {
	      m_rd_from_pg = 0; curr_page++;
	    }
	}
      
      return buf;
    }
  
  
  /**
   * returns the numbers of tuples written
   *@return the numbers of tuples written
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public long flush()throws IOException, Exception 
    {
      int count;
      byte [] tempbuf = new byte [m_size];
      
      flushed = true;
      if (dirty)
	{
	  for (count = 0; count <= curr_page; count++)
	    {
	      MID mid;

	      // Will have to go thru entire buffer writing tuples to disk
	      for (int i = 0; i < m_wr_to_pg; i++)
		{
		  System.arraycopy(_bufs[count],m_size*i,tempbuf,0,m_size);
		  try {
		    mid =  _temp_fd.insertMap(tempbuf);
		  }
		  catch (Exception e){
		    throw e;
		  }
		}
	    }
	  dirty = false;
	}
      
      return m_written;
    }
  
  /**
   *if WRITE_BUFFER is true, call this mehtod to switch to read buffer.
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public void reread()
    throws IOException,
	   Exception
    {
      
      mode = READ_BUFFER;
      if (flushed)                   // Has the output buffe been flushed?
	{
	  // flush all the remaining tuples to disk.
	  flush();
	  i_buf.init(_temp_fd, _bufs, _n_pages, m_size, (int)m_written);
	}
      else
	{
	  // All the tuples are in the buffer, just read them out.
	  m_rd_from_pg = 0;
	  curr_page    = 0;
	}
    }

  public void close(){
    i_buf.close();

  }   
  
  public static final int WRITE_BUFFER =0;
  public static final int READ_BUFFER  =1;
  private boolean done;
  private  boolean dirty;              // Does this buffer contain dirty pages?
  private  int  m_per_pg,              // # of tuples that fit in 1 page
    m_in_buf;                        // # of tuples that fit in the buffer
  private  int  m_wr_to_pg,          // # of tuples written to current page
    m_wr_to_buf;                      // # of tuples written to buffer.
  private  int  curr_page;            // Current page being written to.
  private  byte _bufs[][];            // Array of pointers to buffer pages.
  private  int  _n_pages;             // number of pages in array
  private  int  m_size;               // Size of a tuple
  private  long m_written;           // # of tuples written so far
  private  int  _TEST_temp_fd;       // fd of a temporary file
  private  bigT _temp_fd;
  private  boolean  flushed;        // TRUE => buffer has been flushed.
  private  int  mode;
  private  int  m_rd_from_pg;      // # of tuples read from current page
  private  SpoofIbuf i_buf;        // gets input from a temporary file
}








