package com.example.fileIO;


import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
//import com.datatorrent.lib.io.fs.AbstractFileOutputOperator.FSFilterStreamContext;

/**
 * Write incoming line to output file
 */
public class FileWriter extends AbstractFileOutputOperator<StringCount>
{
  private static final Logger LOG = LoggerFactory.getLogger(com.example.fileIO.FileWriter.class);
  private static final String CHARSET_NAME = "UTF-8";
  private static final String NL = System.lineSeparator();
  private static final char START_FILE = FileReader.START_FILE, FINISH_FILE = FileReader.FINISH_FILE;

  private String fileName;    // current file name

  private boolean eof;

  /**
   * control port for file start/finish control tuples
   */
  /*
  public final transient DefaultInputPort<String> control = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processControlTuple(tuple);
    }
  };

  private void processControlTuple(final String tuple)
  {
    if (START_FILE == tuple.charAt(0)) {                          // start of file
      LOG.debug("start tuple = {}", tuple);

      // sanity check
      if (null != fileName) {
        //throw new RuntimeException(String.format("Error: fileName = %s, expected null", fileName));
    	return;   // we just use the orig file name used
      }

      fileName = tuple.substring(1);

      return;
    }
    */

    /*
    final int last = tuple.length() - 1;
    if (FINISH_FILE == tuple.charAt(last)) {        // end of file
      LOG.debug("finish tuple = {}", tuple);
      String name = tuple.substring(0, last);

      // sanity check : should match what we got with start control tuple
      if (null == fileName || ! fileName.equals(name)) {
        throw new RuntimeException(String.format("Error: fileName = %s != %s = tuple", fileName, tuple));
      }

      eof = true;
      return;
    }

    // should never happen
    throw new RuntimeException("Error: Bad control tuple: {}" + tuple);
    */
  //}

  @Override
  public void processTuple(StringCount tuple)
  {
    super.processTuple(tuple);
  }
  
  private static int suffix = 1;
  
  private static synchronized String  getFilenameSuffix() {
	  return "" + (suffix++);
  }
  
  @Override
  public void beginWindow(long windowId)
  {
	  if (fileName == null) {
		  fileName = ""+windowId+"."+getFilenameSuffix();
	  }
	  super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
   // if ( ! eof ) {
   // 	LOG.debug("returning since not eof");
    //  return;
    //}

    // got an EOF, so must have a file name
    if (null == fileName) {
      throw new RuntimeException("Error: fileName empty");
    }

    LOG.info("requesting finalize of {}", fileName);
    requestFinalize(fileName);
    super.endWindow();

    //eof = false;
    fileName = null;
  }

  @Override
  protected String getFileName(StringCount tuple)
  {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(StringCount line)
  {
    LOG.debug("getBytesForTuple: line.length = {}", line);

    byte result[] = null;
    try {
      result = (""+line + NL).getBytes(CHARSET_NAME);
    } catch (Exception e) {
      LOG.info("Error: got exception {}", e);
      throw new RuntimeException(e);
    }
    return result;
  }
  
  @Override
  public void setup(Context.OperatorContext context)
  {
	  LOG.debug("setup initiated");
	  
	  String fullPath = super.filePath + "/fw_" + getFileName(null);
	  
	  
	  super.setup(context);
  }

}
