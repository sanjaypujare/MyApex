/**
 * 
 */
package com.example.fileIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

/**
 * @author sanjaypujare
 *
 */
public class Codec4 extends KryoSerializableStreamCodec<StringCount> {
	
	  private static final Logger LOG = LoggerFactory.getLogger(com.example.fileIO.Codec4.class);
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public int getPartition(StringCount tuple) {
    	String word = tuple.word;
    
    	int part;
    	if (word.startsWith("part")) {
    		part = word.charAt(4) - '0';
    		LOG.debug("Returning {} for {}", ""+part, word);
    		return part;
    	} else {
    		LOG.debug("Returning 0 for {}", word);
    		return 0;
    	}
    }

}
