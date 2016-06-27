/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example.fileIO;


import java.util.HashMap;
import java.util.Map;

//import org.apache.commons.lang.mutable.MutableInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

/**
 * Aggregator operator that receives tuples from input connectors
 */
public class StringCountAggregator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(StringCountAggregator.class);
  
  protected Map<String, Integer> stringCounts = new HashMap<String, Integer>();


  public final transient DefaultInputPort<StringCount> input = new DefaultInputPort<StringCount>()
  {
    @Override
    public void process(StringCount sc)
    {
    	Integer cnt = stringCounts.get(sc.word);
    	LOG.info("StringCountAggregator: process, sc="+sc+", cnt="+cnt);	
    	
    	if (cnt == null) {
    		cnt = 0;
    	}
    	cnt += sc.freq;
    	LOG.info("StringCountAggregator: process, putting word="+sc.word+", cnt="+cnt);
    	stringCounts.put(sc.word, cnt);
    }
  };

  public final transient DefaultOutputPort<StringCount>
  output = new DefaultOutputPort<StringCount>();
 

  @Override
  public void setup(OperatorContext context)
  {
	  if (null == stringCounts) {
		  stringCounts = new HashMap<String, Integer>();
	  }
  }

  @Override
  public void endWindow()
  {
    LOG.info("StringCountAggregator: endWindow");

    if (stringCounts.isEmpty() == false) {
    	for (Map.Entry<String, Integer> entry : stringCounts.entrySet()) {
    		StringCount sc = new StringCount(entry.getKey(), entry.getValue());
    		
    		output.emit(sc);
    	}
    	stringCounts.clear();
    }

  }


}
