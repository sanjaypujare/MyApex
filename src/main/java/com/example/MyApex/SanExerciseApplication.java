/**
 * Put your copyright and license info here.
 */
package com.example.MyApex;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.example.fileIO.Codec4;
import com.example.fileIO.FileReader;
import com.example.fileIO.FileReaderMultiDir;
import com.example.fileIO.FileWriter;
import com.example.fileIO.PartitionedFileOutput;
import com.example.fileIO.StringCountAggregator;

@ApplicationAnnotation(name="FileIO")
public class SanExerciseApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
	    // create operators                                                                                  
	    FileReader reader = dag.addOperator("read",  FileReader.class);
	    StringCountAggregator aggr = dag.addOperator("aggregator", StringCountAggregator.class);
	    //FileWriter writer = dag.addOperator("write", FileWriter.class);
	    PartitionedFileOutput writer = dag.addOperator("write", PartitionedFileOutput.class);

	    reader.setScanner(new FileReaderMultiDir.SlicedDirectoryScanner());
	    
	    // next 2 lines added to try custom partitioning
	    Codec4 codec = new Codec4();
	    dag.setInputPortAttribute(writer.input, PortContext.STREAM_CODEC, codec);

	    // using parallel partitioning ensures that lines from a single file are handled                     
	    // by the same writer                                                                                
	    //                                                                                                   
	    //dag.setInputPortAttribute(writer.input, PortContext.PARTITION_PARALLEL, true);
	    dag.setInputPortAttribute(aggr.input, PortContext.PARTITION_PARALLEL, true);
	    //dag.setInputPortAttribute(writer.control, PortContext.PARTITION_PARALLEL, true);
	    
	    dag.addStream("data", reader.output, aggr.input);

	    dag.addStream("odata", aggr.output, writer.input);
	    //dag.addStream("ctrl", reader.control, writer.control);

  }
}
