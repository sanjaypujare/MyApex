package com.example.fileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.BaseOperator;

public class PartitionedFileOutput extends BaseOperator implements Partitioner<PartitionedFileOutput> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileOutput.class);

	@Min(1) @Max(20)
	private int nPartitions = 2;

	private transient int id;             // operator/partition id
	private transient long curWindowId;   // current window id
	private transient long cnt;           // per-window tuple count
	
	  /**
	   * The path of the directory to where files are written.
	   */
	  @NotNull
	  protected String filePath;
	  
	  public PartitionedFileOutput() {
		  LOG.debug("entering ctor");
	  }

	public PartitionedFileOutput(String filePath2) {
		this.filePath = filePath2;
	}

	public final transient DefaultInputPort<StringCount> input = new DefaultInputPort<StringCount>() {
		@Override
		public void process(StringCount tuple)
		{
			LOG.debug("{}: tuple = {}, operator id = {}", cnt, tuple, id);
			++cnt;
		}
	};

	  /**
	   * The file system used to write to.
	   */
	  protected transient FileSystem fs;
	  
	  /**
	   * The replication level for your output files.
	   */
	  @Min(0)
	  protected int replication = 0;



	@Override
	public Collection<com.datatorrent.api.Partitioner.Partition<PartitionedFileOutput>> definePartitions(
			Collection<com.datatorrent.api.Partitioner.Partition<PartitionedFileOutput>> partitions,
			com.datatorrent.api.Partitioner.PartitioningContext context) {
		int oldSize = partitions.size();
		LOG.debug("partitionCount: current = {} requested = {}", oldSize, nPartitions);

		// each partition i in 0...nPartitions receives tuples divisible by i but not by any other
		// j in that range; all other tuples ignored
		//
		if (2 != nPartitions) return getPartitions(partitions, context);

		// special case of 2 partitions: part1 to 1 and part2 to 2.

		// mask used to extract discriminant from tuple hashcode
		int mask = 0x01;

		Partition<PartitionedFileOutput>[] newPartitions = new Partition[] {
				new DefaultPartition<PartitionedFileOutput>(new PartitionedFileOutput(this.filePath)),
				new DefaultPartition<PartitionedFileOutput>(new PartitionedFileOutput(this.filePath)) };

		HashSet<Integer>[] set
		= new HashSet[] {new HashSet<>(), new HashSet<>(), new HashSet<>()};
		set[0].add(0);
		set[1].add(1);

		PartitionKeys[] keys = {
				new PartitionKeys(mask, set[0]),
				new PartitionKeys(mask, set[1])};

		for (int i = 0; i < 2; ++i ) {
			Partition<PartitionedFileOutput> partition = newPartitions[i];
			partition.getPartitionKeys().put(input, keys[i]);
		}

		return new ArrayList<Partition<PartitionedFileOutput>>(Arrays.asList(newPartitions));
	}

	private Collection<Partition<PartitionedFileOutput>> getPartitions(
			Collection<Partition<PartitionedFileOutput>> partitions,
			PartitioningContext context)
	{
		// create array of partitions to return
		Collection<Partition<PartitionedFileOutput>> result
		= new ArrayList<Partition<PartitionedFileOutput>>(nPartitions);

		int mask = getMask(nPartitions);
		for (int i = 0; i < nPartitions; ++i) {
			HashSet<Integer> set = new HashSet<>();
			set.add(i);
			PartitionKeys keys = new PartitionKeys(mask, set);
			Partition partition = new DefaultPartition<PartitionedFileOutput>(new PartitionedFileOutput());
			partition.getPartitionKeys().put(input, keys);
		}

		return result;
	}  // getPartitions

	// return mask with bits 0..N set where N is the highest set bit of argument
	private int getMask(final int n) {
		return -1 >>> Integer.numberOfLeadingZeros(n);
	}  // getMask

	@Override
	public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<PartitionedFileOutput>> partitions) {
		//Do nothing

	}

	  /**
	   * Override this method to change the FileSystem instance that is used by the operator.
	   * This method is mainly helpful for unit testing.
	   * @return A FileSystem object.
	   * @throws IOException
	   */
	  protected FileSystem getFSInstance() throws IOException
	  {
	    FileSystem tempFS = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());

	    if (tempFS instanceof LocalFileSystem) {
	      tempFS = ((LocalFileSystem)tempFS).getRaw();
	    }

	    return tempFS;
	  }
	
	@Override
	public void setup(Context.OperatorContext context)
	{
		super.setup(context);

		long appWindowId = context.getValue(OperatorContext.ACTIVATION_WINDOW_ID);
		id = context.getId();
		LOG.debug("Started setup, appWindowId = {}, operator id = {}", appWindowId, id);
		
	    //Getting required file system instance.
	    try {
	      fs = getFSInstance();
	    } catch (IOException ex) {
	      throw new RuntimeException(ex);
	    }

	    if (replication <= 0) {
	      replication = fs.getDefaultReplication(new Path(filePath));
	    }

	    LOG.debug("FS class {}", fs.getClass());
	}


	@Override
	public void beginWindow(long windowId)
	{
		cnt = 0;
		curWindowId = windowId;
		LOG.debug("window id = {}, operator id = {}", curWindowId, id);
	}

	@Override
	public void endWindow()
	{
		LOG.debug("window id = {}, operator id = {}, cnt = {}", curWindowId, id, cnt);
	}

	// accessors
	public int getNPartitions() { return nPartitions; }
	public void setNPartitions(int v) { nPartitions = v; }

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

}
