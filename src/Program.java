import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Program extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Only 2 args accepted: Input path and number of cluster centers");
			return -1;
		}
		String inputPath = args[0]; // Data path
		DataLoader dataLoader=new DataLoader();
		dataLoader.setNumberOfClusters(args[1]);
		dataLoader.loadData(args[0]);
		JobRunner jobRunner = new JobRunner();
		jobRunner.setNumberOfClusters(args[1]); // Number of clusters argument
		jobRunner.runJob();
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new Program(), args);
		System.exit(exitCode);
	}
}