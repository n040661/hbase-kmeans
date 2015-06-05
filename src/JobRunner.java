import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class JobRunner {
	/**
	 * Used to run jobs and set stopping conditions based on values of counter 
	 */
	public static void runJob() throws IOException, ClassNotFoundException, InterruptedException{
		try {
			int maxIterations=20;
			boolean keepIterating=true;
			for (int i=0;i<maxIterations && keepIterating;i++){			
				Configuration config = HBaseConfiguration.create();
				Job job = new Job(config, "KMeans --Iteration--"+Integer.toString(i));
				job.setJarByClass(JobRunner.class);     // class that contains mapper
				Scan scan = new Scan();
				scan.setCaching(800);
				scan.setCacheBlocks(false);  
				TableMapReduceUtil.initTableMapperJob(
				  "data",        // input HBase table name
				  scan,             // Scan instance to control CF and attribute selection
				  JobRunner.KMeansMapper.class,   // mapper
				  Text.class,             // mapper output key
				  Text.class,             // mapper output value
				  job);
				job.setReducerClass(JobRunner.KMeansReducer.class);
				String intermediatePath="intermediatePath";
				deleteOutputDirectoryIfExists(config, intermediatePath);
				FileOutputFormat.setOutputPath(job, new Path(intermediatePath));
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
			    job.setOutputFormatClass(TextOutputFormat.class);
				boolean b=job.waitForCompletion(true);
				if (!b) {
					throw new IOException("error with job!");
				}
				long reducerCounterValue=job.getCounters().findCounter(JobRunner.KMeansReducer.Counters.UPDATECENTER).getValue();
				keepIterating=reducerCounterValue>0;
			}
				System.out.println("Jobs Completed");
	}
	catch (Exception e) {
		e.printStackTrace();
	}
	}
	private static void deleteOutputDirectoryIfExists(Configuration conf, String outputPath) throws IOException {
  	  FileSystem fs = FileSystem.get(conf);
  	  if(fs.exists(new Path(outputPath))){
  		  fs.delete(new Path(outputPath),true);
  		}
	}
	public static class KMeansMapper extends TableMapper<Text, Text> 
	{
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			HTable Table = new HTable(HBaseConfiguration.create(), "center");
			ResultScanner centerTableScanner = Table.getScanner(new Scan());
			String rowHolder = "";
			clusterCenters.clear();
			for (Result r : centerTableScanner) {
				for (KeyValue keyValue : r.raw()) {
					rowHolder= rowHolder + new String(keyValue.getValue()) + "\t";
				}
				clusterCenters.add(rowHolder);
				rowHolder = "";
			}
		}
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			String row=buildRow(value);
			int clusterAssignment=assignCluster(row,clusterCenters);
			context.write(new Text(Integer.toString(clusterAssignment)), new Text(row));
		}
		
		private static int  assignCluster(String row, List<String> clusterCenters) {
			double[] distances=new double [numberOfClusters];
			for (int i=0;i<numberOfClusters;i++){
				distances[i]=computeEuclideanDistance(row,clusterCenters,i);
			}
			double d= Double.POSITIVE_INFINITY;
			int index=0;
		    for(int i = 0; i < distances.length; i++){
		        if(distances[i] < d) {
		        	d = distances[i];
		            index = i;
		        }
		    }
		    return index+1;
		}

		private static double computeEuclideanDistance(String row,
				List<String> clusterCenters, int i) {
			String clusterCenter=clusterCenters.get(i);
			List<Double> rowVector = parseString(row);
			List<Double> clusterVector = parseString(clusterCenter);
			
			double distance=0;
			for (int j=0;j<rowVector.size();j++){
				distance=distance+Math.pow((rowVector.get(j)-clusterVector.get(j)),2.0);
			}
			return distance;
		}
		private String buildRow(Result value) {
			String x1 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X1")));
			String x2 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X2")));
			String x3 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X3")));
			String x4 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X4")));
			String x5 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X5")));
			String x6 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("X6")));
			String x7 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X7")));
			String x8 = new String(value.getValue(Bytes.toBytes("Property"), Bytes.toBytes("X8")));
			String y1 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("Y1")));
			String y2 = new String(value.getValue(Bytes.toBytes("Area"), Bytes.toBytes("Y2")));
			String row=x1+"\t"+x5+"\t"+x6+"\t"+y1+"\t"+y2+"\t"+x2+"\t"+x3+"\t"+x4+"\t"+x7+"\t"+x8+"\t";
			return row;
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
	public static class KMeansReducer
    extends TableReducer<Text,Text,Text> {
		 public static enum Counters { UPDATECENTER }
		@Override
		public void reduce(Text key,  Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
			List<String> oldClusterCenters = new ArrayList<String>();
			HTable Table = new HTable(HBaseConfiguration.create(), "center");
			ResultScanner centerTableScanner = Table.getScanner(new Scan());
			String rowHolder = "";
			for (Result r : centerTableScanner) {
				for (KeyValue keyValue : r.raw()) {
					rowHolder= rowHolder + new String(keyValue.getValue()) + "\t";
				}
				oldClusterCenters.add(rowHolder);
				rowHolder = "";
			}
			StringBuilder reducerRowBuilder = new StringBuilder();
			for (Text val : values) {
				reducerRowBuilder.append(val.toString());
			}
			String row=reducerRowBuilder.toString();
			List<Double> rowVectors=parseString(row);
			int numberOfRows=(int)(rowVectors.size()/10);
			int numberOfColumns=10;
			double [][] Matrix =new double[numberOfRows][numberOfColumns];
			for (int i=0;i<numberOfRows;i++){
				for (int j=0;j<numberOfColumns;j++){
					Matrix[i][j]=rowVectors.get(i*numberOfColumns+j);	
				}
			}
			List<Double> newClusterCenter=new ArrayList<Double>();
			double tempHolder=0;
			for (int j=0;j<numberOfColumns;j++){
				tempHolder=0;
				for (int i=0;i<numberOfRows;i++){
					tempHolder=tempHolder+Matrix[i][j];
				}
				newClusterCenter.add(tempHolder/numberOfRows);
			}
			Put HPut = new Put(Bytes.toBytes(key.toString())); 
            HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("X1"), Bytes.toBytes(Double.toString(newClusterCenter.get(0))));  
            HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("X5"), Bytes.toBytes(Double.toString(newClusterCenter.get(1))));  
            HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("X6"), Bytes.toBytes(Double.toString(newClusterCenter.get(2))));  
            HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("Y1"), Bytes.toBytes(Double.toString(newClusterCenter.get(3))));  
            HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("Y2"), Bytes.toBytes(Double.toString(newClusterCenter.get(4))));  
            HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X2"), Bytes.toBytes(Double.toString(newClusterCenter.get(5))));  
            HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X3"), Bytes.toBytes(Double.toString(newClusterCenter.get(6))));  
            HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X4"), Bytes.toBytes(Double.toString(newClusterCenter.get(7))));  
            HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X7"), Bytes.toBytes(Double.toString(newClusterCenter.get(8))));  
            HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X8"), Bytes.toBytes(Double.toString(newClusterCenter.get(9)))); 
            Configuration conf = HBaseConfiguration.create(); 
            HTable hTable = new HTable(conf, "center");
            hTable.put(HPut);
            double distance=computeEuclideanDistance(oldClusterCenters, newClusterCenter);
            if (distance>0.01){
            	context.getCounter(Counters.UPDATECENTER).increment(1);
            }
			}
			
		private static List<Double> parseString(String row) {
			String[] tokens=row.split("\\s");
			List<Double> rowVector= new ArrayList<Double>();
			for (int i=0;i<tokens.length;i++){
				rowVector.add(Double.parseDouble(tokens[i]));
			}
			return rowVector;
		}
	}
	public static void setNumberOfClusters(String numOfClusters) {
		numberOfClusters=Integer.parseInt(numOfClusters);
	  }
	private static int numberOfClusters;
	private static List<String> clusterCenters = new ArrayList<String>();
	private static double computeEuclideanDistance(List<String> oldClusterCenters,
			List<Double> clusterCenters) {
		double distance=0;
		for (int i=0;i<oldClusterCenters.size();i++){
			String oldClusterCenter=oldClusterCenters.get(i);
			List<Double> clusterVector =clusterCenters;
			List<Double> oldClusterVector = parseString(oldClusterCenter);
			for (int j=0;j<clusterVector.size();j++){
				distance=distance+Math.pow((clusterVector.get(j)-oldClusterVector.get(j)),2.0);
			}
		}
		return distance;
	}
	private static List<Double> parseString(String row) {
		String[] tokens=row.split("\\s");
		List<Double> rowVector= new ArrayList<Double>();
		for (int i=0;i<tokens.length;i++){
			rowVector.add(Double.parseDouble(tokens[i]));
		}
		return rowVector;
	}
}
