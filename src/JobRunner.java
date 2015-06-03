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
	
	public static void runJob() throws IOException, ClassNotFoundException, InterruptedException{
		try {
			Configuration config = HBaseConfiguration.create();
			Job job = new Job(config, "ExampleReadWrite");
			job.setJarByClass(JobRunner.class);     // class that contains mapper
			Scan scan = new Scan();
			TableMapReduceUtil.initTableMapperJob(
			  "data",        // input HBase table name
			  scan,             // Scan instance to control CF and attribute selection
			  JobRunner.MyMapper.class,   // mapper
			  Text.class,             // mapper output key
			  Text.class,             // mapper output value
			  job);
			job.setReducerClass(JobRunner.MyReducer.class);
			/*job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			*/
			/*
			TableMapReduceUtil.initTableReducerJob(
					"center",
					KMeans.kmeansReducer.class,
					job);
					*/
			String intermediatePath="intermediatePath";
			deleteOutputDirectoryIfExists(config, intermediatePath);
			FileOutputFormat.setOutputPath(job, new Path(intermediatePath));
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
			System.out.println("Job About to Begin");
			boolean b=job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
			System.out.println("Job Completed");
	}
	catch (Exception e) {
		e.printStackTrace();
	}
	
	}
	private static void deleteOutputDirectoryIfExists(Configuration conf, String outputPath) throws IOException {
  	  FileSystem fs = FileSystem.get(conf);
  	  System.out.println("Deleting existing path: "+outputPath);
  	  if(fs.exists(new Path(outputPath))){
  		   fs.delete(new Path(outputPath),true);
  		}
	}
	
	public static class MyMapper extends TableMapper<Text, Text> 
	{
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			HTable Table = new HTable(HBaseConfiguration.create(), "center");
			ResultScanner centerTableScanner = Table.getScanner(new Scan());
			String rowHolder = "";	
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
			//System.out.println("Map");
			String row=buildRow(value);
//			System.out.println(row);
//			System.out.println("--------------------");
//			System.out.println(clusterCenters);
			int clusterAssignment=assignCluster(row,clusterCenters);
			context.write(new Text(Integer.toString(clusterAssignment)), new Text(row));
		}
		
		private static int  assignCluster(String row, List<String> clusterCenters) {
			// TODO Auto-generated method stub
			double[] distances=new double [numberOfClusters];
			for (int i=0;i<numberOfClusters;i++){
				distances[i]=computeEuclideanDistance(row,clusterCenters,i);
			}
			double d= Double.POSITIVE_INFINITY;
			int index=0;
		    for(int i = 0; i < distances.length; i++)
		        if(distances[i] < d) {
		            d = distances[i];
		            index = i;
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

		private static List<Double> parseString(String row) {
			String[] tokens=row.split("\t");
			List<Double> rowVector= new ArrayList<Double>();
			for (int i=0;i<tokens.length;i++){
				rowVector.add(Double.parseDouble(tokens[i]));
			}
			return rowVector;
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
			String row=x1+"\t"+x5+"\t"+x6+"\t"+y1+"\t"+y2+"\t"+x2+"\t"+x3+"\t"+x4+"\t"+x7+"\t"+x8;
			return row;
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	
	}
	
	public static class MyReducer
    extends TableReducer<Text,Text,Text> {
		@Override
		public void reduce(Text key,  Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
			System.out.println("here");
			
		}
		/*
		public MyReducer()
		{
			System.out.println("Reducer : " + arg0.toString());	
		}
		protected void reduce(Text arg0, Iterable<Text> arg1,
				org.apache.hadoop.mapreduce.Reducer.Context arg2)
				throws IOException, InterruptedException {
			
			System.out.println("arg0 : " + arg0.toString());
			arg2.write(new Text("hi"), new Text("hello"));
		}
		*/
				
	}
	public static void setNumberOfClusters(String numOfClusters) {
		numberOfClusters=Integer.parseInt(numOfClusters);
	  }
	private static int numberOfClusters;
	private static List<String> clusterCenters = new ArrayList<String>();
}
