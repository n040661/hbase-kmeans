import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
/**
 * @author prateek
 *
 */
public class DataLoader {
	/**
	 * Used to load dataset and initial cluster centers into 2 different HBase tables
	 * 
	 */
		  public void setInputPath(String input){
			  DatasetInputPath=input;
		  }
		  
		  public void createTable(String tableName, HBaseAdmin admin, String[] family) throws IOException{
		      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		      for (int i=0;i<family.length;i++){
		    	  tableDescriptor.addFamily(new HColumnDescriptor(family[i]));  
		      }
		      admin.createTable(tableDescriptor);
		  }
		  @SuppressWarnings("deprecation")
		public int loadData(String input) throws IOException, ClassNotFoundException, InterruptedException{
			   setInputPath(input);
			   Configuration conf = HBaseConfiguration.create(); 
			   HBaseAdmin admin = new HBaseAdmin(conf);
	           String inputPath = DatasetInputPath;  
	           String outputPath = "./output_hbase"; 
	           deleteOutputDirectoryIfExists(conf,outputPath);
	           deletetableIfExists(DataTableName, admin);
	           String[] dataFamily={"Area","Property"};
	           createTable(DataTableName, admin,dataFamily);
	           HTable dataHTable = new HTable(conf, DataTableName);  
	           @SuppressWarnings("deprecation")
			   Job jobData = new Job(conf,"Data_Loader");
	           jobData.setMapOutputKeyClass(ImmutableBytesWritable.class);  
	           jobData.setMapOutputValueClass(Put.class);  
	           jobData.setSpeculativeExecution(false);  
	           jobData.setReduceSpeculativeExecution(false);  
	           jobData.setInputFormatClass(TextInputFormat.class);  
	           jobData.setOutputFormatClass(HFileOutputFormat.class);  
	           jobData.setJarByClass(DataLoader.class);  
	           jobData.setMapperClass(DataLoader.BulkLoadMap.class);  
	           FileInputFormat.setInputPaths(jobData, inputPath);  
	           FileOutputFormat.setOutputPath(jobData,new Path(outputPath));
	           HFileOutputFormat.configureIncrementalLoad(jobData, dataHTable);
	           
	           boolean b=jobData.waitForCompletion(true);
				if (!b) {
					throw new IOException("Error with loading dataset into table!");
				}
	           createCenterTable(admin, dataFamily);
	           return -1;
		  }
	      private void createCenterTable(HBaseAdmin admin, String[] dataFamily) throws IOException {
	    	  deletetableIfExists(CenterTableName, admin);
	          createTable(CenterTableName, admin,dataFamily);
	          HTable datatable = new HTable(HBaseConfiguration.create(), DataTableName);
	          HTable centertable = new HTable(HBaseConfiguration.create(), CenterTableName);
	          Scan scan = new Scan();
	          ResultScanner scanner = datatable.getScanner(scan);
	          for (int i=0;i<numberOfClusters;i++) {
	        	  Result result = scanner.next();
	        	  Put HPut = new Put(Bytes.toBytes(Integer.toString(i+1)));
	        	    for(KeyValue keyValue : result.raw()) {
	        	        HPut.add(keyValue.getFamily(), keyValue.getQualifier(), keyValue.getValue());
	        	    }
	        	    centertable.put(HPut);
	        	}
	          scanner.close();
	          datatable.close();
	          centertable.close();
	          return;
		}

		private void deletetableIfExists(String tableName, HBaseAdmin admin) throws IOException {
	    	  if (admin.tableExists(tableName)){
	    		  System.out.println("Deleting existing table: "+tableName);
	    		  admin.disableTable(tableName);
	    		  admin.deleteTable(tableName);
	    	  }
		}
		private void deleteOutputDirectoryIfExists(Configuration conf, String outputPath) throws IOException {
	    	  FileSystem fs = FileSystem.get(conf);
	    	  if(fs.exists(new Path(outputPath))){
	    		   fs.delete(new Path(outputPath),true);
	    		}
			
		}
		public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
			/**
			 * Mapreduce job to load dataset into table
			 */
			   public static enum Counters { ROWNUMBER }
	           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {        
	        	    context.getCounter(Counters.ROWNUMBER).increment(1);
	        	   	String[] column = new String[10];
	                column = value.toString().split("\\s");
	                Put HPut = new Put(Bytes.toBytes(key.toString())); 
	                HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("X1"), Bytes.toBytes(column[0]));  
	                HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("X5"), Bytes.toBytes(column[4]));  
	                HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("X6"), Bytes.toBytes(column[5]));  
	                HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("Y1"), Bytes.toBytes(column[8]));  
	                HPut.add(Bytes.toBytes("Area"), Bytes.toBytes("Y2"), Bytes.toBytes(column[9]));  
	                HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X2"), Bytes.toBytes(column[1]));  
	                HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X3"), Bytes.toBytes(column[2]));  
	                HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X4"), Bytes.toBytes(column[3]));  
	                HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X7"), Bytes.toBytes(column[6]));  
	                HPut.add(Bytes.toBytes("Property"), Bytes.toBytes("X8"), Bytes.toBytes(column[7])); 
	                Configuration conf = HBaseConfiguration.create(); 
	                HTable hTable = new HTable(conf, DataTableName);
	                hTable.put(HPut);
	                hTable.close();
	           }   
	      }
		  public void setNumberOfClusters(String numOfClusters) {
			numberOfClusters=Integer.parseInt(numOfClusters);
		  }
	      private String DatasetInputPath;
	      private static String DataTableName="data";
	      private static String CenterTableName="center";
	      private static int numberOfClusters;
		
}