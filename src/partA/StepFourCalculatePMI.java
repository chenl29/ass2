package partA;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.springframework.util.StringUtils;

import utils.*;

import utils.Utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;


 
public class StepFourCalculatePMI {

	public static class MapClass extends
	Mapper<Pair, Pair, Pair, Pair> {


		@Override
		protected void map(Pair key, Pair value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}

	}

	public static class PartitionClass extends
	Partitioner<Pair, Pair> {

		@Override
		public int getPartition(Pair key, Pair value,
				int partitionNum) {
			int ans = (17*key.getDecade()*key.getWordOne().hashCode()*key.getWordTwo().hashCode())% partitionNum;
			while (ans < 0) {
				ans += partitionNum;
			}
			return ans;
		}

	}

	public static class ReduceClass extends
	Reducer<Pair, Pair, Pair, Pair> {

		@Override
		protected void reduce(Pair key, Iterable<Pair> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Pair> iterator = values.iterator();
			List<Pair> listOfPairs = new ArrayList<>();
			int N = 0;
			while (iterator.hasNext()){
				Pair currPair = iterator.next();
				// decade counter
				if (currPair.isBothDummies()){
					N = currPair.getNumberOfOcc();
				} else // regular pair
				if (currPair.isNoneDummy()){
					listOfPairs.add(new Pair(currPair));
				}
			}
			for (Iterator iterator2 = listOfPairs.iterator(); iterator2.hasNext();) {
				Pair pair = (Pair) iterator2.next();
				// calculate PMI
				pair.calculatePMIForPair(N);
				context.write(pair, pair);
			}
		}

	}
	
	
	


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		AWSCredentials credentials = null;
		try {
			credentials = new PropertiesCredentials(Utils.class.getResourceAsStream("AwsCredentials.properties"));
		} catch (IOException e) {
			System.out.println("can't load credentials");
			return;
		}

		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Assingment2");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(StepFourCalculatePMI.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionClass.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(Pair.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(Pair.class);
		job.setSortComparatorClass(SortingByDecade.class);
		FileInputFormat.addInputPath(job, new Path("s3n://lifshitz.ass2.bucket/output/StepThreeOutputV2/"));
		FileOutputFormat.setOutputPath(job, new Path("s3n://lifshitz.ass2.bucket/output/StepFourOutputV2/"));
		job.waitForCompletion(true);
		Counter mapOutputCounter = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
		Counter outputSize = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES);
		Utils.writeToFileInS3(mapOutputCounter.getValue() + "(" + outputSize.getValue() + " Bytes)", "StepFour", credentials);
	}


}
