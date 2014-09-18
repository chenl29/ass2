package partA;



import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.util.StringUtils;

import utils.*;

import utils.Utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;



public class stepFiveGetResults {

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

			AWSCredentials credentials = null;
			try {
				credentials = new PropertiesCredentials(Utils.class.getResourceAsStream("AwsCredentials.properties"));
			} catch (IOException e) {
				System.out.println("can't load credentials - could not write to file");
				credentials = null;
			}
			AmazonS3Client s3Client = new AmazonS3Client(credentials);
			int k = Integer.parseInt(context.getConfiguration().get("kPairs","10"));

			Iterator<Pair> iterator = values.iterator();
			List<Pair> listOfPairs = new ArrayList<>();
			while (iterator.hasNext()){
				Pair currPair = iterator.next();
				listOfPairs.add(new Pair(currPair));
			}
			sortListDesc(listOfPairs);
			int listSize = listOfPairs.size();
			List<Pair> topKPairs = new ArrayList<>();

			if (key.getDecade() == 2000){
				File allWordsFile = new File(key.getDecade() + "_allWords.txt");
				FileWriter allWordsWriter = new FileWriter(allWordsFile);
				for (int i=0 ; i < listSize; i++){
					if (i<k){
						topKPairs.add(listOfPairs.get(i));
					}
					Pair pair = listOfPairs.get(i);
					String line = pair.getWordOne() + ","
							+ pair.getWordTwo() + "," 
							+ pair.getPMI() + "\n";					
					allWordsWriter.write(line);
				}
				allWordsWriter.close();
				if (credentials != null){
					s3Client.putObject(new PutObjectRequest("lifshitz.ass2.bucket", "output/ALL_PAIRS_" + key.getDecade() + ".txt" , allWordsFile));
				}
			} else {
				for (int i=0 ;i < k && i < listSize; i++){
					topKPairs.add(listOfPairs.get(i));
				}
			}


			File file = new File(key.getDecade() + ".txt");
			FileWriter w = new FileWriter(file);
			w.write("the " + k + " pairs with the highest PMI in " + key.getDecade() + " are: ");

			for (int i=0; i < topKPairs.size() ; i++){
				Pair pair = topKPairs.get(i);
				String line = pair.getWordOne() + ","
						+ pair.getWordTwo() + "," 
						+ pair.getPMI() + "\n";
				w.write(line);
			}
			w.close();

			if (credentials!=null){
				s3Client.putObject(new PutObjectRequest("lifshitz.ass2.bucket", "output/pairs_" + key.getDecade() + ".txt" , file));
			}
		}

		private void sortListDesc(List<Pair> listOfPairs) {
			Collections.sort(listOfPairs, new Comparator(){
				public int compare (Object o1, Object o2){
					Double PMI1 = new Double(((Pair)o1).getPMI());
					Double PMI2 = new Double(((Pair)o2).getPMI());
					return -PMI1.compareTo(PMI2);
				}
			});
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
		conf.set("kPairs", args[1]);
		Job job = new Job(conf, "Assingment2");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setJarByClass(stepFiveGetResults.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionClass.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(Pair.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(Pair.class);
		job.setSortComparatorClass(SortingByDecade.class);
		FileInputFormat.addInputPath(job, new Path("s3n://lifshitz.ass2.bucket/output/StepFourOutputV2/"));
		FileOutputFormat.setOutputPath(job, new Path("s3n://lifshitz.ass2.bucket/output/PartAV2"));
		job.waitForCompletion(true);
		Counter mapOutputCounter = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
		Counter outputSize = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES);
		Utils.writeToFileInS3(mapOutputCounter.getValue() + "(" + outputSize.getValue() + " Bytes)", "StepFive", credentials);
	}


}
