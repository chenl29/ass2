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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
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


/**
 * This step reads the corpos and split them to Pairs
 **/
public class StepOneCreateAllPairs {

	public static class MapClass extends
	Mapper<LongWritable, Text, Pair, IntWritable> {



		HashSet<String> stopWordsAsSet = new HashSet<String>();



		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			String[] stopWordsAsArr = Utils.stopWordsAsArr;
			for (String s : stopWordsAsArr) {
				stopWordsAsSet.add(s.replaceAll("'", ""));
			}

			super.setup(context);
		}


		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String valueAsStr = value.toString();
			StringTokenizer st = new StringTokenizer(valueAsStr, "\t");
			if (st.countTokens() < 3){
				return;
			}
			String line = st.nextToken();
			String decadeAsStr = st.nextToken();
			String occurencesAsStr = st.nextToken();

			int decade;
			int occurences;
			try {
				int year = Integer.valueOf(decadeAsStr);
				if (year < 1900){
					return;
				}
				decade = year - (year % 10);
				occurences = Integer.valueOf(occurencesAsStr);
			} catch (NumberFormatException e) {
				return;
			}

			line = line.replaceAll("[^a-zA-Z ]", "").toLowerCase();
			String[] words = line.split(" ");

			// 5-gram dataset
			if (words.length < 2 || words.length > 5) {
				return;
			}

			int index = words.length / 2;
			String mid = words[index];
			if (stopWordsAsSet.contains(mid)) {
				return;
			}
			List<Pair> allPairs = new ArrayList<>();

			// count mid
			allPairs.add(new Pair(mid, Utils.getDummyString(), occurences, decade));

			int numberOfWordsRelaventInLine = 1;
			for (int i = 0; i < words.length; i++) {
				String currWord = words[i];
				if (mid.equals(currWord) || stopWordsAsSet.contains(currWord)) {
					continue;
				} else {

					// take care of words order
					String wordOne = mid;
					String wordTwo = currWord;
					if (mid.compareTo(currWord) > 0){
						String tmp = mid;
						wordOne = currWord;
						wordTwo = tmp;
					} 

					// count the pair
					allPairs.add(new Pair(wordOne, wordTwo, occurences, decade));

					// count the currWord
					allPairs.add(new Pair(currWord, Utils.getDummyString(), occurences, decade));

					numberOfWordsRelaventInLine++;
				}
			}
			// count the decade
			int numberOfTotalOccurences = occurences * numberOfWordsRelaventInLine;
			allPairs.add(new Pair(Utils.getDummyString(), Utils.getDummyString(), numberOfTotalOccurences, decade));

			Iterator<Pair> iterator = allPairs.iterator();
			while (iterator.hasNext()){
				Pair currPair = iterator.next();
				context.write(currPair, new IntWritable(currPair.getNumberOfOcc()));
			}
		}

	}

	public static class PartitionClass extends
	Partitioner<Pair, IntWritable> {

		@Override
		public int getPartition(Pair key, IntWritable value,
				int partitionNum) {
			int ans = (17*key.getDecade()*key.getWordOne().hashCode()*key.getWordTwo().hashCode())% partitionNum;
			while (ans < 0) {
				ans += partitionNum;
			}
			return ans;
		}

	}

	public static class ReduceClass extends
	Reducer<Pair, IntWritable, Pair, IntWritable> {

		@Override
		protected void reduce(Pair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()){
				IntWritable currInt = iterator.next();
				sum += currInt.get();
			}
			key.setNumberOfOcc(sum);
			// write each pair with the number of time it appeared
			context.write(key, new IntWritable(sum));
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
		job.setJarByClass(StepOneCreateAllPairs.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(PartitionClass.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("s3n://lifshitz.ass2.bucket/output/StepOneOutputV2/"));
		job.waitForCompletion(true);
		Counter mapOutputCounter = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
		Counter outputSize = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES);
		Utils.writeToFileInS3(mapOutputCounter.getValue() + "(" + outputSize.getValue() + " Bytes)", "StepOne", credentials);
	}


}
