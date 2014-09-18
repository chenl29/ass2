package partA;

import java.io.IOException;

import utils.Utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Ass2PartA {
	public static AWSCredentials credentials;
	public static String bucketLocation = "s3n://lifshitz.ass2.bucket/";
	public static String firstJar = "StepOne.jar";
	public static String secondJar = "StepTwo.jar";
	public static String thirdJar = "StepThree.jar";
	public static String fourthJar = "StepFour.jar";
	public static String fifthJar = "StepFive.jar";
	public static String stepOneMainClass = "StepOneCreateAllPairs";
	public static String stepTwoMainClass = "StepTwoCountFirstWordInPair";
	public static String stepThreeMainClass = "StepThreeCountSecondWordInPair";
	public static String stepFourMainClass = "StepFourCountSecondWordInPair";
	public static String stepFiveMainClass = "stepFiveGetResults";
	public static String inputFile = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data";
//	public static String inputFile = bucketLocation + "eng.corp.10k";
	//	public static String inputFile = bucketLocation + "testInput.txt";
	public static void main(String[] args){
		try {
			credentials = new PropertiesCredentials(Utils.class.getResourceAsStream("AwsCredentials.properties"));
		} catch (IOException e) {
			System.out.println("can't load credentials");
			return;
		}
		if (args.length < 1){
			System.out.println("not enough arguments");
			return;
		}
		AmazonElasticMapReduce elasticMR = new AmazonElasticMapReduceClient(credentials);
		
		System.out.println("creating run flow request");
		RunJobFlowRequest runFlowRequest = createRunFlowRequest(args);
		
		System.out.println("running job flow");
		RunJobFlowResult runJobFlowResult = elasticMR.runJobFlow(runFlowRequest);
		
		System.out.println("waiting for work to finish");
		waitForWorkToFinish(elasticMR, runJobFlowResult);

		System.out.println("done");
	}
	private static void waitForWorkToFinish(AmazonElasticMapReduce elasticMR,
			RunJobFlowResult runJobFlowResult) {
		String jobFlowId = runJobFlowResult.getJobFlowId();
		DescribeJobFlowsRequest jobAttributesRequest = new DescribeJobFlowsRequest().withJobFlowIds(jobFlowId);
		DescribeJobFlowsResult jobAttributes =  elasticMR.describeJobFlows(jobAttributesRequest);
		JobFlowDetail jobDetail = jobAttributes.getJobFlows().get(0);
		while (true){
			try {
				System.out.println("went to sleep");
				Thread.sleep(10000);

			} catch (InterruptedException e) {
			}
			String state = jobDetail.getExecutionStatusDetail().getState();
			System.out.println("woke up - checking if job is finished: " + state);
			if (
				state.equals("COMPLETED") ||
				state.equals("FAILED") ||
				state.equals("TERMINATED")
				){
				break;
			}
			jobAttributes = elasticMR.describeJobFlows(jobAttributesRequest);
			jobDetail = jobAttributes.getJobFlows().get(0);
		}
	}
	private static RunJobFlowRequest createRunFlowRequest(String[] args) {
		HadoopJarStepConfig stepOne = new HadoopJarStepConfig()
		.withJar(bucketLocation + firstJar)
		.withMainClass(stepOneMainClass).withArgs(inputFile);
		StepConfig firstStepConfig = new StepConfig().withName("StepOne")
				.withHadoopJarStep(stepOne)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig stepTwo = new HadoopJarStepConfig()
		.withJar(bucketLocation + secondJar)
		.withMainClass(stepTwoMainClass).withArgs();
		StepConfig secondStepConfig = new StepConfig().withName("stepTwo")
				.withHadoopJarStep(stepTwo)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig stepThree = new HadoopJarStepConfig()
		.withJar(bucketLocation + thirdJar)
		.withMainClass(stepThreeMainClass).withArgs();
		StepConfig thirdStepConfig = new StepConfig().withName("stepThree")
				.withHadoopJarStep(stepThree)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig stepFour = new HadoopJarStepConfig()
		.withJar(bucketLocation + fourthJar)
		.withMainClass(stepFourMainClass).withArgs();
		StepConfig fourthStepConfig = new StepConfig().withName("stepFour")
				.withHadoopJarStep(stepFour)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig stepFive = new HadoopJarStepConfig()
		.withJar(bucketLocation + fifthJar)
		.withMainClass(stepFiveMainClass).withArgs(args[0]);
		StepConfig fifthStepConfig = new StepConfig().withName("stepFive")
				.withHadoopJarStep(stepFive)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		.withInstanceCount(20)
		.withMasterInstanceType(InstanceType.M1Medium.toString())
		.withSlaveInstanceType(InstanceType.M1Medium.toString())
		.withEc2KeyName("Chen Lifshitz")
		.withKeepJobFlowAliveWhenNoSteps(false)
		.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		.withName("Assingment2")
		.withInstances(instances)
		.withAmiVersion("3.1.0")
		.withSteps(
				firstStepConfig, 
				secondStepConfig, 
				thirdStepConfig, 
				fourthStepConfig,
				fifthStepConfig
				)
				.withLogUri(bucketLocation + "logs/");
		return runFlowRequest;
	}
}
