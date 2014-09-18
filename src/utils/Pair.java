package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>{
	private String wordOne;
	private String wordTwo;
	private int numberOfOcc;
	private int decade;
	private int counterOne;
	private int counterTwo;
	private String PMI;

	public Pair(){

	}

	public Pair(String wordOne, String wordTwo, int numberOfOcc, int decade){
		this.wordOne = wordOne;
		this.wordTwo = wordTwo;
		this.numberOfOcc = numberOfOcc;
		this.decade = decade;
		this.PMI = "0";
	}

	public Pair(Pair other){
		this.wordOne = other.wordOne;
		this.wordTwo = other.wordTwo;
		this.numberOfOcc = other.numberOfOcc;
		this.decade = other.decade;
		this.counterOne = other.counterOne;
		this.counterTwo = other.counterTwo;
		this.PMI = other.PMI;
	}

	public void calculatePMIForPair(int N) {
		
		double pmi;
		if (this.numberOfOcc!=0 && this.counterOne!=0 && this.counterTwo!=0 && N!=0){
			pmi = Math.log(this.numberOfOcc) + Math.log(N)
					- Math.log(this.counterOne) - Math.log(this.counterTwo);
			pmi = round(pmi, 4);
			this.PMI = "" + pmi;
		}
		
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		String s = dataInput.readUTF();
		String[] strArray = s.split(",");
		this.wordOne = strArray[0];
		this.wordTwo = strArray[1];
		this.decade = Integer.valueOf(strArray[2]);
		this.PMI = strArray[3];
		this.counterOne = Integer.valueOf(strArray[4]);
		this.counterTwo = Integer.valueOf(strArray[5]);
		this.numberOfOcc = Integer.valueOf(strArray[6]);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		String dataOut = this.wordOne + "," + this.wordTwo + ","
				+ this.decade + "," + this.PMI + ","
				+ this.counterOne + "," + this.counterTwo + "," + this.numberOfOcc;
		dataOutput.writeUTF(dataOut);		
	}


	public int getNumberOfOcc() {
		return this.numberOfOcc;
	}

	public void setNumberOfOcc(int numberOfOcc) {
		this.numberOfOcc = numberOfOcc;
	}

	public String getWordOne() {
		return wordOne;
	}

	public String getWordTwo() {
		return wordTwo;
	}

	public int getDecade() {
		return decade;
	}

	public double getPMI() {
		return Double.parseDouble(this.PMI);
	}

	@Override
	public int compareTo(Pair other) {
		int res = this.decade - other.decade;
		if (res != 0) {
			return res;
		}

		res = strCompare(wordOne, other.wordOne);

		if (res != 0) {
			return res;
		}
		return strCompare(wordTwo, other.wordTwo);
	}

	private int strCompare(String a, String b) {
		if (a.equals(Utils.getDummyString()) && b.equals(Utils.getDummyString())){
			return 0;
		}
		if (a.equals(Utils.getDummyString())){
			return -1;
		}
		if (b.equals(Utils.getDummyString())){
			return 1;
		}
		return a.compareTo(b);
	}

	@Override
	public String toString(){
		return this.wordOne + "," + this.wordTwo + ","
				+ this.decade + "," + this.numberOfOcc + ","
				+ this.counterOne + "," + this.counterTwo + "," + this.PMI;
	}

	public boolean isNoneDummy(){
		return !wordOne.equals(Utils.getDummyString()) && !wordTwo.equals(Utils.getDummyString());
	}

	public boolean isOneDummy(){
		return !wordOne.equals(Utils.getDummyString()) && wordTwo.equals(Utils.getDummyString());
	}

	public boolean isBothDummies(){
		return wordOne.equals(Utils.getDummyString()) && wordTwo.equals(Utils.getDummyString());
	}

	public int getCounterOne(){
		return this.counterOne;
	}

	public int getCounterTwo(){
		return this.counterTwo;
	}

	public void setCounterOne(int counterOne){
		this.counterOne = counterOne;
	}

	public void setCounterTwo(int counterTwo){
		this.counterTwo = counterTwo;
	}

	private double round(double value, int places) {

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

}
