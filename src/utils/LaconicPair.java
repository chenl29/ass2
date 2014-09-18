package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import javax.rmi.CORBA.Util;

import org.apache.hadoop.io.WritableComparable;

public class LaconicPair{
	private String wordOne;
	private String wordTwo;
	private double PMI;
	private Boolean relateOrNonRelate;

	public LaconicPair(){

	}

	public LaconicPair(String wordOne, String wordTwo){
		this.wordOne = wordOne;
		this.wordTwo = wordTwo;
		this.PMI = 0;
		this.setRelateOrNonRelate(null);
	}
	
	public LaconicPair(String wordOne, String wordTwo, double PMI){
		this.wordOne = wordOne;
		this.wordTwo = wordTwo;
		this.PMI = PMI;
		this.setRelateOrNonRelate(null);
	}

	public String getWordOne() {
		return wordOne;
	}

	public String getWordTwo() {
		return wordTwo;
	}

	public double getPMI() {
		return PMI;
	}

	@Override
	public String toString(){
		return this.wordOne + "\t" + this.wordTwo + "\t"
				+ this.PMI;
	}


	@Override
	public boolean equals(Object o) {
		if (o instanceof LaconicPair) {
			LaconicPair otherPair = (LaconicPair) o;
			if (this.wordOne.equals(otherPair.wordOne) && 
					this.wordTwo.equals(otherPair.wordTwo)){
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	public Boolean getRelateOrNonRelate() {
		return relateOrNonRelate;
	}

	public void setRelateOrNonRelate(Boolean relateOrNonRelate) {
		this.relateOrNonRelate = relateOrNonRelate;
	}

}
