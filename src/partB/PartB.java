package partB;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import utils.LaconicPair;
import utils.Pair;

public class PartB {
	private static HashSet<String> relatedWords = new HashSet<>();
	private static HashSet<String> nonRelatedWords = new HashSet<>();
	private static List<LaconicPair> listOfPairs = new ArrayList<>();
	private static double threshold;

	public static void main(String[] args){
		if (args.length < 1){
			System.out.println("threshold argument is missing");
			return;
		}
		try {
			threshold = Double.parseDouble(args[0]);
		} catch (Exception e){
			System.out.println("please enter a valid threshold");
			return;
		}
		
		try {
			createWordsFromTestSet("files/wordsim-pos.txt", true);
			createWordsFromTestSet("files/wordsim-neg.txt", false);
		} catch (IOException e) {
			System.out.println("could not get test-sets");
			e.printStackTrace();
		}	

		try {
			getListOfPairs("files/partAWords.txt");
		} catch (IOException e) {
			System.out.println("could not get PartA words");
			e.printStackTrace();
		}	

		double F = calculateFMeasure();
		System.out.println(F);
	}

	private static double calculateFMeasure() {

		double f = 0;
		double precision = 0;
		double recall = 0;
		double trueNegative = 0;
		double falseNegative = 0;
		double falsePositive = 0;
		double truePositive = 0;

		Iterator<LaconicPair> iterator = listOfPairs.iterator();
		while (iterator.hasNext()){
			LaconicPair currPair = iterator.next();
			Boolean relatedOrNonRelated = currPair.getRelateOrNonRelate();
			if (relatedOrNonRelated != null){
				if (relatedOrNonRelated.booleanValue()){
					if (currPair.getPMI()>=threshold){
						truePositive++;
					} else {
						falseNegative++;
					}
				}
				else if (!relatedOrNonRelated.booleanValue()) {
					if (currPair.getPMI()>=threshold){
						falsePositive++;
					} else {
						trueNegative++;
					}
				}
			}
		}

		recall = (truePositive + falseNegative == 0) ? 0 : truePositive / (truePositive + falseNegative);
		precision = (truePositive + falsePositive == 0) ? 0 : truePositive / (truePositive + falsePositive);

		if (precision+recall == 0){
			return 0;
		}
		f = 2*(precision*recall)/(precision+recall);
		return f;

	}

	private static void getListOfPairs(String file) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		//		for (Iterator iterator = relatedWords.iterator(); iterator.hasNext();) {
		//			LaconicPair pair = (LaconicPair) iterator.next();
		//			System.out.println(pair);
		//			
		//		}
		while ((line = br.readLine()) != null) {
			String[] splited = line.split(",");
			String wordOne = splited[0];
			String wordtwo = splited[1];
			double pmi = Double.parseDouble(splited[2]);

			LaconicPair currPair = new LaconicPair(wordOne, wordtwo, pmi);
			String toContains = wordOne + "\t" + wordtwo;
			boolean contained = false;
			if (relatedWords.contains(toContains)){
				currPair.setRelateOrNonRelate(new Boolean(true));
				contained = true;
			} else if (nonRelatedWords.contains(toContains)) {
				currPair.setRelateOrNonRelate(new Boolean(false));
				contained = true;
			}
			if (contained){
				listOfPairs.add(currPair);
			}
		}
		br.close();

	}

	private static void createWordsFromTestSet(String file, boolean related) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		HashSet<String> hs = related ? PartB.relatedWords : PartB.nonRelatedWords; 
		while ((line = br.readLine()) != null) {
			hs.add(line);
		}
		br.close();

	}

}
