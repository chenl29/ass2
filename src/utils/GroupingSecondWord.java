package utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingSecondWord extends WritableComparator{
	
	public GroupingSecondWord(){
		super(Pair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b){
		Pair p1 = (Pair) a;
		Pair p2 = (Pair) b;
		int ans = p1.getDecade() - p2.getDecade();
		if (ans != 0) {
			return ans;
		} else {
			if (p1.isOneDummy() == true) {
				return p1.getWordOne().compareTo(p2.getWordTwo());
			}
			if (p2.isOneDummy() == true) {
				return p2.getWordOne().compareTo(p1.getWordTwo());
			}
			return p1.getWordTwo().compareTo(p2.getWordTwo());
		}
	//}
//		Pair pa = (Pair)a;
//		Pair pb = (Pair)b;
//		int res;
//		res = pa.getDecade() - pb.getDecade();
//		if (res != 0){
//			return res;
//		}
//		
//		if (pa.isOneDummy() && pb.isOneDummy()){
//			return pa.getWordOne().compareTo(pb.getWordOne());
//		}
//		
//		if (pa.isOneDummy()){
//			return -1;
//		}
//		
//		if (pb.isOneDummy()){
//			return 1;
//		}
//		
		//return pa.getWordTwo().compareTo(pb.getWordTwo());
//	//	
//		res = pa.getWordTwo().compareTo(pb.getWordTwo());
//		if (res == 0){
//			if (pa.isOneDummy()){
//				return -1;
//			}
//			if (pb.isOneDummy()){
//				return 1;
//			}
//		} 
//		return res;

//		return pa.getWordTwo().compareTo(pb.getWordTwo());
	}
}
