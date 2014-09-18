package utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingFirstWord extends WritableComparator{
	
	public GroupingFirstWord(){
		super(Pair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b){
		Pair pa = (Pair)a;
		Pair pb = (Pair)b;
		int res;
		res = pa.getDecade() - pb.getDecade();
		if (res != 0){
			return res;
		}
		res = pa.getWordOne().compareTo(pb.getWordOne());
		if (res == 0){
			if (pa.isOneDummy()){
				return -1;
			}
			if (pb.isOneDummy()){
				return 1;
			}
		} 
		return res;
	}
}
