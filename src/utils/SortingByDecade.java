package utils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingByDecade extends WritableComparator{

	public SortingByDecade (){
		super(Pair.class, true);
	}

	@Override
	public int compare(WritableComparable a,
			WritableComparable b){
		Pair p1 = (Pair) a;
		Pair p2 = (Pair) b;
		return p1.getDecade() - p2.getDecade();
	}
}
