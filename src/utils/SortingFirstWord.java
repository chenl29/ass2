package utils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

	public class SortingFirstWord extends WritableComparator{
		
		public SortingFirstWord (){
			super(Pair.class, true);
		}
		
		@Override
		public int compare(WritableComparable a,
				WritableComparable b){
			Pair pa = (Pair)a;
			Pair pb = (Pair)b;
			int res;
			res = pa.getDecade() - pb.getDecade();
			if (res != 0){
				return res;
			}
			return pa.getWordOne().compareTo(pb.getWordOne());
		}
	}
