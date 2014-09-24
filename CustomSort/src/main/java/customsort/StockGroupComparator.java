package customsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StockGroupComparator extends WritableComparator {

	protected StockGroupComparator(){
		super(Stock.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Stock lhs = (Stock) a;
		Stock rhs = (Stock) b;
		
		return lhs.getSymbol().compareTo(rhs.getSymbol());
	}
	
}
