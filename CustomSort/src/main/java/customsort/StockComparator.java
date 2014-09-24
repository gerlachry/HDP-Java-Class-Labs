package customsort;

import org.apache.hadoop.io.WritableComparator;

public class StockComparator extends WritableComparator {
	
	public StockComparator() {
		super(Stock.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// need to get the length of the symbol field
		int strLength1 = readUnsignedShort(b1,s1);
		int strLength2 = readUnsignedShort(b2,s2);

		//compare the symbol fields
		int response = compareBytes(b1, s1 + 2, strLength1, b2, s2 + 2, strLength2);
		if(response != 0) {
			// symbols are not the same so return
			return response;
		} 
		// compare the dates 
		return response = compareBytes(b1, s1 + 2 + strLength1 + 2, 10,
									   b2, s2 + 2 + strLength2 + 2, 10);
	

		
	}
	
}
