package average;

import java.util.LinkedList;

public class StockWindow {
	private String symbol;
	private String endDate;
	private LinkedList<Double> prices = new LinkedList<Double>();
	

	public StockWindow(String symbol) {
		super();
		this.symbol = symbol;
	}

	public String getSymbol() {
		return symbol;
	}

	public String getEndDate() {
		return endDate;
	}

	public void addPrice(double price, String endDate) {
		prices.add(price);
		this.endDate = endDate;
	}
	
	public double getAverage() {
		double sum = 0.0;
		int count = 0;
		for(double price: prices) {
			count++;
			sum += price;
		}
		return sum/count;
	}
	
	public String toString() {
		return symbol + "," + endDate;
	}
}
