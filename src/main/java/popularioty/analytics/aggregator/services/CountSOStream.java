package popularioty.analytics.aggregator.services;

public class CountSOStream {
	
	private String soId;
	private String stream;
	private int count;
	
	
	public CountSOStream(String soId, String stream, int count) {
		super();
		this.soId = soId;
		this.stream = stream;
		this.count = count;
	}
	
	public String getSoId() {
		return soId;
	}
	public void setSoId(String soId) {
		this.soId = soId;
	}
	public String getStream() {
		return stream;
	}
	public void setStream(String stream) {
		this.stream = stream;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
}
