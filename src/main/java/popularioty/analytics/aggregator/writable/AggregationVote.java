package popularioty.analytics.aggregator.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class AggregationVote implements WritableComparable<AggregationVote>{

	public static final String TYPE_OF_VOTE_USER = "user";
	private String typeOfVote="";
	public static String TYPE_OF_VOTE_ACTIVITY = "activity";
	public static String TYPE_OF_VOTE_POPULARITY = "popularity";
	public static String TYPE_OF_VOTE_FEEDBACK = "feedback";
	
	private double  value=0;
	//only for accounting and merging... not emmited.
	private int count=0;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		typeOfVote = in.readUTF();
		value= in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(typeOfVote);
		out.writeDouble(value);
		
	}

	@Override
	public int compareTo(AggregationVote o) {
		return 0;
	}
	
	
	public String getTypeOfVote() {
		return typeOfVote;
	}

	public AggregationVote setTypeOfVote(String typeOfVote) {
		this.typeOfVote = typeOfVote;
		return this;
	}

	public double  getValue() {
		return value;
	}

	public AggregationVote setValue(double  value) {
		this.value = value;
		return this;
	}

	public void merge(AggregationVote v){
		
		if(v.getTypeOfVote().equals(typeOfVote)){
			this.count++;
			this.value += v.getValue(); 
		}
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
		
	

}
