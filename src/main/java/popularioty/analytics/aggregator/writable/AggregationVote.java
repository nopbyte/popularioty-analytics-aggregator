package popularioty.analytics.aggregator.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;


public class AggregationVote implements WritableComparable<AggregationVote>{

	private String typeOfVote="";
	
	private float  value;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		typeOfVote = in.readUTF();
		value= in.readFloat();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(typeOfVote);
		out.writeFloat(value);
		
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

	public float  getValue() {
		return value;
	}

	public AggregationVote setValue(float  value) {
		this.value = value;
		return this;
	}


		

}
