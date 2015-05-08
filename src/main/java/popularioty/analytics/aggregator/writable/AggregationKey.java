package popularioty.analytics.aggregator.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class AggregationKey implements WritableComparable<AggregationKey>{
	
	

	// Chosse entity id from 
	private String entityId;
	private String entityType;
	
	
	public AggregationKey(String entityId, String entityType) 
	{
		super();
		this.entityId = entityId;
		this.entityType = entityType;
	}
	public AggregationKey()
	{
		
	}
		@Override
	public void readFields(DataInput in) throws IOException {
		entityId = in.readUTF();
		entityType = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(entityId);
		out.writeUTF(entityType);
	
	}

	@Override
	public String toString() {

		return entityType+"\t"+
				entityId;
	}

	@Override
	public int compareTo(AggregationKey o) {
		return ComparisonChain.start().compare(entityId, o.entityId).
												compare(entityType, o.entityType).result();
	}
	
	public String getEntityId() {
		return entityId;
	}
	
	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}
	
	public String getEntityType() {
		return entityType;
	}
	
	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}
	
	

}
