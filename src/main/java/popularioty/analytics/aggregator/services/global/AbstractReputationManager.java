package popularioty.analytics.aggregator.services.global;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;

import popularioty.analytics.aggregator.services.ReputationSearch;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;

public class AbstractReputationManager {
	protected ReputationSearch query = new ReputationSearch();
	
	protected void emmitForEntity(String entityId, String type,String typeOfVote, float value, Context context ) throws IOException, InterruptedException {
		AggregationKey exportKey =new AggregationKey();
		exportKey.setEntityId(entityId);
		exportKey.setEntityType(type);
		AggregationVote vote = new AggregationVote();
		vote.setValue(value);
		vote.setTypeOfVote(typeOfVote);
		context.write(exportKey, vote);
	}
	
	
	
}
