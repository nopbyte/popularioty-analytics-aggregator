package popularioty.analytics.aggregator.services.global;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.mapreduce.Mapper.Context;

import popularioty.analytics.aggregator.services.ReputationSearch;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.exception.PopulariotyException;

public class AbstractReputationManager {
	protected ReputationSearch query = new ReputationSearch();
	protected static String COUNT = "count";
	protected static String VALUE = "value";
	
	protected void emmitForEntity(String entityId, String type,String typeOfVote, float value, Context context ) throws IOException, InterruptedException {
		AggregationKey exportKey =new AggregationKey();
		exportKey.setEntityId(entityId);
		exportKey.setEntityType(type);
		AggregationVote vote = new AggregationVote();
		vote.setValue(value);
		vote.setTypeOfVote(typeOfVote);
		context.write(exportKey, vote);
	}
	
	protected void createSubReputationDocForEntity(String entityId, String entityType, long currentTotal, float currentValue, String reputationType) throws PopulariotyException
	 {
		
		Map<String, Object> finalDoc = new HashMap<String, Object>();
		float previousScore = (float) 5.5;
		long previousCount = 1;
		
		Map<String, Object> res = query.getSubReputationSearch(entityId, entityType, reputationType);
		if(res != null)
		{
			previousScore = (Float.parseFloat((String) res.get(VALUE).toString()));
			if(res.containsKey(COUNT))
				previousCount = (Long.parseLong((String) res.get(COUNT).toString()));
			
		}
		long total = currentTotal + previousCount;
		float newValue = (float)(	(float) ( ((float) previousScore)* ((float) previousCount) )+  
									(float) ( ((float) currentValue)* ((float) currentTotal) )
								  );
		newValue = newValue/total;		
		
		finalDoc.put(COUNT, total);
		finalDoc.put(VALUE, newValue);
		finalDoc.put("entity_type", entityType);
		finalDoc.put("entity_id", entityId);
		finalDoc.put("date", System.currentTimeMillis());
		finalDoc.put("sub_reputation_type",reputationType);
		query.storeSubReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);

	 }
	
}
