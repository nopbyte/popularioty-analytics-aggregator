package popularioty.analytics.aggregator.services.global;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.hadoop.mapreduce.Mapper.Context;

import popularioty.analytics.aggregator.services.ReputationSearch;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;

public class ServiceReputationManager extends AbstractReputationManager{

	private ReputationSearch query = new ReputationSearch();

	private static String EVENTS_OK = "events_ok";
	private static String EVENTS_WRONG = "events_wrong";
	private static String COUNT = "count";
	private static String VALUE = "value";
	
	 /*(id,service_instance,activity,numok,numwrong)
	  * src_id as service_id, 'service_instance' as type, 'activity' as reptype,serviceactok as ok, serviceactwrong as wrong;
	  * */
	public  void handleActivity (String entityId, String type, StringTokenizer t, Context context) throws PopulariotyException, IOException, InterruptedException{
		
		long   ok=1;
		long 	wrong=1;  
		long count = 2;
		
		Map<String, Object> res;
		try {
			res = query.getSubReputationSearch(entityId, type, EntityTypeConstants.REPUTATION_TYPE_ACTIVITY);
			if(res != null)
			{
				if(res.containsKey(EVENTS_OK))
					ok = (Long.parseLong((String) res.get(EVENTS_OK).toString()));
				if(res.containsKey(EVENTS_WRONG))
					wrong = (Long.parseLong((String) res.get(EVENTS_WRONG).toString()));
			}
		} catch (PopulariotyException e) {
			e.printStackTrace();
		}
						
		//we can still keep going since initialization guarantees an array with 1s.
		
		ok += Long.parseLong(t.nextToken()); 
		wrong   += Long.parseLong(t.nextToken());
		count = ok+wrong;		
		
		float EpG= (float)((float)ok/(float)count);
		System.out.println("EpG"+EpG);
		float value = 1+ (( EpG )*9);
		Map<String, Object> finalDoc = new HashMap<String, Object>();
		
		finalDoc.put(EVENTS_OK, ok);
		finalDoc.put(EVENTS_WRONG, wrong);
		finalDoc.put(COUNT, count);
		finalDoc.put(VALUE, value);
		finalDoc.put("entity_type", type);
		finalDoc.put("entity_id", entityId);
		finalDoc.put("date", System.currentTimeMillis());
		finalDoc.put("sub_reputation_type","activity");
		
			
		query.storeSubReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);
		emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_POPULARITY, value, context);	
		
	}


	/*
	service_id, 
	'service_instance' as type, 
	'popularity' as reptype,  
	(1 + (messages*9/maxmessages)) as rating, 
	messages as count;
	*/
	public  void handlePopularity(String entityId, String type,
			StringTokenizer t,
			Context context) throws PopulariotyException, IOException, InterruptedException {

		long previousCount = 1;
		float previousScore = (float) 5.5;
		//according to Dirichlet Reputation Systems from Josang. et. al.
		float Lambda = (float) 0.9;
		
		Map<String, Object> res;
		try {
			res = query.getSubReputationSearch(entityId, type, EntityTypeConstants.REPUTATION_TYPE_POPULARITY);
			if(res != null)
			{
				previousScore = (Float.parseFloat((String) res.get(VALUE).toString()));
				if(res.containsKey(COUNT))
					previousCount = (Long.parseLong((String) res.get(COUNT).toString()));
				
			}
		} catch (PopulariotyException e) {
			e.printStackTrace();
		}
						
		//we can still keep going since initialization guarantees an array with unities.
		float current = Float.parseFloat(t.nextToken());
		long currentMessages = Long.parseLong(t.nextToken());
		long totalMessages  = previousCount +  currentMessages;
		
		float value = (float)(((float)previousScore)*((float)previousCount));
		value += (float)((float)current*(float)currentMessages); 
				
		
		value = (float)((float)value/(float)totalMessages);
		
		Map<String, Object> finalDoc = new HashMap<String, Object>();
		
		finalDoc.put(COUNT, currentMessages+previousCount);
		finalDoc.put(VALUE, value);
		finalDoc.put("entity_type", type);
		finalDoc.put("entity_id", entityId);
		finalDoc.put("date", System.currentTimeMillis());
		finalDoc.put("sub_reputation_type",EntityTypeConstants.REPUTATION_TYPE_POPULARITY);
		
			
		query.storeSubReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);
		emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_POPULARITY, value, context);

		
	}
}
