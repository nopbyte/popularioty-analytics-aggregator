package popularioty.analytics.aggregator.services.global;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.hadoop.mapreduce.Mapper.Context;

import com.couchbase.client.java.query.Query;

import popularioty.analytics.aggregator.services.ReputationSearch;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;

public class StreamReputationManager extends AbstractReputationManager{

	
	private static String KEY_EVENTS = "events";
	private static String KEY_NON_EVENTS= "non_events";
	private static String KEY_DISC_JS = "discard_js";
	private static String KEY_DISC_POLICY = "discard_policy";
	private static String KEY_DISC_FILTER = "discard_filter";
	private static String COUNT = "count";
	private static String VALUE = "value";


	//vote.setTypeOfVote()
	 /*CONCAT(CONCAT(src_id, '#!'),src_stream) as stream_oid, 
	'service_object_stream' as type, 
	'activity' as reptype,
	 tot_events as tot_events:long  ,
	 tot_ne as tot_ne:long,  
	 tot_discjs as tot_discjs:long,
	 tot_discpolicy as tot_discpolicy:long,  
	 tot_discfilter as tot_discfilter:long;*/
	public void handleAcitity (String entityId, String type, StringTokenizer t, Context context) throws PopulariotyException, IOException, InterruptedException{
		
		long   events=1;
		long 	ne=1;  
		long	discjs =1; 
		long	discpolicy =1;
		long	discfilter=1;
		long count = 5;
			
		Map<String, Object> res;
		try {
			res = query.getSubReputationSearch(entityId,type, EntityTypeConstants.REPUTATION_TYPE_ACTIVITY);
			if(res != null)
			{
				if(res.containsKey(KEY_EVENTS))
					events = (Long.parseLong((String) res.get(KEY_EVENTS).toString()));
				if(res.containsKey(KEY_NON_EVENTS))
					ne = (Long.parseLong((String) res.get(KEY_NON_EVENTS).toString()));
				if(res.containsKey(KEY_DISC_JS))
					discjs = (Long.parseLong((String) res.get(KEY_DISC_JS).toString()));
				if(res.containsKey(KEY_DISC_POLICY))
					discpolicy = (Long.parseLong((String) res.get(KEY_DISC_POLICY).toString()));
				if(res.containsKey(KEY_DISC_FILTER))
					discfilter = (Long.parseLong((String) res.get(KEY_DISC_FILTER).toString()));
				
			}
		} catch (PopulariotyException e) {
			e.printStackTrace();
		}
						
		//we can still keep going since initialization guarantees an array with 1s.
		
		
		events += Long.parseLong(t.nextToken()); 
		ne   += Long.parseLong(t.nextToken());
		discjs += Long.parseLong(t.nextToken());
		discpolicy  += Long.parseLong(t.nextToken());
		discfilter += Long.parseLong(t.nextToken());
		count = events +  ne  +  discjs + discpolicy  +  discfilter;		
		
		float EpDelivered = (events+ne);
		float EpFilter= (discfilter);
		EpDelivered = EpDelivered/count;
		EpFilter = EpFilter/count;		
		float value = 1+ (( EpDelivered+EpFilter )*9);
		Map<String, Object> finalDoc = new HashMap<String, Object>();
		
		
		finalDoc.put(KEY_EVENTS, events );
		finalDoc.put(KEY_NON_EVENTS, ne);
		finalDoc.put(KEY_DISC_JS, discjs);
		finalDoc.put(KEY_DISC_POLICY, discpolicy);
		finalDoc.put(KEY_DISC_FILTER, discfilter);
		finalDoc.put(COUNT, count);
		finalDoc.put(VALUE, value);
		finalDoc.put("entity_type", type);
		finalDoc.put("entity_id", entityId);
		finalDoc.put("date", System.currentTimeMillis());
		finalDoc.put("sub_reputation_type","activity");
		
			
		query.storeSubReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);
		
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM)){
			emmitForEntity(entityId.split("-")[0], EntityTypeConstants.ENTITY_TYPE_SO,  AggregationVote.TYPE_OF_VOTE_ACTIVITY, value, context);
		}		
		emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_ACTIVITY, value, context);	
	}

	/*14375534417693d7c484847884f85b80d5ae617f27618#!thermopile       service_object_stream   popularity      2       56164
	  * */
	public void handlePopularity(String entityId, String type, StringTokenizer t, Context context)
		throws PopulariotyException, IOException, InterruptedException {
		 
		
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
				
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM)){
			emmitForEntity(entityId.split("-")[0], EntityTypeConstants.ENTITY_TYPE_SO,  AggregationVote.TYPE_OF_VOTE_POPULARITY, value, context);
		}
		query.storeSubReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);
		emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_POPULARITY, value, context);
		
	}
	

}
