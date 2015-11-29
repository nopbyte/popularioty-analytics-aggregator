package popularioty.analytics.aggregator.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import popularioty.analytics.aggregator.services.JsonConverter;
import popularioty.analytics.aggregator.services.MathValueCalculator;
import popularioty.analytics.aggregator.services.ReputationSearch;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;


public class AggregationReducer extends Reducer< AggregationKey, AggregationVote, AggregationKey, Text>  
{
	private MathValueCalculator calculator = new MathValueCalculator();
	
	private ReputationSearch query = new ReputationSearch();

	
	private JsonConverter converter = new JsonConverter();
	
	
	protected void reduce(AggregationKey key, Iterable<AggregationVote> values, Context context) throws java.io.IOException, InterruptedException 
	{
		try {
			if(	key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_USER_DEVELOPER)||
				key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_USER_GIVING_RATING)){
				
				processUser(key, values,context);
			}
			else
				processSOorStream(key, values,context);
		
		} catch (PopulariotyException e) {
			e.printStackTrace();
		}
		
	}

	/*
	 * From FeebackReputationManager
	 * emmitForEntity(entityId, type, AggregationVote.TYPE_OF_VOTE_USER, reputation, context);
	 * 
	 */
	private void processUser(AggregationKey key,
			Iterable<AggregationVote> values, Context context) throws PopulariotyException, IOException, InterruptedException  
	{
		int count = 0;
		double ini = 5.5;
		double total = 0;
		
		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println("type: "+key.getEntityType());
		System.out.println();
		System.out.println();
		System.out.println();
		//if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_USER_DEVELOPER))
			for(AggregationVote val: values){
				total+=val.getValue();
				count++;
			}
			if(count!=0){
				 
				ini = query.getCurrentOverAllValue(key.getEntityId(), EntityTypeConstants.ENTITY_TYPE_USER);
				double rep = (ini*0.7)+ (0.3*(total/count));
				
				Map<String, Object> finalDoc = new HashMap<>();
				finalDoc.put("entity_type", EntityTypeConstants.ENTITY_TYPE_USER);
				finalDoc.put("entity_id", key.getEntityId());
				finalDoc.put("date", System.currentTimeMillis());
				finalDoc.put("value", rep);
				finalDoc.put("reputation", rep);
				query.storeFinalReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);
				emmitId(key.getEntityId(), EntityTypeConstants.ENTITY_TYPE_USER,context);
						
				
			}
	}

	private void emmitId(String entityId, String entityType,
			Context context) throws IOException, InterruptedException {
		AggregationKey exportKey = new AggregationKey();
		exportKey.setEntityId(entityId);
		exportKey.setEntityType(entityType);
		Text data = new Text();
		context.write(exportKey, data);
		
		
	}

	private void processSOorStream(AggregationKey key, Iterable<AggregationVote> values, Context context) throws IOException, InterruptedException, PopulariotyException
	{
			
				Map<String,Object >res = null; 
		
				int popularityVotes = 0;
				int activityVotes = 0;
				int feedbackVotes = 0;
				
				double newPopularity = 0;
				double newActivity = 0;
				double newFeedback = 0;
				
				for(AggregationVote v: values){
					
					if(v.getTypeOfVote().equals(EntityTypeConstants.REPUTATION_TYPE_ACTIVITY)){
						
						newActivity+= v.getValue();
						activityVotes ++;
					}
					else if(v.getTypeOfVote().equals(EntityTypeConstants.REPUTATION_TYPE_POPULARITY)){
						
						newPopularity+= v.getValue();
						popularityVotes++;
					}
					else if(v.getTypeOfVote().equals(EntityTypeConstants.REPUTATION_TYPE_FEEDBACK)){
						
						newFeedback+= v.getValue();
						feedbackVotes++;
					}
				}
				
				if(activityVotes == 0){
					res = query.getSubReputationSearch( key.getEntityId(),key.getEntityType(), EntityTypeConstants.REPUTATION_TYPE_ACTIVITY);
					if(res != null)
						newActivity = (Double.parseDouble((String) res.get("value").toString()));
					else
						newActivity = 5.5;
				}
				else
					newActivity /= activityVotes;
				
				if(popularityVotes == 0){
					res = query.getSubReputationSearch(key.getEntityId(), key.getEntityType(),EntityTypeConstants.REPUTATION_TYPE_POPULARITY);
					if(res != null)
						newPopularity= (Double.parseDouble((String) res.get("value").toString()));
					else
						newPopularity = 5.5;
				}
				else
					newPopularity /= popularityVotes; 
				
				if(feedbackVotes == 0){
					res = query.getSubReputationSearch(key.getEntityId(), key.getEntityType(), EntityTypeConstants.REPUTATION_TYPE_FEEDBACK);
					if(res != null)
						newFeedback= (Double.parseDouble((String) res.get("value").toString()));
					else
						newFeedback = 5.5;
				}
				else
					newFeedback/= feedbackVotes;
				
				double finalRep = (newPopularity*0.2)+(newActivity*0.3)+(newFeedback*0.5);
		
				Map<String, Object> finalDoc = new HashMap<>();
				finalDoc.put("entity_type", key.getEntityType());
				finalDoc.put("entity_id", key.getEntityId());
				finalDoc.put("activity", newActivity);
				finalDoc.put("popularity", newPopularity);
				finalDoc.put("feedback", newFeedback);
				finalDoc.put("date", System.currentTimeMillis());
				//the same, but due to compatilbity with old API version is required
				finalDoc.put("value", finalRep);
				finalDoc.put("reputation", finalRep);
				query.storeFinalReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), finalDoc);
				emmitId(key, finalRep, context);

				
	}


	private void emmitId(AggregationKey exportKey, double reputation,  Context context) throws IOException, InterruptedException {
		Text data = new Text();
		data.set(Double.toString(reputation));
		context.write(exportKey, data);		
	}

	
	
}
