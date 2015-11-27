package popularioty.analytics.aggregator.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import popularioty.analytics.aggregator.services.MathValueCalculator;
import popularioty.analytics.aggregator.services.global.FeedbackReputationManager;
import popularioty.analytics.aggregator.services.global.ServiceReputationManager;
import popularioty.analytics.aggregator.services.global.StreamReputationManager;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;

public class AggregateMapper extends Mapper<Text, Text, AggregationKey, AggregationVote>
{

	/*
	 * 1	user_giving_rating	feedback	1
	 * == userid user_giving_rating feedback count_meta_feedbacks
	 * owner_id	developer	feedback	5.203199863433838	4
	 *  == userid feedback repvalue count
	 * 
	 * 
	 * userid developer repvalue count
		
	 * service_instance1	service_instance	feedback	6.804800033569336	27.219199657440186	4
	 * runtime
	 * (id,service_instance,activity,numok,numwrong)
	 * (0c739e21-decbb7959-4010-b244-61237789,service_instance,popularity,rating,count)
*/
	
	private MathValueCalculator calc = new MathValueCalculator();
	private StreamReputationManager streamReputationManager = new StreamReputationManager();
	private ServiceReputationManager serviceReputationManager = new ServiceReputationManager();
	private FeedbackReputationManager feedbackManager = new FeedbackReputationManager();
	
	protected void map(Text key, Text value, Context context)
		      throws java.io.IOException, InterruptedException 
	{
		try{

			
			String entityId = key.toString();
			String values = value.toString();
			StringTokenizer t = new StringTokenizer(values);
			String type = t.nextToken();
			String reputationType = t.nextToken();
			if(reputationType.equals("feedback")){
				handleFeedback(entityId, type, t, context);
			}else if(reputationType.equals("activity")){
				handleActivity(entityId, type, t, context);
			}else if(reputationType.equals("popularity")){
				handlePopularity(entityId, type, t, context);
			}
			else{
				System.err.println("unhandled type of reputation in aggregation! "+reputationType);
			}
		}catch(Exception e){
			System.out.println(PopulariotyException.getStackTrace(e));
			System.err.println("error: "+e.getMessage());
		}
		
		
		
		
		
		
	
	}
	private void handlePopularity(String entityId, String type, StringTokenizer t, Context context) throws PopulariotyException, IOException, InterruptedException {
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
			streamReputationManager.handlePopularity( entityId.replace("#!", "-"),  type,  t, context);
		else if(type.equals(EntityTypeConstants.ENTITY_TYPE_SERVICE))
			serviceReputationManager.handlePopularity(entityId, type, t, context);
		else
			System.err.println("undefined entity type for popularity "+type);
			
	}
	
	private void handleActivity(String entityId, String type, StringTokenizer t, Context context) throws PopulariotyException, IOException, InterruptedException {
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
			streamReputationManager.handleAcitity( entityId.replace("#!", "-"), type,  t, context);
		else if(type.equals(EntityTypeConstants.ENTITY_TYPE_SERVICE))
			serviceReputationManager.handleActivity(entityId, type, t, context);
		else
			System.err.println("undefined entity type for activity "+type);
			
	}
	private void handleFeedback(String entityId, String entityType,
			StringTokenizer t, Context context) throws PopulariotyException, IOException, InterruptedException {
		feedbackManager .handleFeedback(entityId, entityType, t, context);
	}
	
	
	/*
	private void emitToSOAndDeveloper( Context context,String entityId, StringTokenizer t) throws IOException, InterruptedException 
	{
		AggregationKey userKey = new AggregationKey(entityId,EntityTypeConstants.ENTITY_TYPE_USER_DEVELOPER);
		AggregationKey soKey = new AggregationKey(entityId,EntityTypeConstants.ENTITY_TYPE_SO);
		
		String typeOfVote = t.nextToken();
		if(typeOfVote.equals("runtime"))
		{
			  float popularity = calc.getCurrentEntityDimensionValue(EntityTypeConstants.ENTITY_TYPE_SO, entityId, "popularity",Float.parseFloat(t.nextToken()));
			  float activity = calc.getCurrentEntityDimensionValue(EntityTypeConstants.ENTITY_TYPE_SO, entityId,"activity", Float.parseFloat(t.nextToken()));
			
			  AggregationVote vote = new AggregationVote().setTypeOfVote("popularity").setValue(popularity);
			  context.write(soKey, vote);
			  context.write(userKey, vote);
			  vote = new AggregationVote().setTypeOfVote("activity").setValue(activity);
			  context.write(soKey, vote);
			  context.write(userKey, vote);
		}
		else if (typeOfVote.equals("feedback"))
		{
			float feedbackvalue = calc.getCurrentEntityDimensionValue(EntityTypeConstants.ENTITY_TYPE_SO, entityId, "feedback",Float.parseFloat(t.nextToken()));
			AggregationVote vote = new AggregationVote();
			vote.setTypeOfVote("feedback").setValue(feedbackvalue);
			context.write(soKey, vote);
		}
		
	}

	private void emitToSOStreamAndDeveloper( Context context,String entityId, StringTokenizer t) throws IOException, InterruptedException 
	{
		String typeOfVote = t.nextToken();
		String popularity = t.nextToken();
		String activity = t.nextToken();
		
		AggregationKey streamKey = new AggregationKey(entityId,EntityTypeConstants.ENTITY_TYPE_SO_STREAM);
		streamKey.setEntityId(entityId);
		
		if(typeOfVote.equals("runtime"))
		{
			float pop = calc.getCurrentEntityDimensionValue(EntityTypeConstants.ENTITY_TYPE_SO_STREAM, entityId,"popularity", Float.parseFloat(popularity));
			float act = calc.getCurrentEntityDimensionValue(EntityTypeConstants.ENTITY_TYPE_SO_STREAM, entityId, "activity",Float.parseFloat(activity));
			AggregationVote vote = new AggregationVote().setTypeOfVote("popularity").setValue(pop);
			context.write(streamKey, vote);
			vote = new AggregationVote().setTypeOfVote("activity").setValue(act);
			context.write(streamKey, vote);
		}
		else if (typeOfVote.equals("feedback"))
		{
			float feedbackvalue = calc.getCurrentEntityDimensionValue(EntityTypeConstants.ENTITY_TYPE_SO_STREAM, entityId, "feedback",Float.parseFloat(t.nextToken()));
			AggregationVote vote = new AggregationVote();
			vote.setTypeOfVote("feedback").setValue(feedbackvalue);
			context.write(streamKey, vote);
		}
		
	}*/
	
	
}
