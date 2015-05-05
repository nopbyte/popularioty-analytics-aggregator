package popularioty.analytics.aggregator.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import popularioty.analytics.aggregator.services.MathValueCalculator;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;

public class AggregateMapper extends Mapper<Text, Text, AggregationKey, AggregationVote>
{

	
	private MathValueCalculator calc = new MathValueCalculator();
	
	protected void map(Text key, Text value, Context context)
		      throws java.io.IOException, InterruptedException 
	{
		String entityId = key.toString();
		String values = value.toString();
		StringTokenizer t = new StringTokenizer(values);
		String type = t.nextToken();
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
			emitToSOStreamAndDeveloper(context, entityId, t);
		if(type.equals(EntityTypeConstants.ENTITY_TYPE_SO))
			emitToSOAndDeveloper(context, entityId, t);
		
		
	
	}
	//TODO include emmision to the developer...
	private void emitToSOAndDeveloper( Context context,String entityId, StringTokenizer t) throws IOException, InterruptedException 
	{
		AggregationKey userKey = new AggregationKey(entityId,EntityTypeConstants.ENTITY_TYPE_USER);
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
		String popularity = t.nextToken();
		String activity = t.nextToken();
		String typeOfVote = t.nextToken();
		AggregationKey streamKey = new AggregationKey(entityId,EntityTypeConstants.ENTITY_TYPE_SO);
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
		
	}
	
	
}
