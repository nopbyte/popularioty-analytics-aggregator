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


public class AggregationReducer extends Reducer< AggregationKey, AggregationVote, AggregationKey, Text>  
{
	private MathValueCalculator calculator = new MathValueCalculator();
	
	private JsonConverter converter = new JsonConverter();
	
	private void emmitWithRandomId(String json, String typeOfVote, Context context) throws IOException, InterruptedException
	{
		Text data = new Text();
		data.set(json);
		AggregationKey exportKey =new AggregationKey();
		exportKey.setEntityId(UUID.randomUUID().toString().replaceAll("-", ""));
		exportKey.setEntityType(typeOfVote);
		context.write(exportKey, data);
	}
	
	protected void reduce(AggregationKey key, Iterable<AggregationVote> values, Context context) throws java.io.IOException, InterruptedException 
	{
		if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
			processSOorStream(key, values,context);
		else if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_SO))
			processSOorStream(key, values,context);
		else if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_USER))
			processUser(key, values,context);
		
		//Text t = new Text();
		//t.set("something");
		
		//TODO remove this
		//context.write(key, t);
	}

	private void processUser(AggregationKey key,
			Iterable<AggregationVote> values, Context context)  
	{
		
		//This gets from mapper SO (only not so-stream) votes with type of vote activity or popularity
	}

	private void processSOorStream(AggregationKey key, Iterable<AggregationVote> values, Context context) throws IOException, InterruptedException
	{
		//This gets from mapper stream votes with type of vote activity or popularity or feedback
				Map<String, Object> so = new HashMap<String, Object>();
				float average = 0;
				int n = 0;
				
				for(AggregationVote v: values)		//popularity activity or feedback
				{
					average+=v.getValue();
					emmitWithRandomId(converter.buildJsonVote(key, v), v.getTypeOfVote(), context);
					n++;
					
				}
				//Final reputation Value => MAP
				average =average/n;
				average = calculator.getCurrentOverAllValue(key.getEntityId(), key.getEntityType(),average);
				String json = converter.buildJsonFinalReputationValueForEntity(key.getEntityId(), key.getEntityType(), average);
				emmitWithRandomId(json,"reputation", context);
	}
	/*private void processSo(AggregationKey key, Iterable<AggregationVote> values, Context context) throws IOException, InterruptedException
	{
		
		Map<String, Object> so = new HashMap<String, Object>();
		float average = 0;
		int n = 0;
		for(AggregationVote v: values)		//popularity activity or feedback
		{
			average+=v.getValue();
			Text data = new Text();
			data.set(converter.buildJsonVote(key, v));
			AggregationKey exportKey =new AggregationKey();
			exportKey.setEntityId(UUID.randomUUID().toString().replaceAll("-", ""));
			exportKey.setEntityType(v.getTypeOfVote());
			context.write(exportKey, data);// emmit the value because the mapper already computed the previous value
			n++;
		}
		//Final reputation Value
		
		average =average/n;
			
	}

	private void processSoStream(AggregationKey key, Iterable<AggregationVote> values, Context context) throws IOException, InterruptedException  
	{
		//This gets from mapper stream votes with type of vote activity or popularity or feedback
		Map<String, Object> so = new HashMap<String, Object>();
		float average = 0;
		int n = 0;
		for(AggregationVote v: values)		//popularity activity or feedback
		{
			average+=v.getValue();
			Text data = new Text();
			data.set(converter.buildJsonVote(key, v));
			AggregationKey exportKey =new AggregationKey();
			exportKey.setEntityId(UUID.randomUUID().toString().replaceAll("-", ""));
			exportKey.setEntityType(v.getTypeOfVote());
			context.write(exportKey, data);// emmit the value because the mapper already computed the previous value
			n++;
		}
		//Final reputation Value
		average =average/n;
		
	}*/

	
	
}
