package popularioty.analytics.aggregator.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import popularioty.analytics.aggregator.services.JsonConverter;
import popularioty.analytics.aggregator.services.ReputationSearch;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;

public class AggregationReducer extends Reducer< AggregationKey, AggregationVote, AggregationKey, Text>  
{
	private ReputationSearch search = new ReputationSearch();
	
	protected void reduce(AggregationKey key, Iterable<AggregationVote> values, Context context) throws java.io.IOException, InterruptedException 
	{
		if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
			processSoStream(key, values,context);
		else if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_SO))
			processSo(key, values,context);
		else if(key.getEntityType().equals(EntityTypeConstants.ENTITY_TYPE_USER))
			processUser(key, values,context);
		
		Text t = new Text();
		t.set("something");
		
		//TODO remove this
		context.write(key, t);
	}

	private void processUser(AggregationKey key,
			Iterable<AggregationVote> values, Context context)  
	{
		
		//This gets from mapper SO (only not so-stream) votes with type of vote activity or popularity
	}

	private void processSo(AggregationKey key, Iterable<AggregationVote> values, Context context) throws IOException, InterruptedException
	{
		
		//popularity activity or feedback
		Map<String, Object> so = new HashMap<String, Object>();
		float average = 0;
		int n = 0;
		for(AggregationVote v: values)
		{
			average+=v.getValue();
			Text data = new Text();
			data.set(JsonConverter.buildJsonVote(key, v));
			AggregationKey exportKey =new AggregationKey();
			exportKey.setEntityId(UUID.randomUUID().toString().replaceAll("-", ""));
			exportKey.setEntityType(v.getTypeOfVote());
			context.write(exportKey, data);// emmit the value because the mapper already computed the previous value
			n++;
		}
		average =average/n;
			
	}

	private void processSoStream(AggregationKey key, Iterable<AggregationVote> values, Context context) throws IOException, InterruptedException  
	{
		//This gets from mapper stream votes with type of vote activity or popularity or feedback
		Map<String, Object> so = new HashMap<String, Object>();
		float average = 0;
		int n = 0;
		for(AggregationVote v: values)
		{
			average+=v.getValue();
			Text data = new Text();
			data.set(JsonConverter.buildJsonVote(key, v));
			AggregationKey exportKey =new AggregationKey();
			exportKey.setEntityId(UUID.randomUUID().toString().replaceAll("-", ""));
			exportKey.setEntityType(v.getTypeOfVote());
			context.write(exportKey, data);// emmit the value because the mapper already computed the previous value
			n++;
		}
		average =average/n;
		
	}

	
	
}
