package popularioty.analytics.aggregator.services;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;

public class JsonConverter 
{
   public static String getJson(Map<String, Object> map)
   {
		try {
			return new ObjectMapper().writeValueAsString(map);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
   }
   public static String buildJsonVote(AggregationKey key, AggregationVote v)
   {
	   String ret = "";
	   
	   return ret;
   }
}
