package popularioty.analytics.aggregator.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;

public class JsonConverter 
{
   private ObjectMapper mapper = new ObjectMapper();
   
   private String mapToString(Map<String,Object> map)
   {
	   try {
			return mapper.writeValueAsString(map);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	   
	   return null;
	   
   }
   public String buildJsonVote(AggregationKey key, AggregationVote v)
   {
	   
	   String ret = "";
	   Map<String, Object> document = new HashMap<String, Object>();
	   document.put("entity_type", key.getEntityType());
	   document.put("entity_id", key.getEntityId());
	   document.put("sub_reputation_type", v.getTypeOfVote());
	   document.put("value", v.getValue());
	   document.put("date", System.currentTimeMillis());
	   return mapToString(document);
   }
   
   
   public String buildJsonFinalReputationValueForUser(String id, String username, float developerReputation, float reviewReputation)
   {
	    Map<String, Object> document = new HashMap<String, Object>();
	    document.put("entity_id", id);
	    document.put("entity_type", "user");
	    document.put("userName", username);
	    document.put("developer_reputation", developerReputation);
	    document.put("end_user_reputation", reviewReputation);
	    document.put("date", System.currentTimeMillis()); 
	    return mapToString(document);
	   
   }
   
   
   public String buildJsonFinalReputationValueForEntity(String id, String entityType, float reputation)
   {
	   String ret = "";
	   Map<String, Object> document = new HashMap<String, Object>();
	   document.put("entity_type", entityType);
	   document.put("entity_id", id);
	   document.put("reputation", reputation);
	   document.put("date", System.currentTimeMillis());
	   return mapToString(document);
   }
   
   
}
