package popularioty.analytics.aggregator.services;

import java.util.Map;

import popularioty.commons.constants.EntityTypeConstants;
import popularioty.commons.exception.PopulariotyException;

public class MathValueCalculator {
	
	private ReputationSearch search = new ReputationSearch();
	/**
	 * Calculates the current value for a dimension (activity, popularity, feedback)
	 * @param entityType
	 * @param id
	 * @param repType
	 * @param f
	 * @return
	 * @throws PopulariotyException 
	 */
	public float getCurrentEntityDimensionValue(String entityType, String id, String repType, float newvalue)
	{
		float  ret = newvalue;
		try{
			//TODO remove this after checking the changes on the pig scripts...
			if(id.contains("#!") && entityType.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
				id = id.replaceAll("#!", "-");
			Map<String,Object> data =search.getSubReputationSearch(id, entityType, repType);
			if(data != null)
			{
				
				float oldValue = (float) Float.parseFloat((data.get("value").toString()));
				ret = (float) (oldValue*0.9+newvalue*0.1);
			}			
		}
		catch(Exception e)
		{
			if(e instanceof PopulariotyException)
			{
				System.out.println("sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss");
				System.out.println(((PopulariotyException) e).getHTTPErrorCode());
				System.out.println(((PopulariotyException) e).getMessage());
				System.out.println("sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss");
			}
			else{
				System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
				e.printStackTrace();
				System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			}
		}
		
		return ret;
	}
	
	
	public float getCurrentOverAllValue( String id, String entityType, float newvalue) 
	{
		float ret = newvalue;
		try{
			
			Map<String,Object> data =search.getFinalReputationValueForEntity(entityType, id);
			if(data !=null)
			{
				float oldValue = (float) Float.parseFloat((data.get("reputation").toString()));
				ret = (float) (oldValue*0.9+newvalue*0.1);
			}
		}
		catch(Exception e)
		{
			if(e instanceof PopulariotyException)
			{
				System.out.println("sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss");
				System.out.println(((PopulariotyException) e).getHTTPErrorCode());
				System.out.println(((PopulariotyException) e).getMessage());
				System.out.println("sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss");
			}
			else{
				System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
				e.printStackTrace();
				System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
			}
		}
		return ret;
	}
}
