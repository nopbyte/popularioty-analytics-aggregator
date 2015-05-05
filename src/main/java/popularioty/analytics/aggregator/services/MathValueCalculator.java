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
	public float getCurrentEntityDimensionValue(String entityType, String id, String repType, float f)
	{
		float  ret = f;
		try{
			if(id.contains("#") && entityType.equals(EntityTypeConstants.ENTITY_TYPE_SO_STREAM))
				id = id.replaceAll("#", "");
			Map<String,Object> data =search.getSubReputationSearch(id, entityType, repType);
			if(data != null)
			{
				Float f1 = (Float) data.get("value"); // when this is a user... look for 
				ret = (float) (f1.floatValue()*0.9+0.1*f);
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
	
	
	public float getCurrentOverAllValue(String entityType, String id, String repType, float oldvalue) throws PopulariotyException
	{
		float ret = oldvalue;
		try{
			
			Map<String,Object> data =search.getFinalReputationValueForEntity(entityType, id);
			if(data !=null)
			{
				
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
				throw e;
			}
		}
		return ret;
	}
}
