package popularioty.analytics.aggregator.services;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.river.RiverIndexName.Conf;

import popularioty.analytics.aggregator.services.global.SearchInstanceBuilder;
import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaType;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteria;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteriaConstants;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteriaType;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryType;
import popularioty.commons.util.io.FileUtils;

public class ReputationSearch 
{

	private static String POP_CONF_FILE = "popularioty-db.properties";
	private Map<String,String> conf_properties;
	SearchProvider search;
	String index_reputation_aggregations = "uninitialized";
	String index_subreputation = "uninitialized";
	public ReputationSearch()
	{
		try {
				search = SearchInstanceBuilder.getInstance().getSearchProvider(POP_CONF_FILE);
				conf_properties = FileUtils.loadProperties(POP_CONF_FILE, this.getClass());
				index_reputation_aggregations = conf_properties.get("index.aggregated");
				index_subreputation = conf_properties.get("index.subreputation");
				
				System.out.println("indexes");
				
				System.out.println(index_reputation_aggregations);
				System.out.println(index_subreputation);
				
				
				System.out.println("end of indexes");
				
			} catch (PopulariotyException e) {
				e.printStackTrace();
		}
	}
	
	public Map<String, Object> getSubReputationSearch(
			String entityId, String entityType, String classReputationType) throws PopulariotyException{
		
		try{
				Query q = new Query(QueryType.SELECT_ID);
				if(entityType!=null&& !entityType.equals(""))
					q.addCriteria(new SearchCriteria<String>("entity_type", entityType, SearchCriteriaType.MUST_MATCH));
				q.addCriteria(new SearchCriteria<String>("sub_reputation_type", classReputationType, SearchCriteriaType.MUST_MATCH));
				q.addCriteria(new SearchCriteria<String>("entity_id", entityId, SearchCriteriaType.MUST_MATCH));
				
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_FROM, 0, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_SIZE, 1, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<String>("date", SortCriteriaConstants.VALUE_DESC, SortCriteriaType.SORT));
				
				QueryResponse res = search.execute(q,this.index_subreputation);
				return res.getMapResult();
		}
		catch(SearchPhaseExecutionException esException)// likely that there is no data in the dtabase....
		{
			System.err.println("SearchPhaseExecutionException "+esException.toString()+" Possibly there is no data yet??");
			return null;
		}
		
	}
	
	
	public Map<String,Object> getFinalReputationValueForEntity(String entityType, String entityId) throws PopulariotyException
	{
		try{
				Query q = new Query(QueryType.SELECT);
					
				if(entityType!=null&& !entityType.equals(""))
					q.addCriteria(new SearchCriteria<String>("entity_type", entityType, SearchCriteriaType.MUST_MATCH));
				q.addCriteria(new SearchCriteria<String>("entity_id", entityId, SearchCriteriaType.MUST_MATCH));
				
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_FROM, 0, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_SIZE, 1, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<String>("date", SortCriteriaConstants.VALUE_DESC, SortCriteriaType.SORT));
				QueryResponse res = search.execute(q,index_reputation_aggregations);
				List<Map<String,Object>> r = res.getListofMapsResult();
				//if it was empty, we would have gotten an exception...
				return r.get(0);
		}
		catch(SearchPhaseExecutionException esException)// likely that there is no data in the dtabase....
		{
			System.err.println("SearchPhaseExecutionException "+esException.toString()+" Possibly there is no data yet??");
			return null;
		}

	}
}
