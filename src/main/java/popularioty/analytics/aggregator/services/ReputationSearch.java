package popularioty.analytics.aggregator.services;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.river.RiverIndexName.Conf;

import popularioty.analytics.aggregator.services.global.SearchAndStorageInstanceBuilder;
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

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.exception.PopulariotyException.Level;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaType;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteria;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteriaConstants;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteriaType;
import popularioty.commons.services.searchengine.elasticsearch.ElasticSearchAdapter;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryType;
import popularioty.commons.services.storageengine.factory.StorageProvider;
import popularioty.commons.util.io.FileUtils;

public class ReputationSearch 
{

	private static Logger LOG = LoggerFactory.getLogger(ReputationSearch.class);
	
	private static String POP_CONF_FILE = "popularioty-db.properties";
	private Map<String,String> conf_properties;
	SearchProvider search;
	StorageProvider storage;
	String index_reputation_aggregations = "uninitialized";
	String index_subreputation = "uninitialized";
	String index_feedback= "uninitialized";
	String index_meta_feedback= "uninitialized";
	public ReputationSearch()
	{
		try {
				search = SearchAndStorageInstanceBuilder.getInstance().getSearchProvider(POP_CONF_FILE);
				storage = SearchAndStorageInstanceBuilder.getInstance().getStorageProvider(POP_CONF_FILE);
				conf_properties = FileUtils.loadProperties(POP_CONF_FILE, this.getClass());
				index_reputation_aggregations = conf_properties.get("index.aggregated");
				index_subreputation = conf_properties.get("index.subreputation");
				index_feedback = conf_properties.get("index.feedback");
				index_meta_feedback = conf_properties.get("index.metafeedback");
				
				System.out.println("indexes");
				
				System.out.println(index_reputation_aggregations);
				System.out.println(index_subreputation);
				System.out.println(index_feedback);
				System.out.println(index_meta_feedback);
				
				System.out.println("end of indexes");
				
			} catch (PopulariotyException e) {
				e.printStackTrace();
		}
	}
	
	public Map<String, Object> getSubReputationSearch(
			String entityId, String entityType, String classReputationType) throws PopulariotyException{
		
		try{
				Query q = new Query(QueryType.SELECT);
				if(entityType!=null&& !entityType.equals(""))
					q.addCriteria(new SearchCriteria<String>("entity_type", entityType, SearchCriteriaType.MUST_MATCH));
				q.addCriteria(new SearchCriteria<String>("sub_reputation_type", classReputationType, SearchCriteriaType.MUST_MATCH));
				q.addCriteria(new SearchCriteria<String>("entity_id", entityId, SearchCriteriaType.MUST_MATCH));
				
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_FROM, 0, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_SIZE, 1, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<String>("date", SortCriteriaConstants.VALUE_DESC, SortCriteriaType.SORT));
				try
				{
					QueryResponse res = search.execute(q,this.index_subreputation);
					return res.getListofMapsResult().get(0);
					
				}catch (PopulariotyException ex)
				{
					if(ex.getHTTPErrorCode() != 404)//no content
						throw ex;
				}
				return null;
				
		}
		catch(SearchPhaseExecutionException esException)// likely that there is no data in the dtabase....
		{
			System.err.println("SearchPhaseExecutionException "+esException.toString()+" Possibly there is no data yet??");
			return null;
		}
		
	}
	
	public Map<String,Object> getFeedbackById(String id) throws PopulariotyException
	{
		try{
				Query q = new Query(QueryType.SELECT);
				
				q.addCriteria(new SearchCriteria<String>("feedback_id", id, SearchCriteriaType.MUST_MATCH));
				
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_FROM, 0, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_SIZE, 1, SortCriteriaType.RANGE));
				q.addCriteria(new SortCriteria<String>("date", SortCriteriaConstants.VALUE_DESC, SortCriteriaType.SORT));
				QueryResponse res = search.execute(q,this.index_feedback);
				List<Map<String, Object>> l = res.getListofMapsResult();
				if(l == null || l.isEmpty())
					throw new PopulariotyException("Feedback with id"+id+" not found",null,LOG,"Feedback with id"+id+" not found" ,Level.DEBUG,404);
				return l.get(0);
		}catch(SearchPhaseExecutionException esException)// likely that there is no data in the dtabase....
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

	public Map<String, Object> getMetaFeedbackById(String metaFeedbackId) throws PopulariotyException {
		try{
			Query q = new Query(QueryType.SELECT);
			
			q.addCriteria(new SearchCriteria<String>("meta_feedback", metaFeedbackId, SearchCriteriaType.MUST_MATCH));
			q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_FROM, 0, SortCriteriaType.RANGE));
			q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_SIZE, 1, SortCriteriaType.RANGE));
			q.addCriteria(new SortCriteria<String>("date", SortCriteriaConstants.VALUE_DESC, SortCriteriaType.SORT));
			QueryResponse res = search.execute(q,this.index_meta_feedback);
			List<Map<String, Object>> l = res.getListofMapsResult();
			if(l == null || l.isEmpty())
				throw new PopulariotyException("Meta Feedback with id"+metaFeedbackId+" not found",null,LOG,"metaFeedback with id"+metaFeedbackId+" not found" ,Level.DEBUG,404);
			return l.get(0);
	}catch(SearchPhaseExecutionException esException)// likely that there is no data in the dtabase....
	{
		System.err.println("SearchPhaseExecutionException "+esException.toString()+" Possibly there is no data yet??");
		return null;
	}
	}
	
	public float getCurrentOverAllValue( String id, String entityType) 
	{
		try{
			
			Map<String,Object> data =getFinalReputationValueForEntity(entityType, id);
			if(data !=null)
			{
				float oldValue = (float) Float.parseFloat((data.get("reputation").toString()));
				return oldValue;
			}
		}
		catch(Exception e)
		{
			if(e instanceof PopulariotyException)
			{
				System.out.println(((PopulariotyException) e).getHTTPErrorCode());
				System.out.println(((PopulariotyException) e).getMessage());
				
			}
			else{
				e.printStackTrace();
				
			}
		}
		return (float) 5.5;
	}
	public void storeFeedback(String id, Map<String,Object> doc) throws PopulariotyException{
		
		storage.storeData(id, doc, this.index_feedback);
	}
	
	public void storeMetaFeedback(String id, Map<String,Object> doc) throws PopulariotyException{
		
		storage.storeData(id, doc, this.index_meta_feedback);
	}
	
	public void storeSubReputationDocument(String id, Map<String,Object> doc) throws PopulariotyException{
		
		storage.storeData(id, doc, this.index_subreputation);
	}

	public void storeFinalReputationDocument(String id, 
			Map<String, Object> doc) throws PopulariotyException {
		
		storage.storeData(id, doc, this.index_reputation_aggregations);
		
	}

	
}
