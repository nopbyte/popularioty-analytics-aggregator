package popularioty.analytics.aggregator.services;


import java.util.LinkedList;
import java.util.List;

import popularioty.analytics.aggregator.services.global.SearchInstanceBuilder;
import popularioty.commons.services.searchengine.factory.SearchProvider;

public class SearchRuntimeService 
{
	 private SearchProvider search;
	 public SearchRuntimeService()
	 {
		 search =  SearchInstanceBuilder.getInstance().getSearchProvider("search.properties");
	 }
	 
	 public List<CountSOStream> getCountDataFlowsForSO(String soid, String stream)
	 {
		 List<CountSOStream> count = new LinkedList<CountSOStream>();
		 
		 return count;
	 }

}
