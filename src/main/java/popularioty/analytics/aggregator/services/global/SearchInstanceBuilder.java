package popularioty.analytics.aggregator.services.global;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.directory.shared.kerberos.codec.krbCredInfo.actions.StorePRealm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.searchengine.factory.SearchEngineFactory;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.storageengine.factory.StorageFactory;
import popularioty.commons.services.storageengine.factory.StorageProvider;

public class SearchInstanceBuilder 
{
	private static Logger LOG = LoggerFactory.getLogger(SearchInstanceBuilder.class);	
	

	private static SearchInstanceBuilder instance;
	
	/**
	 * Private attribute to keep track of the different searchProviders that are available
	 */
	private Map<String,SearchProvider> searchProviders;
	
	private Map<String,StorageProvider> storageProviders;
	
	
	protected SearchInstanceBuilder()
	{
		searchProviders = new HashMap<>();
		storageProviders = new HashMap<>();
	}
	/**
	 * Looks for the search provider in its Map
	 * @param configurationFile configuration file containing the properties to build the Search provider
	 * @return SearchProvider singleton instance for the given properties file/
	 * @throws PopulariotyException in case the properties file can not be properly read.
	 */
	public synchronized SearchProvider getSearchProvider(String configurationFile) {
			
			if(searchProviders.containsKey(configurationFile))
				return searchProviders.get(configurationFile);
			
			Map properties = loadProperties(configurationFile);
			SearchProvider provider = SearchEngineFactory.getSearchProvider((String) properties.get("search.engine"));
			  
			StorageProvider store = StorageFactory.getStorageProvider((String) properties .get("storage.engine"));
		 	
			try {
				store.init(properties);
				properties.put("storage.provider.object", store);	
				provider.init(properties);
				
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("could not initialize searchprovider properly with properties file"+configurationFile);
			}
			
			storageProviders.put(configurationFile, store);
			searchProviders.put(configurationFile,provider);
			return provider;
	}
	/**
	 * Load properties on demand
	 * @param file path inside the resources folder.
	 * @return properties loaded in a map or null of the file is not found
	 */
	private  Map<String, String> loadProperties(String file) 
	{
		Map<String,String> properties = new HashMap<String, String>();
		
		Properties props = new Properties();
		try {
				props.load(getClass().getClassLoader().getResourceAsStream(file));
				for (String key : props.stringPropertyNames()) {
					properties.put(key, props.getProperty(key));
	            }
				
		} catch (IOException e) {
			//throw new PopulariotyException("unable to load properties file to configure search node", e, LOG,"unable to load properties file to configure search node" , PopulariotyException.Level.FATAL,500);
			System.err.println("unable to load properties file to configure search node");
		}
		return properties;
		
	}
	/**
	 * Typical singleton, but ensuring thread safetiness
	 * @return the singleton instance 
	 */
	public static SearchInstanceBuilder getInstance( ) {
		   
	      if(instance == null) {
	    	  synchronized(SearchInstanceBuilder.class)
			   {  
	    		  instance = new SearchInstanceBuilder();
			   }
	      }
	      return instance;
	}
	/**
	 * Trying to close the connections to search clusters propperly... Probably there is a better way? TODO check
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		for(String key: searchProviders.keySet())
			searchProviders.get(key).close(null);
	}

	
	
}
