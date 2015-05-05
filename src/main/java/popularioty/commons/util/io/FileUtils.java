package popularioty.commons.util.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;

public class FileUtils
{
	private static Logger LOG = LoggerFactory.getLogger(FileUtils.class);	

	public static  Map<String, String> loadProperties(String file, Class c) throws PopulariotyException 
	{
		Map<String,String> properties = new HashMap<String, String>();
		
		Properties props = new Properties();
		try {
				props.load(c.getClassLoader().getResourceAsStream(file));
				for (String key : props.stringPropertyNames()) {
	                properties.put(key, props.getProperty(key));
	            }
				System.out.println("Properties file loaded "+properties);
				System.out.println("Keys of properties file"+properties.keySet());
				
		} catch (IOException e) {
			throw new PopulariotyException("unable to load properties file to load properties file :" + file,e, LOG, "unable to load properties file to load properties file :" + file, PopulariotyException.Level.ERROR, 500);
		}
		return properties;
		
	}

}
