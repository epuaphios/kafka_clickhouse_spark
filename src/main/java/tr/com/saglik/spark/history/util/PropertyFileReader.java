package tr.com.saglik.spark.history.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to read property file
 */
public class PropertyFileReader {
	private static final Logger logger = Logger.getLogger(PropertyFileReader.class);
	private static final PropertyFileReader instance = new PropertyFileReader();
	private final Properties prop;

	private PropertyFileReader() {
		this.prop = readPropertyFile();
	}
	
	// Get the only object available
	public static PropertyFileReader getInstance() {
		return instance;
	}

	private Properties readPropertyFile() {
		Properties properties = new Properties();
		logger.error("propURL: "
				+ getClass().getResource("/kafka-stream.spark.properties"));

		try (InputStream input = getClass()
				.getResourceAsStream("/kafka-stream.spark.properties")) {
			properties.load(input);
		} catch (IOException ex) {
			logger.error(ex);
		}
		
		return properties;
	}
	
	public Properties getProperties() {
		return prop;
	}
}
