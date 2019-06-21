
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;


/**
* dgunify
*/
public class SparkUtil {
	public SparkUtil() {}

	
	/**
	 * @param parMap
	 * put("AppResource","xxxx.jar");
	 * put("MainClass","com.xxx.xxx.xx");
	 * 
	 */
	public Map<String,String> startSpark(Map<String,String> parMap)throws Exception{
		Map<String,String> reMap = new HashMap<String,String>();
		
		try {
			HashMap<String, String> envParams = new HashMap<>();
			
			envParams.put("YARN_CONF_DIR", "/opt/apps/hadoop-3.1.2");
			envParams.put("HADOOP_CONF_DIR", "/opt/apps/hadoop-3.1.2");
			envParams.put("SPARK_HOME","/opt/apps/spark-2.3.3-bin-hadoop2.7");
			envParams.put("SPARK_PRINT_LAUNCH_COMMAND", "1");
			SparkAppHandle spark = new SparkLauncher(envParams)
			        .setAppResource("hdfs://127.0.0.1:9000/apps/dgunify.jar")
			        .setMainClass(parMap.get("MainClass"))//com.dgunify.main1
			        .setMaster("spark://hadoop0:7077")
			        .startApplication();
			reMap.put("status", "ok");
			return reMap;
		} catch (Exception e) {
			e.printStackTrace();
			reMap.put("status", "fail");
			throw e;
		}
	}
}
