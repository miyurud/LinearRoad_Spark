package org.linear.spark.util;

import java.util.logging.Logger;

public class ZookeeperInterface {
	static Logger logger = Logger.getLogger(ZookeeperInterface.class.getName());
//	public static String getValueAtPath(String path){
//		String result = null;
//		
//		Zookeeper zk = new Zookeeper("127.0.0.1", 2000, this); // Session timeout is in milliseconds. We assume that there is already a Zookeeper instance running in the local host.
//		boolean fl = zk.exists(path, false);
//		
//		//"/lr/history/host"
//		if(fl){
//			
//		}
//		
//		return result;
//	}
	
	public static void setValueAtPath(String path, String value){
		try{
			KVS store = new KVS();
			store.connect("127.0.0.1");
		
			store.write(path, value);
			store.close();
		}catch(Exception e){
			logger.info(e.getMessage());
		}
	}
	
	public static String getValueAtPath(String path){
		String result = null;
		try{
			KVS store = new KVS();
			store.connect("127.0.0.1");
		
			result = store.read(path, null);
			store.close();
		}catch(Exception e){
			logger.info(e.getMessage());
		}
		
		return result;
	}
	
	public static boolean createGroup(String path){
	    try{
	    	GroupMgt group = new GroupMgt();
			group.connect("127.0.0.1");

	    	group.create(path);
		    group.close();
	    }catch(Exception ec){
	    	logger.info(ec.getMessage());
	    	return false;
	    }
	    
	    return true;
	}
	
}
