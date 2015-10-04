/**
 Copyright 2015 Miyuru Dayarathna

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package org.linear.spark.layer.dailyexp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.Logger;

import org.linear.spark.util.Constants;
import org.linear.spark.util.ZookeeperInterface;

/**
 * @author miyuru
 *
 */
public class HistoryLoadingNotifierClient {
	//private static Log log = LogFactory.getLog(HistoryLoadingNotifierClient.class);
	static Logger logger = Logger.getLogger(HistoryLoadingNotifierClient.class.getName());
	
	public static boolean isHistoryLoaded(){
		boolean result = false;
		
        Properties properties = new Properties();
        InputStream propertiesIS = HistoryLoadingNotifierClient.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);

        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
		
		//String host = properties.getProperty(Constants.HISTORY_COMPONENT_HOST);
        
        String host = ZookeeperInterface.getValueAtPath("/lr/history/host");
        logger.info("AAAAAAAAAAA host : |" + host + "|");
        
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("done?");
			out.flush();
			
			String response = buff.readLine();
			if(response != null){
					if(response.trim().equals("yes")){
						result = true;
					}				
			}
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static boolean sendRUOK(){
		boolean result = false;
        Properties properties = new Properties();
        InputStream propertiesIS = HistoryLoadingNotifierClient.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
		
		//String host = properties.getProperty(Constants.HISTORY_COMPONENT_HOST);
        
        String host = ZookeeperInterface.getValueAtPath("/lr/history/host");
        logger.info("AAAAAAAAAAA host : |" + host + "|");        
		
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("ruok");
			out.flush();
			
			String response = buff.readLine();
			if(response != null){
					if(response.trim().equals("imok")){
						result = true;
					}				
			}
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static void shutdownLoadingNotifier(){
        Properties properties = new Properties();
        InputStream propertiesIS = HistoryLoadingNotifierClient.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try{
        	properties.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
		
		//String host = properties.getProperty(Constants.HISTORY_COMPONENT_HOST);
        
        String host = ZookeeperInterface.getValueAtPath("/lr/history/host");
        logger.info("AAAAAAAAAAA host : |" + host + "|");        
		
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("shtdn");
			out.flush();
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
