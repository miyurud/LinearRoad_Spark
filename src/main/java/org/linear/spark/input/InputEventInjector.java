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
package org.linear.spark.input;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.spark.layer.dailyexp.HistoryLoadingNotifier;
import org.linear.spark.layer.dailyexp.HistoryLoadingNotifierClient;
import org.linear.spark.util.Constants;

/**
 * @author miyuru
 *
 *In Spark version of Linear Road it was decided to have a separate component running as the input event injector.
 *This is different from other LR implementations. However, it seems it is the natural way of receiving input data for Spark
 *
 *This service is single threaded because the input data stream need to be sequential. Otherwise there are many issues
 *such as understanding where to start pumping data on new serving threads.
 */
public class InputEventInjector {
	private static ServerSocket svr = null;
	private static boolean shtdnFlag = false;
	private static BufferedReader in = null; 
	private static Log log = LogFactory.getLog(InputEventInjector.class);
	private static LinkedList<InputInjectorSession> injectorSessionList = null;
	
    private static long tupleCounter = 0;
    private static int tupleCountingWindow = 5000;//This is in miliseconds
    private static long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private static PrintWriter outLogger = null;
    private static int dataRate = 0;
    private static long currentTime = 0;	
    private static long expStartTime = 0;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		injectorSessionList = new LinkedList<InputInjectorSession>();
		initializeDataLoader();
		new Thread(){
			public void run(){
				runServer();
			}
		}.start();
		
		startDataInjection();
	}
	
	public static void initializeDataLoader(){
        Properties props = new Properties();
        InputStream propertiesIS = InputEventInjector.class.getClassLoader().getResourceAsStream(org.linear.spark.util.Constants.CONFIG_FILENAME);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.spark.util.Constants.CONFIG_FILENAME + "' not found in the classpath");
        }

        try{
        	props.load(propertiesIS);
        }catch(IOException e){
        	e.printStackTrace();
        }
		
        //We also open the connection to the file stream
		String carDataFile = props.getProperty(org.linear.spark.util.Constants.LINEAR_CAR_DATA_POINTS);    
		
		try{
			in = new BufferedReader(new FileReader(carDataFile));
		}catch(IOException ec){
			ec.printStackTrace();
		}
		
		
		currentTime = System.currentTimeMillis();
		try {
			outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-inputinjector-rate.csv", true)));
            outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
			outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
			outLogger.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void runServer(){
		try {
			//There will be at least three client connections for position reports
			svr = new ServerSocket(Constants.INPUT_INJECTOR_EXP_REPORT_PORT);
			
			while (!shtdnFlag){
				Socket skt = svr.accept();
				
				/*
				//BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
				PrintWriter out = new PrintWriter(skt.getOutputStream());
				
				try{
					String line = null;
					
					while((line = in.readLine()) != null){			
						line = line.substring(2, line.length() - 1);
						out.println(line);
						out.flush();
					}
					System.out.println("Done emitting the input tuples...");
				}catch(IOException ec){
					ec.printStackTrace();
				}
				*/
				
				InputInjectorSession inputInjectorSession = new InputInjectorSession(skt);
				injectorSessionList.add(inputInjectorSession);
			}
		} catch (IOException e) {
			System.out.println("There is already a History Loading Notifier running in the designated port...");
		}
	}
	
	public static void startDataInjection(){
		//BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
		//PrintWriter out = new PrintWriter(skt.getOutputStream());
		
		while(true){			
			if(injectorSessionList.size() == 0){
				try{
					Thread.sleep(1000);
				}catch(InterruptedException e){
					//Just ignore
				}
				continue;
			}
			
			//Once we have a clinet connection that is comming from the Spark DStreaming flow graph, the next step is to
			//check on which host the HistoryLoading has occurred and whethere the history loading process has finished by now.
	        //Next we wait until the history information loading gets completed
	        int c = 0;
	        while(!HistoryLoadingNotifierClient.isHistoryLoaded()){
	        	try{
	        		Thread.sleep(1000);//just wait one second and check again
	        	}catch(InterruptedException e){
	        		//Just ignore
	        	}
	        	System.out.println(c + " : isHistoryLoading...");
	        	c++;
	        }
	        
	        System.out.println("Done loading the history....");
			
			try{
				String line = null;
				
				while((line = in.readLine()) != null){			
					line = line.substring(2, line.length() - 1);
					
					synchronized(injectorSessionList){
						for(Iterator<InputInjectorSession> i = injectorSessionList.iterator(); i.hasNext();){
							i.next().sendTuple(line);
						}
					}
					
					//Important just for testing
					try {
						Thread.currentThread().sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					tupleCounter += 1;
					currentTime = System.currentTimeMillis();
					
					if (previousTime == 0){
						previousTime = System.currentTimeMillis();
			            expStartTime = previousTime;
					}
					
					if ((currentTime - previousTime) >= tupleCountingWindow){
						dataRate = Math.round((tupleCounter*1000)/(currentTime - previousTime));//need to multiply by thousand to compensate for ms time unit
						Date date = new Date(currentTime);
						DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
						outLogger.println(formatter.format(date) + "," + Math.round((currentTime - expStartTime)/1000) + "," + tupleCounter + "," + dataRate);
						outLogger.flush();
						tupleCounter = 0;
						previousTime = currentTime;
					} 
					
				}
				System.out.println("Done emitting the input tuples...");
				
				while(true){
					try{
						Thread.sleep(1000);
					}catch(InterruptedException e){
						//Just ignore
					}
					continue;
				}
				
				
			}catch(IOException ec){
				ec.printStackTrace();
			}
		}
	}	
}
