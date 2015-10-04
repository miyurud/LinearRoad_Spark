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
package org.linear.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.linear.spark.events.AccidentEvent;
import org.linear.spark.events.AccountBalanceEvent;
import org.linear.spark.events.PositionReportEvent;
import org.linear.spark.layer.dailyexp.HistoryLoadingNotifier;
import org.linear.spark.util.Constants;
import org.linear.spark.util.ZookeeperInterface;

import java.io.*;
import java.net.InetAddress;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

;



/**
 * @author miyuru
 *
 */
public class SparkLinearRoad {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static LinkedList<org.linear.spark.events.HistoryEvent> historyEvtList = new LinkedList<org.linear.spark.events.HistoryEvent>();
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		System.setProperty("spark.executor.memory", "4g");
		System.setProperty("spark.streaming.unpersist", "false");
		HashMap map = new HashMap();
		map.put("spark.executor.memory", "4g");
		map.put("spark.streaming.unpersist", "false");
	    // Create the context with a 1 second batch size
	    final JavaStreamingContext ssc = new JavaStreamingContext(args[0], "spark-LinearRoad",
	            new Duration(1000), System.getenv("SPARK_HOME"),
	            JavaStreamingContext.jarOfClass(SparkLinearRoad.class), map);
	    JavaDStream<String> inputTuples = ssc.socketTextStream(args[1], Integer.parseInt(args[2]));
	    
	   //----------------------------- Input Layer Functions ---------------------------------------------
	    Function<String, Boolean> positionResportsFilterFunction = new Function<String, Boolean>(){
			@Override
			public Boolean call(String inputTuple) throws Exception {
				String[] fields = inputTuple.split("\\s+");
				byte typeField = Byte.parseByte(fields[0]); 
				
				if(typeField == 0){
					//This is a position report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
					return true;
				}
				
				return false;
			}
	    };
	    
	    Function<String, Boolean> accountBalanceFilterFunction = new Function<String, Boolean>(){
			@Override
			public Boolean call(String inputTuple) throws Exception {
				
				String[] fields = inputTuple.split("\\s+");
				byte typeField = Byte.parseByte(fields[0]); 
				
				if(typeField == 2){
					//This is an Account Balance report (Type=2, Time, VID, QID)
					return true;
				}
								
				return false;
			}
	    };
	    
	    Function<String, Boolean> expenditureReportsFilterFunction = new Function<String, Boolean>(){
			@Override
			public Boolean call(String inputTuple) throws Exception {
				String[] fields = inputTuple.split("\\s+");
				byte typeField = Byte.parseByte(fields[0]); 
				
				if(typeField == 3){
					//This is an Expenditure report (Type=3, Time, VID, XWay, QID, Day)
					return true;
				}
				
				return false;
			}
	    };
	    
	    Function<String, Boolean> travelTimeReportsFilterFunction = new Function<String, Boolean>(){
			@Override
			public Boolean call(String inputTuple) throws Exception {
				String[] fields = inputTuple.split("\\s+");
				byte typeField = Byte.parseByte(fields[0]); 
				
				if(typeField == 4){
					//This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
					return true;
				}
				
				return false;
			}
	    };
	    
	    //----------------------------- Segstat Layer Functions ---------------------------------------------//
	    final Function<String, String> segStatLowLevelFunction = new Function<String, String>(){
	    	private final Pattern SPACE = Pattern.compile(" ");
	    	private LinkedList<String> outputQueueNOV = new LinkedList<String>();
	    	private LinkedList<String> outputQueueLAV = new LinkedList<String>();

	    	private long currentSecond;
	    	private LinkedList<PositionReportEvent> posEvtList = new LinkedList<PositionReportEvent>();
	    	private ArrayList<PositionReportEvent> evtListNOV = new ArrayList<PositionReportEvent>();
	    	private ArrayList<PositionReportEvent> evtListLAV = new ArrayList<PositionReportEvent>();
	    	private byte minuteCounter;
	    	private int lavWindow = 5; //This is LAV window in minutes
	    	
	    	private String host;
	    	private int port;
	    	
	        private Log log = LogFactory.getLog(Function.class);
	        
	        private long tupleCounter = 0;
	        private int tupleCountingWindow = 5000;//This is in miliseconds
	        private long previousTime = 0; //This is the time that the tuple measurement was taken previously
	        private transient PrintWriter outLogger = null;
	        private int dataRate = 0;
	        private long currentTime = 0;	
	        private long expStartTime = 0;
	        	        
	        private transient boolean firstFlag = true;
	        
			@Override
			public String call(String arg0) throws Exception {
				//This is very tricky, but if we get an empty packet we should not process it, it seems
				//there is no other alternative than just to forward it to the next step. Otherwise we may get NullPointer exception in the latter half of the code.
				//when we try to parse it to obtain numerical values.
				if(arg0.equals("")){
					return "";
				}				

				String[] fields = arg0.split("\\s+");
				return process(new org.linear.spark.events.PositionReportEvent(fields));
			}

		    private String process(PositionReportEvent evt){   
		    	StringBuilder resBuilder = new StringBuilder();
		    	
				if(currentSecond == -1){
					currentSecond = evt.time;
				}else{
					if((evt.time - currentSecond) > 60){
						resBuilder.append(calculateNOV());
						
						evtListNOV.clear();
						
						currentSecond = evt.time;
						minuteCounter++;
						
						if(minuteCounter >= lavWindow){
							resBuilder.append(calculateLAV(currentSecond));
								
							//LAV list cannot be cleared because it need to keep the data for 5 minutes prior to any time t
							//evtListLAV.clear();
							
							minuteCounter = 0;
						}
					}
				}
				evtListNOV.add(evt);
				evtListLAV.add(evt);
				
				return resBuilder.toString();
		    }

			private String calculateLAV(long currentTime) {
				
		        try {
		            BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/abcfile.txt"));
	                    out.write("calculateLAV at " + InetAddress.getLocalHost().getHostName() + "\n");
		                out.close();
		            } catch (IOException e) {}
				
				
				float result = -1;
				float avgVelocity = -1;
				StringBuilder result2 = new StringBuilder();
				
				ArrayList<Byte> segList = new ArrayList<Byte>();
				Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  

				//First identify the number of segments
				java.util.Iterator<PositionReportEvent> itr = evtListLAV.iterator();
				byte curID = -1;
				while(itr.hasNext()){
					curID = itr.next().mile;
					
					if(!segList.contains(curID)){
						segList.add(curID);
					}
				}
				
				ArrayList<PositionReportEvent> tmpEvtListLAV = new ArrayList<PositionReportEvent>(); 
				float lav = -1;
				
				for(byte i =0; i < 2; i++){ //We need to do this calculation for both directions (west = 0; East = 1)
					java.util.Iterator<Byte> segItr = segList.iterator();
					int vid = -1;
					byte mile = -1;
					ArrayList<Integer> tempList = null;
					PositionReportEvent evt = null;
					long totalSegmentVelocity = 0;
					long totalSegmentVehicles = 0;
					
					//We calculate LAV per segment
					while(segItr.hasNext()){
						mile = segItr.next();
						itr = evtListLAV.iterator();
						
						while(itr.hasNext()){
							evt = itr.next();
							
							if((Math.abs((evt.time - currentTime)) < 300)){
								if((evt.mile == mile) && (i == evt.dir)){ //Need only last 5 minutes data only
									vid = evt.vid;
									totalSegmentVelocity += evt.speed;
									totalSegmentVehicles++;
								}
								
								if(i == 1){//We need to add the events to the temp list only once. Because we iterate twice through the list
									tmpEvtListLAV.add(evt);//Because of the filtering in the previous if statement we do not accumulate events that are older than 5 minutes
								}
							}
						}
						
						lav = ((float)totalSegmentVelocity/totalSegmentVehicles);
						if(!Float.isNaN(lav)){				
							result2.append(Constants.LAV_EVENT_TYPE + " " + mile + " " + lav + " " + i + "|");
							
							totalSegmentVelocity = 0;
							totalSegmentVehicles = 0;
						}
					}
				}
					
				//We assign the updated list here. We have discarded the events that are more than 5 minutes duration
				//evtListLAV = tmpEvtListLAV;	
				
				return result2.toString();
			}
			
			private String calculateNOV() {
				ArrayList<Byte> segList = new ArrayList<Byte>();
				Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  
				StringBuilder result2 = new StringBuilder();
				
				//Get the list of segments first
				java.util.Iterator<PositionReportEvent> itr = evtListNOV.iterator();
				byte curID = -1;
				while(itr.hasNext()){
					curID = itr.next().mile;
					
					if(!segList.contains(curID)){
						segList.add(curID);
					}
				}
				
				java.util.Iterator<Byte> segItr = segList.iterator();
				int vid = -1;
				byte mile = -1;
				ArrayList<Integer> tempList = null;
				PositionReportEvent evt = null;

				//For each segment		
				while(segItr.hasNext()){
					mile = segItr.next();
					itr = evtListNOV.iterator();
					while(itr.hasNext()){
						evt = itr.next();
						
						if(evt.mile == mile){
							vid = evt.vid;
							
							if(!htResult.containsKey(mile)){
								tempList = new ArrayList<Integer>();
								tempList.add(vid);
								htResult.put(mile, tempList);
							}else{
								tempList = htResult.get(mile);
								tempList.add(vid);
								
								htResult.put(mile, tempList);
							}
						}
					}
				}
				
				Set<Byte> keys = htResult.keySet();
				
				java.util.Iterator<Byte> itrKeys = keys.iterator();
				int numVehicles = -1;
				mile = -1;
				
				while(itrKeys.hasNext()){
					mile = itrKeys.next();
					numVehicles = htResult.get(mile).size();

					result2.append(Constants.NOV_EVENT_TYPE + " " + ((int)Math.floor(currentSecond/60)) + " " + mile + " " + numVehicles + "|");
				}
				return result2.toString();
			}
	    };
	    
	    
	    
	    Function<JavaRDD<String>, JavaRDD<String>> segStatFunction = new Function<JavaRDD<String>, JavaRDD<String>>(){
			private transient PrintWriter outLogger = null;
	        private long currentTime = 0;

	        //Since we cannot have constructors in anonymous classes, we declare an instance initializer here
	        {
				try {
					outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-segstat-rate.csv", true)));
					outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>");
					outLogger.println("Date,Wall clock time (s),TTuples");
					outLogger.flush();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
	        }
	        
			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {	
				
				currentTime = System.currentTimeMillis();
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				//outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-segstat-rate.csv", true)));
				outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
				outLogger.flush();
								
				return arg0.map(segStatLowLevelFunction);
			}
	    };
	    
	    //----------------------------- AccountBalance Layer Functions ---------------------------------------------//
		
		final Function<String, String> accBalanceLowLevelFunction = new Function<String, String>(){
		    private LinkedList<AccountBalanceEvent> accEvtList = new LinkedList<AccountBalanceEvent>();
		    private HashMap<Integer, Integer> tollList = new HashMap<Integer, Integer>();

			@Override
			public String call(String arg0) throws Exception {
				//This is very tricky, but if we get an empty packet we should not process it, it seems
				//there is no other alternative than just to forward it to the next step. Otherwise we may get NullPointer exception in the latter half of the code.
				//when we try to parse it to obtain numerical values.
				if(arg0.equals("")){
					return "";
				}
				
				StringBuilder sbResults = new StringBuilder();
				//Here we need to check whether the incoming string contains "|". If so we need to split the string.
				String[] packets = null;
				
				//outLogger.println("The packet is : " + arg0);
				//outLogger.flush();
				if(arg0.contains("|")){
					packets = arg0.split("\\|");
				}else{
					packets = new String[]{arg0};
				}

				int i = 0;
				for(i = 0; i < packets.length; i++){
					//outLogger.println("The packet[i] : |" + packets[i] + "|");
					//outLogger.flush();
				    String[] fields = packets[i].split("\\s+");
				    
				    //outLogger.println("The fields[i] : |" + fields[0] + "|");
				    //outLogger.flush();
			        byte typeField = Byte.parseByte(fields[0]);

			        switch(typeField){
			        	   case Constants.ACC_BAL_EVENT_TYPE:
			        		   AccountBalanceEvent et = new AccountBalanceEvent(Long.parseLong(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[9]));
			        		   sbResults.append(process(et) + "|");
			        		   //System.out.println("ACC BAL Evt : " + et.toString());
			        		   break;
			 	       case Constants.TOLL_EVENT_TYPE:
			 	    	   int key = Integer.parseInt(fields[1]);
			 	    	   int value = Integer.parseInt(fields[2]);
			 	    	   
			 	    	   Integer kkey = (Integer)tollList.get(key);
			 	    	   
			 	    	   //System.out.println("key : " + key + " value : " + value);
			 	    	   
			 	    	   if(kkey != null){
			 	    		   tollList.put(key, (kkey + value)); //If the car id is already in the hashmap we need to add the tool to the existing toll.
			 	    	   }else{
			 	    		   tollList.put(key, value);
			 	    	   }
			 	    	   
			 	    	   break;
			        }
				}
				
				return sbResults.toString();
			}
			
		    public String process(AccountBalanceEvent evt){	
		    	String result = null;
		    	
				if(evt != null){
					result = Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid);
				}
				
				return result;
		    }
			
		};
		
	    Function<JavaRDD<String>, JavaRDD<String>> accBalanceFunction = new Function<JavaRDD<String>, JavaRDD<String>>(){
		    private transient PrintWriter outLogger = null;
		    private long currentTime = 0;
	        {
				try {
					outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-accbalance-rate.csv", true)));
					outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>");
					outLogger.println("Date,Wall clock time (s),TTuples");
					outLogger.flush();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
	        }
		    
			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {
				
				currentTime = System.currentTimeMillis();
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				
				outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
				outLogger.flush();
				
				return arg0.map(accBalanceLowLevelFunction);
			}
	    };	
	    
	    //----------------------------- Toll Layer Functions ---------------------------------------------//	    
	    
		final Function<String, String> tollLowLevelFunction = new Function<String, String>(){
		    private LinkedList<AccountBalanceEvent> accEvtList = new LinkedList<AccountBalanceEvent>();
		    private HashMap<Integer, Integer> tollList = new HashMap<Integer, Integer>();
		    
			LinkedList cars_list = new LinkedList();
			HashMap<Integer, org.linear.spark.layer.toll.Car> carMap = new HashMap<Integer, org.linear.spark.layer.toll.Car>(); 
			HashMap<Byte, org.linear.spark.layer.toll.AccNovLavTuple> segments = new HashMap<Byte, org.linear.spark.layer.toll.AccNovLavTuple>();
			byte NUM_SEG_DOWNSTREAM = 5; //Number of segments downstream to check whether an accident has happened or not
			int BASE_TOLL = 2; //This is a predefined constant (mentioned in Richard's thesis)
			private LinkedList<PositionReportEvent> posEvtList = new LinkedList<PositionReportEvent>();

			/*
			 * The output from this function will contain "|" delimiters. These need to be split in the next stages.
			 * (non-Javadoc)
			 * @see org.apache.spark.api.java.function.WrappedFunction1#call(java.lang.Object)
			 */
			@Override
			public String call(String arg0) throws Exception {
				//This is very tricky, but if we get an empty packet we should not process it, it seems
				//there is no other alternative than just to forward it to the next step. Otherwise we may get NullPointer exception in the latter half of the code.
				//when we try to parse it to obtain numerical values.
				if(arg0.equals("")){
					return "";
				}

				StringBuilder sbResult = new StringBuilder();
				
				//At this point we have to check whether the tuple contains "|" delimiting characters. If so we have to split the
				//batch of tuples by | to get individual tuples
				String[] packets = null;
				
//				outLogger.println(arg0);
//				outLogger.flush();
				
				if(arg0.contains("|")){
					packets = arg0.split("\\|");
				}else{
					packets = new String[]{arg0};
				}

				int i = 0;
				for(i = 0; i < packets.length; i++){
					if(!packets[i].equals("")){
					    String[] fields = packets[i].split("\\s+");
						
						//byte typeField = -100;
						
						byte typeField = Byte.parseByte(fields[0]);

						switch(typeField){
					       case Constants.POS_EVENT_TYPE:
					    	   sbResult.append(process(new PositionReportEvent(fields, true)));
					    	   break;
						   case Constants.LAV_EVENT_TYPE:
						       org.linear.spark.events.LAVEvent obj = new org.linear.spark.events.LAVEvent(Byte.parseByte(fields[1]), Float.parseFloat(fields[2]), Byte.parseByte(fields[3]));
						       lavEventOcurred(obj);
						   	   break;
						   case Constants.NOV_EVENT_TYPE:
						       try{
						    	   org.linear.spark.events.NOVEvent obj2 = new org.linear.spark.events.NOVEvent(Integer.parseInt(fields[1]), Byte.parseByte(fields[2]), Integer.parseInt(fields[3]));
						           novEventOccurred(obj2);
						       }catch(NumberFormatException e){
						    	   System.out.println("Not Number Format Exception for tuple : " + arg0);
						       }
						       break;
						    case Constants.ACCIDENT_EVENT_TYPE:
						       accidentEventOccurred(new AccidentEvent(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Byte.parseByte(fields[3]), Byte.parseByte(fields[4]), Byte.parseByte(fields[5]), Long.parseLong(fields[6])));
						       break;
					       }
					}
				}
				//toString() might be heavy operation. But there seems no alternative. 
				return sbResult.toString();
			}
			
			private void accidentEventOccurred(AccidentEvent accEvent) {
				System.out.println("Accident Occurred :" + accEvent.toString());
				boolean flg = false;
				
				synchronized(this){
					flg = segments.containsKey(accEvent.mile);
				}
				
				if(!flg){
					org.linear.spark.layer.toll.AccNovLavTuple obj = new org.linear.spark.layer.toll.AccNovLavTuple();
					obj.isAcc = true;
					synchronized(this){
						segments.put(accEvent.mile, obj);
					}
				}else{
					synchronized(this){
						org.linear.spark.layer.toll.AccNovLavTuple obj = segments.get(accEvent.mile);
						obj.isAcc = true;
						segments.put(accEvent.mile, obj);
					}
				}
			}
		    
		    private void novEventOccurred(org.linear.spark.events.NOVEvent novEvent){
				boolean flg = false;

				flg = segments.containsKey(novEvent.segment);
			
				if(!flg){
					org.linear.spark.layer.toll.AccNovLavTuple obj = new org.linear.spark.layer.toll.AccNovLavTuple();
					obj.nov = novEvent.nov;
					segments.put(novEvent.segment, obj);
				}else{
					org.linear.spark.layer.toll.AccNovLavTuple obj = segments.get(novEvent.segment);
					obj.nov = novEvent.nov;

					segments.put(novEvent.segment, obj);
				}    	
		    }
		    
		    private void lavEventOcurred(org.linear.spark.events.LAVEvent lavEvent){
				boolean flg = false;
				
				flg = segments.containsKey(lavEvent.segment); 
				
				if(!flg){
					org.linear.spark.layer.toll.AccNovLavTuple obj = new org.linear.spark.layer.toll.AccNovLavTuple();
					obj.lav = lavEvent.lav;
					segments.put(lavEvent.segment, obj);
				}else{
					org.linear.spark.layer.toll.AccNovLavTuple obj = segments.get(lavEvent.segment);
					obj.lav = lavEvent.lav;
					segments.put(lavEvent.segment, obj);
				}
		    }

			public StringBuilder process(PositionReportEvent evt){
				int len = 0;
				StringBuilder fullResult = new StringBuilder();
						
				java.util.Iterator<org.linear.spark.layer.toll.Car> itr = cars_list.iterator();
			
				if(!carMap.containsKey(evt.vid)){
					org.linear.spark.layer.toll.Car c = new org.linear.spark.layer.toll.Car();
					c.carid = evt.vid;
					c.mile = evt.mile;
					carMap.put(evt.vid, c);
				}else{
					org.linear.spark.layer.toll.Car c = carMap.get(evt.vid);

						if(c.mile != evt.mile){ //Car is entering a new mile/new segment
							c.mile = evt.mile;
							carMap.put(evt.vid, c);

							if((evt.lane != 0)&&(evt.lane != 7)){ //This is to make sure that the car is not on an exit ramp
								org.linear.spark.layer.toll.AccNovLavTuple obj = null;
								
								obj = segments.get(evt.mile);

								if(obj != null){									
									if(isInAccidentZone(evt)){
										System.out.println("Its In AccidentZone");
									}
									
									if(((obj.nov < 50)||(obj.lav > 40))||isInAccidentZone(evt)){
										org.linear.spark.events.TollCalculationEvent tollEvt = new org.linear.spark.events.TollCalculationEvent(); //In this case we set the toll to 0
										tollEvt.vid = evt.vid;
										tollEvt.segment = evt.mile;
																												
										//String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
										
										fullResult.append(Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
										

									}else{
										org.linear.spark.events.TollCalculationEvent tollEvt = new org.linear.spark.events.TollCalculationEvent(); //In this case we need to calculate a toll
										tollEvt.vid = evt.vid;
										tollEvt.segment = evt.mile;
										
										if(segments.containsKey(evt.mile)){
											org.linear.spark.layer.toll.AccNovLavTuple tuple = null;
											
											synchronized(this){
												tuple = segments.get(evt.mile);
											}
																						
											tollEvt.toll = BASE_TOLL*(tuple.nov - 50)*(tuple.nov - 50);
																				
											//String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
											
											fullResult.append(Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
											

										}
									}						
								}
							}
						}
				}
				
				return fullResult;
			}
		    
			private boolean isInAccidentZone(PositionReportEvent evt) {
				byte mile = evt.mile;
				byte checkMile = (byte) (mile + NUM_SEG_DOWNSTREAM);
				
				while(mile < checkMile){
					if(segments.containsKey(mile)){
						org.linear.spark.layer.toll.AccNovLavTuple obj = segments.get(mile);
						
						if(Math.abs((evt.time - obj.time)) > 20){
							obj.isAcc = false;
							mile++;
							continue; //May be we remove the notification for a particular mile down the xway. But another mile still might have accident. Therefore, we cannot break here.
						}
						
						if(obj.isAcc){
							return true;
						}
					}
					mile++;
				}
				
				return false;
			}
			
			
		};
		
	    Function<JavaRDD<String>, JavaRDD<String>> tollFunction = new Function<JavaRDD<String>, JavaRDD<String>>(){
		    private transient PrintWriter outLogger = null;
		    private long currentTime = 0;	
	    	
	        {
				try {
					outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-toll-rate.csv", true)));
					outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>");
					outLogger.println("Date,Wall clock time (s),TTuples");
					outLogger.flush();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
	        }
		    
			/**
			 * Note that in future it would be better to change this function's signature to a function that returns a JavaRDD.
			 * Therefore, we need not to worry about the encoding with "|".
			 */
	    	
	    	@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {
	    		
				currentTime = System.currentTimeMillis();
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				
				outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
				outLogger.flush();
	    		
				return arg0.map(tollLowLevelFunction);
			}
	    };
	    
	    //----------------------------- Accident Layer Functions ---------------------------------------------//	    
	    
		final Function<String, String> accidentLowLevelFunction = new Function<String, String>(){
		    private LinkedList<AccountBalanceEvent> accEvtList = new LinkedList<AccountBalanceEvent>();
		    private HashMap<Integer, Integer> tollList = new HashMap<Integer, Integer>();

			@Override
			public String call(String arg0) throws Exception {		
				//This is very tricky, but if we get an empty packet we should not process it, it seems
				//there is no other alternative than just to forward it to the next step. Otherwise we may get NullPointer exception in the latter half of the code.
				//when we try to parse it to obtain numerical values.
				if(arg0.equals("")){
					return "";
				}

		        String fields[] = arg0.split("\\s+");
		        
		        PositionReportEvent posEvt = new PositionReportEvent(fields);
				org.linear.spark.layer.accident.Car c = new org.linear.spark.layer.accident.Car(posEvt.time, posEvt.vid, posEvt.speed,
						posEvt.xWay, posEvt.lane, posEvt.dir, posEvt.mile);

				return detect(c);
			}
			
		    private String detect(org.linear.spark.layer.accident.Car c) {
		    	StringBuilder sbResult = new StringBuilder();  
		    	
				if (c.speed > 0) {
					remove_from_smashed_cars(c);
					remove_from_stopped_cars(c);
				} else if (c.speed == 0) {
					if (is_smashed_car(c) == false) {
						if (is_stopped_car(c) == true) {
							renew_stopped_car(c);
						} else {
							stopped_cars.add(c);
						}
						
						int flag = 0;
						for (int i = 0; i < stopped_cars.size() -1; i++) {
							org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)stopped_cars.get(i);
							if ((t_car.carid != c.carid)&&(!t_car.notified)&&((c.time - t_car.time) <= 120) && (t_car.posReportID >= 3) &&
									((c.xway0 == t_car.xway0 && c.mile0 == t_car.mile0 && c.lane0 == t_car.lane0 && c.offset0 == t_car.offset0 && c.dir0 == t_car.dir0) &&
									(c.xway1 == t_car.xway1 && c.mile1 == t_car.mile1 && c.lane1 == t_car.lane1 && c.offset1 == t_car.offset1 && c.dir1 == t_car.dir1) &&
									(c.xway2 == t_car.xway2 && c.mile2 == t_car.mile2 && c.lane2 == t_car.lane2 && c.offset2 == t_car.offset2 && c.dir2 == t_car.dir2) &&
									(c.xway3 == t_car.xway3 && c.mile3 == t_car.mile3 && c.lane3 == t_car.lane3 && c.offset3 == t_car.offset3 && c.dir3 == t_car.dir3))) {
								
								if (flag == 0) {
									org.linear.spark.events.AccidentEvent a_event = new org.linear.spark.events.AccidentEvent();
									a_event.vid1 = c.carid;
									a_event.vid2 = t_car.carid;
									a_event.xway = c.xway0;
									a_event.mile = c.mile0;
									a_event.dir = c.dir0;
									a_event.time = t_car.time;
									
									//here we append this tuple with a trailing |. Have to remove it at the next stage.
									sbResult.append(Constants.NOV_EVENT_TYPE + " " + a_event.vid1 + " " + a_event.vid2 + " " + a_event.xway + " " + a_event.mile + " " + a_event.dir+"|");
									
									t_car.notified = true;
									c.notified = true;
									flag = 1;
								}
								//The cars c and t_car have smashed with each other
								add_smashed_cars(c);
								add_smashed_cars(t_car);
								
								break;
							}
						}
					}
				}
				
				return sbResult.toString();
			}
		    
		    private LinkedList smashed_cars = new LinkedList();
		    private LinkedList stopped_cars = new LinkedList();
		    private LinkedList accidents = new LinkedList();
			
			private boolean is_smashed_car(org.linear.spark.layer.accident.Car car) {
				for (int i = 0; i < smashed_cars.size(); i++) {
					org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)smashed_cars.get(i);

					if (((org.linear.spark.layer.accident.Car)smashed_cars.get(i)).carid == car.carid){
						return true;
					}
				}
				return false;
			}
			
			private void add_smashed_cars(org.linear.spark.layer.accident.Car c) {
				for (int i = 0; i < smashed_cars.size(); i++) {
					org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)smashed_cars.get(i);
					if (c.carid == t_car.carid) {
						smashed_cars.remove(i);
					}
				}
				smashed_cars.add(c);
			}
			
			private boolean is_stopped_car(org.linear.spark.layer.accident.Car c) {
				for (int i = 0; i < stopped_cars.size(); i++) {
					org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)stopped_cars.get(i);
					if (c.carid == t_car.carid) {
						return true;
					}
				}
				return false;
			}
			
			private void remove_from_smashed_cars(org.linear.spark.layer.accident.Car c) {
				for (int i = 0; i < smashed_cars.size(); i++) {
					org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)smashed_cars.get(i);
					if (c.carid == t_car.carid) {
						smashed_cars.remove();
					}
				}
			}
			
			private void remove_from_stopped_cars(org.linear.spark.layer.accident.Car c) {
				for (int i = 0; i < stopped_cars.size(); i++) {
					org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)stopped_cars.get(i);
					if (c.carid == t_car.carid) {
						stopped_cars.remove();
					}
				}
			}
			
			private void renew_stopped_car(org.linear.spark.layer.accident.Car c) {
				for (int i = 0; i < stopped_cars.size(); i++) {
					org.linear.spark.layer.accident.Car t_car = (org.linear.spark.layer.accident.Car)stopped_cars.get(i);
					if (c.carid == t_car.carid) {
						c.xway3 = t_car.xway2;
						c.xway2 = t_car.xway1;
						c.xway1 = t_car.xway0;
						c.mile3 = t_car.mile2;
						c.mile2 = t_car.mile1;
						c.mile1 = t_car.mile0;				
						c.lane3 = t_car.lane2;
						c.lane2 = t_car.lane1;
						c.lane1 = t_car.lane0;
						c.offset3 = t_car.offset2;
						c.offset2 = t_car.offset1;
						c.offset1 = t_car.offset0;				
						c.dir3 = t_car.dir2;
						c.dir2 = t_car.dir1;
						c.dir1 = t_car.dir0;
						c.notified = t_car.notified;
						c.posReportID = (byte)(t_car.posReportID + 1);
						
						stopped_cars.remove(i);
						stopped_cars.add(c);
						
						//Since we already found the car from the list we break at here
						break;
					}
				}
			}
		};
		
	    Function<JavaRDD<String>, JavaRDD<String>> accidentFunction = new Function<JavaRDD<String>, JavaRDD<String>>(){
		    private transient PrintWriter outLogger = null;
		    private long currentTime = 0;
	    	
	        {
				try {
					outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-accident-rate.csv", true)));
					outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>");
					outLogger.println("Date,Wall clock time (s),TTuples");
					outLogger.flush();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
	        }
		    
			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {	
				
				currentTime = System.currentTimeMillis();
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				
				outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
				outLogger.flush();
				
				return arg0.map(accidentLowLevelFunction);
			}
	    };
	    
	    //----------------------------- DailyExpense Layer Functions ---------------------------------------------//	    
	    
		final Function<String, String> dailyExpenseLowLevelFunction = new Function<String, String>(){
		    private LinkedList<org.linear.spark.events.ExpenditureEvent> expEvtList = new LinkedList<org.linear.spark.events.ExpenditureEvent>();
		    //private LinkedList<org.linear.spark.events.HistoryEvent> historyEvtList = new LinkedList<org.linear.spark.events.HistoryEvent>();
		    private boolean flag = false;
		    
		    @Override
			public String call(String arg0) throws Exception {
				//This is very tricky, but if we get an empty packet we should not process it, it seems
				//there is no other alternative than just to forward it to the next step. Otherwise we may get NullPointer exception in the latter half of the code.
				//when we try to parse it to obtain numerical values.
				if(arg0.equals("")){
					return "";
				}
		    	
		    	//This seems the best solution for the moment, but its not the ideal solution for loading the history data.
		    	//+April28-2014: Its better to have Zookeeper based communication mechanism like done with S4 and Storm for here as well
		    	//But have to be careful, because its chicken and egg problem, Spark needs some data to be injected to the cluster
		    	//for it to start deploying the application. But before injecting tuples to the flow graph, we have to load the history information.
		    	
		        return process(new org.linear.spark.events.ExpenditureEvent(arg0.split("\\s+")));
			}
			
		    private String process(org.linear.spark.events.ExpenditureEvent evt){    	
				java.util.Iterator<org.linear.spark.events.HistoryEvent> itr = historyEvtList.iterator();
				int sum = 0;
				
				while(itr.hasNext()){
					org.linear.spark.events.HistoryEvent histEvt = (org.linear.spark.events.HistoryEvent)itr.next();
					
					if((histEvt.carid == evt.vid) && (histEvt.x == evt.xWay) && (histEvt.d == evt.day)){
						sum += histEvt.daily_exp;
					}					
				}
								
				return (Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum);
		    }
		};
		
	    Function<JavaRDD<String>, JavaRDD<String>> dailyExpenseFunction = new Function<JavaRDD<String>, JavaRDD<String>>(){
		    private transient PrintWriter outLogger = null;
		    private long currentTime = 0;	    	
	    	
	        {
	        	//First we need to load the history information.
	        	
				try {
					outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-dailyexp-rate.csv", true)));
					outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>");
					outLogger.println("Date,Wall clock time (s),TTuples");
					outLogger.flush();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				
	    		Properties properties = new Properties();
	    		InputStream propertiesIS = SparkLinearRoad.class.getClassLoader().getResourceAsStream(org.linear.spark.util.Constants.CONFIG_FILENAME);
	            try {
	    			properties.load(propertiesIS);
	            }catch (IOException e) {
	    			e.printStackTrace();
	    		}
	    		
	    		String historyFile = properties.getProperty(org.linear.spark.util.Constants.LINEAR_HISTORY);
	    		
        		HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false); 
        		notifierObj.start();//The notification server starts at this point

	    		loadHistoricalInfo(historyFile);
	        	notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data
	        	
	        	//We need to announce on the Zookeeper where (on which host) did the history loading happened.
              	try{
            		String host = ZookeeperInterface.getValueAtPath("/lr/history/host");

            		if(host == null){
            			ZookeeperInterface.createGroup("lr");
            			ZookeeperInterface.createGroup("lr/history");
            			ZookeeperInterface.createGroup("lr/history/host");
            		}    		

            		ZookeeperInterface.setValueAtPath("/lr/history/host", InetAddress.getLocalHost().getHostAddress());

            	}catch(Exception e){
            		e.printStackTrace();
            	}	    		
	        }
		    
			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {	
				
				currentTime = System.currentTimeMillis();
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				
				outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
				outLogger.flush();
				
				return arg0.map(accidentLowLevelFunction);
			}
			
			private void loadHistoricalInfo(String inputFileHistory) {
				BufferedReader in;
				try {
					in = new BufferedReader(new FileReader(inputFileHistory));
				
					String line;
					int counter = 0;
					int batchCounter = 0;
					int BATCH_LEN = 10000;//A batch size of 1000 to 10000 is usually OK		
					Statement stmt;
					StringBuilder builder = new StringBuilder();
							
					//log.info(Utilities.getTimeStamp() + " : Loading history data");
					while((line = in.readLine()) != null){			
						//#(1 8 0 55)
						/*
						0 - Car ID
						1 - day
						2 - x - Expressway number
						3 - daily expenditure
						*/
						
						String[] fields = line.split("\\s+");
						fields[0] = fields[0].substring(2);
						fields[3] = fields[3].substring(0, fields[3].length() - 1);
						
						historyEvtList.add(new org.linear.spark.events.HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
						counter++;
					}
					
					try {
						PrintWriter customLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-custom-logger.text", true)));
						customLogger.println("Loaded " + counter + " history events.");
						customLogger.flush();
						customLogger.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				//log.info(Utilities.getTimeStamp() + " : Done Loading history data");
				//Just notfy this to the input event injector so that it can start the data emission process
				try {
					PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
					writer.println("\n");
					writer.flush();
					writer.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				
		}
	    };	
	        
	    final Function<String, String> persistFunctionLowLevel = new Function<String, String>(){

		    private long tupleCounter = 0;
		    private int tupleCountingWindow = 5000;//This is in miliseconds
		    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
		    private transient PrintWriter outLogger = null;
		    private int dataRate = 0;
		    private long currentTime = 0;	
		    private long expStartTime = 0;
		    
		    private transient boolean firstFlag = true;	    	
	    	
			@Override
			public String call(String arg0) throws Exception {
				
				//This is very tricky, but if we get an empty packet we should not process it, it seems
				//there is no other alternative than just to forward it to the next step. Otherwise we may get NullPointer exception in the latter half of the code.
				//when we try to parse it to obtain numerical values.

				return arg0;
			}    	
	    };
	    
	    Function<JavaRDD<String>, JavaRDD<String>> persistFunction = new Function<JavaRDD<String>, JavaRDD<String>>(){
		    private transient PrintWriter outLogger = null;
		    private long currentTime = 0;
		    
	        {
				try {
					outLogger = new PrintWriter(new BufferedWriter(new FileWriter("/home/miyuru/projects/pamstream/experiments/spark-output-rate.csv", true)));
					outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>");
					outLogger.println("Date,Wall clock time (s),TTuples");
					outLogger.flush();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
	        }
		    
			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {	
				
				currentTime = System.currentTimeMillis();
				Date date = new Date(currentTime);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				
				outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
				outLogger.flush();
				
				return arg0.map(persistFunctionLowLevel);
			}
	    };
	    
	  //----------------------------- Data flow graph definition ---------------------------------------------//
	    
	    JavaDStream<String> position_reports_tuples = inputTuples.filter(positionResportsFilterFunction); 
	    JavaDStream<String> account_balance_tuples = inputTuples.filter(accountBalanceFilterFunction);
	    	    
	    JavaDStream<String> expenditure_report_tuples = inputTuples.filter(expenditureReportsFilterFunction);
	    	    
	    //The following function will not be implemented in this version of Linear Road
	    JavaDStream<String> travel_time_tuples = inputTuples.filter(travelTimeReportsFilterFunction);
	    
	    JavaDStream<String> accident_result_tuples = position_reports_tuples.transform(accidentFunction);	    
	    JavaDStream<String> segstat_result_tuples = position_reports_tuples.transform(segStatFunction);
	    
	    //segstat_result_tuples.print();
	    
	    
	    //below we merge three DStreams to a single one and feed it to toll calculation function
	    JavaDStream<String> position_reports_tuples_union_segstat_result_tuples_union_accident_result_tuples = position_reports_tuples.union(accident_result_tuples.union(segstat_result_tuples));
	    
	    JavaDStream<String> toll_result_tuples = position_reports_tuples_union_segstat_result_tuples_union_accident_result_tuples.transform(tollFunction);
	    JavaDStream<String> dailyExpense_result_tuples = expenditure_report_tuples.transform(dailyExpenseFunction);
	    //below we merge two DStreams in to one and feed it to account balance calculation function
	    JavaDStream<String> account_balance_tuples_union_toll_result_tuples = account_balance_tuples.union(toll_result_tuples);
	    	    
	    JavaDStream<String> accbalance_result_tuples = account_balance_tuples_union_toll_result_tuples.transform(accBalanceFunction);
	    	    
	    //Here the output will receive batches of tuples of the form "|"
	    JavaDStream<String> output_tuples = dailyExpense_result_tuples.union(toll_result_tuples.union(accbalance_result_tuples));
	    //The final step
	    output_tuples.print();
	    
	    JavaDStream<String> final_tuples = output_tuples.transform(persistFunction);
	    final_tuples.print(); //We have to have this kind of thing rather directly using output_tuples.print(); because we need to measure the output data rate.
	    
	    ssc.start();
	    ssc.awaitTermination();
	}
}
