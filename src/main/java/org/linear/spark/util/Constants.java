/**
 * 
 */
package org.linear.spark.util;

/**
 * @author miyuru
 *
 */
public class Constants {
	public static final String INPUT_INJECTOR_HOST = "input.injector.host";//This is the host where the input data injector service runs on
	public static final int INPUT_INJECTOR_EXP_REPORT_PORT = 3787;
	public static final int INPUT_INJECTOR_ACCBALANCE_REPORT_PORT = 3788;	
	public static final int INPUT_INJECTOR_POSITION_REPORT_PORT = 3789;
	public static final int ACCDETECT_PORT = 3790;
	public static final int SEGSTAT_NOV_PORT = 3791;
	public static final int SEGSTAT_LAV_PORT = 3792;
	public static final int DAILYEXP_PORT = 3793;
	public static final int TOLLEXP_OUTPUT_PORT = 3794;
	public static final int TOLLEXP_ACCBALANCE_PORT = 3795;
	public static final int ACCBALANCE_PORT = 3796;
	
	//There are set of temporary ports for just idling. This is required for the sake of initiating the Spark Applications
	public static final int TEMP_INPUT_INJECTOR_EXP_REPORT_PORT = 3887;
	public static final int TEMP_INPUT_INJECTOR_ACCBALANCE_REPORT_PORT = 3888;	
	public static final int TEMP_INPUT_INJECTOR_POSITION_REPORT_PORT = 3889;
	public static final int TEMP_ACCDETECT_PORT = 3890;
	public static final int TEMP_SEGSTAT_NOV_PORT = 3891;
	public static final int TEMP_SEGSTAT_LAV_PORT = 3892;
	public static final int TEMP_DAILYEXP_PORT = 3893;
	public static final int TEMP_TOLLEXP_OUTPUT_PORT = 3894;
	public static final int TEMP_TOLLEXP_ACCBALANCE_PORT = 3895;
	public static final int TEMP_ACCBALANCE_PORT = 3896;
	
	public static final String CONFIG_FILENAME = "spark-LinearRoad.properties";
    public static final String LINEAR_HISTORY = "linear-history-file";
    public static final String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    
    public static final int POS_EVENT_TYPE = 0;
    public static final int ACC_BAL_EVENT_TYPE = 2;
    public static final int DAILY_EXP_EVENT_TYPE = 3;
    public static final int TRAVELTIME_EVENT_TYPE = 4;
    public static final int NOV_EVENT_TYPE = -5;
    public static final int LAV_EVENT_TYPE = 6;
    public static final int TOLL_EVENT_TYPE = 7;
    public static final int ACCIDENT_EVENT_TYPE = 8;
    
    public static final int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    //Once the input injector figures out that on which host each Spark function is running, it will 
    //send OK to proceed for connections through this service.
    public static final int CONNECTION_INITIATION_NOTIFIER_PORT = 2234;
}
