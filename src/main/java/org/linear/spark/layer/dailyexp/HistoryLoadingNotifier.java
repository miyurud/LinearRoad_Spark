/**
 * 
 */
package org.linear.spark.layer.dailyexp;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

import org.linear.spark.util.Constants;

/**
 * @author miyuru
 *
 */
public class HistoryLoadingNotifier extends Thread {
	private ServerSocket svr;
	private ArrayList<HistoryLoadingNotifierSession> sessionList;
	private boolean statusFlag;
	private boolean shtdnFlag;
	
	public HistoryLoadingNotifier(boolean status){
		statusFlag = status;
		sessionList = new ArrayList<HistoryLoadingNotifierSession>();
	}
	
	public void setStatus(boolean flg){
		statusFlag = flg;
		
		Iterator<HistoryLoadingNotifierSession> itr = sessionList.iterator();
		while(itr.hasNext()){
			HistoryLoadingNotifierSession obj = itr.next();
			obj.setStatus(statusFlag);
		}
	}
	
	public void run(){
		try {
			svr = new ServerSocket(Constants.HISTORY_LOADING_NOTIFIER_PORT);
			
			while (!shtdnFlag){
				Socket skt = svr.accept();
				HistoryLoadingNotifierSession session = new HistoryLoadingNotifierSession(this, skt, statusFlag);
				sessionList.add(session);
				session.start();//start running the thread
			}
		} catch (IOException e) {
			System.out.println("There is already a History Loading Notifier running in the designated port...");
		}
	}

	public void shutdown() {
		shtdnFlag = true;
	}
}
