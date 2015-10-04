/**
 * 
 */
package org.linear.spark.input;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @author miyuru
 * This is a separate session created for each socket connection received by the InputEventInjector
 * component.
 */
public class InputInjectorSession {
	private Socket skt;
	PrintWriter out = null;

	public InputInjectorSession(Socket s){
		skt = s;
		try {
			out = new PrintWriter(skt.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void sendTuple(String tuple){
		out.println(tuple);
		out.flush();
	}
	
	public void close(){
		out.close();
	}
}
