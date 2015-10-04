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
