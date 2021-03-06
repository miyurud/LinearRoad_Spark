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
package org.linear.spark.events;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author miyuru
 *
 */
public class PositionReportEvent implements Serializable{
	public long time; //A timestamp measured in seconds since the start of the simulation
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getVid() {
		return vid;
	}

	public void setVid(int vid) {
		this.vid = vid;
	}

	public byte getSpeed() {
		return speed;
	}

	public void setSpeed(byte speed) {
		this.speed = speed;
	}

	public byte getxWay() {
		return xWay;
	}

	public void setxWay(byte xWay) {
		this.xWay = xWay;
	}

	public byte getMile() {
		return mile;
	}

	public void setMile(byte mile) {
		this.mile = mile;
	}

	public short getOffset() {
		return offset;
	}

	public void setOffset(short offset) {
		this.offset = offset;
	}

	public byte getLane() {
		return lane;
	}

	public void setLane(byte lane) {
		this.lane = lane;
	}

	public byte getDir() {
		return dir;
	}

	public void setDir(byte dir) {
		this.dir = dir;
	}

	public int vid; //vehicle identifier
	public byte speed; // An integer number of miles per hour between 0 and 100 
	public byte xWay; //Express way number 0 .. 9
	public byte mile; //Mile number 0..99
	public short offset; // Yards since last Mile Marker 0..1759
	public byte lane; //Travel Lane 0..7. The lanes 0 and 7 are entrance/exit ramps
	public byte dir; //Direction 0(West) or 1 (East)
	private static Log log = LogFactory.getLog(PositionReportEvent.class);
	
	public PositionReportEvent(String[] fields) {	
		this.time = Long.parseLong(fields[1]);//Seconds since start of simulation
		this.vid = Integer.parseInt(fields[2]);//Car ID
		this.speed = Byte.parseByte(fields[3]);//An integer number of miles per hour
		this.xWay = Byte.parseByte(fields[4]);//Expressway number 
		this.mile = Byte.parseByte(fields[7]);//Mile (This corresponds to the seg field in the original table)
		this.offset  = (short)(Integer.parseInt(fields[8]) - (this.mile * 5280)); //Distance from the last mile post
		this.lane = Byte.parseByte(fields[5]); //The lane number
		this.dir = Byte.parseByte(fields[6]); //Direction (west = 0; East = 1)
	}
	
	//This is special constructor made to support internal protocol based packets. In future this constartur should be merged with the 
	//general one which accept only a String[]
	public PositionReportEvent(String[] fields, boolean flg) {	
		this.time = Long.parseLong(fields[2]);//Seconds since start of simulation
		this.vid = Integer.parseInt(fields[3]);//Car ID
		this.speed = Byte.parseByte(fields[4]);//An integer number of miles per hour
		this.xWay = Byte.parseByte(fields[5]);//Expressway number 
		try{
			this.mile = Byte.parseByte(fields[8]);//Mile (This corresponds to the seg field in the original table)
		}catch(NumberFormatException e){
			log.error("PositionReportEvent object : " + fields.toString() + " have incorrect mile value : " + fields[8]);
		}
		this.offset  = (short)(Integer.parseInt(fields[9]) - (this.mile * 5280)); //Distance from the last mile post
		this.lane = Byte.parseByte(fields[6]); //The lane number
		this.dir = Byte.parseByte(fields[7]); //Direction (west = 0; East = 1)
	}
	
	public String toString(){
		return "PositionReportEvent -> Time : " + this.time + " vid : " + this.vid + " speed : " + this.speed + " xWay : " + this.xWay + "...";
	}

}
