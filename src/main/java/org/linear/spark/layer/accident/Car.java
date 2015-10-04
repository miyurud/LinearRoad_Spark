package org.linear.spark.layer.accident;

public class Car {
	public long time;
	public int carid;
	public byte speed, mile0, mile1, mile2, mile3, xway0, xway1, xway2, xway3, lane0, lane1, lane2, lane3, dir0, dir1, dir2, dir3, offset0, offset1, offset2, offset3;
	public boolean notified;
	public byte posReportID; //This value can range from 0 to 3 (4 position reports)
	
	public Car() {
		this.time = -1; this.carid = 0; this.speed = 0;
		this.xway0 = -1; this.xway1 = -1; this.xway2 = -1; this.xway3 = -1;
		this.lane0 = -1; this.lane1 = -1; this.lane2 = -1; this.lane3 = -1;
		this.dir0 = -1; this.dir1 = -1; this.dir2 = -1; this.dir3 = -1;
	}
	
	public Car(long time, int carid, byte speed, byte xway0, byte lane0, byte dir0, byte mile) {
		this.time = time; this.carid = carid; this.speed = speed;
		this.xway0 = xway0; this.lane0 = lane0; this.dir0 = dir0;
		this.mile0 = mile;
	}
	
	public String toString(){
		return "Car carid=" + this.carid;
	}
	
	/**
	 * We override the default equals method for Car.
	 * @param obj2
	 * @return
	 */
	public boolean equals(Car obj2){
		if(this.carid == obj2.carid){
			return true;
		}else{
			return false;
		}
	}
}
