import java.io.Serializable;
import java.nio.ByteBuffer;


public class DataRecord implements Serializable{

	
//	private byte[] deviceId;
	private int deviceId;
	private String arrivalTime;
	private String departureTime;
//	private byte[] durationSeconds;
	private long durationSeconds;
	private String streetMarker;
	private String sign;
	private String area;
//	private byte[]  streetId;
	private int  streetId;
	private String streetName;
	private String betweenStreet1;
	private String betweenStreet2;
//	private byte[]  sideofstreet;
	private int sideofstreet;
	private String inViolation;
	
	private int recordlength;
	

	public DataRecord() {
		
	}
//	
//	public DataRecord(byte[] deviceId, String arrivalTime, String departureTime, byte[] durationSeconds,
//			String streetMarker, String sign, String area, byte[] streetId, String streetName, String betweenStreet1,
//			String betweenStreet2, byte[] sideofstreet, String inViolation) {
//		super();
//		this.deviceId = deviceId;
//		this.arrivalTime = arrivalTime;
//		this.departureTime = departureTime;
//		this.durationSeconds = durationSeconds;
//		this.streetMarker = streetMarker;
//		this.sign = sign;
//		this.area = area;
//		this.streetId = streetId;
//		this.streetName = streetName;
//		this.betweenStreet1 = betweenStreet1;
//		this.betweenStreet2 = betweenStreet2;
//		this.sideofstreet = sideofstreet;
//		this.inViolation = inViolation;
//		int l = deviceId.length + arrivalTime.getBytes().length + departureTime.getBytes().length+durationSeconds.length +
//				streetMarker.getBytes().length + sign.getBytes().length + area.getBytes().length + streetId.length +
//				streetName.getBytes().length + betweenStreet1.getBytes().length + betweenStreet2.getBytes().length +
//				sideofstreet.length + inViolation.getBytes().length;
//		this.recordlength = l;
//	}

	public DataRecord(int deviceId, String arrivalTime, String departureTime, long durationSeconds, String streetMarker,
			String sign, String area, int streetId, String streetName, String betweenStreet1, String betweenStreet2,
			int sideofstreet, String inViolation) {
		super();
		this.deviceId = deviceId;
		this.arrivalTime = arrivalTime;
		this.departureTime = departureTime;
		this.durationSeconds = durationSeconds;
		this.streetMarker = streetMarker;
		this.sign = sign;
		this.area = area;
		this.streetId = streetId;
		this.streetName = streetName;
		this.betweenStreet1 = betweenStreet1;
		this.betweenStreet2 = betweenStreet2;
		this.sideofstreet = sideofstreet;
		this.inViolation = inViolation;
		int l = 4 + arrivalTime.getBytes().length + departureTime.getBytes().length+4+
				streetMarker.getBytes().length + sign.getBytes().length + area.getBytes().length + 4 +
				streetName.getBytes().length + betweenStreet1.getBytes().length + betweenStreet2.getBytes().length +
				4+ inViolation.getBytes().length;
		this.recordlength = l;
	}
	
	public int getRecordLength() {
		return recordlength;
	}

	
//	public byte[] getDeviceId() {
//		return deviceId;
//	}



//
//
//	public void setDeviceId(byte[] deviceId) {
//		this.deviceId = deviceId;
//	}





	public String getArrivalTime() {
		return arrivalTime;
	}





	public void setArrivalTime(String arrivalTime) {
		this.arrivalTime = arrivalTime;
	}


	public byte[] getArrivalByte() {
		return this.arrivalTime.getBytes();
	}


	public String getDepartureTime() {
		return departureTime;
	}





	public void setDepartureTime(String departureTime) {
		this.departureTime = departureTime;
	}

	public byte[] getDepartureByte() {
		return this.departureTime.getBytes();
	}



//
//	public byte[] getDurationSeconds() {
//		return durationSeconds;
//	}




//
//	public void setDurationSeconds(byte[] durationSeconds) {
//		this.durationSeconds = durationSeconds;
//	}





	public String getStreetMarker() {
		return streetMarker;
	}
	




	public void setStreetMarker(String streetMarker) {
		this.streetMarker = streetMarker;
	}


	public byte[] getMarkerByte() {
		return this.streetMarker.getBytes();
	}


	public String getSign() {
		return sign;
	}





	public void setSign(String sign) {
		this.sign = sign;
	}


	public byte[] getSignByte() {
		return this.sign.getBytes();
	}



	public String getArea() {
		return area;
	}





	public void setArea(String area) {
		this.area = area;
	}

	public byte[] getAreaByte() {
		return this.area.getBytes();
	}



//
//	public byte[] getStreetId() {
//		return streetId;
//	}




//
//	public void setStreetId(byte[] streetId) {
//		this.streetId = streetId;
//	}





	public String getStreetName() {
		return streetName;
	}





	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}

	public byte[] getStreetNameByte() {
		return this.streetName.getBytes();
	}



	
	public String getBetweenStreet1() {
		return betweenStreet1;
	}





	public void setBetweenStreet1(String betweenStreet1) {
		this.betweenStreet1 = betweenStreet1;
	}

	public byte[] getBetweenStreet1Byte() {
		return this.betweenStreet1.getBytes();
	}



	public String getBetweenStreet2() {
		return betweenStreet2;
	}





	public void setBetweenStreet2(String betweenStreet2) {
		this.betweenStreet2 = betweenStreet2;
	}

	public byte[] getBetweenStreet2Byte() {
		return this.betweenStreet2.getBytes();
	}



//	public byte[] getSideofstreet() {
//		return sideofstreet;
//	}
//
//
//
//
//
//	public void setSideofstreet(byte[] sideofstreet) {
//		this.sideofstreet = sideofstreet;
//	}


	


	public String getInViolation() {
		return inViolation;
	}





	public void setInViolation(String inViolation) {
		this.inViolation = inViolation;
	}


	public byte[] getInViolationByte() {
		return this.inViolation.getBytes();
	}





	@Override
	public String toString() {
		return "DataRecord [deviceId=" + deviceId + ", arrivalTime=" + arrivalTime + ", departureTime=" + departureTime
				+ ", durationSeconds=" + durationSeconds + ", streetMarker=" + streetMarker + ", sign=" + sign
				+ ", area=" + area + ", streetId=" + streetId + ", streetName=" + streetName + ", betweenStreet1="
				+ betweenStreet1 + ", betweenStreet2=" + betweenStreet2 + ", sideofstreet=" + sideofstreet
				+ ", inViolation=" + inViolation + "]";
	}
	
	
	
	
	
	

}
