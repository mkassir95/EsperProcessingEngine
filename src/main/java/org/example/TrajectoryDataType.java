package org.example;

public class TrajectoryDataType {

    private String id;
    private long timestamp;
    private double latitude;
    private double longitude;
    private  double speed;

    public TrajectoryDataType(String id, long timestamp, double latitude,double longitude, double speed) {
        this.id = id;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude=longitude;
        this.speed = speed;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getSpeed() {
        return speed;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public double getLongitude() {
        return longitude;
    }
}
