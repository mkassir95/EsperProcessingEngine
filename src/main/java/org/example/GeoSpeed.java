package org.example;

import java.util.List;

public class GeoSpeed {

    private static final double EARTH_RADIUS = 6371e3; // Earth's radius in meters

    /**
     * Calculate the Haversine distance between two geographic points.
     *
     * @param lat1 Latitude of the first point.
     * @param lon1 Longitude of the first point.
     * @param lat2 Latitude of the second point.
     * @param lon2 Longitude of the second point.
     * @return The distance between the points in meters.
     */
    private static double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c; // distance in meters
    }

    /**
     * Calculate the weighted average speed over a list of trajectory points.
     *
     * @param points List of TrajectoryDataType points.
     * @return The weighted average speed in m/s.
     */
    public static double calculateWeightedAverageSpeed(List<TrajectoryDataType> points) {
        if (points == null || points.size() < 2) {
            throw new IllegalArgumentException("At least two points are required to calculate the weighted average speed.");
        }

        double totalDistance = 0;
        double weightedSpeedSum = 0;

        for (int i = 1; i < points.size(); i++) {
            TrajectoryDataType p1 = points.get(i - 1);
            TrajectoryDataType p2 = points.get(i);

            double distance = haversineDistance(p1.getLatitude(), p1.getLongitude(), p2.getLatitude(), p2.getLongitude());
            double avgSpeed = (p1.getSpeed() + p2.getSpeed()) / 2;

            totalDistance += distance;
            weightedSpeedSum += avgSpeed * distance;
        }

        return totalDistance > 0 ? weightedSpeedSum / totalDistance : 0;
    }
}
