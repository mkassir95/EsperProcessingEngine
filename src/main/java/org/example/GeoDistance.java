package org.example;

public class GeoDistance {

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
    public static double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c; // Distance in meters
    }

    /**
     * Calculate the total distance traveled over a sequence of trajectory points.
     *
     * @param points An array of TrajectoryDataType points representing a path.
     * @return The total distance traveled in meters.
     */
    public static double calculateTotalDistance(TrajectoryDataType[] points) {
        if (points == null || points.length < 2) {
            return 0; // Not enough points to calculate distance
        }

        double totalDistance = 0;
        TrajectoryDataType previousPoint = points[0];

        for (int i = 1; i < points.length; i++) {
            TrajectoryDataType currentPoint = points[i];
            totalDistance += haversineDistance(previousPoint.getLatitude(), previousPoint.getLongitude(),
                    currentPoint.getLatitude(), currentPoint.getLongitude());
            previousPoint = currentPoint;
        }

        return totalDistance;
    }
}
