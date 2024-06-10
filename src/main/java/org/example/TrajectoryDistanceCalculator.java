package org.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

public class TrajectoryDistanceCalculator {

    /**
     * Calculates the distance between a given trajectory point and a predefined trajectory.
     * @param conn The database connection.
     * @param latitude Latitude of the trajectory point.
     * @param longitude Longitude of the trajectory point.
     * @param predefinedTrajectory The predefined trajectory as a WKT string.
     * @return The distance in meters.
     * @throws SQLException If a SQL error occurs during the query.
     */
    public static double calculateDistance(Connection conn, double latitude, double longitude, String predefinedTrajectory) throws SQLException {
        // Construct the WKT string for the point with a specific locale
        String pointWKT = String.format(Locale.US, "POINT(%f %f)", longitude, latitude);

        // SQL query to calculate the distance
        String query = "SELECT ST_Distance(ST_GeomFromText(?, 4326), ST_GeomFromText(?, 4326))";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, pointWKT);
            stmt.setString(2, predefinedTrajectory);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getDouble(1);
            }
        }
        return -1; // Indicate an error or no data
    }
}
