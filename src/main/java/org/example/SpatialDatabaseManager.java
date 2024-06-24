package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Manages database operations related to spatial data using H2GIS.
 */
public class SpatialDatabaseManager {

    // Connection URL for the H2 database without INIT to run an external script
    private static final String DB_URL = "jdbc:h2:~/spatialDB;AUTO_SERVER=TRUE;DATABASE_TO_UPPER=false";

    /**
     * Initializes and returns a connection to the H2GIS database.
     * This method ensures H2GIS functions are available by loading them directly via SQL.
     * @return A connection to the database.
     * @throws SQLException If a database access error occurs.
     */
    public static Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL, "sa", "");
        loadH2GISFunctions(conn);  // Load H2GIS functions
        return conn;
    }

    /**
     * Loads H2GIS functions into the database if not already loaded.
     * This method executes SQL commands directly to ensure H2GIS functionality.
     * @param conn The database connection.
     * @throws SQLException If executing the SQL command fails.
     */
    private static void loadH2GISFunctions(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE ALIAS IF NOT EXISTS H2GIS_SPATIAL FOR \"org.h2gis.functions.factory.H2GISFunctions.load\";");
            stmt.execute("CALL H2GIS_SPATIAL();");
        }
    }

    /**
     * Ensures that the polygon table exists and contains the predefined polygon.
     * @param conn The database connection.
     * @throws SQLException If a SQL error occurs.
     */
    public static void initializePolygonTable(Connection conn) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS Polygons (id INT PRIMARY KEY, geom GEOMETRY);";
        String clearTableSQL = "DELETE FROM Polygons;";
        String insertPolygonsSQL = "INSERT INTO Polygons (id, geom) VALUES "
                + "(1, 'POLYGON ((46.3389 3.4336, 46.3393 3.4337, 46.3392 3.4339, 46.3391 3.4340, 46.3390 3.4338, 46.3389 3.4336))');";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            stmt.execute(clearTableSQL);
            stmt.execute(insertPolygonsSQL);
        }
    }


    /**
     * Retrieves the stored polygon as a WKT string.
     * @param conn The database connection.
     * @return The polygon in WKT format or null if not found.
     * @throws SQLException If a SQL error occurs.
     */
    public static String getPolygon(Connection conn) throws SQLException {
        String query = "SELECT ST_AsText(geom) FROM Polygons WHERE id = 1;";
        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    /**
     * Checks if a given point is inside the stored polygon.
     * @param latitude The latitude of the point.
     * @param longitude The longitude of the point.
     * @return true if the point is inside the polygon, false otherwise.
     */
    public static boolean isPointInsidePolygon(double latitude, double longitude) {
        try (Connection conn = getConnection()) {
            return isPointInsidePolygon(conn, latitude, longitude);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Checks if a given point is inside the stored polygon.
     * @param conn The database connection.
     * @param latitude The latitude of the point.
     * @param longitude The longitude of the point.
     * @return true if the point is inside the polygon, false otherwise.
     * @throws SQLException If a SQL error occurs.
     */
    private static boolean isPointInsidePolygon(Connection conn, double latitude, double longitude) throws SQLException {
        String query = "SELECT ST_Contains(" +
                "(SELECT geom FROM Polygons WHERE id = 1), " +
                "ST_GeomFromText('POINT(' || ? || ' ' || ? || ')', 4326))";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setDouble(1, latitude);
            stmt.setDouble(2, longitude);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    public static void main(String[] args) {
        // Initialize the polygon table and print the stored polygon
        try (Connection conn = SpatialDatabaseManager.getConnection()) {
            SpatialDatabaseManager.initializePolygonTable(conn);
            String polygonWKT = SpatialDatabaseManager.getPolygon(conn);
            System.out.println("Stored Polygon: " + polygonWKT);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Check if points are inside the polygon
        boolean isInside = SpatialDatabaseManager.isPointInsidePolygon(0, 0); // Point inside the hexagon
        System.out.println("Is Point Inside Polygon: " + isInside);

        isInside = SpatialDatabaseManager.isPointInsidePolygon(100, 100); // Point outside the hexagon
        System.out.println("Is Point Inside Polygon: " + isInside);
    }

    /**
     * Initializes the trajectory table and inserts a predefined trajectory if not present.
     * @param conn The database connection.
     * @throws SQLException If a SQL error occurs.
     */
    public static void initializeTrajectoryTable(Connection conn) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS Trajectories (id INT PRIMARY KEY, geom GEOMETRY);";
        String insertTrajectorySQL = "MERGE INTO Trajectories KEY(id) VALUES (1, 'LINESTRING (30 10, 10 30, 40 40, 20 20, 50 50, 70 70, 90 90, 110 110, 130 130, 150 150, 170 170, 190 190, 210 210, 230 230, 250 250, 270 270, 290 290)');";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            stmt.execute(insertTrajectorySQL);
        }
    }

    /**
     * Retrieves the predefined trajectory as a WKT string.
     * @param conn The database connection.
     * @return The trajectory in WKT format or null if not found.
     * @throws SQLException If a SQL error occurs.
     */
    public static String getPredefinedTrajectory(Connection conn) throws SQLException {
        String query = "SELECT ST_AsText(geom) FROM Trajectories WHERE id = 1;";
        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

}
