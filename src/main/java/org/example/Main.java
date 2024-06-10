package org.example;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;

import java.sql.Connection;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        // connect to database
        try {
            Connection conn = SpatialDatabaseManager.getConnection();
            SpatialDatabaseManager.initializePolygonTable(conn);
            String polygonWKT = SpatialDatabaseManager.getPolygon(conn);
            SpatialDatabaseManager.initializeTrajectoryTable(conn);  // Ensure this is included if you have implemented it

            String trajectoryWKT = SpatialDatabaseManager.getPredefinedTrajectory(conn); // Retrieve the predefined trajectory
            if (trajectoryWKT != null) {
                System.out.println("Predefined Trajectory WKT: " + trajectoryWKT);
            } else {
                System.out.println("No predefined trajectory found.");
            }
            if (polygonWKT != null) {
                System.out.println("Polygon WKT: " + polygonWKT);
            } else {
                System.out.println("No polygon found.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // Setup the Esper configuration and runtime
        Configuration config = new Configuration();
        config.getCommon().addEventType(TrajectoryDataType.class);
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);

        // Setup the trajectory inside polygon checker
        new TrajectoryInsidePolygonChecker(runtime);

        // Setup the average speed calculation
        AverageSpeedCalculator averageSpeedCalculator = new AverageSpeedCalculator(runtime);
        averageSpeedCalculator.setupAverageSpeedCalculation();

        // Setup the travelled distance calculation
        DistanceTravelledCalculator distanceCalculator = new DistanceTravelledCalculator(runtime);
        distanceCalculator.setupDistanceCalculation();

        // Setup the distance to predefined trajectory calculation
        new DistanceToPredefinedTrajectoryCalculator(runtime);

        // Main loop to simulate real-time data
        while (!Thread.currentThread().isInterrupted()) {
            TrajectoryDataType randomTrajectoryData = RandomTrajectoryDataTypeGenerator.generateRandomTrajectoryDataType();
            runtime.getEventService().sendEventBean(randomTrajectoryData, "TrajectoryDataType");

            System.out.println("ID: " + randomTrajectoryData.getId() + ", " +
                    "Timestamp: " + randomTrajectoryData.getTimestamp() + ", " +
                    "Latitude: " + randomTrajectoryData.getLatitude() + ", " +
                    "Longitude: " + randomTrajectoryData.getLongitude() + ", " +
                    "Speed: " + randomTrajectoryData.getSpeed());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Main thread interrupted, shutting down.");
                Thread.currentThread().interrupt(); // Proper handling to ensure clean exit
            }
        }
    }
}
