package org.example;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // Setup the Esper configuration and runtime
        Configuration config = new Configuration();
        config.getCommon().addEventType(TrajectoryDataType.class);
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);

        // Start the socket server and pass the runtime
        startSocketServer(runtime);

        // Connect to the database and set up tables and predefined data
        try (Connection conn = SpatialDatabaseManager.getConnection()) {
            SpatialDatabaseManager.initializePolygonTable(conn);
            String polygonWKT = SpatialDatabaseManager.getPolygon(conn);
            SpatialDatabaseManager.initializeTrajectoryTable(conn);
            String trajectoryWKT = SpatialDatabaseManager.getPredefinedTrajectory(conn);

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

        // Initialize calculators and checkers
        new TrajectoryInsidePolygonChecker(runtime);
        AverageSpeedCalculator averageSpeedCalculator = new AverageSpeedCalculator(runtime);
        averageSpeedCalculator.setupAverageSpeedCalculation();
        DistanceTravelledCalculator distanceCalculator = new DistanceTravelledCalculator(runtime);
        distanceCalculator.setupDistanceCalculation();
        new DistanceToPredefinedTrajectoryCalculator(runtime);
        SpeedTrendAnalyzer speedTrendAnalyzer = new SpeedTrendAnalyzer(runtime);

        // Keep the main thread alive or perform other tasks
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Main thread interrupted, shutting down.");
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void startSocketServer(EPRuntime runtime) {
        Thread thread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(5000)) {
                System.out.println("Server is listening on port 5000.");
                while (!Thread.currentThread().isInterrupted()) {
                    try (Socket clientSocket = serverSocket.accept();
                         BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                        String inputLine;
                        while ((inputLine = reader.readLine()) != null) {
                            String[] dataParts = inputLine.split(",");
                            long timestamp = 0;
                            String id = "";
                            double latitude = 0.0;
                            double longitude = 0.0;
                            float speed = 0.0f;

                            for (String part : dataParts) {
                                String[] keyValue = part.split(":");
                                if (keyValue.length == 2) {
                                    String key = keyValue[0].trim();
                                    String value = keyValue[1].trim();
                                    switch (key) {
                                        case "Timestamp":
                                            timestamp = Long.parseLong(value);
                                            break;
                                        case "Robot ID":
                                            id = value;
                                            break;
                                        case "Latitude":
                                            latitude = Double.parseDouble(value);
                                            break;
                                        case "Longitude":
                                            longitude = Double.parseDouble(value);
                                            break;
                                        case "Speed":
                                            speed = Float.parseFloat(value);
                                            break;
                                    }
                                }
                            }

                            TrajectoryDataType trajectoryData = new TrajectoryDataType(id, timestamp, latitude, longitude, speed);
                            runtime.getEventService().sendEventBean(trajectoryData, "TrajectoryDataType");
                            //System.out.println("Received: " + trajectoryData.getId());
                        }
                    } catch (Exception e) {
                        System.out.println("Error handling client: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                System.out.println("Server socket error: " + e.getMessage());
                e.printStackTrace();
            }
        });
        thread.start();
    }
}
