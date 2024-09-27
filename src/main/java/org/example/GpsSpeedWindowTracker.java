package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class GpsSpeedWindowTracker {
    private EPRuntime runtime;
    private Map<String, TrajectoryDataType> previousDataPoints;
    private KafkaProducer<String, String> producer;
    private String currentDeploymentId;
    private Map<String, List<TrajectoryDataType>> allDataPointsByRobot;
    private Gson gson = new Gson(); // Create a Gson instance for object serialization
    private int counter=0;

    public GpsSpeedWindowTracker(EPRuntime runtime) {
        this.runtime = runtime;
        this.previousDataPoints = new HashMap<>();
        this.allDataPointsByRobot = new HashMap<>();
        setupKafkaProducer();
        try {
            configureWindow("60");  // Default to a window of 60 seconds initially
        } catch (EPUndeployException e) {
            System.err.println("Error during initial configuration: " + e.getMessage());
        }
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.63.64.48:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void configureWindow(String seconds) throws EPUndeployException {
        if (currentDeploymentId != null) {
            // Undeploy the current EPL statement by deployment ID
            try {
                runtime.getDeploymentService().undeploy(currentDeploymentId);
            } catch (EPUndeployException e) {
                System.err.println("Error undeploying the EPL statement: " + e.getMessage());
                throw e; // Rethrow to handle it at the caller level
            }
        }

        String epl = String.format(
                "select id, latitude, longitude, speed, timestamp " +
                        "from TrajectoryDataType.win:time_batch(%s sec) " +
                        "order by id, timestamp",
                seconds
        );

        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            EPCompiled compiledQuery = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiledQuery);
            currentDeploymentId = deployment.getDeploymentId();
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                Map<String, Double> totalDistances = new HashMap<>();
                Map<String, TrajectoryDataType> lastDataPoints = new HashMap<>();
                String newWindowSeconds = "60"; // Default to 60 seconds

                for (EventBean eventBean : newData) {
                    processEventBean(eventBean, totalDistances, lastDataPoints);
                }

                for (Map.Entry<String, Double> entry : totalDistances.entrySet()) {
                    TrajectoryDataType lastData = lastDataPoints.get(entry.getKey());
                    if (lastData != null) {
                        newWindowSeconds = determineWindowSize(lastData, entry.getValue());
                    }
                    publishToKafka(lastData, entry.getValue(), newWindowSeconds);
                }

                // Update window size if needed
                try {
                    configureWindow(newWindowSeconds);
                } catch (EPUndeployException e) {
                    System.err.println("Error reconfiguring window: " + e.getMessage());
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL for window every " + seconds + " seconds: " + e.getMessage());
        }
    }

    private void processEventBean(EventBean eventBean, Map<String, Double> totalDistances, Map<String, TrajectoryDataType> lastDataPoints) {
        String robotId = (String) eventBean.get("id");
        double latitude = (double) eventBean.get("latitude");
        double longitude = (double) eventBean.get("longitude");
        double speed = (double) eventBean.get("speed");
        long timestamp = (long) eventBean.get("timestamp");

        TrajectoryDataType currentData = new TrajectoryDataType(robotId, timestamp, latitude, longitude, speed);
        TrajectoryDataType previousData = previousDataPoints.get(robotId);
        allDataPointsByRobot.computeIfAbsent(robotId, k -> new ArrayList<>()).add(currentData); // Add current data point to list for this robot


        if (previousData != null) {
            double distance = calculateDistance(
                    previousData.getLatitude(), previousData.getLongitude(),
                    currentData.getLatitude(), currentData.getLongitude());

            totalDistances.put(robotId, totalDistances.getOrDefault(robotId, 0.0) + distance);
        }

        lastDataPoints.put(robotId, currentData);
        previousDataPoints.put(robotId, currentData);
    }

    private String determineWindowSize(TrajectoryDataType data, double lastGpsDistance) {
        if (data.getSpeed() == 0 || data.getSpeed() >= 46 || lastGpsDistance < 5) {
            return "3"; // Window of 3 seconds
        } else if ((data.getSpeed() >= 1 && data.getSpeed() <= 15) || (data.getSpeed() >= 30 && data.getSpeed() <= 45) ||
                (lastGpsDistance >= 6 && lastGpsDistance <= 10)) {
            return "30"; // Window of 30 seconds
        } else {
            return "60"; // Window of 60 seconds
        }
    }

    private void publishToKafka(TrajectoryDataType data, double totalDistance, String windowSeconds) {
        if (data != null) {
            producer.send(new ProducerRecord<>("topic_test", data.getId(), windowSeconds));
            // Retrieve all data points for the robot
            List<TrajectoryDataType> allDataPoints = allDataPointsByRobot.get(data.getId());

            // Generate filtered data points for the robot
            List<TrajectoryDataType> filteredDataPoints = new ArrayList<>();
            TrajectoryDataType previousPoint = null;
            for (TrajectoryDataType point : allDataPoints) {
                if (shouldIncludePoint(point, previousPoint)) {
                    filteredDataPoints.add(point);
                }
                previousPoint = point;
            }

            // Determine speed and distance status
            String speedStatus = determineSpeedStatus(data.getSpeed());
            String distanceStatus = determineDistanceStatus(totalDistance);

        // Serialize the filtered data points to JSON only if both conditions are "ok"
            String filteredDataPointsJson = (speedStatus.equals("speed_ok") && distanceStatus.equals("distance_ok"))
                    ? "[]"  // Provide an empty array if both conditions are "ok"
                    : gson.toJson(filteredDataPoints); // Include filtered data otherwise




            // Construct the message
            String message = String.format(Locale.US,
                    "%d %s %.6f %.6f %.6f",
                    data.getTimestamp(), // Assuming the timestamp is a long representing Unix time
                    data.getId(),
                    data.getLatitude(), data.getLongitude(), // Include latitude and longitude
                    data.getSpeed()); // Moving speed to the end of the message
            // Send the message to the same Kafka topic as before
            producer.send(new ProducerRecord<>("last_speed_gps", data.getId(), message));
            if (!speedStatus.equals("speed_ok") || !distanceStatus.equals("distance_ok")) {
                counter++;
                int messageCode=-1;
                if(speedStatus.equals("speed_alert") || distanceStatus.equals("distance_alert") ){
                    messageCode=1;
                }
                else if(speedStatus.equals("speed_warning") || distanceStatus.equals("distance_warning") ){
                    messageCode=0;
                }

                String newMessage = String.format(Locale.US,
                        "%d %s %d %.6f %.6f %.6f %d",
                        data.getTimestamp(), // Assuming the timestamp is a long representing Unix time
                        data.getId(),
                        counter,
                        data.getSpeed(),
                        data.getLatitude(), data.getLongitude(), // Include latitude and longitude,
                        messageCode
                        ); // Moving speed to the end of the message
                producer.send(new ProducerRecord<>("r2k_a_rob", data.getId(), newMessage));
                }

            System.out.println("important Published to Kafka: " + message);
            // If conditions are not "ok", send each filtered data point separately
            if (!speedStatus.equals("speed_ok") || !distanceStatus.equals("distance_ok")) {
                for (TrajectoryDataType filteredPoint : filteredDataPoints) {
                    String individualMessage = String.format(Locale.US,
                            "%d %s %.6f %.6f %.6f",
                            filteredPoint.getTimestamp(), // Assuming the timestamp is a long representing Unix time
                            filteredPoint.getId(),
                            filteredPoint.getLatitude(), filteredPoint.getLongitude(),
                            filteredPoint.getSpeed()); // Moving speed to the end of the message
                    // Send the individual message to the Kafka topic
                   // producer.send(new ProducerRecord<>("last_speed_gps", filteredPoint.getId(), individualMessage));
                    System.out.println("Published individual filtered point to Kafka: " + individualMessage);
                }
            } else {
                // If all conditions are "ok", no need to send the points separately
                System.out.println("No filtered data points needed to be sent.");
            }
        }
    }

    // Utility method to determine speed status based on the last speed
    private String determineSpeedStatus(double speed) {
        if (speed == 0 || speed >= 46) {
            return "speed_alert";
        } else if ((speed >= 1 && speed <= 15) || (speed >= 30 && speed <= 45)) {
            return "speed_warning";
        } else {
            return "speed_ok";
        }
    }

    // Utility method to determine distance status based on the last GPS distance
    private String determineDistanceStatus(double lastGpsDistance) {
        if (lastGpsDistance < 5) {
            return "distance_alert";
        } else if (lastGpsDistance >= 6 && lastGpsDistance <= 10) {
            return "distance_warning";
        } else {
            return "distance_ok";
        }
    }


    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371000; // Radius of the earth in meters
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // Distance in meters
    }



    // Adjust the shouldIncludePoint method to include only necessary conditions
    private boolean shouldIncludePoint(TrajectoryDataType currentPoint, TrajectoryDataType previousPoint) {
        return (currentPoint.getSpeed() == 0 || currentPoint.getSpeed() >= 46 ||
                (currentPoint.getSpeed() >= 1 && currentPoint.getSpeed() <= 15) ||
                (currentPoint.getSpeed() >= 30 && currentPoint.getSpeed() <= 45)) ||
                (previousPoint != null && checkDistance(previousPoint, currentPoint));
    }


    // Helper function to check the distance condition during normal filtering
    private boolean checkDistance(TrajectoryDataType previousPoint, TrajectoryDataType currentPoint) {
        double distance = calculateDistance(
                previousPoint.getLatitude(), previousPoint.getLongitude(),
                currentPoint.getLatitude(), currentPoint.getLongitude()
        );

        return (distance < 5 || (distance >= 6 && distance <= 10));
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.getCommon().addEventType(TrajectoryDataType.class);
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);
        new GpsSpeedWindowTracker(runtime);
    }
}
