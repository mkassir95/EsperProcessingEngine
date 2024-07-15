package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class GpsSpeedWindowTracker {
    private EPRuntime runtime;
    private Map<String, TrajectoryDataType> previousDataPoints;
    private KafkaProducer<String, String> producer;
    private String currentDeploymentId;

    public GpsSpeedWindowTracker(EPRuntime runtime) {
        this.runtime = runtime;
        this.previousDataPoints = new HashMap<>();
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
            String message = String.format(Locale.US, "Robot ID: %s, Last Speed: %.6f, Last GPS: %.6f meters, Window Size: %s seconds",
                    data.getId(), data.getSpeed(), totalDistance, windowSeconds);
            producer.send(new ProducerRecord<>("last_speed_gps", data.getId(), message));
            System.out.println(message);
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

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.getCommon().addEventType(TrajectoryDataType.class);
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);
        new GpsSpeedWindowTracker(runtime);
    }
}
