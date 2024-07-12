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

    public GpsSpeedWindowTracker(EPRuntime runtime) {
        this.runtime = runtime;
        this.previousDataPoints = new HashMap<>();
        setupKafkaProducer();
        setupWindowsForTracking();
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.63.64.48:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void setupWindowsForTracking() {
        configureWindow("60");
        configureWindow("30");
        configureWindow("3");
    }

    private void configureWindow(String seconds) {
        String epl = String.format(
                "select id, latitude, longitude, speed, timestamp " +
                        "from TrajectoryDataType.win:time_batch(%s sec) " +
                        "order by id, timestamp",
                seconds);

        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            EPCompiled compiledQuery = compiler.compile(epl, arguments);
            EPStatement statement = runtime.getDeploymentService().deploy(compiledQuery).getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null) {
                    Map<String, Double> totalDistances = new HashMap<>();
                    Map<String, TrajectoryDataType> lastDataPoints = new HashMap<>();

                    for (EventBean eventBean : newData) {
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

                    for (Map.Entry<String, Double> entry : totalDistances.entrySet()) {
                        String robotId = entry.getKey();
                        double totalDistance = entry.getValue();
                        TrajectoryDataType lastData = lastDataPoints.get(robotId);

                        if (lastData != null) {
                            String speedStatus;
                            double speed = lastData.getSpeed();
                            if (speed == 0 || speed >= 46) {
                                speedStatus = "speed_alert";
                            } else if ((speed >= 1 && speed <= 15) || (speed >= 30 && speed <= 45)) {
                                speedStatus = "speed_warning";
                            } else {
                                speedStatus = "speed_ok";
                            }

                            String distanceStatus;
                            if (totalDistance <= 5) {
                                distanceStatus = "distance_alert";
                            } else if (totalDistance >= 6 && totalDistance <= 10) {
                                distanceStatus = "distance_warning";
                            } else {
                                distanceStatus = "distance_ok";
                            }

                            String message = String.format(Locale.US, "Robot ID: %s, Last Speed: %.6f, Last GPS: %.6f meters, Window Size: %s seconds, Speed Status: %s, Distance Status: %s",
                                    lastData.getId(), lastData.getSpeed(), totalDistance, seconds, speedStatus, distanceStatus);
                            producer.send(new ProducerRecord<>("last_speed_gps", robotId, message));
                            System.out.println(message);
                        }
                    }

                    // Clear the previous data points to start fresh for the next window
                    previousDataPoints.clear();
                    previousDataPoints.putAll(lastDataPoints);
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL for window every " + seconds + " seconds: " + e.getMessage());
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
