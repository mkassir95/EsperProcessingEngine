package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class MultiWindowDistanceCalculator {
    private EPRuntime runtime;
    private Map<String, List<Double>> robotDistances = new HashMap<>();
    private Producer<String, String> producer;
    private int currentWindowSeconds = 60; // Start with 60 seconds as the initial window size

    public MultiWindowDistanceCalculator(EPRuntime runtime) {
        this.runtime = runtime;
        setupKafkaProducer();
        setupDistanceCalculationQuery(currentWindowSeconds); // Start with a 60 seconds window
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.63.64.48:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void setupDistanceCalculationQuery(int windowSeconds) {
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
        String epl = String.format("select * from TrajectoryDataType.win:time_batch(%d sec)", windowSeconds);

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];
            statement.addListener((newData, oldData, stat, rt) -> calculateDistances(newData));
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL for window " + windowSeconds + " seconds: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void calculateDistances(EventBean[] newData) {
        if (newData == null || newData.length == 0) {
            System.out.println("No new data received for the " + currentWindowSeconds + " second window.");
            return;
        }

        try (Connection conn = SpatialDatabaseManager.getConnection()) {
            String predefinedTrajectory = SpatialDatabaseManager.getPredefinedTrajectory(conn);
            for (EventBean eventBean : newData) {
                TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                double distance = TrajectoryDistanceCalculator.calculateDistance(
                        conn, trajectory.getLatitude(), trajectory.getLongitude(), predefinedTrajectory);
                robotDistances.computeIfAbsent(trajectory.getId(), k -> new ArrayList<>()).add(distance);
            }

            robotDistances.forEach((id, distances) -> {
                Collections.sort(distances);
                double medianDistance = calculateMedian(distances);
                adjustWindowBasedOnDistance(medianDistance);
                String status = getDistanceStatus(medianDistance);
                String message = id + "@" + medianDistance + "@" + status + "@" + currentWindowSeconds + " sec";
                producer.send(new ProducerRecord<>("distance_trajectory_ref", id, message));
                System.out.println("Multi window Distance Sent to Kafka -> Window " + currentWindowSeconds + "s, Message: " + message);
            });

            robotDistances.clear();
        } catch (SQLException e) {
            System.err.println("Database error during distance calculation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void adjustWindowBasedOnDistance(double medianDistance) {
        int newWindowSeconds = (medianDistance <= 5) ? 3 : (medianDistance <= 10) ? 30 : 60;
        if (newWindowSeconds != currentWindowSeconds) {
            try {
                // Attempt to undeploy all deployments
                runtime.getDeploymentService().undeployAll();
                currentWindowSeconds = newWindowSeconds;
                setupDistanceCalculationQuery(currentWindowSeconds);
            } catch (EPUndeployException e) {
                System.err.println("Failed to undeploy previous deployment: " + e.getMessage());
                try {
                    // Retry undeployment
                    runtime.getDeploymentService().undeployAll();
                    currentWindowSeconds = newWindowSeconds;
                    setupDistanceCalculationQuery(currentWindowSeconds);
                } catch (EPUndeployException retryException) {
                    System.err.println("Retry failed, check Esper runtime state: " + retryException.getMessage());
                    retryException.printStackTrace();
                }
            }
        }
    }

    private double calculateMedian(List<Double> distances) {
        int size = distances.size();
        if (size % 2 == 0) {
            return (distances.get(size / 2 - 1) + distances.get(size / 2)) / 2.0;
        } else {
            return distances.get(size / 2);
        }
    }

    private String getDistanceStatus(double distance) {
        if (distance <= 5) {
            return "distance_alert";
        } else if (distance <= 10) {
            return "distance_warning";
        } else {
            return "distance_ok";
        }
    }
}
