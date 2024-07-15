package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SpeedWindowTracker {
    private EPRuntime runtime;
    private KafkaProducer<String, String> producer;
    private String currentDeploymentId = null;

    public SpeedWindowTracker(EPRuntime runtime) {
        this.runtime = runtime;
        setupKafkaProducer();
        configureInitialWindow();
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.63.64.48:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void configureInitialWindow() {
        deployWindow(60);  // Start with a default window of 60 seconds
    }

    private void deployWindow(int seconds) {
        if (currentDeploymentId != null) {
            try {
                runtime.getDeploymentService().undeploy(currentDeploymentId);
            } catch (Exception e) {
                System.err.println("Failed to undeploy previous window: " + e.getMessage());
            }
        }

        String epl = String.format(
                "select id, avg(speed) as avgSpeed from TrajectoryDataType.win:time_batch(%d sec) group by id",
                seconds);

        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            EPCompiled compiledQuery = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiledQuery);
            currentDeploymentId = deployment.getDeploymentId();
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null) {
                    for (EventBean eventBean : newData) {
                        String robotId = (String) eventBean.get("id");
                        double avgSpeed = (double) eventBean.get("avgSpeed");
                        String speedStatus = determineSpeedStatus(avgSpeed);
                        int newWindowSeconds = determineWindowSize(avgSpeed);
                        if (newWindowSeconds != seconds) {  // Only redeploy if the window size needs to change
                            deployWindow(newWindowSeconds);
                        }

                        String message = robotId + "@" + avgSpeed + "@" + speedStatus + "@" + newWindowSeconds + " seconds";
                        producer.send(new ProducerRecord<>("average_speed", robotId, message));
                        System.out.printf("Published to Kafka -> Robot ID: %s, Avg Speed: %.2f m/s, Status: %s, Window: %s%n",
                                robotId, avgSpeed, speedStatus, newWindowSeconds + " seconds");
                    }
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
        }
    }

    private int determineWindowSize(double avgSpeed) {
        if (avgSpeed == 0 || avgSpeed >= 46) {
            return 3;
        } else if ((avgSpeed >= 1 && avgSpeed <= 15) || (avgSpeed >= 30 && avgSpeed <= 45)) {
            return 30;
        } else {
            return 60;
        }
    }

    private String determineSpeedStatus(double speed) {
        if (speed == 0 || speed >= 46) {
            return "speed_alert";
        } else if ((speed >= 1 && speed <= 15) || (speed >= 30 && speed <= 45)) {
            return "speed_warning";
        } else {
            return "speed_ok";
        }
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.getCommon().addEventType(TrajectoryDataType.class);
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);
        new SpeedWindowTracker(runtime);
    }
}
