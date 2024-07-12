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

    public SpeedWindowTracker(EPRuntime runtime) {
        this.runtime = runtime;
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
        configureWindow(60);
        configureWindow(30);
        configureWindow(3);
    }

    private void configureWindow(int seconds) {
        String epl = String.format(
                "select id, avg(speed) as avgSpeed " +
                        "from TrajectoryDataType.win:time_batch(%d sec) " +
                        "group by id",
                seconds);

        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            EPCompiled compiledQuery = compiler.compile(epl, arguments);
            EPStatement statement = runtime.getDeploymentService().deploy(compiledQuery).getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null) {
                    for (EventBean eventBean : newData) {
                        String robotId = (String) eventBean.get("id");
                        double avgSpeed = (double) eventBean.get("avgSpeed");
                        String speedStatus = determineSpeedStatus(avgSpeed);
                        String windowDesc = seconds + " seconds"; // Descriptive window size
                        String message = robotId + "@" + avgSpeed + "@" + speedStatus + "@" + windowDesc;
                        producer.send(new ProducerRecord<>("average_speed", robotId, message));
                        System.out.printf("Published to Kafka -> Robot ID: %s, Avg Speed: %.2f m/s, Status: %s, Window: %s%n",
                                robotId, avgSpeed, speedStatus, windowDesc);
                    }
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL for window every " + seconds + " seconds: " + e.getMessage());
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
