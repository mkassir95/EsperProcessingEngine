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

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class AverageSpeedCalculator {
    private EPRuntime runtime;
    private KafkaProducer<String, String> producer;

    public AverageSpeedCalculator(EPRuntime runtime) {
        this.runtime = runtime;
        setupKafkaProducer();
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.63.64.48:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void setupAverageSpeedCalculation() {
        String epl = "select id, window(*) as points from TrajectoryDataType.win:time_batch(15 sec) group by id";
        EPCompiled compiledQuery;
        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            compiledQuery = compiler.compile(epl, arguments);
            EPStatement statement = runtime.getDeploymentService().deploy(compiledQuery).getStatements()[0];

            // Add listener to the statement
            statement.addListener((newData, oldData, stat, rt) -> calculateAndPublishAverageMetrics(newData));

        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
        }
    }

    private void calculateAndPublishAverageMetrics(EventBean[] newData) {
        if (newData != null && newData.length > 0) {
            for (EventBean eventBean : newData) {
                String id = (String) eventBean.get("id");
                Object[] pointsArray = (Object[]) eventBean.get("points");
                List<TrajectoryDataType> points = new ArrayList<>();
                for (Object point : pointsArray) {
                    points.add((TrajectoryDataType) point);
                }

                if (!points.isEmpty()) {
                    double averageWeightedSpeed = GeoSpeed.calculateWeightedAverageSpeed(points);
                    TrajectoryDataType firstPoint = points.get(0);
                    TrajectoryDataType lastPoint = points.get(points.size() - 1);

                    // Get first and last timestamps
                    long firstTimestamp = firstPoint.getTimestamp();
                    long lastTimestamp = lastPoint.getTimestamp();

                    // Construct message with timestamps included
                    String message = id + "@" + firstPoint.getLatitude() + "@" + firstPoint.getLongitude() + "@" + lastPoint.getLatitude() + "@" + lastPoint.getLongitude() + "@" + averageWeightedSpeed + " m/s@" + firstTimestamp + "@" + lastTimestamp;
                    System.out.println("Metrics for Robot " + id + " over last 15 seconds: First(Lat=" + firstPoint.getLatitude() + ", Lon=" + firstPoint.getLongitude() + "), Last(Lat=" + lastPoint.getLatitude() + ", Lon=" + lastPoint.getLongitude() + "), Speed=" + averageWeightedSpeed + " m/s, First Timestamp=" + firstTimestamp + ", Last Timestamp=" + lastTimestamp);
                    producer.send(new ProducerRecord<>("r2k_pos2", id, message));
                }
            }
        }
    }

}
