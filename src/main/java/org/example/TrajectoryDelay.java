package org.example;

import com.espertech.esper.common.client.*;
import com.espertech.esper.compiler.client.*;
import com.espertech.esper.runtime.client.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.*;

import static javax.management.remote.JMXConnectorFactory.connect;

public class TrajectoryDelay {
    private EPRuntime runtime;
    // JDBC URL, username and password of the PostgreSQL server
    private static final String url = "jdbc:postgresql://10.63.64.48:5432/superRob2";
    private static final String user = "pgadmin4";
    private static final String password = "romea63*";
    // Map to keep track of cumulative time delays for each robot
    private Map<String, Double> cumulativeTimeDelays = new HashMap<>();

    private KafkaProducer<String, String> producer;
    private static final String KAFKA_TOPIC = "delay_traj";

    public TrajectoryDelay(EPRuntime runtime){
        this.runtime=runtime;
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.63.64.48:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);

    }

    public void getDataPoints() {
        String epl = "select id, window(*) as points from TrajectoryDataType.win:time_batch(15 sec) group by id";
        EPCompiled compiledQuery;
        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            compiledQuery = compiler.compile(epl, arguments);
            EPStatement statement = runtime.getDeploymentService().deploy(compiledQuery).getStatements()[0];

            // Attach listener
            statement.addListener((newData, oldData, stat, rt) -> calculateDelayInWindow(newData));

        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
        }
    }




    private void calculateDelayInWindow(EventBean[] newData) {
        if (newData != null && newData.length > 0) {
            // Fetch the reference trajectory once, assuming it doesn't change often
            List<TrajectoryDataType> trajectoryPoints = executeQuery(4);

            for (EventBean eventBean : newData) {
                String id = (String) eventBean.get("id");
                Object[] pointsArray = (Object[]) eventBean.get("points");
                List<TrajectoryDataType> points = new ArrayList<>();
                for (Object point : pointsArray) {
                    points.add((TrajectoryDataType) point);
                }

                // Calculate minimum distance and time delay across the whole window
                if (!points.isEmpty()) {
                    double minDistance = Double.MAX_VALUE;
                    double speed = 0.0;
                    for (TrajectoryDataType point : points) {
                        for (TrajectoryDataType trajectoryPoint : trajectoryPoints) {
                            double distance = calculateDistance(point, trajectoryPoint);
                            if (distance < minDistance) {
                                minDistance = distance;
                                speed = point.getSpeed();
                            }
                        }
                    }

                    // Convert the minimum distance from kilometers to meters
                    minDistance *= 1000; // Convert to meters

                    // Calculate time delay in seconds
                    double timeDelay = 0.0;
                    if (speed > 0) {
                        timeDelay = minDistance / speed; // Assuming speed is in meters/second
                    }

                    // Update cumulative time delay for the robot
                    cumulativeTimeDelays.put(id, cumulativeTimeDelays.getOrDefault(id, 0.0) + timeDelay);

                    System.out.println("Minimum distance for robot " + id + " in this window is " + minDistance + " meters");
                    System.out.println("Time delay for robot " + id + " in this window is " + timeDelay + " seconds");
                    System.out.println("Cumulative time delay for robot " + id + " is " + cumulativeTimeDelays.get(id) + " seconds");

                    // Send the cumulative delay to Kafka
                    sendToKafka(id, cumulativeTimeDelays.get(id));
                }
            }
        }
    }

    private void sendToKafka(String id, double cumulativeDelay) {
        String message = "Robot ID: " + id + ", Cumulative Delay: " + cumulativeDelay;
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, id, message);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message to Kafka: " + exception.getMessage());
            } else {
                System.out.println("Message sent to Kafka: " + message);
            }
        });
    }


    // Connect to PostgreSQL database
    public static Connection connect() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println("Connection failure.");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    // Method to execute SQL query and return an ArrayList of TrajectoryDataType
    public static List<TrajectoryDataType> executeQuery(int id) {
        String SQL = "SELECT id, ST_X(point::geometry) AS longitude, ST_Y(point::geometry) AS latitude, speed, ord_id, storage_timestamp " +
                "FROM public.point_timeref WHERE id = ?;";
        List<TrajectoryDataType> trajectories = new ArrayList<>();

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {
            pstmt.setInt(1, id);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                trajectories.add(new TrajectoryDataType(
                        String.valueOf(rs.getInt("id")),
                        rs.getTimestamp("storage_timestamp").getTime(),
                        rs.getDouble("latitude"),
                        rs.getDouble("longitude"),
                        rs.getDouble("speed")
                ));
            }
        } catch (SQLException ex) {
            System.out.println("Database connection/query issue: " + ex.getMessage());
        }
        printTrajectoryData(trajectories);
        return trajectories;
    }

    // Method to print details of each TrajectoryDataType in the list
    public static void printTrajectoryData(List<TrajectoryDataType> trajectories) {
        for (TrajectoryDataType traj : trajectories) {
            System.out.println("ID: " + traj.getId() +
                    ", Timestamp: " + new Timestamp(traj.getTimestamp()) +
                    ", Latitude: " + traj.getLatitude() +
                    ", Longitude: " + traj.getLongitude() +
                    ", Speed: " + traj.getSpeed());
        }
    }


    public static double calculateDistance(TrajectoryDataType p1, TrajectoryDataType p2) {
        double earthRadius = 6371.01; // Earth's radius in kilometers

        double lat1 = Math.toRadians(p1.getLatitude());
        double lon1 = Math.toRadians(p1.getLongitude());
        double lat2 = Math.toRadians(p2.getLatitude());
        double lon2 = Math.toRadians(p2.getLongitude());

        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(lat1) * Math.cos(lat2) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return earthRadius * c;
    }



}
