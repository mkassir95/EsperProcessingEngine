package org.example;

import java.util.Random;
import java.util.UUID;

public class RandomTrajectoryDataTypeGenerator {
    private static final Random random = new Random();

    public static TrajectoryDataType generateRandomTrajectoryDataType() {
        String id = UUID.randomUUID().toString(); // Generate a random UUID for the ID
        long timestamp = System.currentTimeMillis(); // Use the current system time for the timestamp
        double latitude = -90 + (90 - (-90)) * random.nextDouble(); // Random latitude between -90 and 90
        double longitude = -180 + (180 - (-180)) * random.nextDouble(); // Random longitude between -180 and 180
        double speed = 0 + (100 - 0) * random.nextDouble(); // Random speed between 0 and 100 m/s

        return new TrajectoryDataType(id, timestamp, latitude, longitude,speed);
    }
}
