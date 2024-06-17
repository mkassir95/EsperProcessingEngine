package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.runtime.client.*;

import java.util.ArrayList;
import java.util.List;

public class SpeedTrendAnalyzer {

    private EPRuntime runtime;
    private KalmanFilter kalmanFilter;
    private static final double STABLE_THRESHOLD = 5.0; // Define a threshold for stability

    public SpeedTrendAnalyzer(EPRuntime runtime) {
        this.runtime = runtime;
        this.kalmanFilter = new KalmanFilter(1.0, 1.0);  // Initial process noise and measurement noise parameters
        setupEsperQuery();
    }

    private void setupEsperQuery() {
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());

        // EPL to collect all speed events in a 15-second time window
        String epl = "select * from TrajectoryDataType.win:time_batch(15 sec)";

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                List<Double> filteredSpeeds = new ArrayList<>();
                if (newData != null) {
                    for (EventBean eventBean : newData) {
                        TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                        double filteredSpeed = kalmanFilter.filter(trajectory.getSpeed());
                        filteredSpeeds.add(filteredSpeed);
                        System.out.println("Filtered speed: " + filteredSpeed);
                    }

                    String trend = analyzeSpeedTrend(filteredSpeeds);
                    System.out.println("Speed trend over the last 15 seconds: " + trend);
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String analyzeSpeedTrend(List<Double> speeds) {
        if (speeds.isEmpty()) return "No data";

        // Assuming speeds.size() > 1, calculate trend
        double first = speeds.get(0);
        double last = speeds.get(speeds.size() - 1);

        if (Math.abs(last - first) <= STABLE_THRESHOLD) {
            return "Stable";
        } else if (last > first) {
            return "Increasing";
        } else {
            return "Decreasing";
        }
    }

    class KalmanFilter {
        private double estimate = 0.0;
        private double errorCovariance = 1.0;
        private final double processNoise;
        private final double measurementNoise;

        public KalmanFilter(double processNoise, double measurementNoise) {
            this.processNoise = processNoise;
            this.measurementNoise = measurementNoise;
        }

        public double filter(double measurement) {
            double kalmanGain = errorCovariance / (errorCovariance + measurementNoise);
            estimate = estimate + kalmanGain * (measurement - estimate);
            errorCovariance = (1 - kalmanGain) * errorCovariance + processNoise;
            return estimate;
        }
    }
}
