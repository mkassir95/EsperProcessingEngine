package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.runtime.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpeedTrendAnalyzer {

    private EPRuntime runtime;
    private Map<String, RobotData> robotDataMap = new HashMap<>();
    private static final double STABLE_THRESHOLD = 0.5; // Further reduced threshold

    public SpeedTrendAnalyzer(EPRuntime runtime) {
        this.runtime = runtime;
        setupEsperQuery();
    }

    private void setupEsperQuery() {
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());

        String epl = "select * from TrajectoryDataType.win:time_batch(15 sec) group by id";

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null) {
                    for (EventBean eventBean : newData) {
                        TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                        String robotId = trajectory.getId();
                        RobotData data = robotDataMap.computeIfAbsent(robotId, k -> new RobotData());

                        double filteredSpeed = data.kalmanFilter.filter(trajectory.getSpeed());
                        data.speeds.add(filteredSpeed);
                        System.out.println("Filtered speed for robot " + robotId + ": " + filteredSpeed);

                        if (data.speeds.size() > 1) {
                            String trend = analyzeSpeedTrend(data.speeds);
                            System.out.println("Speed trend for robot " + robotId + " over the last 15 seconds: " + trend);
                        }
                    }
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String analyzeSpeedTrend(List<Double> speeds) {
        if (speeds.size() < 2) return "No data";

        double trend = 0;
        for (int i = 1; i < speeds.size(); i++) {
            trend += speeds.get(i) - speeds.get(i - 1);
        }

        if (Math.abs(trend) <= STABLE_THRESHOLD) {
            return "Stable";
        } else if (trend > 0) {
            return "Increasing";
        } else {
            return "Decreasing";
        }
    }

    class RobotData {
        KalmanFilter kalmanFilter = new KalmanFilter(0.1, 0.1); // More sensitive settings
        List<Double> speeds = new ArrayList<>();
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
