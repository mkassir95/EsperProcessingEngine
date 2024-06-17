package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.util.ArrayList;
import java.util.List;

public class SpeedTrendAnalyzer {
    private EPRuntime runtime;

    public SpeedTrendAnalyzer(EPRuntime runtime) {
        this.runtime = runtime;
        setupSpeedTrendDetection();
    }

    private void setupSpeedTrendDetection() {
        String epl = "select * from TrajectoryDataType.win:time_batch(15 sec)";

        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener(this::analyzeSpeedTrend);
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL for speed trend analysis: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void analyzeSpeedTrend(EventBean[] newData, EventBean[] oldData, EPStatement statement, EPRuntime runtime) {
        if (newData == null || newData.length == 0) {
            return;
        }

        List<Double> speeds = new ArrayList<>();
        for (EventBean eventBean : newData) {
            TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
            speeds.add(trajectory.getSpeed());
        }

        if (speeds.size() < 2) {
            System.out.println("Not enough data to determine speed trend.");
            return;
        }

        double averageSpeed = calculateAverage(speeds);
        double stdDevSpeed = calculateStandardDeviation(speeds, averageSpeed);

        int increasingCount = 0, decreasingCount = 0, stableCount = 0;

        for (int i = 1; i < speeds.size(); i++) {
            double prevSpeed = speeds.get(i - 1);
            double currentSpeed = speeds.get(i);

            if (currentSpeed > prevSpeed + stdDevSpeed * 0.5) {
                increasingCount++;
            } else if (currentSpeed < prevSpeed - stdDevSpeed * 0.5) {
                decreasingCount++;
            } else {
                stableCount++;
            }
        }

        if (increasingCount > decreasingCount && increasingCount > stableCount) {
            System.out.println("Speed is generally increasing over the last 15 seconds.");
        } else if (decreasingCount > increasingCount && decreasingCount > stableCount) {
            System.out.println("Speed is generally decreasing over the last 15 seconds.");
        } else if (stableCount >= increasingCount && stableCount >= decreasingCount) {
            System.out.println("Speed is stable over the last 15 seconds.");
        } else {
            System.out.println("No clear speed trend over the last 15 seconds.");
        }
    }

    private double calculateAverage(List<Double> values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum / values.size();
    }

    private double calculateStandardDeviation(List<Double> values, double mean) {
        double sum = 0;
        for (double value : values) {
            sum += Math.pow(value - mean, 2);
        }
        return Math.sqrt(sum / (values.size() - 1)); // Sample standard deviation
    }
}
