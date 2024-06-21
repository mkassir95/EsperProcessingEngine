package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.util.HashMap;
import java.util.Map;

public class DistanceTravelledCalculator {
    private EPRuntime runtime;
    private Map<String, Double> cumulativeDistances;  // Store the cumulative distance for each robot

    public DistanceTravelledCalculator(EPRuntime runtime) {
        this.runtime = runtime;
        this.cumulativeDistances = new HashMap<>();
    }

    public void setupDistanceCalculation() {
        String epl = "select * from TrajectoryDataType.win:ext_timed_batch(timestamp, 15 sec)";
        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            EPCompiled compiledQuery = compiler.compile(epl, arguments);
            EPStatement statement = runtime.getDeploymentService().deploy(compiledQuery).getStatements()[0];

            statement.addListener(this::calculateAndPrintTotalDistanceTravelled);
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
        }
    }

    private void calculateAndPrintTotalDistanceTravelled(EventBean[] newData, EventBean[] oldData, EPStatement statement, EPRuntime runtime) {
        if (newData != null && newData.length > 0) {
            Map<String, TrajectoryDataType> previousPoints = new HashMap<>();

            for (EventBean event : newData) {
                TrajectoryDataType currentPoint = (TrajectoryDataType) event.getUnderlying();
                String robotId = currentPoint.getId();
                TrajectoryDataType previousPoint = previousPoints.get(robotId);

                if (previousPoint != null) {
                    double distance = GeoDistance.haversineDistance(
                            previousPoint.getLatitude(), previousPoint.getLongitude(),
                            currentPoint.getLatitude(), currentPoint.getLongitude());

                    cumulativeDistances.put(robotId, cumulativeDistances.getOrDefault(robotId, 0.0) + distance);
                }

                previousPoints.put(robotId, currentPoint);
            }

            // Print the cumulative distances for all robots
            for (Map.Entry<String, Double> entry : cumulativeDistances.entrySet()) {
                String robotId = entry.getKey();
                double distance = entry.getValue();
                printFormattedDistance(robotId, distance);
            }
        }
    }

    private void printFormattedDistance(String robotId, double distance) {
        if (distance >= 1000) {
            System.out.printf("Robot ID: %s, Distance Travelled: %.2f km\n", robotId, distance / 1000);
        } else {
            System.out.printf("Robot ID: %s, Distance Travelled: %.2f meters\n", robotId, distance);
        }
    }
}
