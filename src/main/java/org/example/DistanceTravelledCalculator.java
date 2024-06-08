package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

public class DistanceTravelledCalculator {
    private EPRuntime runtime;
    private double cumulativeDistance = 0;  // Store the cumulative distance across all windows

    public DistanceTravelledCalculator(EPRuntime runtime) {
        this.runtime = runtime;
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
            TrajectoryDataType previousPoint = (TrajectoryDataType) newData[0].getUnderlying();
            double windowDistance = 0;

            for (int i = 1; i < newData.length; i++) {
                TrajectoryDataType currentPoint = (TrajectoryDataType) newData[i].getUnderlying();
                windowDistance += GeoDistance.haversineDistance(
                        previousPoint.getLatitude(), previousPoint.getLongitude(),
                        currentPoint.getLatitude(), currentPoint.getLongitude());
                previousPoint = currentPoint;
            }

            cumulativeDistance += windowDistance;
            printFormattedDistance(cumulativeDistance);
        }
    }

    private void printFormattedDistance(double distance) {
        if (distance >= 1000) {
            System.out.printf("Distance Travelled: %.2f km\n", distance / 1000);
        } else {
            System.out.printf("Distance Travelled: %.2f meters\n", distance);
        }
    }
}
