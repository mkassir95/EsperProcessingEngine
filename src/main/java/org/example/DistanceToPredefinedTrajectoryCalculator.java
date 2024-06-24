package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class DistanceToPredefinedTrajectoryCalculator {
    private EPRuntime runtime;
    private Map<String, List<Double>> robotDistances = new HashMap<>();

    public DistanceToPredefinedTrajectoryCalculator(EPRuntime runtime) {
        this.runtime = runtime;
        setupDistanceCalculationQuery();
    }

    private void setupDistanceCalculationQuery() {
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
        String epl = "select * from TrajectoryDataType.win:time_batch(15 sec)";
        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];
            statement.addListener(this::calculateDistanceToPredefinedTrajectory);
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void calculateDistanceToPredefinedTrajectory(EventBean[] newData, EventBean[] oldData, EPStatement stat, EPRuntime rt) {
        if (newData == null) return;
        try (Connection conn = SpatialDatabaseManager.getConnection()) {
            String predefinedTrajectory = SpatialDatabaseManager.getPredefinedTrajectory(conn);
            for (EventBean eventBean : newData) {
                TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                double distance = TrajectoryDistanceCalculator.calculateDistance(
                        conn, trajectory.getLatitude(), trajectory.getLongitude(), predefinedTrajectory);

                robotDistances.computeIfAbsent(trajectory.getId(), k -> new ArrayList<>()).add(distance);
            }

            robotDistances.forEach((id, distances) -> {
                if (!distances.isEmpty()) {
                    Collections.sort(distances);
                    double medianDistance = calculateMedian(distances);
                    System.out.println("Robot ID: " + id + ", Median distance: " + medianDistance);

                    boolean isDiverging = isTrajectoryDiverging(distances);
                    System.out.println("Robot ID: " + id + ", is diverging: " + isDiverging);
                }
            });
        } catch (SQLException e) {
            System.err.println("Database error during distance calculation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private double calculateMedian(List<Double> distances) {
        int size = distances.size();
        if (size % 2 == 0) {
            return (distances.get(size / 2 - 1) + distances.get(size / 2)) / 2.0;
        } else {
            return distances.get(size / 2);
        }
    }

    private boolean isTrajectoryDiverging(List<Double> distances) {
        int size = distances.size();
        if (size < 3) return false;
        return distances.get(size - 1) > distances.get(size - 2) && distances.get(size - 2) > distances.get(size - 3);
    }
}
