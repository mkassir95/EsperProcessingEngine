package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DistanceToPredefinedTrajectoryCalculator {
    private EPRuntime runtime;
    private List<Double> previousDistances = new ArrayList<>();

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

        List<Double> distances = new ArrayList<>();
        try (Connection conn = SpatialDatabaseManager.getConnection()) {
            String predefinedTrajectory = SpatialDatabaseManager.getPredefinedTrajectory(conn);
            for (EventBean eventBean : newData) {
                TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                double distance = TrajectoryDistanceCalculator.calculateDistance(
                        conn, trajectory.getLatitude(), trajectory.getLongitude(), predefinedTrajectory);
                distances.add(distance);
            }

            if (!distances.isEmpty()) {
                Collections.sort(distances);
                double medianDistance;
                int size = distances.size();
                if (size % 2 == 0) {
                    medianDistance = (distances.get(size / 2 - 1) + distances.get(size / 2)) / 2.0;
                } else {
                    medianDistance = distances.get(size / 2);
                }
                System.out.println("Median distance from trajectory to predefined path: " + medianDistance);

                // Add the current median distance to the list of previous distances
                previousDistances.add(medianDistance);
                // Check if the distances are diverging
                boolean isDiverging = isTrajectoryDiverging();
                System.out.println("Is the trajectory diverging: " + isDiverging);
            }
        } catch (SQLException e) {
            System.err.println("Database error during distance calculation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean isTrajectoryDiverging() {
        // Determine if the distances are increasing over time
        if (previousDistances.size() < 3) {
            return false; // Not enough data to determine divergence
        }

        // Check if the last three distances are increasing
        int lastIndex = previousDistances.size() - 1;
        return previousDistances.get(lastIndex) > previousDistances.get(lastIndex - 1) &&
                previousDistances.get(lastIndex - 1) > previousDistances.get(lastIndex - 2);
    }
}
