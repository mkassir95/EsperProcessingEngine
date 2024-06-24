package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPStatement;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class TrajectoryInsidePolygonChecker {

    private EPRuntime runtime;

    public TrajectoryInsidePolygonChecker(EPRuntime runtime) {
        this.runtime = runtime;
        setupEsperQuery();
    }

    private void setupEsperQuery() {
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());

        // Update EPL to collect all events in a 15-second window and group by id
        String epl = "select * from TrajectoryDataType.win:time_batch(15 sec)";

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null) {
                    // Using a Map to group by robot ID and analyze each group
                    Map<String, List<TrajectoryDataType>> groupedData = new HashMap<>();

                    for (EventBean eventBean : newData) {
                        TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                        groupedData.putIfAbsent(trajectory.getId(), new ArrayList<>());
                        groupedData.get(trajectory.getId()).add(trajectory);
                    }

                    // Evaluate each group for being inside the polygon
                    groupedData.forEach((id, trajectories) -> {
                        boolean allInside = trajectories.stream()
                                .allMatch(trajectory -> org.example.SpatialDatabaseManager.isPointInsidePolygon(trajectory.getLatitude(), trajectory.getLongitude()));
                        System.out.println("Robot ID " + id + ": All trajectories in the last 15 seconds are inside the polygon: " + allInside);
                    });
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
