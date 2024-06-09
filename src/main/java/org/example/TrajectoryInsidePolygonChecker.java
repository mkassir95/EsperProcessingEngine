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

public class TrajectoryInsidePolygonChecker {

    private EPRuntime runtime;

    public TrajectoryInsidePolygonChecker(EPRuntime runtime) {
        this.runtime = runtime;
        setupEsperQuery();
    }

    private void setupEsperQuery() {
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());

        // EPL to collect all events in a 15-second window and check each for being inside the polygon
        String epl = "select * from TrajectoryDataType.win:time_batch(5 sec)";

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null && newData.length > 0) {
                    boolean allInside = true;
                    for (EventBean eventBean : newData) {
                        TrajectoryDataType trajectory = (TrajectoryDataType) eventBean.getUnderlying();
                        if (!org.example.SpatialDatabaseManager.isPointInsidePolygon(trajectory.getLatitude(), trajectory.getLongitude())) {
                            allInside = false;
                            break;
                        }
                    }
                    System.out.println("All trajectories in the last 5 seconds are inside the polygon: " + allInside);
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
