package org.example;

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

        String epl = "expression latestTrajectory { (select * from TrajectoryDataType.std:lastevent()) } " +
                "select org.example.SpatialDatabaseManager.isPointInsidePolygon(latestTrajectory.latitude, latestTrajectory.longitude) as inside " +
                "from pattern [every timer:interval(15 sec)]";

        try {
            EPCompiled compiled = compiler.compile(epl, arguments);
            EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
            EPStatement statement = deployment.getStatements()[0];

            statement.addListener((newData, oldData, stat, rt) -> {
                if (newData != null) {
                    boolean isInside = (boolean) newData[0].get("inside");
                    System.out.println("Is the latest trajectory inside the polygon? " + isInside);
                }
            });
        } catch (EPCompileException | EPDeployException e) {
            e.printStackTrace();
        }
    }
}
