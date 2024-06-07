package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.util.List;
import java.util.ArrayList;

public class AverageSpeedCalculator {
    private EPRuntime runtime;

    public AverageSpeedCalculator(EPRuntime runtime) {
        this.runtime = runtime;
    }

    public void setupAverageSpeedCalculation() {
        String epl = "select * from TrajectoryDataType.win:time_batch(15 sec)";
        EPCompiled compiledQuery;
        try {
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            CompilerArguments arguments = new CompilerArguments(runtime.getConfigurationDeepCopy());
            compiledQuery = compiler.compile(epl, arguments);
            EPStatement statement = runtime.getDeploymentService().deploy(compiledQuery).getStatements()[0];

            // Add listener to the statement
            statement.addListener((newData, oldData, stat, rt) -> calculateAndPrintAverageSpeed(newData));

        } catch (EPCompileException | EPDeployException e) {
            System.err.println("Error in compiling or deploying EPL: " + e.getMessage());
        }
    }

    private void calculateAndPrintAverageSpeed(EventBean[] newData) {
        if (newData != null && newData.length > 0) {
            List<TrajectoryDataType> points = new ArrayList<>();
            for (EventBean eventBean : newData) {
                points.add((TrajectoryDataType) eventBean.getUnderlying());
            }
            if (!points.isEmpty()) {
                double averageWeightedSpeed = GeoSpeed.calculateWeightedAverageSpeed(points);
                System.out.println("Average Weighted Speed over last 15 seconds: " + averageWeightedSpeed + " m/s");
            }
        }
    }
}
