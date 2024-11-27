<h1>Project Overview</h1>
This project utilizes Esper for its lightweight processing capabilities to handle continuous queries on robot trajectories. We've added a spatial-temporal extension to support specific trajectory queries for agricultural robots. Esper is designed to be implemented on robot edges and to process robot queries locally.

<h1>Key Features</h1>
- Add a spatial library extension to support spatial-temporal queries.<br>
- Implement a dynamic window resizing mechanism to adjust the window size (data transmission time) according to the error.

<h1>Query Support Includes:</h1>
- Average Weight Speed: Calculates the average speed weighted by various parameters over time.
<br>
- Trajectory Delay: Monitors and reports delays in the trajectory, helping to optimize route planning.
<br>
- Speed Trend Analysis: Analyzes trends in speed, assisting in predictive maintenance and efficiency improvements.
<br>
- Divergence Checks: Ensures the robots' paths do not diverge from planned routes, maintaining operational accuracy.
<br>
- Inside Zone Checks: Verifies whether a robot is within a designated dangerous zone (polygon).
