package org.ray.streaming.runtime.schedule;

import java.util.HashMap;
import org.ray.api.Ray;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.TestHelper;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.graph.ExecutionGraphTest;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Only test for {@link WorkerLifecycleController}
 *
 * <p>Note: {@link org.ray.streaming.runtime.master.scheduler.JobScheduler} is hard to have a
 * independent unit test because it need JobMaster, ResourceManager and GraphManager to worker
 * together.
 * Please refer to {@link org.ray.streaming.runtime.demo.WordCountTest} to test the full functions
 * of JobScheduler.
 */
public class JobSchedulerTest extends BaseUnitTest {

  @org.testng.annotations.BeforeClass
  public void setUp() {
    // ray init
    Ray.init();
    TestHelper.setUTFlag();
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTFlag();
  }

  @Test
  public void testWorkerLifecycleControllerApi() {
    // create job master
    JobMaster jobMaster = new JobMaster(new HashMap<>());

    //build ExecutionGraph
    GraphManager graphManager = new GraphManagerImpl(jobMaster.getRuntimeContext());
    JobGraph jobGraph = ExecutionGraphTest.buildJobGraph();
    ExecutionGraph executionGraph = ExecutionGraphTest.buildExecutionGraph(graphManager, jobGraph);

    WorkerLifecycleController controller = new WorkerLifecycleController();

    executionGraph.getAllExecutionVertices().stream().forEach(vertex -> {
      // create
      Assert.assertNull(vertex.getWorkerActor());
      boolean creationResult = controller.createWorker(vertex);
      Assert.assertTrue(creationResult);
      Assert.assertNotNull(vertex.getWorkerActor());

      // init (expect fail)
      Assert.assertTrue(!controller.initWorker(vertex.getWorkerActor(),
          new JobWorkerContext(vertex.getWorkerActorId(), jobMaster.getJobMasterActor() , vertex)));

      // start (expect fail)
      Assert.assertTrue(!controller.startWorker(vertex.getWorkerActor()));

      // destroy
      Assert.assertTrue(controller.destroyWorker(vertex.getWorkerActor()));
    });
  }

}
