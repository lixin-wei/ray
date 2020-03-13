package org.ray.streaming.runtime.master.scheduler;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertexState;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job scheduler implementation.
 */
public class JobSchedulerImpl implements JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerImpl.class);

  private StreamingConfig jobConf;

  private final JobMaster jobMaster;
  private final ResourceManager resourceManager;
  private final GraphManager graphManager;
  private final WorkerLifecycleController workerController;
  private final SlotAssignStrategy strategy;

  public JobSchedulerImpl(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.graphManager = jobMaster.getGraphManager();
    this.resourceManager = jobMaster.getResourceManager();
    this.workerController = new WorkerLifecycleController();
    this.strategy = resourceManager.getSlotAssignStrategy();
    this.jobConf = jobMaster.getRuntimeContext().getConf();

    LOG.info("Scheduler init success.");
  }

  @Override
  public boolean scheduleJob(ExecutionGraph executionGraph) {
    LOG.info("Start to schedule job: {}.", executionGraph.getJobName());

    // get max parallelism
    int maxParallelism = executionGraph.getMaxParallelism();

    // get containers
    List<Container> containers = resourceManager.getRegisteredContainers();
    Preconditions.checkState(containers != null && !containers.isEmpty(),
        "containers is invalid: %s", containers);

    // allocate slot
    int slotNumPerContainer = strategy.getSlotNumPerContainer(containers, maxParallelism);
    resourceManager.getResources().setSlotNumPerContainer(slotNumPerContainer);
    LOG.info("Slot num per container: {}.", slotNumPerContainer);

    strategy.allocateSlot(containers, slotNumPerContainer);
    LOG.info("Container slot map is: {}.", resourceManager.getResources().getAllocatingMap());

    // assign slot
    Map<ContainerID, List<Slot>> allocatingMap = strategy.assignSlot(executionGraph);
    LOG.info("Allocating map is: {}.", allocatingMap);

    // start all new added workers
    createWorkers(executionGraph);

    // init worker context and start to run
    initAndStart(executionGraph);

    return true;
  }

  /**
   * Init JobMaster and JobWorkers then start JobWorkers.
   *
   * @param executionGraph physical plan
   */
  private void initAndStart(ExecutionGraph executionGraph) {
    initWorkers(executionGraph);
    initMaster();
    startWorkers(executionGraph);
  }

  /**
   * Create JobWorkers according to the physical plan.
   *
   * @param executionGraph physical plan
   */
  public void createWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin creating workers.");
    long startTs = System.currentTimeMillis();

    // Create JobWorker actors
    executionGraph.getAllAddedExecutionVertices().stream()
        .forEach(vertex -> {
          Container container = resourceManager.getResources()
              .getRegisterContainerByContainerId(vertex.getSlot().getContainerID());

          // allocate by resource manager
          resourceManager.allocateResource(container, vertex.getResources());

          // create actor by controller
          workerController.createWorker(vertex);

          // update state
          vertex.setState(ExecutionVertexState.RUNNING);
        });

    LOG.info("Finish creating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
  }

  /**
   * Init JobWorkers according to the physical plan.
   *
   * @param executionGraph physical plan
   */
  public void initWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin initiating workers.");
    long startTs = System.currentTimeMillis();

    RayActor<JobMaster> masterActor = jobMaster.getJobMasterActor();

    // init worker
    executionGraph.getAllExecutionVertices().forEach(vertex -> {
      JobWorkerContext ctx = buildJobWorkerContext(vertex, masterActor);
      boolean initResult = workerController.initWorker(vertex.getWorkerActor(), ctx);

      if (!initResult) {
        LOG.error("Init workers occur error.");
        return;
      }
    });

    LOG.info("Finish initiating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
  }

  /**
   * Start JobWorkers according to the physical plan.
   */
  public void startWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin starting workers.");
    long startTs = System.currentTimeMillis();

    try {
      startWorkersByList(executionGraph.getAllActors());
    } catch (Exception e) {
      LOG.error("Failed to start workers.", e);
      return;
    }

    LOG.info("Finish to start workers, cost {} ms.", System.currentTimeMillis() - startTs);
  }

  private void startWorkersByList(List<RayActor<JobWorker>> addedActors) {
    ExecutionGraph executionGraph = graphManager.getExecutionGraph();

    LOG.info("Start source workers.");
    executionGraph.getSourceActors()
        .stream()
        .filter(addedActors::contains)
        .forEach(actor -> workerController.startWorker(actor));

    LOG.info("Start non-source workers.");
    executionGraph.getNonSourceActors()
        .stream()
        .filter(addedActors::contains)
        .forEach(actor -> workerController.startWorker(actor));
  }

  private JobWorkerContext buildJobWorkerContext(
      ExecutionVertex executionVertex,
      RayActor<JobMaster> masterActor) {

    // create worker context
    JobWorkerContext ctx = new JobWorkerContext(
        executionVertex.getWorkerActorId(),
        masterActor,
        executionVertex
    );

    return ctx;
  }

  /**
   * Destroy JobWorkers according to the physical plan.
   *
   * @param executionGraph physical plan
   */
  public void destroyWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin destroying workers.");
    long startTs = System.currentTimeMillis();

    executionGraph.getAllExecutionVertices()
        .forEach(vertex -> {
          Container container = resourceManager.getResources()
              .getRegisterContainerByContainerId(vertex.getSlot().getContainerID());

          // deallocate by resource manager
          resourceManager.deallocateResource(container, vertex.getResources());

          // destroy by worker controller
          workerController.destroyWorker(vertex.getWorkerActor());

          // update slot
          vertex.getSlot().getActorCount().decrementAndGet();
        });

    LOG.info("Finish initiating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
  }

  private void initMaster() {
    jobMaster.init(false);
  }

}
