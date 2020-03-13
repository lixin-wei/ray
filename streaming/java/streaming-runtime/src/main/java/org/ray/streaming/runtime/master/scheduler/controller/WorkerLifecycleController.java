package org.ray.streaming.runtime.master.scheduler.controller;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.options.ActorCreationOptions;
import org.ray.streaming.api.Language;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.rpc.RemoteCallWorker;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker lifecycle controller is used to control JobWorker's creation, initiation and so on.
 * It takes the communication job from {@link org.ray.streaming.runtime.master.JobMaster}
 * to {@link org.ray.streaming.runtime.worker.JobWorker}.
 */
public class WorkerLifecycleController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerLifecycleController.class);

  /**
   * Create JobWorker actor according to the execution vertex.
   *
   * @param executionVertex target execution vertex
   * @return creation result
   */
  public boolean createWorker(ExecutionVertex executionVertex) {
    LOG.info("Start to create JobWorker actor for vertex: {} with resource: {}.",
        executionVertex.getVertexId(), executionVertex.getResources());

    Language language = executionVertex.getLanguage();

    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(executionVertex.getResources())
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTION)
        .createActorCreationOptions();
    
    RayActor<JobWorker> actor = null;
    if (Language.JAVA == language) {
      actor = RemoteCallWorker.createWorker(options);
    } else {
      // TODO(chentianyi)
    }

    if (null == actor) {
      LOG.error("Create actor failed.");
      return false;
    }

    executionVertex.setWorkerActor(actor);

    if (executionVertex.getSlot() != null) {
      executionVertex.getSlot().getActorCount().incrementAndGet();
    }

    LOG.info("Create JobWorker actor succeeded, actor: {}, vertex: {}.",
        executionVertex.getWorkerActorId(), executionVertex.getVertexId());
    return true;
  }

  /**
   * Using context to init JobWorker.
   *
   * @param rayActor target JobWorker actor
   * @param jobWorkerContext JobWorker's context
   * @return init result
   */
  public boolean initWorker(RayActor<JobWorker> rayActor, JobWorkerContext jobWorkerContext) {
    LOG.info("Start to init JobWorker [actor={}] with context: {}.",
        rayActor.getId(), jobWorkerContext);

    RayObject<Boolean> initResult = RemoteCallWorker.initWorker(rayActor, jobWorkerContext);
    Ray.get(initResult.getId());

    if (!initResult.get()) {
      LOG.error("Init JobWorker [actor={}] failed.", rayActor.getId());
      return false;
    }

    LOG.info("Init JobWorker [actor={}] succeed.", rayActor.getId());
    return true;
  }

  /**
   * Start JobWorker to run task.
   *
   * @param rayActor target JobWorker actor
   * @return start result
   */
  public boolean startWorker(RayActor<JobWorker> rayActor) {
    LOG.info("Start to start JobWorker [actor={}].", rayActor.getId());

    RayObject<Boolean> initResult = RemoteCallWorker.startWorker(rayActor);
    Ray.get(initResult.getId());

    if (!initResult.get()) {
      LOG.error("Start JobWorker [actor={}] failed.", rayActor.getId());
      return false;
    }

    LOG.info("Start JobWorker [actor={}] succeed.", rayActor.getId());
    return true;
  }

  /**
   * Stop and destroy JobWorker.
   *
   * @param rayActor target JobWorker actor
   * @return destroy result
   */
  public boolean destroyWorker(RayActor<JobWorker> rayActor) {
    LOG.info("Start to destroy JobWorker [actor={}].", rayActor.getId());

    RayObject<Boolean> destroyResult = RemoteCallWorker.destroyWorker(rayActor);
    Ray.get(destroyResult.getId());

    if (!destroyResult.get()) {
      LOG.error("Failed to destroy JobWorker actor; {}.", rayActor);
      return false;
    }

    LOG.info("Destroy JobWorker succeeded, actor: {}.", rayActor);
    return true;
  }

}