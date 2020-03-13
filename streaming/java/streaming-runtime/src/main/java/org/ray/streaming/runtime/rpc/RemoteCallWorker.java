package org.ray.streaming.runtime.rpc;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.options.ActorCreationOptions;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ray call worker.
 */
public class RemoteCallWorker extends RemoteCallBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  /**
   * Create JobWorker actor.
   *
   * @param options actor creation options
   * @return JobWorker actor
   */
  public static RayActor<JobWorker> createWorker(ActorCreationOptions options) {
    return Ray.createActor(JobWorker::new, options);
  }

  /**
   * Call JobWorker actor to init.
   *
   * @param actor target JobWorker actor
   * @param ctx JobWorker's context
   * @return init result
   */
  public static RayObject<Boolean> initWorker(RayActor<JobWorker> actor, JobWorkerContext ctx) {
    LOG.info("Call worker to init, actor: {}, context: {}.", actor.getId(), ctx);
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = actor.call(JobWorker::init, ctx);
    }

    LOG.info("Finish call worker to init.");
    return result;
  }

  /**
   * Call JobWorker actor to start.
   *
   * @param actor target JobWorker actor
   * @return start result
   */
  public static RayObject<Boolean> startWorker(RayActor<JobWorker> actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = actor.call(JobWorker::start);
    }

    LOG.info("Finish call worker to start.");
    return result;
  }

  /**
   * Call JobWorker actor to destroy.
   *
   * @param actor target JobWorker actor
   * @return destroy result
   */
  public static RayObject<Boolean> destroyWorker(RayActor<JobWorker> actor) {
    LOG.info("Call worker to destroy, actor is {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = actor.call(JobWorker::destroy);
    }

    LOG.info("Finish call worker to destroy.");
    return result;
  }
}
