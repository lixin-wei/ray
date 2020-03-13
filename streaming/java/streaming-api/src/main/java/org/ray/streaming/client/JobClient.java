package org.ray.streaming.client;

import java.util.Map;
import org.ray.streaming.jobgraph.JobGraph;

/**
 * Interface of the job scheduler.
 */
public interface JobClient {

  /**
   * Submit job with logical plan to run.
   *
   * @param jobGraph the logical plan
   */
  void submit(JobGraph jobGraph, Map<String, String> conf);
}
