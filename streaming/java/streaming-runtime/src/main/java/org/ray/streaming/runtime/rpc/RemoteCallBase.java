package org.ray.streaming.runtime.rpc;

import java.util.Arrays;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteCallBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallBase.class);

  private static final int WAIT_TIMEOUT_MS = 5000;

  protected static <T> T wait(RayObject<T> object) {
    List<RayObject<T>> waitList = Arrays.asList(object);
    WaitResult<T> result = Ray.wait(waitList, waitList.size(), WAIT_TIMEOUT_MS);
    if (result.getReady().isEmpty()) {
      LOG.error("Wait timeout, timeoutMs is {}.", WAIT_TIMEOUT_MS);
      return null;
    }

    try {
      return result.getReady().get(0).get();
    } catch (RayException e) {
      LOG.error("Remote call has exception when waiting.", e);
      return null;
    }
  }
}