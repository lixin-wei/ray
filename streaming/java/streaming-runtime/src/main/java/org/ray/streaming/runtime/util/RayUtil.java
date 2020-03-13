package org.ray.streaming.runtime.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.Ray;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;

/**
 * Utils for ray using.
 */
public class RayUtil {

  /**
   * Get node infos from ray runtime. Will return mock resources if it is single-process.
   *
   * @return node infos
   */
  public static List<NodeInfo> getNodeInfoList() {
    List<NodeInfo> nodeInfoList;
    if (Ray.getRuntimeContext().isSingleProcess()) {
      // just for UT
      nodeInfoList = getAllNodeInfoMock();
    } else {
      nodeInfoList = Ray.getRuntimeContext().getAllNodeInfo();
    }
    return nodeInfoList;
  }

  /**
   * Get node infos from ray runtime. Will return mock resources if it is single-process.
   *
   * @return node infos' map
   */
  public static Map<UniqueId, NodeInfo> getNodeInfoMap() {
    return getNodeInfoList().stream().filter(nodeInfo -> nodeInfo.isAlive).collect(
        Collectors.toMap(nodeInfo -> nodeInfo.nodeId, nodeInfo -> nodeInfo));
  }

  /**
   * Mock resources.
   *
   * @return mocked node infos.
   */
  private static List<NodeInfo> getAllNodeInfoMock() {
    return mockContainerResources();
  }

  /**
   * Mock container resources.
   *
   * @return mocked node infos.
   */
  public static List<NodeInfo> mockContainerResources() {
    List<NodeInfo> nodeInfos = new LinkedList<>();

    for (int i = 1; i <= 5; i++) {
      Map<String, Double> resources = new HashMap<>();
      resources.put("MEM", 16.0);
      switch (i) {
        case 1:
          resources.put("CPU", 3.0);
          break;
        case 2:
          resources.put("CPU", 4.0);
          break;
        case 3:
          resources.put("CPU", 2.0);
          break;
        default:
          resources.put("CPU", 5.0);
      }

      byte[] nodeIdBytes = new byte[UniqueId.LENGTH];
      for (int byteIndex = 0; byteIndex < UniqueId.LENGTH; ++byteIndex) {
        nodeIdBytes[byteIndex] = String.valueOf(i).getBytes()[0];
      }
      NodeInfo nodeInfo = new NodeInfo(new UniqueId(nodeIdBytes),
          "localhost" + i, "localhost" + i, true, resources);
      nodeInfos.add(nodeInfo);
    }
    return nodeInfos;
  }

}
