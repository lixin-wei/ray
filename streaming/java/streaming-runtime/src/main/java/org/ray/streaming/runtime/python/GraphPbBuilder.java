package org.ray.streaming.runtime.python;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.streaming.api.function.Function;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonPartition;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.generated.RemoteCall;
import org.ray.streaming.runtime.generated.Streaming;

public class GraphPbBuilder {

  private MsgPackSerializer serializer = new MsgPackSerializer();

  /**
   * For simple scenario, a single ExecutionNode is enough. But some cases may need
   * sub-graph information, so we serialize entire graph.
   */
  public RemoteCall.ExecutionGraph buildExecutionGraphPb(ExecutionGraph graph) {
    RemoteCall.ExecutionGraph.Builder builder = RemoteCall.ExecutionGraph.newBuilder();
    builder.setBuildTime(graph.getBuildTime());
    for (ExecutionJobVertex executionJobVertex : graph.getExecutionJobVertices()) {
      RemoteCall.ExecutionGraph.ExecutionNode.Builder nodeBuilder =
          RemoteCall.ExecutionGraph.ExecutionNode.newBuilder();
      nodeBuilder.setNodeId(executionJobVertex.getJobVertexId());
      nodeBuilder.setParallelism(executionJobVertex.getParallelism());
      nodeBuilder.setNodeType(
          Streaming.NodeType.valueOf(executionJobVertex.getVertexType().name()));
      nodeBuilder.setLanguage(Streaming.Language.valueOf(executionJobVertex.getLanguage().name()));

      byte[] functionBytes = serializeFunction(
          executionJobVertex.getStreamOperator().getFunction());
      nodeBuilder.setFunction(ByteString.copyFrom(functionBytes));

      // build tasks
      for (ExecutionVertex executionVertex : executionJobVertex.getExecutionVertices()) {
        RemoteCall.ExecutionGraph.ExecutionTask.Builder taskBuilder =
            RemoteCall.ExecutionGraph.ExecutionTask.newBuilder();
        byte[] serializedActorHandle = ((NativeRayActor) executionVertex.getWorkerActor())
            .toBytes();
        taskBuilder
            .setTaskId(executionVertex.getVertexId())
            .setTaskIndex(executionVertex.getVertexIndex())
            .setWorkerActor(ByteString.copyFrom(serializedActorHandle));
        nodeBuilder.addExecutionTasks(taskBuilder.build());

        // build edges
        for (ExecutionEdge executionEdge : executionVertex.getInputEdges()) {
          nodeBuilder.addInputEdges(buildEdgePb(executionEdge));
        }
        for (ExecutionEdge edge : executionVertex.getOutputEdges()) {
          nodeBuilder.addOutputEdges(buildEdgePb(edge));
        }
      }
      builder.addExecutionNodes(nodeBuilder.build());
    }

    return builder.build();
  }

  private RemoteCall.ExecutionGraph.ExecutionEdge buildEdgePb(ExecutionEdge edge) {
    RemoteCall.ExecutionGraph.ExecutionEdge.Builder edgeBuilder =
        RemoteCall.ExecutionGraph.ExecutionEdge.newBuilder();
    edgeBuilder.setSrcNodeId(edge.getSourceVertexId());
    edgeBuilder.setTargetNodeId(edge.getTargetVertexId());
    edgeBuilder.setPartition(ByteString.copyFrom(serializePartition(edge.getPartition())));
    return edgeBuilder.build();
  }

  private byte[] serializeFunction(Function function) {
    if (function instanceof PythonFunction) {
      PythonFunction pyFunc = (PythonFunction) function;
      // function_bytes, module_name, class_name, function_name, function_interface
      return serializer.serialize(Arrays.asList(
          pyFunc.getFunction(), pyFunc.getModuleName(),
          pyFunc.getClassName(), pyFunc.getFunctionName(),
          pyFunc.getFunctionInterface()
      ));
    } else {
      return new byte[0];
    }
  }

  private byte[] serializePartition(Partition partition) {
    if (partition instanceof PythonPartition) {
      PythonPartition pythonPartition = (PythonPartition) partition;
      // partition_bytes, module_name, class_name, function_name
      return serializer.serialize(Arrays.asList(
          pythonPartition.getPartition(), pythonPartition.getModuleName(),
          pythonPartition.getClassName(), pythonPartition.getFunctionName()
      ));
    } else {
      return new byte[0];
    }
  }

}
