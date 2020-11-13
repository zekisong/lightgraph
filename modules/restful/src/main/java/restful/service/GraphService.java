package restful.service;

import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.server.Server;
import restful.model.GraphMetaModel;
import restful.model.PutModel;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/graph")
public class GraphService {

    @POST
    @Path("/{graph}/create/vertex/{vertex}")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean addVertexMeta(@PathParam("graph") String graph, @PathParam("vertex") String name, VertexMetaInfo vertexMetaInfo) {
        vertexMetaInfo.setGraph(graph);
        vertexMetaInfo.setName(name);
        Server server = Server.getInstance();
        server.getMasterLeader().addVertexMeta(vertexMetaInfo);
        return true;
    }

    @POST
    @Path("/{graph}/create/edge/{edge}")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean addVertexMeta(@PathParam("graph") String graph, @PathParam("edge") String name, EdgeMetaInfo edgeMetaInfo) {
        edgeMetaInfo.setGraph(graph);
        edgeMetaInfo.setName(name);
        Server server = Server.getInstance();
        server.getMasterLeader().addEdgeMeta(edgeMetaInfo);
        return true;
    }

    @POST
    @Path("/{graph}/put")
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean put(@PathParam("graph") String graph, PutModel putModel) {
        Server server = Server.getInstance();
        String key = putModel.getKey();
        String value = putModel.getValue();
        server.put(graph, new KeyValue(key.getBytes(), "d".getBytes(), value.getBytes()));
        return true;
    }

    @GET
    @Path("_cat/vertex/meta/{graph}/{vertex}")
    @Produces({MediaType.APPLICATION_JSON})
    public VertexMeta geVertexMeta(@PathParam("graph") String graph, @PathParam("vertex") String vertex) {
        return Server.getInstance().getVertexMeta(graph, vertex);
    }

    @GET
    @Path("_cat/edge/meta/{graph}/{edge}")
    @Produces({MediaType.APPLICATION_JSON})
    public EdgeMeta getEdgeMeta(@PathParam("graph") String graph, @PathParam("edge") String edge) {
        return Server.getInstance().getEdgeMeta(graph, edge);
    }

    @GET
    @Path("/{graph}/get/{key}")
    public String get(@PathParam("graph") String graph, @PathParam("key") String key) {
        Server server = Server.getInstance();
        return new String(server.get(graph, new KeyValue(key.getBytes(), "d".getBytes())).getValue());
    }
}
