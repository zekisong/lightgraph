package restful.service;

import com.lightgraph.graph.data.Edge;
import com.lightgraph.graph.data.Vertex;
import com.lightgraph.graph.exception.GraphException;
import com.lightgraph.graph.graph.EdgeMetaInfo;
import com.lightgraph.graph.graph.VertexMetaInfo;
import com.lightgraph.graph.meta.EdgeMeta;
import com.lightgraph.graph.meta.VertexMeta;
import com.lightgraph.graph.modules.storage.KeyValue;
import com.lightgraph.graph.server.Server;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import restful.model.EdgeModel;
import restful.model.PutModel;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import restful.model.VertexModel;

@Path("/graph")
public class GraphService {

    @POST
    @Path("/{graph}/create/vertex/{vertex}")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean addVertexMeta(@PathParam("graph") String graph, @PathParam("vertex") String name,
            VertexMetaInfo vertexMetaInfo) {
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
    public boolean addEdgeMeta(@PathParam("graph") String graph, @PathParam("edge") String name,
            EdgeMetaInfo edgeMetaInfo) {
        edgeMetaInfo.setGraph(graph);
        edgeMetaInfo.setName(name);
        Server server = Server.getInstance();
        server.getMasterLeader().addEdgeMeta(edgeMetaInfo);
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

    @POST
    @Path("/{graph}/add/vertex/{vertex}/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean addVertex(@PathParam("graph") String graph, @PathParam("vertex") String vertex,
            @PathParam("id") Long id,
            Map<String, Object> properties) {
        Server server = Server.getInstance();
        return server.addVertex(graph, vertex, id, properties);
    }

    @POST
    @Path("/{graph}/add/edge/{edge}/{subject}/to/{object}")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean addEdge(@PathParam("graph") String graph, @PathParam("edge") String edge,
            @PathParam("subject") Long subject, @PathParam("object") Long object, Map<String, Object> properties) {
        Server server = Server.getInstance();
        return server.addEdge(graph, edge, subject, object, properties);
    }

    @POST
    @Path("/{graph}/add/edges")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean addEdge(@PathParam("graph") String graph, List<EdgeModel> edges) {
        Server server = Server.getInstance();
        Map<String, List<Edge>> labelGroup = new HashMap<>();
        edges.forEach(e -> {
            if (e.getSubject() == null || e.getObject() == null || e.getLabel() == null) {
                throw new GraphException(
                        String.format("invalid data!,subject:%s,object:%s,label:%s", e.getSubject(), e.getObject(),
                                e.getLabel()));
            }
            String label = e.getLabel();
            Edge edge = new Edge();
            edge.setSubject(e.getSubject());
            edge.setObject(e.getObject());
            edge.setProperties(edge.getProperties());
            if (labelGroup.containsKey(label)) {
                labelGroup.get(label).add(edge);
            } else {
                List<Edge> es = new ArrayList<>();
                es.add(edge);
                labelGroup.put(label, es);
            }
        });
        for (String label : labelGroup.keySet()) {
            server.addEdges(graph, label, labelGroup.get(label));
        }
        return true;
    }

    @GET
    @Path("/{graph}/get/vertex/{vertex}/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public VertexModel geVertex(@PathParam("graph") String graph, @PathParam("vertex") String vertex,
            @PathParam("id") Long id) {
        Vertex v = Server.getInstance().getVertex(graph, vertex, id);
        return new VertexModel(v);
    }

    @GET
    @Path("/{graph}/get/edge/{edge}/{subject}/to/{object}")
    @Produces({MediaType.APPLICATION_JSON})
    public EdgeModel getEdge(@PathParam("graph") String graph, @PathParam("edge") String edge,
            @PathParam("subject") Long subject, @PathParam("object") Long object) {
        Edge e = Server.getInstance().getEdge(graph, edge, subject, object);
        return new EdgeModel(e);
    }

    @GET
    @Path("/{graph}/{edge}/{subject}/out")
    @Produces({MediaType.APPLICATION_JSON})
    public List<EdgeModel> out(@PathParam("graph") String graph, @PathParam("edge") String edge,
            @PathParam("subject") Long subject) {
        List<Edge> es = Server.getInstance().outEdges(graph, edge, subject);
        return es.stream().map(e -> new EdgeModel(e)).collect(Collectors.toList());
    }

    @GET
    @Path("/{graph}/{subject}/out")
    @Produces({MediaType.APPLICATION_JSON})
    public List<EdgeModel> out(@PathParam("graph") String graph,
            @PathParam("subject") Long subject) {
        List<Edge> es = Server.getInstance().outEdges(graph, subject, 1);
        return es.stream().map(e -> new EdgeModel(e)).collect(Collectors.toList());
    }

    @GET
    @Path("/{graph}/{subject}/out/{step}")
    @Produces({MediaType.APPLICATION_JSON})
    public List<EdgeModel> outN(@PathParam("graph") String graph,
            @PathParam("subject") Long subject, @PathParam("step") Integer step) {
        List<Edge> es = Server.getInstance().outEdges(graph, subject, step);
        return es.stream().map(e -> new EdgeModel(e)).collect(Collectors.toList());
    }

    @GET
    @Path("/{graph}/{subject}/cout/{step}")
    @Produces({MediaType.APPLICATION_JSON})
    public Long coutN(@PathParam("graph") String graph,
            @PathParam("subject") Long subject, @PathParam("step") Integer step) {
        return Server.getInstance().coutEdges(graph, subject, step);
    }
}
