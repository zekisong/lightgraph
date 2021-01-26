package restful.service;

import com.lightgraph.graph.meta.LabelMeta;
import com.lightgraph.graph.meta.MetaType;
import com.lightgraph.graph.server.Server;
import com.lightgraph.graph.settings.GraphSetting;
import java.util.HashMap;
import java.util.Map;
import restful.model.ClusterModel;
import restful.model.GraphMetaModel;
import restful.model.GraphMetaRequestModel;
import restful.model.RouttingModel;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path("/cluster")
public class ClusterService {

    @GET
    @Path("_cat")
    @Produces({MediaType.APPLICATION_JSON})
    public ClusterModel getClusterContext() {
        return new ClusterModel();
    }

    @GET
    @Path("_cat/graph/meta/{graph}")
    @Produces({MediaType.APPLICATION_JSON})
    public GraphMetaModel getGraphMeta(@PathParam("graph") String graph) {
        return new GraphMetaModel(Server.getInstance().getGraphMeta(graph));
    }

    @GET
    @Path("_cat/graph/metas")
    @Produces({MediaType.APPLICATION_JSON})
    public List<GraphMetaModel> getGraphMeta() {
        List<GraphMetaModel> models = new ArrayList<>();
        Server.getInstance().listGraphMeta().forEach(m -> models.add(new GraphMetaModel(m)));
        return models;
    }

    @GET
    @Path("_cat/graph/{graph}/metas")
    @Produces({MediaType.APPLICATION_JSON})
    public Map<String, List<LabelMeta>> getLabelMeta(@PathParam("graph") String name) {
        Map map = new HashMap();
        List<LabelMeta> edges = Server.getInstance().listLabelMeta(name, MetaType.EDGE);
        map.put("edges", edges);
        List<LabelMeta> vertices = Server.getInstance().listLabelMeta(name, MetaType.VERTEX);
        map.put("vertices", vertices);
        return map;
    }

    @GET
    @Path("_cat/graph/routting")
    @Produces({MediaType.APPLICATION_JSON})
    public RouttingModel getGraph() {
        return new RouttingModel();
    }

    @POST
    @Path("/create/graph/{name}")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean createGraph(@PathParam("name") String name, GraphMetaRequestModel graphMetaRequestModel) {
        GraphSetting setting = new GraphSetting(name);
        setting.set(GraphSetting.GRAPH_PARTITION_COUNT, String.valueOf(graphMetaRequestModel.getPartition()));
        setting.set(GraphSetting.GRAPH_PEPLICATION_COUNT, String.valueOf(graphMetaRequestModel.getReplicas()));
        Server server = Server.getInstance();
        server.getMasterLeader().createGraph(setting);
        return true;
    }
}
