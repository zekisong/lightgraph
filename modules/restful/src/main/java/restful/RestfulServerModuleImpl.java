package restful;

import com.lightgraph.graph.config.Configurable;
import com.lightgraph.graph.meta.cluster.GraphMeta;
import com.lightgraph.graph.modules.restful.RestfulServerHandler;
import com.lightgraph.graph.modules.restful.RestfulServerModule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import restful.constant.RestfulConstant;

public class RestfulServerModuleImpl implements RestfulServerModule {

    private static final Log LOG = LogFactory.getLog(RestfulServerModuleImpl.class);
    private ResourceConfig resourceConfig = new ResourceConfig();
    private Server server;

    public void createGraph(GraphMeta meta) {

    }

    public void setup(Configurable config) {
        int serverPort = config.get(RestfulConstant.RESTFUL_SERVER_PORT, RestfulConstant.RESTFUL_SERVER_PORT_DEFAULT);
        resourceConfig.packages("restful.service");
        resourceConfig.register(JacksonFeature.class);
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder sh = new ServletHolder(servletContainer);
        server = new Server(serverPort);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(sh, "/*");
        server.setHandler(context);
    }

    public void registerService(String packageName) {
        resourceConfig.packages(packageName);
    }

    public boolean start() {
        try {
            server.start();
        } catch (Exception e) {
            LOG.error("server start failed!", e);
        }
        return true;
    }

    public boolean shutdown() {
        return false;
    }

    public String moduleInfo() {
        return null;
    }

    @Override
    public RestfulServerHandler getRestfulServiceHandler() {
        return null;
    }
}
