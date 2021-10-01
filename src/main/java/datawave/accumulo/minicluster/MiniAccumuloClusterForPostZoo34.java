package datawave.accumulo.minicluster;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Set;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. The use of this utility will yield results which closely match a normal Accumulo instance.
 *
 * @since 1.5.0
 */
public class MiniAccumuloClusterForPostZoo34 {
    
    private MiniAccumuloClusterImplForPostZoo34 impl;
    
    private MiniAccumuloClusterForPostZoo34(MiniAccumuloConfigImpl config) throws IOException {
        impl = new MiniAccumuloClusterImplForPostZoo34(config);
    }
    
    /**
     *
     * @param dir
     *            An empty or nonexistant temp directoy that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7,
     *            Guava, and Junit provide methods for creating temporary directories.
     * @param rootPassword
     *            Initial root password for instance.
     */
    public MiniAccumuloClusterForPostZoo34(File dir, String rootPassword) throws IOException {
        this(new MiniAccumuloConfigImpl(dir, rootPassword));
    }
    
    /**
     * @param config
     *            initial configuration
     */
    public MiniAccumuloClusterForPostZoo34(MiniAccumuloConfig config) throws IOException {
        this(getImpl(config));
    }
    
    private static final MiniAccumuloConfigImpl getImpl(MiniAccumuloConfig config) {
        try {
            Field field = MiniAccumuloConfig.class.getDeclaredField("impl");
            field.setAccessible(true);
            return (MiniAccumuloConfigImpl) (field.get(config));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Starts Accumulo and Zookeeper processes. Can only be called once.
     */
    public void start() throws IOException, InterruptedException {
        impl.start();
    }
    
    /**
     * @return generated remote debug ports if in debug mode.
     * @since 1.6.0
     */
    public Set<Pair<ServerType,Integer>> getDebugPorts() {
        return impl.getDebugPorts();
    }
    
    /**
     * @return Accumulo instance name
     */
    public String getInstanceName() {
        return impl.getInstanceName();
    }
    
    /**
     * @return zookeeper connection string
     */
    public String getZooKeepers() {
        return impl.getZooKeepers();
    }
    
    /**
     * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. However its probably best to
     * call stop in a finally block as soon as possible.
     */
    public void stop() throws IOException, InterruptedException {
        impl.stop();
    }
    
    /**
     * @since 1.6.0
     */
    public MiniAccumuloConfig getConfig() {
        try {
            Constructor<MiniAccumuloConfig> c = MiniAccumuloConfig.class.getConstructor(MiniAccumuloConfigImpl.class);
            c.setAccessible(true);
            return c.newInstance(impl.getConfig());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Utility method to get a connector to the MAC.
     *
     * @since 1.6.0
     */
    public Connector getConnector(String user, String passwd) throws AccumuloException, AccumuloSecurityException {
        return impl.getConnector(user, new PasswordToken(passwd));
    }
    
    /**
     * @since 1.6.0
     */
    public ClientConfiguration getClientConfig() {
        return impl.getClientConfig();
    }
}
