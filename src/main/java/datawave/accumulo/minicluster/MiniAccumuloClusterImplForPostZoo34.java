package datawave.accumulo.minicluster;

import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MiniAccumuloClusterImplForPostZoo34 extends org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl {
    
    public MiniAccumuloClusterImplForPostZoo34(File dir, String rootPassword) throws IOException {
        super(dir, rootPassword);
    }
    
    public MiniAccumuloClusterImplForPostZoo34(MiniAccumuloConfigImpl config) throws IOException {
        super(config);
        
        if (!config.useExistingInstance() && !config.useExistingZooKeepers()) {
            // we need to append to the zoo.cfg to allow later versions of zookeeper to work
            
            // load the existing zoo configuration file
            File zooCfgFile = new File(config.getConfDir(), "zoo.cfg");
            
            Reader fileReader = new InputStreamReader(new FileInputStream(zooCfgFile), StandardCharsets.UTF_8);
            Properties zooCfg = new Properties();
            zooCfg.load(fileReader);
            fileReader.close();
            
            // add the new properties
            zooCfg.setProperty("4lw.commands.whitelist", "ruok,wchs");
            zooCfg.setProperty("admin.enableServer", "false");
            
            // overwrite the file
            Writer fileWriter = new OutputStreamWriter(new FileOutputStream(zooCfgFile), StandardCharsets.UTF_8);
            zooCfg.store(fileWriter, null);
            fileWriter.close();
        }
    }
    
}
