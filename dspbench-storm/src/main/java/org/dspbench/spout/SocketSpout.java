package org.dspbench.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.dspbench.constants.BaseConstants;
import org.dspbench.spout.parser.Parser;
import org.dspbench.util.config.ClassLoaderUtils;
import org.dspbench.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class SocketSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SocketSpout.class);
    private static final int DEFAULT_PORT = 6860;
    
    protected Parser parser;
    private String host;
    private int port;
    
    private ServerSocket server;
    private BufferedReader reader;
    
    @Override
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_PARSER));
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        host = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_SOCKET_HOST), null);
        port = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_SOCKET_PORT), DEFAULT_PORT);
        
        try {
            if (host == null) {
                server = new ServerSocket(port);
            } else {
                server = new ServerSocket(port, 1, InetAddress.getByName(host));
            }
            
            Socket connection = server.accept();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        } catch (IOException ex) {
            LOG.error("SocketSpout error: " + ex.getMessage(), ex);
            throw new RuntimeException("SocketSpout error: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void nextTuple() {
        while (true) {
            try {
                String packet = reader.readLine();
                if (packet != null) {
                    List<StreamValues> tuples = parser.parse(packet);
        
                    if (tuples != null) {
                        for (StreamValues values : tuples)
                            collector.emit(values.getStreamId(), values);
                    }
                }
            } catch (IOException ex) {
                LOG.error("SocketSpout error: " + ex.getMessage(), ex);
                throw new RuntimeException("SocketSpout error: " + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        
        try {
            server.close();
        } catch (IOException ex) {
            LOG.error("Error while closing socket server", ex);
        }
    }
    
}
