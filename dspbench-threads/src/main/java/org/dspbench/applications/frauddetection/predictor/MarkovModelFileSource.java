package org.dspbench.applications.frauddetection.predictor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * @author maycon
 */
public class MarkovModelFileSource implements IMarkovModelSource {
    private Charset charset;

    public MarkovModelFileSource() {
        charset = Charset.defaultCharset();
    }

    public String getModel(String key) {
        byte[] encoded;
        try {
            encoded = Files.readAllBytes(Paths.get(key));
            return charset.decode(ByteBuffer.wrap(encoded)).toString();
        } catch (IOException ex) {
            return null;
        }
    }
    
}
