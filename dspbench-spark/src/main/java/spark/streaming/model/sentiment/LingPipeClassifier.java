package spark.streaming.model.sentiment;

import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.SentimentAnalysisConstants;
import spark.streaming.util.Configuration;

import java.io.File;
import java.io.IOException;

public class LingPipeClassifier implements SentimentClassifier {
    private static final Logger LOG = LoggerFactory.getLogger(LingPipeClassifier.class);
    private static final String DEFAULT_PATH = "sentimentanalysis/classifier.bin";
    private LMClassifier classifier;
    
    @Override
    public void initialize(Configuration config) {
        try {
            String clsPath = config.get(SentimentAnalysisConstants.Config.LINGPIPE_CLASSIFIER_PATH, DEFAULT_PATH);
            classifier = (LMClassifier) AbstractExternalizable.readObject(new File(clsPath));
        } catch (ClassNotFoundException | IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Unable to initialize the sentiment classifier");
        }
    }

    @Override
    public SentimentResult classify(String str) {
        ConditionalClassification classification = classifier.classify(str);
        String cat = classification.bestCategory();
        
        SentimentResult result = new SentimentResult();
        result.setScore(classification.score(0));
        
        switch (cat) {
            case "pos":
                result.setSentiment(SentimentResult.Sentiment.Positive);
                break;
            case "neg":
                result.setSentiment(SentimentResult.Sentiment.Negative);
                break;
            default:
                result.setSentiment(SentimentResult.Sentiment.Neutral);
                break;
        }
        
        return result;
    }
    
}
