package flink.application.tweetslatency;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;



public class TweetsLatencyEvent implements Serializable { 
    private int id;
    private String authorName;
    private String content;

    // Synthetic data
    private List<Integer> durations;

    // Generated data
    private List<TweetsLatencyMetadata> metadatas;

    public TweetsLatencyEvent(int id, String authorName, String content) {
        this.id = id;
        this.authorName = authorName;
        this.content = content;

        this.durations = new ArrayList();

        this.metadatas = new ArrayList();
    }

    public TweetsLatencyEvent(int id, String authorName, String content, List<Integer> durations) {
        this.id = id;
        this.authorName = authorName;
        this.content = content;

        this.durations = durations;

        this.metadatas = new ArrayList();
    }

    public boolean isSynthetic() {
        return this.durations.size() > 0;
    }

    public int getId() {
        return this.id;
    }

    public String getAuthorName(){
        return this.authorName;
    }

    public String getContent() {
        return this.content;
    }

    public List<Integer> getDurations() {
        return this.durations;
    }

    public List<TweetsLatencyMetadata> getMetadatas() {
        return this.metadatas;
    }

    @Override
    public String toString() {
        return "TweetsLatencyEvent(" + "id=" + this.id + "authorName=" + this.authorName + ", content=" + this.content + ")";
    }
}
