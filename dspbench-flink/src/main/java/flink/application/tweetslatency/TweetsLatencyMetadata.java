package flink.application.tweetslatency;

import java.io.Serializable;

public class TweetsLatencyMetadata implements Serializable { 
    private int eventId;
    private int duration;
    private String content;

    public TweetsLatencyMetadata(int eventId, int duration, String content) {
        this.eventId = eventId;
        this.duration = duration;
        this.content = content;
    }

    public int getEventId() {
        return this.id;
    }

    public int getDuration() {
        return this.duration;
    }

    public String getContent() {
        return this.content;
    }

    @Override
    public String toString() {
        return "TweetsLatencyMetadata(" + "eventId=" + this.eventId + ", duration=" + this.duration + ", content=" + this.content + ")";
    }
}