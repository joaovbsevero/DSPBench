package flink.constants;

public interface TweetsLatencyConstants extends BaseConstants {
    String PREFIX = "tl";

    interface Conf extends BaseConf {
        String PARSER_THREADS = "tl.parser.threads";
        String SPLITTER_THREADS = "tl.splitter.threads";
        String REDUCER_THREADS = "tl.reducer.threads";
    }
}
