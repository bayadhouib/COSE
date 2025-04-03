package cs.cose.encoders;

import java.util.Map;

public abstract class AbstractStringEncoder {
    public abstract int encode(String val);
    public abstract boolean isEncoded(String val);
    public abstract String decode(int val);
    public abstract Map<Integer, String> getTable();
    public abstract Map<String, Integer> getRevTable();
}
