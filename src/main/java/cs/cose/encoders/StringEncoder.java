package cs.cose.encoders;


import java.util.HashMap;

public class StringEncoder implements Encoder {
    int counter;
    HashMap<Integer, String> table;
    HashMap<String, Integer> reverseTable;

    public StringEncoder() {
        this.counter = -1;
        this.table = new HashMap<>();
        this.reverseTable = new HashMap<>();
    }

    public int encode(String val) {
        if (reverseTable.containsKey(val)) {
            return reverseTable.get(val);
        } else {
            this.counter++;
            table.put(counter, val);
            reverseTable.put(val, counter);
            return counter;
        }
    }

    public boolean isEncoded(String val) {
        return reverseTable.containsKey(val);
    }


    public HashMap<Integer, String> getTable() {
        return table;
    }

    public String decode(int val) {
        return this.table.get(val);
    }

    public HashMap<String, Integer> getRevTable() {
        return reverseTable;
    }
}