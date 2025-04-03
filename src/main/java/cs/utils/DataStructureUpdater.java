package cs.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import cs.utils.Tuple3;
import cs.utils.SupportConfidence;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataStructureUpdater {

    private Map<Integer, Map<Integer, Set<Integer>>> cpot; // Type-property-object relationships
    private Map<Integer, Integer> cec; // Class-entity counts
    private Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts; // Support-confidence values

    public DataStructureUpdater(
            Map<Integer, Map<Integer, Set<Integer>>> cpot,
            Map<Integer, Integer> cec,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts) {
        this.cpot = cpot;
        this.cec = cec;
        this.sts = sts;
    }


     // Updates the cumulative data structures with new data.

    public void updateDataStructures(Map<Integer, Map<Integer, Set<Integer>>> newCpot,
                                     Map<Integer, Integer> newCec,
                                     Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> newSts) {

        // Update cpot
        newCpot.forEach((type, propWithObj) -> {
            cpot.merge(type, propWithObj, (existingProps, newProps) -> {
                newProps.forEach((property, objTypes) -> {
                    existingProps.merge(property, objTypes, (existingSet, newSet) -> {
                        existingSet.addAll(newSet);
                        return existingSet;
                    });
                });
                return existingProps;
            });
        });

        // Update cec
        newCec.forEach((classType, entityCount) ->
                cec.merge(classType, entityCount, Integer::sum)
        );

        // Update sts
        newSts.forEach((key, newSc) ->
                sts.merge(key, newSc, (existingSc, updatedSc) -> {
                    int combinedSupport = existingSc.getSupport() + updatedSc.getSupport();
                    double combinedConfidence = (existingSc.getConfidence() + updatedSc.getConfidence()) / 2;
                    return new SupportConfidence(combinedSupport, combinedConfidence);
                })
        );
    }


      // Serializes the data structures to a Kryo binary file.

    public void serializeToKryo(String filePath) {
        Kryo kryo = new Kryo();
        try (FileOutputStream fos = new FileOutputStream(filePath);
             Output output = new Output(fos)) {

            kryo.register(HashMap.class);
            kryo.register(Set.class);
            kryo.register(Tuple3.class);
            kryo.register(SupportConfidence.class);

            kryo.writeObject(output, cpot);
            kryo.writeObject(output, cec);
            kryo.writeObject(output, sts);

            System.out.println("Data structures serialized to: " + filePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize data structures: " + e.getMessage(), e);
        }
    }


     // Deserializes data structures from a Kryo binary file.

    @SuppressWarnings("unchecked")
    public static DataStructureUpdater deserializeFromKryo(String filePath) {
        Kryo kryo = new Kryo();
        try (FileInputStream fis = new FileInputStream(filePath);
             Input input = new Input(fis)) {

            kryo.register(HashMap.class);
            kryo.register(Set.class);
            kryo.register(Tuple3.class);
            kryo.register(SupportConfidence.class);

            Map<Integer, Map<Integer, Set<Integer>>> cpot = kryo.readObject(input, HashMap.class);
            Map<Integer, Integer> cec = kryo.readObject(input, HashMap.class);
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> sts = kryo.readObject(input, HashMap.class);

            System.out.println("Data structures deserialized from: " + filePath);
            return new DataStructureUpdater(cpot, cec, sts);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize data structures: " + e.getMessage(), e);
        }
    }
}
