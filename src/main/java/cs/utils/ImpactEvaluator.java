package cs.utils;

import cs.utils.SupportConfidence;
import cs.utils.Tuple3;
import java.util.Map;

public class ImpactEvaluator {

    public static void evaluateImpact(
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> supportBefore,
            Map<Tuple3<Integer, Integer, Integer>, SupportConfidence> supportAfter) {

        for (Map.Entry<Tuple3<Integer, Integer, Integer>, SupportConfidence> entry : supportBefore.entrySet()) {
            Tuple3<Integer, Integer, Integer> triplet = entry.getKey();
            SupportConfidence before = entry.getValue();
            SupportConfidence after = supportAfter.getOrDefault(triplet, new SupportConfidence());

            double supportChange = after.getSupport() - before.getSupport();
            double confidenceChange = after.getConfidence() - before.getConfidence();

            System.out.println("Triplet: " + triplet);
            System.out.println("Support Before: " + before.getSupport() + ", After: " + after.getSupport());
            System.out.println("Confidence Before: " + before.getConfidence() + ", After: " + after.getConfidence());
            System.out.println("Support Change: " + supportChange + ", Confidence Change: " + confidenceChange);
        }
    }
}
