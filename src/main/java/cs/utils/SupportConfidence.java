package cs.utils;

public class SupportConfidence {
    Integer support = 0;
    Double confidence = 0D;

    public SupportConfidence() {}

    public SupportConfidence(Integer support) {
        this.support = support;
    }

    public SupportConfidence(Integer s, Double c) {
        this.support = s;
        this.confidence = c;
    }

    public Integer getSupport() {
        return support;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setSupport(Integer support) {
        this.support = support;
    }
    public void incrementSupport() {
        this.support++;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

}

//public class SupportConfidence {
//
//    private int support;
//    private double confidence;
//
//    public SupportConfidence() {
//        this.support = 0;
//        this.confidence = 0.0;
//    }
//
//    public int getSupport() {
//        return support;
//    }
//
//    public void setSupport(int support) {
//        this.support = support;
//    }
//
//    public double getConfidence() {
//        return confidence;
//    }
//
//    public void setConfidence(double confidence) {
//        this.confidence = confidence;
//    }
//
//    public void incrementSupport() {
//        this.support++;
//    }
//
//    @Override
//    public String toString() {
//        return "SupportConfidence{" +
//                "support=" + support +
//                ", confidence=" + confidence +
//                '}';
//    }
//}
