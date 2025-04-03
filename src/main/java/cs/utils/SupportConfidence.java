package cs.utils;

public class SupportConfidence implements Comparable<SupportConfidence> {
    private Integer support;
    private Double confidence;

    public SupportConfidence() {
        this.support = 0;
        this.confidence = 0.0;
    }

    public SupportConfidence(Integer support) {
        this.support = support;
        this.confidence = 0.0;
    }

    public SupportConfidence(Integer support, Double confidence) {
        this.support = support;
        this.confidence = confidence;
    }

    public Integer getSupport() {
        return support;
    }

    public void setSupport(Integer support) {
        this.support = support;
    }

    public void incrementSupport() {
        this.support++;
    }


    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }


     //Compares this SupportConfidence object to another based on support and confidence.
     //Support is prioritized first; if equal, confidence is compared.

    @Override
    public int compareTo(SupportConfidence other) {
        if (!this.support.equals(other.support)) {
            return this.support.compareTo(other.support);
        }
        return this.confidence.compareTo(other.confidence);
    }

    @Override
    public String toString() {
        return "SupportConfidence{" +
                "support=" + support +
                ", confidence=" + confidence +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SupportConfidence that = (SupportConfidence) obj;
        return support.equals(that.support) && confidence.equals(that.confidence);
    }

    @Override
    public int hashCode() {
        int result = support.hashCode();
        result = 31 * result + confidence.hashCode();
        return result;
    }
}
