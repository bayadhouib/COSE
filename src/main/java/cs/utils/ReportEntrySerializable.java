package cs.utils;

public class ReportEntrySerializable {
    private final String message;
    private final String focusNode;
    private final String resultPath;
    private final String value;

    public ReportEntrySerializable(String message, String focusNode, String resultPath, String value) {
        this.message = message;
        this.focusNode = focusNode;
        this.resultPath = resultPath;
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public String getFocusNode() {
        return focusNode;
    }

    public String getResultPath() {
        return resultPath;
    }

    public String getValue() {
        return value;
    }
}
