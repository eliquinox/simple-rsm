package rsm.node;

public enum MessageType {

    GET('g'), SET('s');

    private final char charCode;

    MessageType(final char charCode) {
        this.charCode = charCode;
    }

    public static MessageType fromCode(final char charCode) {
        return switch (charCode)
        {
            case 'g' -> GET;
            case 's' -> SET;
            default -> throw new IllegalStateException("Unexpected value: " + charCode);
        };
    }

    public char getCharCode() {
        return charCode;
    }
}
