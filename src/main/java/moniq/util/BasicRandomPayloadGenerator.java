package moniq.util;

import java.util.concurrent.ThreadLocalRandom;

public class BasicRandomPayloadGenerator implements IPayloadGenerator {

    private final static String paddingCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private final String preGeneratedPayload;

    public BasicRandomPayloadGenerator(int preIndicesSize) {
        if (preIndicesSize < 0) {
            throw new IllegalArgumentException("preIndicesSize must not be negative");
        }

        this.preGeneratedPayload = generatePayloadPool(preIndicesSize);
    }

    @Override
    public String generatePayload(int payloadSize) {
        if (payloadSize <= 0) {
            return "";
        }

        int totalLength = preGeneratedPayload.length();
        int startIndex = ThreadLocalRandom.current().nextInt(totalLength);

        StringBuilder sb = new StringBuilder(payloadSize);
        int remainingSize = payloadSize;
        while (remainingSize > 0) {
            int availableLength = totalLength - startIndex;
            int bytesToCopy = Math.min(remainingSize, availableLength);
            sb.append(preGeneratedPayload, startIndex, startIndex + bytesToCopy);

            remainingSize -= bytesToCopy;
            startIndex = 0;
        }

        return sb.toString();
    }

    private String generatePayloadPool(int preIndicesSize) {
        StringBuilder payload = new StringBuilder(preIndicesSize);

        for (int i = 0; i < preIndicesSize; i++) {
            int randomIndex = ThreadLocalRandom.current().nextInt(paddingCharacters.length());
            payload.append(paddingCharacters.charAt(randomIndex));
        }
        return payload.toString();
    }
}
