package moniq.util;

import java.util.Random;

public class NaiveMessageGenerator extends NaiveMessageAdaptor {

  private final static String paddingCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  private final static int MAX_CUR_IDX_MULTIPLIER = 10;

  private int[] preGeneratedIndices;

  private int curIdx;

  public NaiveMessageGenerator(int messageSize, int preIndicesSize) {
    super(messageSize);
    init(preIndicesSize);
  }

  private void init(int preIndicesSize) {
    Random random = new Random();
    preGeneratedIndices = new int[preIndicesSize];
    curIdx = 0;

    for (int i = 0; i < preGeneratedIndices.length; i++) {
      preGeneratedIndices[i] = random.nextInt(paddingCharacters.length());
    }
  }

  @Override
  protected String getRandomPadding(int size) {
    StringBuilder paddedString = new StringBuilder();
    for (int i = 0; i < size; i++) {
      if (curIdx >= preGeneratedIndices.length * MAX_CUR_IDX_MULTIPLIER) {
        curIdx = 0; // Reset the index to avoid overflow
      }
      if (curIdx >= preGeneratedIndices.length) {
        curIdx = 0;
      }
      char randomChar = paddingCharacters.charAt(preGeneratedIndices[curIdx % preGeneratedIndices.length]);
      paddedString.append(randomChar);
      curIdx += 1;
    }

    return paddedString.toString();
  }
}
