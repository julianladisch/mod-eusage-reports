package org.folio.tlib.util;

/**
 * Adds Long values with null handling like SQL sum:
 *
 * <p>null + null = null
 *
 * <p>null + l = l
 *
 * <p>l + null = l
 *
 * <p>The initial value of a LongAdder is null.
 */
public class LongAdder {
  private Long sum;

  /**
   * Returns the current value.
   */
  public Long get() {
    return sum;
  }

  /**
   * Adds l to the current value.
   */
  public void add(Long l) {
    if (sum == null) {
      sum = l;
      return;
    }
    if (l == null) {
      return;
    }
    sum += l;
  }

  /**
   * Return a LongAdder array of length n filled with
   * LongAdder objects.
   */
  public static LongAdder [] arrayOfLength(int n) {
    LongAdder [] array = new LongAdder [n];
    for (int i = 0; i < n; i++) {
      array[i] = new LongAdder();
    }
    return array;
  }

  /**
   * Add the values of all array elements and return the sum.
   */
  public static Long sum(LongAdder [] array) {
    LongAdder sum = new LongAdder();
    for (int i = 0; i < array.length; i++) {
      sum.add(array[i].get());
    }
    return sum.get();
  }

  /**
   * Return a Long [] with the values of the array elements.
   */
  public static Long [] longArray(LongAdder [] array) {
    Long [] longArray = new Long [array.length];
    for (int i = 0; i < array.length; i++) {
      longArray[i] = array[i].get();
    }
    return longArray;
  }

}
