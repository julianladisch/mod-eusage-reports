package org.folio.tlib.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class LongAdderTest {

  @Test
  public void addAndGet() {
    LongAdder longAdder = new LongAdder();
    assertThat(longAdder.get(), is(nullValue()));
    longAdder.add(null);
    assertThat(longAdder.get(), is(nullValue()));
    longAdder.add(null);
    assertThat(longAdder.get(), is(nullValue()));
    longAdder.add(3L);
    assertThat(longAdder.get(), is(3L));
    longAdder.add(5L);
    assertThat(longAdder.get(), is(8L));
    longAdder.add(null);
    assertThat(longAdder.get(), is(8L));
  }

  @Test
  public void array() {
    LongAdder [] array = LongAdder.arrayOfLength(4);
    assertThat(array.length, is(4));
    assertThat(array[0].get(), is(nullValue()));
    assertThat(array[1].get(), is(nullValue()));
    assertThat(array[2].get(), is(nullValue()));
    assertThat(array[3].get(), is(nullValue()));
    assertThat(LongAdder.sum(array), is(nullValue()));
    array[1].add(0L);
    assertThat(LongAdder.sum(array), is(0L));
    array[0].add(2L);
    assertThat(LongAdder.sum(array), is(2L));
    array[3].add(5L);
    assertThat(LongAdder.sum(array), is(7L));
    Long [] longArray = LongAdder.longArray(array);
    assertThat(longArray.length, is(4));
    assertThat(longArray[0], is(2L));
    assertThat(longArray[1], is(0L));
    assertThat(longArray[2], is(nullValue()));
    assertThat(longArray[3], is(5L));
  }
}
