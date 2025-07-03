package com.timecho.iotdb.session;

import org.junit.Assert;
import org.junit.Test;

public class BuilderTest {

  @Test
  public void testBuilderChaining() {
    // Test that builder chaining works correctly and returns the right type
    Session.Builder builder =
        new Session.Builder()
            .host("localhost")
            .port(6667)
            .username("root")
            .password("root")
            .fetchSize(1000);

    // Verify the builder is of the correct type
    Assert.assertTrue(builder instanceof Session.Builder);

    // Test that we can build a Session
    Session session = builder.build();
    Assert.assertNotNull(session);
    Assert.assertTrue(session instanceof com.timecho.iotdb.session.Session);
  }
}
