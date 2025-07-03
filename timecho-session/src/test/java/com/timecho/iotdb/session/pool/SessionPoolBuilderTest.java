package com.timecho.iotdb.session.pool;

import org.junit.Assert;
import org.junit.Test;

public class SessionPoolBuilderTest {

  @Test
  public void testSessionPoolBuilderChaining() {
    // Test that SessionPool builder chaining works correctly and returns the right type
    SessionPool.Builder builder =
        new SessionPool.Builder()
            .host("localhost")
            .port(6667)
            .user("root")
            .password("root")
            .maxSize(5)
            .fetchSize(1000);

    // Test that we can build a SessionPool
    SessionPool sessionPool = builder.build();
    Assert.assertNotNull(sessionPool);
    Assert.assertTrue(sessionPool instanceof com.timecho.iotdb.session.pool.SessionPool);

    // Verify some basic properties
    Assert.assertEquals("localhost", sessionPool.getHost());
    Assert.assertEquals(6667, sessionPool.getPort());
    Assert.assertEquals("root", sessionPool.getUser());
    Assert.assertEquals("root", sessionPool.getPassword());
    Assert.assertEquals(5, sessionPool.getMaxSize());
    Assert.assertEquals(1000, sessionPool.getFetchSize());
  }
}
