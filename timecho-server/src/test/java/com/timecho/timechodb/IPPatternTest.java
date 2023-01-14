package com.timecho.timechodb;

import com.timecho.timechodb.service.ClientRPCServiceImplNew;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class IPPatternTest {

  @Test
  public void testIpPattern() {
    Assert.assertTrue(Pattern.matches(ClientRPCServiceImplNew.WHITE_LIST_PATTERN, "192.168.0.1"));
    Assert.assertTrue(Pattern.matches(ClientRPCServiceImplNew.WHITE_LIST_PATTERN, "192.*.0.1"));
    Assert.assertTrue(Pattern.matches(ClientRPCServiceImplNew.WHITE_LIST_PATTERN, "*.*.*.*"));
    Assert.assertTrue(Pattern.matches(ClientRPCServiceImplNew.WHITE_LIST_PATTERN, "127.0.0.1"));
  }
}
