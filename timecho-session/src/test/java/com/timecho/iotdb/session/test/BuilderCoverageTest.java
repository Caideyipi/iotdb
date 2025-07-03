package com.timecho.iotdb.session.test;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class BuilderCoverageTest {

  @Test
  public void testSessionBuilderCoversAllParentMethods() {
    Class<?> parentBuilder = org.apache.iotdb.session.Session.Builder.class;
    Class<?> childBuilder = com.timecho.iotdb.session.Session.Builder.class;

    checkBuilderCoverage(parentBuilder, childBuilder, "Session.Builder");
  }

  @Test
  public void testSessionPoolBuilderCoversAllParentMethods() {
    Class<?> parentBuilder = org.apache.iotdb.session.pool.SessionPool.Builder.class;
    Class<?> childBuilder = com.timecho.iotdb.session.pool.SessionPool.Builder.class;

    checkBuilderCoverage(parentBuilder, childBuilder, "SessionPool.Builder");
  }

  private void checkBuilderCoverage(
      Class<?> parentBuilder, Class<?> childBuilder, String builderName) {
    // 获取父类的所有public方法（除了build方法）
    Set<String> parentMethods =
        Arrays.stream(parentBuilder.getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> m.getReturnType().equals(parentBuilder)) // 只检查返回Builder类型的方法
            .filter(m -> !m.getName().equals("build")) // 排除build方法，因为子类需要返回不同类型
            .map(this::getMethodSignature)
            .collect(Collectors.toSet());

    // 获取子类重写的方法
    Set<String> childOverriddenMethods =
        Arrays.stream(childBuilder.getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> m.getReturnType().equals(childBuilder)) // 只检查返回Builder类型的方法
            .filter(m -> !m.getName().equals("build")) // 排除build方法
            .map(this::getMethodSignature)
            .collect(Collectors.toSet());

    System.out.println("=== " + builderName + " 方法覆盖检查 ===");
    System.out.println("父类方法总数: " + parentMethods.size());
    System.out.println("子类重写方法数: " + childOverriddenMethods.size());

    // 查找未覆盖的方法
    Set<String> uncoveredMethods =
        parentMethods.stream()
            .filter(method -> !childOverriddenMethods.contains(method))
            .collect(Collectors.toSet());

    if (!uncoveredMethods.isEmpty()) {
      System.out.println("未覆盖的方法:");
      uncoveredMethods.forEach(method -> System.out.println("  - " + method));
      Assert.fail(
          builderName + " 有 " + uncoveredMethods.size() + " 个方法未被子类覆盖: " + uncoveredMethods);
    } else {
      System.out.println("✅ 所有方法都已正确覆盖!");
    }

    // 查找多余的方法（子类有但父类没有的）
    Set<String> extraMethods =
        childOverriddenMethods.stream()
            .filter(method -> !parentMethods.contains(method))
            .collect(Collectors.toSet());

    if (!extraMethods.isEmpty()) {
      System.out.println("额外的方法（父类中不存在）:");
      extraMethods.forEach(method -> System.out.println("  - " + method));
    }
  }

  private String getMethodSignature(Method method) {
    StringBuilder signature = new StringBuilder();
    signature.append(method.getName()).append("(");

    Class<?>[] paramTypes = method.getParameterTypes();
    for (int i = 0; i < paramTypes.length; i++) {
      if (i > 0) signature.append(",");
      signature.append(paramTypes[i].getSimpleName());
    }
    signature.append(")");

    return signature.toString();
  }
}
