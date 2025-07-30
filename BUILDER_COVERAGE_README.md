# Builder 覆盖检查系统

## 概述

这个项目包含了一个自动化系统来确保子类 Builder 完全覆盖父类 Builder 的所有方法，以保证类型安全和正确的方法链式调用。

## 相关文件

### 源代码文件
- `timecho-session/src/main/java/com/timecho/iotdb/session/Session.java` - Session 类及其 Builder
- `timecho-session/src/main/java/com/timecho/iotdb/session/pool/SessionPool.java` - SessionPool 类及其 Builder

### 测试文件
- `timecho-session/src/test/java/com/timecho/iotdb/session/test/BuilderCoverageTest.java` - 自动检查 Builder 方法覆盖的测试
- `timecho-session/src/test/java/com/timecho/iotdb/session/BuilderTest.java` - Session Builder 功能测试
- `timecho-session/src/test/java/com/timecho/iotdb/session/pool/SessionPoolBuilderTest.java` - SessionPool Builder 功能测试

### CI/CD 脚本
- `ci_builder_check.sh` - CI/CD 流水线中的 Builder 覆盖检查脚本

## Builder 覆盖检查的重要性

### 问题背景
在 Java 中，当子类继承父类的 Builder 模式时，如果不正确重写父类的方法，会导致：

1. **类型不安全**：父类方法返回父类 Builder 类型，导致无法访问子类特有的方法
2. **链式调用中断**：在方法链中调用父类方法后，无法继续调用子类方法
3. **编译错误**：尝试访问子类特有方法时出现编译错误

### 解决方案
我们的解决方案确保：

1. **完全覆盖**：子类 Builder 重写所有父类 Builder 方法
2. **正确返回类型**：每个重写的方法返回子类 Builder 类型 (`this`)
3. **自动验证**：通过反射自动检查方法覆盖的完整性

## 工作原理

### 1. 反射检查机制
`BuilderCoverageTest.java` 使用 Java 反射来：
- 获取父类 Builder 的所有公共方法
- 检查子类 Builder 是否重写了每个方法
- 验证返回类型是否正确

### 2. 方法重写模式
每个父类方法都按以下模式重写：
```java
@Override
public ChildBuilder methodName(ParameterType param) {
    super.methodName(param);
    return this;
}
```

### 3. CI/CD 集成
- 在 `.gitlab-ci.yml` 中添加了 `builder-coverage-check` job
- 每次 MR 都会自动运行覆盖检查
- 确保代码合并前 Builder 覆盖是完整的

## 使用方法

### 本地开发
```bash
# 完整的 CI 检查（包括编译和功能测试）
./check_session_builder.sh

# 或者直接运行 Maven 测试
cd timecho-session
mvn test -Dtest=BuilderCoverageTest
```

### 添加新的 Builder 方法
1. 在父类 Builder 中添加新方法
2. 在子类 Builder 中重写该方法：
   ```java
   @Override
   public ChildBuilder newMethod(ParameterType param) {
       super.newMethod(param);
       return this;
   }
   ```
3. 运行覆盖检查验证：
   ```bash
   ./check_session_builder.sh
   # 或
   cd timecho-session && mvn test -Dtest=BuilderCoverageTest
   ```

### CI/CD 流水线
Builder 覆盖检查已集成到 GitLab CI 中：
- Job 名称：`builder-coverage-check`
- 阶段：`test`
- 并行运行，不依赖其他 job
- 在以下分支上运行：主分支、`rel/*`、`rc/*`

## 检查内容

### 1. 方法覆盖检查
- 验证所有父类公共方法都被子类重写
- 确保重写方法的签名正确
- 检查返回类型是否为子类类型

### 2. 编译验证
- 确保重构后的代码能够正确编译
- 验证没有引入编译错误

### 3. 功能测试
- 测试 Builder 的基本功能
- 验证方法链式调用工作正常
- 确保能够正确创建对象实例

## 故障排除

### 覆盖检查失败
如果看到类似错误：
```
❌ 方法未被正确覆盖: methodName
```

解决方法：
1. 在子类 Builder 中添加缺失方法的重写
2. 确保方法签名完全匹配父类
3. 确保返回类型为子类 Builder 类型

### 编译失败
如果编译失败：
1. 检查重写方法的语法
2. 确保 import 语句正确
3. 验证方法参数类型匹配

### 功能测试失败
如果功能测试失败：
1. 检查 Builder 构造逻辑
2. 验证链式调用是否正确
3. 确保对象创建过程无误

## 最佳实践

1. **持续检查**：每次修改 Builder 类后运行覆盖检查
2. **及时修复**：发现覆盖问题立即修复，避免积累
3. **完整测试**：不仅检查覆盖，也要测试功能
4. **文档更新**：修改 Builder 接口时更新相关文档

## 技术细节

### 反射查询
使用 `getDeclaredMethods()` 和 `getMethod()` 来比较方法签名：
```java
Method[] parentMethods = parentBuilderClass.getDeclaredMethods();
for (Method parentMethod : parentMethods) {
    // 检查子类是否有相应的重写方法
    childBuilderClass.getMethod(parentMethod.getName(), parentMethod.getParameterTypes());
}
```

### 过滤条件
只检查需要重写的方法：
- 公共方法 (`Modifier.isPublic()`)
- 非静态方法 (`!Modifier.isStatic()`)
- 非 final 方法 (`!Modifier.isFinal()`)
- 排除 Object 类的方法和构造函数

### CI 集成
通过 GitLab CI YAML 配置实现自动化：
```yaml
builder-coverage-check:
  stage: test
  script:
    - './check_session_builder.sh'
  rules:
    - if: $CI_MERGE_REQUEST_TARGET_BRANCH_NAME == $CI_DEFAULT_BRANCH
```

这个系统确保了 Builder 模式的类型安全性和正确性，同时提供了自动化的验证机制。
