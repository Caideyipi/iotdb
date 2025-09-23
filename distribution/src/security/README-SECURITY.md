# TimechoDB 安全分发版本

## 概述

TimechoDB 安全分发版本是 TimechoDB 的企业级安全增强版本，专为对数据安全和代码保护有严格要求的生产环境设计。

## 安全特性

### 1. 代码混淆保护
- **ProGuard 7.7.0**: 使用业界领先的代码混淆工具
- **深度混淆**: 对核心业务逻辑、算法实现和敏感代码进行全面混淆
- **自定义字典**: 使用专业技术词汇字典，混淆后的代码仍具可读性但无业务意义
- **分层保护**: 针对不同模块采用差异化的混淆策略

### 2. 优化的依赖管理
- **原生反射**: 移除第三方反射库依赖，使用 JVM 原生反射机制
- **精简依赖**: 减少外部依赖，降低安全风险
- **兼容性保证**: 确保混淆后的代码与现有系统完全兼容

## 文件说明

### 分发包结构

TimechoDB 安全版本包含两个独立的分发包：

#### 1. 主要安全分发包
```
timechodb-{version}-security-bin.zip
├── conf/           # 配置文件
├── lib/            # 混淆后的 JAR 包
├── sbin/           # 启动脚本
└── tools/          # 管理工具
```

#### 2. 安全文件包（独立分发）
```
timechodb-{version}-security-file.zip
├── README-SECURITY.md              # 安全说明文档
├── obfuscation-mapping.txt         # 混淆映射表
├── obfuscation-seeds.txt           # 保持原名的类/方法
├── obfuscation-usage.txt           # ProGuard 使用报告
└── obfuscation-configuration.txt   # 混淆配置输出
```

**重要说明**: 安全文件包与主分发包分离，确保敏感的混淆信息不会意外暴露在生产环境中。

### 混淆文件详解

#### `obfuscation-mapping.txt`
- **用途**: 混淆前后的类名、方法名、字段名映射关系
- **重要性**: ⚠️ **极其敏感** - 包含完整的反混淆信息
- **用法**: 用于调试和堆栈跟踪分析

#### `obfuscation-seeds.txt` 
- **用途**: 记录保持原始名称的类和方法
- **内容**: API 接口、公共类、注解等必须保持原名的元素

#### `obfuscation-usage.txt`
- **用途**: ProGuard 处理报告
- **内容**: 死代码移除、优化统计等信息

#### `obfuscation-configuration.txt`
- **用途**: 实际使用的混淆配置
- **内容**: 完整的 ProGuard 配置参数

## 安全注意事项

### 🔒 映射文件安全管理
1. **分离式安全设计**
   - 安全文件包 (`timechodb-{version}-security-file.zip`) 与主分发包分离
   - 生产环境只需部署主分发包，避免敏感信息暴露
   - `obfuscation-mapping.txt` 等敏感文件仅在独立的安全文件包中

2. **生产环境隔离**
   - **禁止在生产环境部署安全文件包**
   - 安全文件包仅用于开发调试和故障排查
   - 建议将安全文件包存储在安全的独立位置

3. **版本管理策略**
   - 为每个发布版本单独保管安全文件包
   - 建立安全文件包的备份和归档机制
   - 实施严格的访问控制和审计

3. **调试和故障排查**
   - 使用 ProGuard ReTrace 工具进行堆栈跟踪还原
   - 开发环境可保留映射文件用于调试
   - 建立标准的故障排查流程

### 🛡️ 部署安全建议
1. **网络安全**
   - 在防火墙保护的内网环境部署
   - 限制不必要的网络访问
   - 启用安全的通信协议

2. **系统安全**
   - 使用专用的服务账户运行 TimechoDB
   - 设置适当的文件系统权限
   - 定期更新操作系统和 JVM

3. **监控和审计**
   - 启用详细的审计日志
   - 监控异常的系统行为
   - 建立入侵检测机制

## 构建和部署

### 构建安全版本

#### 完整构建命令
```bash
# 构建完整的安全分发包（包含主分发包和安全文件包）
mvn clean package -pl distribution

# 构建时跳过测试（加速构建）
mvn clean package -pl distribution -DskipTests

# 构建整个项目（包含所有模块）
mvn clean package
```

**说明**: 构建 distribution 模块时会自动进行安全版本的打包，无需额外的 profile 参数。

#### 构建输出
```
distribution/target/
├── timechodb-{version}-security-bin.zip  # 主要安全分发包
├── timechodb-{version}-security-file.zip # 安全文件包
├── obfuscation-mapping.txt               # 混淆映射（敏感文件）
├── obfuscation-seeds.txt                 # 种子文件
├── obfuscation-usage.txt                 # 使用报告
└── obfuscation-configuration.txt         # 配置输出
```

### 部署安全版本

#### 1. 准备部署环境
```bash
# 创建部署目录
mkdir -p /opt/timechodb-secure
cd /opt/timechodb-secure

# 解压主要安全分发包（生产环境）
unzip timechodb-{version}-security-bin.zip

# 解压安全文件包（仅开发/调试环境需要）
# ⚠️ 注意：生产环境不要解压此包，包含敏感信息
# unzip timechodb-{version}-security-file.zip
```

#### 2. 配置安全参数
```bash
# 编辑配置文件
vim conf/iotdb-system.properties

# 建议的安全配置
enable_audit_log=true
max_connection_for_internal_service=50
connection_timeout_in_ms=20000
```

#### 3. 启动服务
```bash
# 启动 ConfigNode
./sbin/start-confignode.sh

# 启动 DataNode  
./sbin/start-datanode.sh

# 检查服务状态
./sbin/check-iotdb.sh
```

## 集成测试

TimechoDB 提供了专门的安全分发包集成测试框架：

### 测试配置文件
- `integration-test/pom.xml` - 安全测试配置
- 支持的测试 Profiles:
  - `SecuritySimpleIT` - 简单单机测试
  - `SecurityMultiClusterIT` - 多集群测试  
  - `SecurityTableSimpleIT` - 表模式简单测试
  - `SecurityTableClusterIT` - 表模式集群测试

### 运行安全测试
```bash
# 运行简单安全测试
mvn verify -P SecuritySimpleIT -pl integration-test

# 运行表模式安全测试
mvn verify -P SecurityTableSimpleIT -pl integration-test

# 并行运行多个测试
mvn verify -P SecuritySimpleIT,SecurityTableSimpleIT -pl integration-test
```

## 技术规格

### 系统要求
- **JDK**: 17 或更高版本
- **内存**: 最小 4GB，推荐 8GB 或更多
- **存储**: 最小 10GB 可用空间
- **操作系统**: Linux (推荐)、Windows、macOS

### 性能特性
- **启动时间**: 由于混淆优化，启动时间与标准版本相当
- **运行性能**: 混淆不影响运行时性能
- **内存占用**: 与标准版本基本一致
- **兼容性**: 完全兼容现有的 TimechoDB 客户端和工具

### 混淆配置
- **配置文件**: `distribution/src/security/proguard.conf`
- **字典文件**: 内置技术词汇混淆字典
- **保护级别**: 类名、方法名、字段名全面混淆
- **API 保护**: 公共 API 和接口保持原名以确保兼容性

## 故障排查

### 常见问题

#### 1. 启动失败
**症状**: 服务无法正常启动
```bash
# 检查日志
tail -f logs/log_datanode_all.log

# 常见原因
- JDK 版本不兼容 (需要 JDK 17+)
- 内存不足
- 端口冲突
- 配置文件错误
```

#### 2. 连接异常  
**症状**: 客户端无法连接
```bash
# 检查端口状态
netstat -tlnp | grep 6667

# 检查防火墙设置
iptables -L | grep 6667
```

#### 3. 混淆相关问题
**症状**: 反射调用失败或类找不到
```bash
# 检查是否使用了不兼容的反射调用
# 从安全文件包中查看 obfuscation-usage.txt 确认被移除的代码
unzip timechodb-{version}-security-file.zip
cat obfuscation-usage.txt
```

### 堆栈跟踪还原

当遇到异常时，使用 ProGuard ReTrace 工具还原混淆的堆栈跟踪：

```bash
# 下载 ProGuard ReTrace
wget https://github.com/Guardsquare/proguard/releases/download/v7.7.0/proguard-7.7.0.zip
unzip proguard-7.7.0.zip

# 解压安全文件包获取映射文件
unzip timechodb-{version}-security-file.zip

# 还原堆栈跟踪
java -jar proguard-7.7.0/lib/retrace.jar obfuscation-mapping.txt < stacktrace.txt
```

### 日志配置

安全版本支持详细的审计日志：

```xml
<!-- logback.xml 配置示例 -->
<configuration>
    <appender name="AUDIT" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/audit.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/audit.%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>30</maxHistory>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <logger name="org.apache.iotdb.audit" level="INFO" additivity="false">
        <appender-ref ref="AUDIT"/>
    </logger>
</configuration>
```

## 版本兼容性

### API 兼容性
- ✅ **JDBC 接口**: 完全兼容
- ✅ **Session API**: 完全兼容  
- ✅ **REST API**: 完全兼容
- ✅ **CLI 工具**: 完全兼容
- ✅ **配置文件**: 完全兼容

### 客户端兼容性
- ✅ **Java 客户端**: 所有版本
- ✅ **Python 客户端**: 所有版本
- ✅ **C++ 客户端**: 所有版本
- ✅ **Go 客户端**: 所有版本

### 第三方工具
- ✅ **Grafana**: 完全支持
- ✅ **Zeppelin**: 完全支持
- ✅ **Flink 连接器**: 完全支持
- ✅ **Spark 连接器**: 完全支持

## 技术支持

### 联系方式
- **技术支持邮箱**: support@timecho.com
- **安全问题报告**: security@timecho.com
- **技术文档**: https://docs.timecho.com/security
- **社区论坛**: https://community.timecho.com

### 支持范围
- ✅ 安装部署指导
- ✅ 配置优化建议  
- ✅ 性能调优支持
- ✅ 故障排查协助
- ✅ 安全配置咨询
- ✅ 版本升级指导

### 企业服务
- 🏢 **现场技术支持**
- 🏢 **定制化安全方案**  
- 🏢 **专业培训服务**
- 🏢 **SLA 服务保障**

---

## 免责声明

⚠️ **重要提醒**: 
1. 请妥善保管 `obfuscation-mapping.txt` 等敏感文件，避免在公开环境暴露
2. 安全版本仅授权给具有有效许可证的企业用户使用
3. 任何安全问题请及时联系技术支持团队

---

*版本: TimechoDB Security Edition*  
*更新时间: 2024年9月23日*  
*文档版本: 1.2*
</content>
</invoke>