# TimechoDB 安全分发

## 概述

TimechoDB 安全分发面向生产环境的安全加固场景，采用内置代码混淆与最小依赖策略，形态包含“运行分发包（bin）”与“安全文件包（security-file）”。

## 分发包结构

1) 运行分发包（bin）

Zip 文件：`timechodb-{version}-bin.zip`

解压后目录：`timechodb-{version}-bin/`

```
timechodb-{version}-bin/
├── conf/           # 配置文件
├── lib/            # 混淆后的 JAR 包
├── sbin/           # 启动/运维脚本（不再提供 *safe-bin* 变体）
├── tools/          # 管理与诊断工具
├── LICENSES/       # 组件许可信息（如包含）
└── README-SECURITY.md
```

2) 安全文件包（security-file）

Zip 文件：`timechodb-{version}-security-file.zip`

解压后目录：`timechodb-{version}-security-file/`

```
timechodb-{version}-security-file/
├── README-SECURITY.md
├── obfuscation-mapping.txt
├── obfuscation-seeds.txt
├── obfuscation-usage.txt
└── obfuscation-configuration.txt
```

校验与认证（建议）：
- 发布包提供 SHA-256 校验值，请在部署前完成校验；内部构建可参考 `distribution/target/checksums.csv`。
- 如提供签名，建议使用 GPG/PGP 验证发布包的完整性与来源可信性。

## 安全特性

- 代码混淆保护（ProGuard 7.7.0）：对核心实现进行强化混淆，保留公共 API 兼容性。
- 依赖精简与原生反射：减少外部依赖面，优先采用 JVM 原生能力。
- 可审计与可运维：支持审计日志与运行期诊断，便于合规与问题定位。

说明：构建阶段会在 `distribution/target/` 生成 `obfuscation-*.txt`（mapping/seeds/usage/configuration）等文件，并被打包至 `timechodb-{version}-security-file.zip`，仅用于开发/排障；请勿在生产环境中分发或存放。

## 部署与加固建议

1) 账号与权限
- 使用专用的最小权限服务账号运行，避免 root。
- 严控目录权限（示例）：`conf/` 600、`lib/` 755、日志目录仅服务账号可写。

2) 网络与通信
- 在内网与零信任边界下部署，限制不必要的入站/出站访问。
- 建议启用 TLS/双向认证（将证书与密钥置于受控路径，并限制读权限）。

3) 配置与审计
- 开启审计与安全限流，示例：
  - `enable_audit_log=true`
  - `max_connection_for_internal_service=50`
  - `connection_timeout_in_ms=20000`
- 避免将密钥/令牌明文写入配置或日志。

4) 运行环境
- 使用已修补的 LTS 操作系统与 JDK 17+。
- 持续应用安全更新与配置基线（如 CIS 基线）。

## 部署步骤

1. 解压
```
mkdir -p /opt/timechodb-secure
cd /opt/timechodb-secure
unzip timechodb-{version}-bin.zip
# 如需调试（非生产）：可在受控环境中解压安全文件包
# unzip timechodb-{version}-security-file.zip
```

2. 配置安全参数
```
vim conf/iotdb-system.properties
# 推荐开启：审计、连接与超时控制、必要的 TLS 参数
```

3. 启动与检查
```
# 启动顺序
./sbin/start-confignode.sh
./sbin/start-datanode.sh

# 健康检查
./sbin/check-iotdb.sh
```

## 构建说明（面向开发者）

构建 distribution 模块将产出运行分发包与安全文件包：

```
mvn clean package -pl distribution
# 如需加速：-DskipTests
```

构建产物（示例）：
```
distribution/target/
├── timechodb-{version}-bin.zip           # 运行分发包（bin）
├── timechodb-{version}-security-file.zip # 安全文件包（mapping/seeds 等）
├── obfuscation-mapping.txt               # 构建期文件（已同时包含在 security-file 包内）
├── obfuscation-seeds.txt
├── obfuscation-usage.txt
└── obfuscation-configuration.txt
```

堆栈还原（如需）：
```
# 推荐使用安全文件包中的 mapping 文件与 ProGuard ReTrace 结合还原
unzip timechodb-{version}-security-file.zip
java -jar proguard-7.7.0/lib/retrace.jar \
  timechodb-{version}-security-file/obfuscation-mapping.txt < stacktrace.txt
```

## 故障排查（简要）

1) 启动失败
- 查看日志：`logs/log_datanode_all.log`、`logs/log_confignode_all.log`
- 常见原因：JDK < 17、端口冲突、内存不足、配置错误。

2) 连接异常
- 端口监听：Linux `ss -tln | grep 6667` 或 `netstat -tln | grep 6667`
- 网络策略/防火墙：确认放行必要端口与网段。

3) 反射/混淆问题
- 使用 `distribution/target/obfuscation-usage.txt` 定位被移除项，按需调整反射用法或混淆配置（开发场景）。

## 兼容性与系统要求

- JDK：17 或更高版本
- OS：Linux（推荐）、macOS、Windows
- 内存：≥4GB（推荐 8GB+）
- 与现有 TimechoDB 客户端、连接器与工具保持协议/接口层面兼容

## 安全事件与升级

- 建议优先采用包含安全修复的补丁版本，定期评估 CVE 影响并升级。
- 如发现潜在安全问题，请通过正式渠道私下披露，避免公开扩散。

联系方式（示例）：
- 安全问题报告：security@timecho.com
- 技术支持：support@timecho.com

---

免责声明：
- 分发包外的构建期调试文件（如 mapping/seeds）不应在生产环境存放或分发。
- 请在合规前提下部署并妥善保护密钥与证书。

文档更新时间：2025-10-24
</content>
</invoke>