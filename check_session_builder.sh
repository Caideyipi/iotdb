#!/bin/bash

# 持续集成中的Builder覆盖检查脚本
# 这个脚本应该在CI/CD流水线中运行，确保Builder覆盖的完整性

set -e

echo "🔍 检查Builder方法覆盖的完整性..."

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# 比较SessionPool.constructNewSession()方法在父类和子类中的实现是否一致
echo "🔍 比较SessionPool.constructNewSession()方法在父类和子类中的实现是否一致..."

# 父类方法文件路径
PARENT_CLASS_FILE="$PROJECT_ROOT/iotdb-client/session/src/main/java/org/apache/iotdb/session/pool/SessionPool.java"
# 子类方法文件路径
CHILD_CLASS_FILE="$PROJECT_ROOT/timecho-session/src/main/java/com/timecho/iotdb/session/pool/SessionPool.java"

# 临时文件，用于存储提取的方法内容
PARENT_METHOD_CONTENT="/tmp/parent_method_content.txt"
CHILD_METHOD_CONTENT="/tmp/child_method_content.txt"

# 提取父类的constructNewSession方法内容
sed -n '/protected ISession constructNewSession()/,/^  }/p' "$PARENT_CLASS_FILE" > "$PARENT_METHOD_CONTENT"

# 提取子类的constructNewSession方法内容
sed -n '/protected ISession constructNewSession()/,/^  }/p' "$CHILD_CLASS_FILE" > "$CHILD_METHOD_CONTENT"

# 比较两个方法内容，忽略空白字符的差异
if diff -wB "$PARENT_METHOD_CONTENT" "$CHILD_METHOD_CONTENT" > /dev/null; then
    echo "✅ SessionPool.constructNewSession() 方法在子类和父类中的实现一致"
else
    echo "❌ SessionPool.constructNewSession() 方法在子类和父类中的实现不一致"
    echo "请确保子类中的方法实现与父类完全相同"
    # 输出差异以便查看
    diff -wB "$PARENT_METHOD_CONTENT" "$CHILD_METHOD_CONTENT"
    # 清理临时文件
    rm -f "$PARENT_METHOD_CONTENT" "$CHILD_METHOD_CONTENT"
    exit 1
fi

# 清理临时文件
rm -f "$PARENT_METHOD_CONTENT" "$CHILD_METHOD_CONTENT"