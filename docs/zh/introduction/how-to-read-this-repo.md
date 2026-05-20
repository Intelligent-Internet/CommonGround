# 如何阅读本仓库

阅读实现细节前，先分清发布入口、当前真源与历史材料。

根目录 [README_CN](../../../README_CN.md) 是 v3r1 preview 发布的中文项目入口。Introduction 文档会更细地解释同一套结构。带编号的 foundation documents 仍是当前设计权威。

## 第一次接触

建议顺序：

1. [根 README_CN](../../../README_CN.md)
2. [what-is-commonground.md](what-is-commonground.md)
3. [../01-constitution.md](../01-constitution.md)
4. [../02-three-plane-model.md](../02-three-plane-model.md)
5. [../03-design-review-principles.md](../03-design-review-principles.md)
6. [../cg-history.md](../cg-history.md)
7. [../guides/open-source-quickstart.md](../guides/open-source-quickstart.md)

## 准备改实现

先读长期基础文档：

1. `01-constitution.md`
2. `02-three-plane-model.md`
3. `03-design-review-principles.md`

然后直接看代码和测试。背景材料可以解释上下文，但当前 contract 以 active docs、implementation 与 executable tests 为准。

## 发布与运行接口

了解发布状态和公开 setup，请读：

1. [../release-notes.md](../release-notes.md)
2. [../guides/open-source-quickstart.md](../guides/open-source-quickstart.md)
3. [../guides/agent-integration-scenarios.md](../guides/agent-integration-scenarios.md)
4. [../reference/cli.md](../reference/cli.md)
5. [../reference/http.md](../reference/http.md)

## 历史材料规则

历史材料可以解释一个设计为什么变化，但不能覆盖当前基础文档或 executable tests。

v3r1 对历史的读法是：CommonGround 从较强 orchestration 假设，转向小而合宪的 Ledger Kernel；先建立 public facts、durable work boundaries、explicit authority 与 causal lineage，再把更高层的 Memory Abstraction 与产品界面建立在这个基础之上。
