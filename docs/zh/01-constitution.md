# 宪法

## 0. 定位

本文是 CommonGround 内核的最高约束文档。

本文只定义系统本体、权力边界、默认法律效力与不可破坏的设计公理。它不记录历史迁移，不冻结实现形态，不描述具体集成流程，也不把临时实现技巧写成长期本体。

如果后续文档、实现或接口与本文冲突，应优先回到本文的公理重新审视设计；如果实现持续证明本文公理不成立，则应先修宪，再扩展实现。

## 0.0 第一性原理

CommonGround 严格遵循这一第一性原理：

> Assume nothing beyond what constraints demand.

也就是说，系统设计与制宪本身，都不应假设超出最基本约束所要求的内容。

这一原则不仅约束系统设计，也约束宪法如何书写、解释与修订。宪法的每一次新增、解释与修订，都应被视为一次 minimum update：只在新约束、持续反例或长期冲突逼迫时，才向前多写一步，而不把局部实现偏好、临时 workaround 或上层策略空间提前冻结为根本法。

据此，本文遵循以下制宪原则：

1. 只有系统成立与一致运行所不可缺之约束，才有资格入宪。
2. 宪法只定义最小本体、权力边界、默认法律效力与不可破坏关系；实现、流程、部署、策略与临时技巧不得冒充本体。
3. 凡主体、权力、归属、责任、失效条件与法律效力，必须显式建模；未被显式赋予之效力，不得自动成立。
4. 条文之间不得互相抵触；每一条都必须可被下位设计、实现与审查直接验证。
5. 在不破坏上位约束之前，宪法不得过早冻结可变实现形态；修宪必须由新约束、持续反例或长期冲突驱动；一切临时例外必须通过修正案显式声明。

以下所有宪法条款，本质上都是为了防御架构演进中的过度假设。

## 0.1 宪法修正案机制

宪法修正案是对本文约束的显式、可审计例外。

修正案可以在明确范围内临时放宽一些限制，也可以为了 PoC / MVP 验证临时加入额外假设。

修正案不得静默改变系统长期本体，也不得把临时实现技巧升级为默认公理。

任何修正案都必须写明适用版本或阶段、被放宽或新增的条款、临时假设、风险、退出条件，以及关联 issue 或决策记录。

在修正案声明的范围与期限内，修正案优先于本文对应条款；超出该范围后，本文恢复为默认约束。

当前没有生效中的宪法修正案。已退休修正案仅作历史审计记录，不优先于本文。

## 1. 内核定位

本编规定 CommonGround 作为内核的法定职责与边界。

### 1.1 CommonGround 是公共事实与因果协调内核

CommonGround Kernel 的法定职责，是为多 Agent 协作建立公共事实底座与可恢复的因果协调基础。

它负责登记可参与协作的正式主体，建立 durable work boundary，保存 Turn-owned 语义与关键因果事实，并为 pull-first 的恢复、观察与审计提供共同依据。

它不是 Agent 的大脑，不是业务编排器，不是部署系统，也不是消息总线。它不因此自动成为 Agent 私有记忆、业务策略或 runtime continuity 的默认承载层。

它不应夹带特定业务编排、拓扑偏好或自动流转假设。在本体层面，它提供的共同基础结构，是 Agent、Turn 与完整语义。

它不参与推理，不越权替 Agent 决策。它应保持“无形之形”，从而承载不同形态的 Agent 协作。

## 2. 法定本体

本编规定 CommonGround 的法定本体及其基本归属关系。

### 2.1 Agent 是稳定逻辑执行主体

Agent 是 CommonGround 中可被委托、可承担责任、可产生语义事实的正式执行主体。

Agent 的成立不取决于其底层由 AI、人类、服务、脚本、组织流程或混合系统承载。任何节点若要作为可被委托、可被授权、可被追责、可产生正式语义事实的主体进入 CommonGround，必须按 Agent 平面同构建模；人类节点不构成本体例外。

为避免产品、接口或部署语境中的歧义，下位文档可以将某些非对话式、非 prompt-facing、由确定性服务或受控后台系统承载的 Agent 称为 Service，例如 Admin Service。Service 不是 Agent 之外的新本体，也不引入新的主体平面；凡 Service 需要作为可被授权、可被追责、可产生 machine-authoritative effect 的主体进入 CommonGround，仍必须按 Agent 平面同构建模，并服从相同的控制边界、授权、审计与失效规则。

Agent 的身份连续性依赖其逻辑身份，而不依赖某个特定宿主、进程、容器、会话、工作目录或网络端点。

部署形态可以变化，逻辑身份仍可保持连续；但物理可达性、在线状态或最近活动，并不自动等同于执行权。

### 2.2 Turn 是最小 durable work boundary

Turn 是 CommonGround 中最小的 durable work boundary，也是一次工作、一次委托、一次交互或一次恢复闭环的法定边界。

每个 Turn 必须具有显式来源、后续控制权来源、生命周期状态、终局状态，以及与之归属的完整语义。

Turn 可以是长时的、异步的、可暂停的，也可以经由派生形成新的工作边界；它不等于同步 reply，也不因发起行为而自动把后续控制权授予发起者。

### 2.3 完整语义归属于 Turn

完整语义是某个 Turn 正式拥有的语义事实边界。

它承载该 Turn 的输入、当前 Turn 主动吸收的观察、过程中的正式记录、最终交付物与终止原因，使该 Turn 的工作过程与结果可被恢复、审计与追责。

完整语义必须按 Turn 归属，而不是按 Agent、宿主、视图、内容层或其他外部对象归属。

### 2.4 Turn 语义内容的二重性

Turn 承载的语义材料可以按来源分为两类，两类具有不同的 durable 保证义务：

跨 Agent 边界的交互 I/O（包括 root input、dispatch input、child bootstrap、child final deliverable、parent 对 child 结果的最终吸收、terminal final payload 与 request/reply 语义中的关键 I/O）必须被强制 durable 化，作为最小可恢复事实。这些材料同时承载责任、因果、恢复与审计，不属于 runtime 或 Agent 私有层的 transient 缓存。

Agent 在 Turn 内的公开工作过程记录（包括关键决策、主动吸收的观察、handoff 理由、验证证据、artifact 引用与失败原因）应以 best effort 鼓励上报到当前 Turn，但不默认成为 Turn completion 的必要前置条件。过程记录的缺失不应阻塞 deliverable 产生，也不应使已完成的工作自动失效。

本条不改变 Agent 私有内态（包括 LLM token-level reasoning、chain-of-thought、runtime-local scratchpad 与 session continuity substrate）默认不属于 CommonGround truth 的法律效力。

## 3. 控制秩序

本编规定控制权、执行权、派生关系与判断边界的成立方式。

### 3.1 发起不当然控制；控制权来源必须显式

发起一个 Turn 的主体，可以促成该 Turn 出生，但不当然取得该 Turn 出生后的推进权、停止权、恢复权或写语义权。

Turn 出生后，任何后续控制行为都必须回到该 Turn 的法定控制边界上判断，而不能由“谁先发起”自动推导。

Turn 的出生边界必须显式确立其后续控制权如何成立，或显式确立其后续解析机制。

Turn 出生后，不得因为实现偶然性、观察信号、历史可见性、调用位置或未建模的默认规则而隐式改派控制权。

若未来需要更复杂的委托、转交、仲裁或解析机制，它们必须作为新的显式制度进入系统，而不能靠实现惯例偷偷成立。

### 3.2 执行权必须临时、可验证、可失效

推进 Turn lifecycle 或 authoritative 地写入 Turn 语义的权力，必须由当前有效的执行授权保护。

该授权必须可验证、可失效、失效后不可复用，并与具体 Turn 的控制边界绑定。

执行授权失效并不当然证明整个 Agent 已死亡；它只意味着该主体不能继续以旧授权推进该 Turn。

### 3.3 能力不等于授权；能力语义不得被 kernel 冒名实现

能力描述某个 Agent 适合承担什么工作；授权描述某个主体被允许控制什么动作。

具备某项能力，不等于可以控制任意 Turn；能力声明、可用性、健康、接活意愿与其他观察信号，都不能替代正式授权。

若某项能力被宣称属于某 Agent，则该能力的业务语义不得由 CommonGround kernel 冒名产生。Kernel 可以保存 truth、验证边界并承载 Agent 发布的数据，但不得伪装成某个 Agent 自行回答其应由该 Agent 或其受约束集成主体闭环提供的能力语义。

### 3.4 Child 派生必须显式；child 不自动改写 parent

Turn 可以派生 child Turn，但派生关系必须显式记录为因果关系，并能够被后续 durable 地观察。

child 拥有自己的工作边界、生命周期、控制边界与完整语义。child 的完成不等于 parent 自动推进，child 也不得直接改写 parent 的完整语义或生命周期。

parent 是否吸收 child 的结果，必须由 parent 控制边界内的合法主体在观察事实后主动决定。

### 3.5 Kernel 记录事实，不代作策略判断

Kernel 的职责是稳定保存公共事实、关键因果关系与控制边界的显式状态。

Kernel 可以记录某个 Turn 已终结、某项授权已失效、某个 child 已完成、某个事实已出现。

Kernel 不代替上层主体判断某个结果是否满足业务条件、某个事实是否应被吸收、某个 parent 是否应恢复，或某个复杂协作策略是否已经成功。

### 3.6 人类节点不构成控制例外

来自人类、operator、界面、群聊、外部平台账号或受信入口的行为，若要推进 Turn lifecycle、authoritative 地写入 Turn 语义、改变控制边界，或产生其他 machine-authoritative effect，必须与其他主体行为一样，经由显式控制边界成立，并归属到可审计的正式执行主体。

人类来源、界面位置、平台账号、群聊上下文或受信入口本身，不自动授予控制权，也不自动产生高于其他正式主体的法律效力。

### 3.7 Kernel 不得解释 record payload

Kernel 可以稳定保存 record envelope（包括归属 Turn、actor、claim、写入顺序、内容引用与时间），但不得解释 envelope 以外的业务字段。

record payload 的语义——包括但不限于某个字段是否表示"批准""选择""证据成立"或"值得复用"——必须由消费者（当前 Agent、其他 Agent、Human、LLM-enabled Agent、外部系统、projection 或 Knowledge Team）在 Kernel 之外自行解释。Kernel 不应因为 payload 中出现了特定字段名或值而自动产生 machine-authoritative effect。

本条不禁止 Kernel 对 record envelope 做 schema 验证、索引或 projection；它禁止的是 Kernel 把 payload 内部的业务语义当作自身判断依据。

## 4. 法律效力秩序

本编规定系统中各类材料的默认法律效力，以及哪些效力默认不成立。

### 4.1 Projection 与 Push 的法律地位

时间线、状态面板、树形关系、等待对象、聚合视图与其他阅读视图，都可以存在；但它们是 projection，不是 kernel truth。

Projection 可以帮助理解事实，但不得反向定义事实。

同样，任何通知、doorbell、webhook、总线或其他 push 机制，都只能作为加速层，提示“有事实值得读取”；它们不能成为事实本身，也不能成为系统正确性的唯一前提。

### 4.2 旧 Turn 历史的默认法律效力

旧 Turn 的 semantic、context、feed、process record、中间 deliverable、观察摘要与诊断材料，默认提供 inspect、audit、reference 与共享观察的法律效力。

它们可以被当前主体读取、引用、吸收，用于理解上下文、恢复对当前 Turn 的判断或支持后续审计。

但除非另有显式建模，这些历史材料不自动产生 Agent 私有状态恢复、身份连续性证明、授权连续性证明、contract effect 或其他 machine-authoritative effect。

### 4.3 CommonGround 不被推定为 Agent-native memory 的默认真源

CommonGround 承载的是跨主体协作所需的公共事实，而不是 Agent 私有内在状态的默认真源。

面向用户的 session continuity、内部思考、长期记忆、runtime-local 工作内态，以及 reboot 后继续工作的私有 continuity substrate，不应被默认建模为 CommonGround truth。

这条约束不禁止 Agent 或 runtime 读取旧 Turn 历史作为外部参考；它限制的是 CommonGround 的默认真源地位，而不是上层策略空间。

### 4.4 Machine-authoritative effect 必须显式建模

若某项记录需要对当前 Turn、当前 contract、当前控制边界或其他正式关系产生 machine-authoritative effect，这种效力必须被显式建模。

它不得仅因被持久化、被纳入 semantic / context、可被程序化提取、能从历史中找到，或恰好处于某个承载位置，就被隐式推导出 authority、continuity、identity、authorization 或 contract effect。

显式建模约束的是法律效力的成立方式，而不是预先冻结唯一实现形态。

## 5. 释义与审查入口

本文只承载正式法条。

三平面的解释性展开见 [02-three-plane-model.md](02-three-plane-model.md)；设计审查问题单见 [03-design-review-principles.md](03-design-review-principles.md)。
