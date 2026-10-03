# 城市交通信号配时与拥堵传播仿真服务

纯后端 Node.js + TypeScript + Fastify 服务，默认无需 Docker、数据库或消息中间件即可运行。

## 启动

```bash
npm install
npm run dev
```

默认端口：`6302`。健康检查：`GET /health`。

## 业务闭环

- 领域状态模型与事件输入
- 可回放的时间序列/仿真流程
- 约束校验与风险门禁
- 方案版本、审计轨迹与异常处置
- 所有状态暂存于进程内，便于本机演示；可扩展为数据库、事件总线和实时流处理架构

## 增强业务闭环

- 路口走廊协调与基于车速的绿波 offset 计算
- 公交、应急车辆优先请求及乘客量/请求类型决策
- 行人过街、无障碍和最小安全过街时间校验
- 仿真后的 CO2、NOx、怠速小时估算
- 疏散场景、封闭路口和上游拥堵外溢推演

扩展接口：`/api/corridors`、`/api/priority-requests`、`/api/pedestrian-phase`、`/api/evacuation-scenarios`、`/api/simulations/:id/emissions`。

## 拥堵自动告警模块

独立模块 `src/alerts.ts`，在**不改变** `POST /api/detectors/events` 接口契约（路由、必填字段、响应形状）的前提下消费检测器事件：

- **窗口计算**：按路口 + 事件时间（`time` 字段）划分滚动窗口（默认 60s），聚合流量/占有率/速度均值；未提供的指标不参与判定。
- **异常判定**：窗口内 流量≥45、占有率≥30%、速度≤15km/h 三项中至少 2 项越限记为异常窗口。
- **告警生命周期**：连续 2 个异常窗口创建告警（`openedAt` 取首个异常窗口起点）；持续异常更新同一告警（指标、触发窗口、检测器列表）；连续 2 个正常窗口自动解除（`resolvedAt` 为恢复确认时刻）。每次拥堵 episode 生成独立告警（路口内 `sequence` 递增），开/解除写入 `/api/audit`。
- **重复投递**：客户端提供 `eventId` 时按之幂等；否则按 检测器+时间+指标 指纹去重，重投返回首次入库的事件，不重复入账、不影响窗口统计。
- **乱序/跨窗口**：事件按事件时间归入对应窗口，每次摄入后对该路口的窗口序列重算状态机，迟到事件（默认容忍 600s）可回填修正 `openedAt`；跨窗口的持续异常按连续窗口计数。
- **手工事故区分**：与时间窗重叠的手工事故（`/api/incidents`）自动关联到告警，`cause` 区分 `incident-correlated` / `unexplained-congestion`，不抑制自动告警。
- **检测器归属**：事件可带可选 `intersectionId`，或通过 `POST /api/detectors/bindings` 绑定，或按 `J-01-D1` 这类路口 id 前缀自动推断；无法归属的事件正常存储但不参与告警。

接口：

- `GET /api/alerts?status=active|resolved&intersectionId=J-01` 告警查询
- `GET /api/alerts/:id` 告警详情（含逐窗口追溯序列）
- `GET /api/alerts/config`、`PUT /api/alerts/config` 阈值与窗口参数（窗口秒数、开/解除连续窗口数、三项阈值等）
- `POST /api/detectors/bindings`、`GET /api/detectors/bindings` 检测器-路口绑定

验证（覆盖正常流量、突发拥堵、重复投递、恢复解除、乱序、多检测器同时异常、手工事故区分与接口兼容）：

```bash
npm run verify:alerts
```
