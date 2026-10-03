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

## 拥堵告警模块

在不改变 `/api/detectors/events` 接口的前提下，检测器事件入库后旁路进入独立告警模块（`src/congestionAlerts.ts`）：

- 按「路口 + 事件时间」滚动窗口（默认 60s）聚合流量/占有率/速度；窗口内检测器异常比例与异常检测器数量同时达标才判窗口异常。
- 连续 2 个异常窗口创建告警，之后每个异常窗口更新同一告警；连续 2 个恢复窗口按事件时间自动解除。
- 重复投递：`eventId` 或 `(检测器, 时间, 指标)` 内容指纹去重。
- 乱序事件：在 watermark 允许迟到范围内（默认 120s）按事件时间落窗并重放状态机，自动解除的告警可被迟到异常重开；超时迟到事件只存档。
- 跨窗口：告警记录全部异常窗口及峰值占有率/最低速度；无数据窗口打断连续计数。
- 与手工事故 `/api/incidents` 独立存储，仅通过 `linkedIncidentIds`/`incidentRelated` 关联，不互相转换；人工解除后不再自动重开。

接口：

- `POST /api/detectors`：注册检测器与路口、期望流量（也支持 `J-01:D3` 前缀自动归属）
- `GET /api/congestion/alerts?intersectionId=&status=`、`GET /api/congestion/alerts/:id`
- `POST /api/congestion/alerts/:id/acknowledge`、`POST /api/congestion/alerts/:id/resolve`
- `GET /api/congestion/windows?intersectionId=`：窗口级诊断
- `GET/PUT /api/congestion/config`：查看/调整窗口长度、连续次数与各阈值

验证正常流量、突发拥堵、恢复、多检测器同时异常、重复/乱序/跨窗口及手工事故区分：

```bash
npm run verify:congestion
```
