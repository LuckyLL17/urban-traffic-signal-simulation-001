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
