import assert from 'node:assert/strict';
import { createCongestionAlertService } from './congestionAlerts.js';

// 用较短窗口加速验证：10s 窗口，连续 2 个异常窗口创建，连续 2 个恢复窗口解除
const svc = createCongestionAlertService({
  windowSeconds: 10,
  abnormalWindowsToAlert: 2,
  recoveryWindowsToClear: 2,
  allowedLatenessSeconds: 60,
});
svc.bindIntersections(['J-01', 'J-02']);
svc.registerDetector({ detectorId: 'J-01:D1', intersectionId: 'J-01', expectedFlow: 100 });
svc.registerDetector({ detectorId: 'J-01:D2', intersectionId: 'J-01', expectedFlow: 100 });
svc.registerDetector({ detectorId: 'J-02:D1', intersectionId: 'J-02', expectedFlow: 100 });

const manualIncidents: Record<string, string[]> = {};
svc.setIncidentProvider((id) => (manualIncidents[id] ?? []).map((x) => ({ id: x })));

const T = (sec: number) => new Date(sec * 1000).toISOString(); // 1970 基准，确定性
let passed = 0;
function check(name: string, cond: boolean) {
  assert.ok(cond, name);
  passed++;
  console.log(`  ✓ ${name}`);
}

// ---------- 1. 正常流量：不创建告警 ----------
console.log('1) 正常流量');
for (let s = 0; s < 30; s += 5) {
  const r = svc.ingest({ detectorId: 'J-01:D1', time: T(s), flow: 95 + (s % 5), occupancy: 0.3, speed: 42 });
  check(`t=${s}s 正常事件处理完成`, r.outcome === 'processed' && !r.alertId);
}
check('正常流量下无告警', svc.listAlerts().length === 0);

// ---------- 2. 突发拥堵：连续 2 个异常窗口创建，后续窗口更新同一告警 ----------
console.log('2) 突发拥堵创建/更新');
// 窗口 [30,40)
svc.ingest({ detectorId: 'J-01:D1', time: T(32), flow: 30, occupancy: 0.82, speed: 9 });
const edge = svc.ingest({ detectorId: 'J-01:D1', time: T(38), flow: 28, occupancy: 0.85, speed: 8 });
check('第 1 个异常窗口不产生告警', !edge.alertId);
// 窗口 [40,50) => 连续 2 个异常窗口，创建
const open = svc.ingest({ detectorId: 'J-01:D1', time: T(41), flow: 25, occupancy: 0.88, speed: 7 });
check('第 2 个连续异常窗口创建告警', open.outcome === 'processed' && !!open.alertId);
const alertId = open.alertId!;
// 窗口 [50,60) 继续异常 => 更新同一告警而非新建
const update = svc.ingest({ detectorId: 'J-01:D1', time: T(52), flow: 26, occupancy: 0.9, speed: 6 });
check('后续异常窗口复用同一告警', update.alertId === alertId);
let alert = svc.getAlert(alertId)!;
check('告警可查询且为 active/critical', alert.status === 'active' && alert.severity === 'critical');
check('异常窗口跨度为 3 个窗口（跨窗口记录）', alert.abnormalWindowCount === 3);
check('startedAt 取首个异常窗口起点', alert.startedAt === T(30));
check('peakOccupancy/minSpeed 聚合正确', alert.peakOccupancy === 0.9 && alert.minSpeed === 6);
check('告警与手工事故区分（kind/source）', alert.kind === 'congestion' && alert.source === 'detector-auto');

// ---------- 3. 重复投递 ----------
console.log('3) 重复投递');
const dup1 = svc.ingest({ detectorId: 'J-01:D1', time: T(52), flow: 26, occupancy: 0.9, speed: 6 });
check('无 eventId 的相同内容判重', dup1.outcome === 'duplicate');
const dup2 = svc.ingest({ eventId: 'evt-100', detectorId: 'J-01:D1', time: T(53), flow: 27, occupancy: 0.9, speed: 7 });
const dup3 = svc.ingest({ eventId: 'evt-100', detectorId: 'J-01:D1', time: T(53), flow: 27, occupancy: 0.9, speed: 7 });
check('相同 eventId 重复投递判重', dup2.outcome === 'processed' && dup3.outcome === 'duplicate');
// 重复事件不改变窗口统计
const windowsAfterDup = svc.listWindows('J-01').find((w) => w.start === T(50))!;
check('重复投递不改变窗口事件计数', windowsAfterDup.eventCount === 2);

// ---------- 4. 恢复：连续 2 个恢复窗口自动解除（事件时间解除） ----------
console.log('4) 连续恢复自动解除');
svc.ingest({ detectorId: 'J-01:D1', time: T(62), flow: 90, occupancy: 0.35, speed: 38 });
const afterFirstRecovery = svc.listAlerts({ status: 'active' });
check('第 1 个恢复窗口仍保持告警', afterFirstRecovery.some((a) => a.id === alertId));
svc.ingest({ detectorId: 'J-01:D1', time: T(72), flow: 92, occupancy: 0.33, speed: 40 });
alert = svc.getAlert(alertId)!;
check('连续 2 个恢复窗口后自动解除', alert.status === 'resolved' && alert.resolutionSource === 'auto');
check('解除时间取第 2 个恢复窗口结束（事件时间）', alert.resolvedAt === T(80));
check('active 列表不再包含该告警', !svc.listAlerts({ status: 'active' }).some((a) => a.id === alertId));

// ---------- 5. 乱序事件：迟到数据把已解除告警按事件时间重开 ----------
console.log('5) 乱序事件重放');
// watermark 已到 72s；补发 62s 窗口内的异常事件，使 [60,70) 翻回异常，
// 则 50s 异常后不应解除；自动解除点落在重放区间 => 同一告警重开
const late = svc.ingest({ detectorId: 'J-01:D1', time: T(63), flow: 20, occupancy: 0.95, speed: 5 });
check('允许迟到范围内的事件被处理', late.outcome === 'processed');
alert = svc.getAlert(alertId)!;
check('乱序异常使原告警重开（非新建）', alert.status === 'active' && alert.reopenCount === 1);
// 超出 allowedLateness 的过晚事件被忽略
const tooLate = svc.ingest({ detectorId: 'J-01:D1', time: T(0), flow: 0, occupancy: 1, speed: 0 });
check('超过 watermark 迟到容忍的事件被忽略', tooLate.outcome === 'late-ignored');
// 再次恢复（70/80 窗口此时已是恢复态，重放后立刻满足条件）
svc.ingest({ detectorId: 'J-01:D1', time: T(82), flow: 95, occupancy: 0.3, speed: 41 });
check('再次连续恢复后重新解除', svc.getAlert(alertId)!.status === 'resolved');

// ---------- 6. 多个检测器同时异常 + 跨窗口延续 ----------
console.log('6) 多个检测器同时异常');
// J-02：D1 与 D2 在 [90,100)、[100,110) 同时异常
for (const s of [91, 92, 101, 102]) {
  for (const d of ['J-02:D1', 'J-02:D2']) {
    svc.ingest({ detectorId: d, time: T(s), flow: 22, occupancy: 0.8, speed: 10 });
  }
}
const j02Alerts = svc.listAlerts({ intersectionId: 'J-02', status: 'active' });
check('J-02 创建独立告警', j02Alerts.length === 1);
const j02 = j02Alerts[0];
check('告警关联全部异常检测器', j02.detectorIds.length === 2 &&
  j02.detectorIds.includes('J-02:D1') && j02.detectorIds.includes('J-02:D2'));
check('J-01 与 J-02 告警互不影响（按路口隔离）',
  svc.listAlerts({ intersectionId: 'J-01' }).every((a) => a.id !== j02.id));
// 只有一个检测器异常、另一个正常时仍满足 requiredAbnormalDetectors=1
svc.ingest({ detectorId: 'J-02:D1', time: T(111), flow: 20, occupancy: 0.9, speed: 8 });
svc.ingest({ detectorId: 'J-02:D2', time: T(111), flow: 90, occupancy: 0.3, speed: 40 });
check('单检测器异常窗口仍更新同一告警', svc.listAlerts({ intersectionId: 'J-02', status: 'active' }).length === 1);

// ---------- 7. 手工事故区分与关联 ----------
console.log('7) 手工事故区分');
manualIncidents['J-02'] = ['INC-MANUAL-1'];
// 新增异常窗口触发更新，此时关联 active 手工事故
svc.ingest({ detectorId: 'J-02:D1', time: T(121), flow: 20, occupancy: 0.9, speed: 8 });
const linked = svc.listAlerts({ intersectionId: 'J-02' })[0];
check('自动告警关联但不替代手工事故（linkedIncidentIds）',
  linked.linkedIncidentIds.includes('INC-MANUAL-1') && linked.incidentRelated === true &&
  linked.kind === 'congestion');
// 人工解除与自动解除区分
const closed = svc.closeAlert(linked.id, 'dispatcher-7', '现场疏导')!;
check('人工解除标记 resolutionSource=operator', closed.resolutionSource === 'operator');
// 再次连续异常不会重开人工关闭的告警，而是产生新的事件段
svc.ingest({ detectorId: 'J-02:D1', time: T(131), flow: 20, occupancy: 0.9, speed: 8 });
const newSeg = svc.ingest({ detectorId: 'J-02:D1', time: T(141), flow: 20, occupancy: 0.9, speed: 8 });
check('人工解除后的新异常开启新告警而非重开', !!newSeg.alertId && newSeg.alertId !== linked.id);

// ---------- 8. 无归属检测器不产生告警但可正常消费 ----------
console.log('8) 未归属检测器');
const orphan = svc.ingest({ detectorId: 'UNKNOWN-X', time: T(200), flow: 0, occupancy: 1, speed: 0 });
check('未注册/无前缀的检测器标记 unassigned 且不产生告警', orphan.outcome === 'unassigned' && !orphan.alertId);

// ---------- 9. 无数据窗口打断连续性（静默不等于恢复/异常延续） ----------
console.log('9) 静默窗口打断连续计数');
// J-01:D1：在 [200,210) 单个异常窗口，之后静默两个窗口，再 [230,240) 异常 => 不连续，不建告警
svc.ingest({ detectorId: 'J-01:D1', time: T(201), flow: 20, occupancy: 0.9, speed: 8 });
const gapped = svc.ingest({ detectorId: 'J-01:D1', time: T(231), flow: 20, occupancy: 0.9, speed: 8 });
check('被静默窗口隔开的两个异常窗口不构成连续异常', !gapped.alertId);
// 相邻两个异常窗口仍可正常创建
const adjacent = svc.ingest({ detectorId: 'J-01:D1', time: T(241), flow: 20, occupancy: 0.9, speed: 8 });
check('紧随其后的相邻异常窗口累计到 2 个后创建告警', !!adjacent.alertId);

console.log(`\n全部 ${passed} 项断言通过 ✅`);
