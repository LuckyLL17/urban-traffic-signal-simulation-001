import { randomUUID } from 'node:crypto';

/**
 * 独立的拥堵告警模块（与手工事故 incidents 解耦）。
 *
 * 设计要点：
 * - 按「路口 + 事件时间」做滚动时间窗口（tumbling window），与事件到达顺序无关；
 * - 单个检测器事件在流量骤降 / 高占有率 / 低速度任一维度越界即记为异常；
 * - 路口窗口内异常检测器比例 / 数量达到阈值才判该窗口异常；
 * - 连续 abnormalWindowsToAlert 个异常窗口 => 创建（或复用/重开）告警，
 *   之后每个异常窗口更新告警；连续 recoveryWindowsToClear 个恢复窗口 => 自动解除；
 * - 重复投递：eventId 或 (detectorId,time,指标) 指纹去重，窗口统计只计一次；
 * - 乱序事件：按事件时间落窗，并重算受影响窗口之后的状态机；
 *   超过 watermark - allowedLateness 的迟到事件只存档不影响状态；
 * - 跨窗口：告警按事件时间跨度记录全部异常窗口；恢复时间也取事件时间窗口边界；
 * - 手工事故：告警独立存储（kind=congestion / source=detector-auto），
 *   仅在创建与更新时关联当时该路口的 active 手工事故，不互相转换。
 */

export interface CongestionConfig {
  windowSeconds: number;              // 窗口长度
  abnormalWindowsToAlert: number;     // 连续异常窗口数达到后创建告警
  recoveryWindowsToClear: number;     // 连续恢复窗口数达到后自动解除
  occupancyThreshold: number;         // 占有率 >= 该值判异常（0~1）
  speedThresholdKmh: number;          // 速度(km/h) >0 且 <= 该值判异常
  flowDropRatio: number;              // 流量相对基准下降比例（flow <= base*(1-ratio) 判异常）
  detectorAnomalyRatio: number;       // 单检测器在窗口内异常事件占比阈值
  requiredAbnormalDetectors: number;  // 路口窗口异常所需的异常检测器数量
  allowedLatenessSeconds: number;     // 允许乱序迟到的时间（相对 watermark）
}

export const DEFAULT_CONFIG: CongestionConfig = {
  windowSeconds: 60,
  abnormalWindowsToAlert: 2,
  recoveryWindowsToClear: 2,
  occupancyThreshold: 0.6,
  speedThresholdKmh: 18,
  flowDropRatio: 0.5,
  detectorAnomalyRatio: 0.5,
  requiredAbnormalDetectors: 1,
  allowedLatenessSeconds: 120,
};

export interface IngestEvent {
  eventId?: string;
  detectorId: string;
  intersectionId?: string; // 可选：显式指定路口；否则按注册表 / 编号前缀推断
  time?: string;
  timeMs?: number;
  flow: number;
  occupancy: number;
  speed: number;
}

export interface IngestResult {
  outcome: 'processed' | 'duplicate' | 'late-ignored' | 'unassigned';
  detectorId: string;
  intersectionId?: string;
  windowStart?: string;
  windowStartMs?: number;
  eventId: string;
  alertId?: string | null;
  alertStatus?: 'active' | 'resolved';
}

interface DetectorRegistration {
  detectorId: string;
  intersectionId: string;
  expectedFlow?: number;
  registeredAt: string;
}

interface DetectorWindowStats {
  n: number;
  abnormalCount: number;
  occSum: number;
  occMax: number;
  speedSum: number; // 仅累计上报了速度(>0)的事件
  speedN: number;
  speedMin: number;
  flowSum: number;
  flowMin: number;
}

interface WindowState {
  intersectionId: string;
  startMs: number;
  endMs: number;
  count: number;
  detectors: Map<string, DetectorWindowStats>;
  abnormal: boolean;
  abnormalDetectors: string[];
  firstEventMs: number;
  lastEventMs: number;
}

export interface AlertWindow {
  start: string;
  end: string;
  detectorIds: string[];
  eventCount: number;
  peakOccupancy: number | null;
  minSpeed: number | null;
  avgFlow: number | null;
}

interface HistoryEntry {
  type: 'opened' | 'updated' | 'reopened' | 'resolved' | 'acknowledged' | 'closed';
  at: string;
  windowStart?: string;
  detail?: string;
}

export interface CongestionAlert {
  id: string;
  kind: 'congestion';
  source: 'detector-auto';
  intersectionId: string;
  status: 'active' | 'resolved';
  severity: 'warning' | 'critical';
  startedAt: string;           // 首个异常窗口起点（事件时间）
  firstEventAt: string;        // 首个异常事件时间
  lastEventAt: string;         // 最近一次异常事件时间
  resolvedAt?: string;         // 自动解除时取第 R 个恢复窗口的结束（事件时间）
  resolutionSource?: 'auto' | 'operator';
  resolutionWindowStartMs?: number;
  acknowledged?: { at: string; by: string; note?: string };
  detectorIds: string[];
  peakOccupancy: number | null;
  minSpeed: number | null;
  incidentRelated: boolean;
  linkedIncidentIds: string[];
  abnormalWindowCount: number;
  updates: number;
  reopenCount: number;
  abnormalWindows: AlertWindow[];
  history: HistoryEntry[];
  createdAt: string;
}

interface IntersectionContext {
  windows: Map<number, WindowState>;
  watermarkMs: number; // 该路口见过的最大事件时间
}

const r2 = (n: number) => Number(n.toFixed(2));
const iso = (ms: number) => new Date(ms).toISOString();
const nowIso = () => new Date().toISOString();

export function createCongestionAlertService(initialConfig: Partial<CongestionConfig> = {}) {
  const config: CongestionConfig = { ...DEFAULT_CONFIG, ...initialConfig };

  const registrations = new Map<string, DetectorRegistration>();
  const contexts = new Map<string, IntersectionContext>();
  const alerts: CongestionAlert[] = [];
  const flowSamples = new Map<string, number[]>();
  const seenEvents = new Map<string, string>(); // 指纹 -> eventId
  const MAX_SEEN = 10000;
  const MAX_SAMPLES = 300;

  let incidentProvider: ((intersectionId: string) => Array<{ id: string }>) | null = null;

  function setIncidentProvider(fn: (intersectionId: string) => Array<{ id: string }>) {
    incidentProvider = fn;
  }

  function getConfig(): CongestionConfig {
    return { ...config };
  }

  function updateConfig(patch: Partial<CongestionConfig>): CongestionConfig {
    const numeric: Array<keyof CongestionConfig> = [
      'windowSeconds', 'abnormalWindowsToAlert', 'recoveryWindowsToClear',
      'occupancyThreshold', 'speedThresholdKmh', 'flowDropRatio',
      'detectorAnomalyRatio', 'requiredAbnormalDetectors', 'allowedLatenessSeconds',
    ];
    for (const key of numeric) {
      if (patch[key] === undefined) continue;
      const v = Number(patch[key]);
      if (!Number.isFinite(v)) throw new Error(`${key} 必须是数字`);
      if (key === 'windowSeconds' && v < 5) throw new Error('windowSeconds 至少 5 秒');
      if ((key === 'abnormalWindowsToAlert' || key === 'recoveryWindowsToClear') && v < 1)
        throw new Error(`${key} 至少为 1`);
      if (key === 'occupancyThreshold' && (v <= 0 || v > 1))
        throw new Error('occupancyThreshold 必须在 (0,1]');
      if (key === 'detectorAnomalyRatio' && (v <= 0 || v > 1))
        throw new Error('detectorAnomalyRatio 必须在 (0,1]');
      (config as any)[key] = v;
    }
    return getConfig();
  }

  function registerDetector(input: {
    detectorId: string;
    intersectionId: string;
    expectedFlow?: number;
  }): DetectorRegistration {
    const reg: DetectorRegistration = {
      detectorId: input.detectorId,
      intersectionId: input.intersectionId,
      expectedFlow: input.expectedFlow === undefined ? undefined : Number(input.expectedFlow),
      registeredAt: nowIso(),
    };
    registrations.set(reg.detectorId, reg);
    return reg;
  }

  function listDetectors(): DetectorRegistration[] {
    return [...registrations.values()];
  }

  /** 显式指定 > 注册表 > 编号前缀（如 J-01:D3 / J-02-N2） */
  function resolveIntersection(event: IngestEvent, knownIntersectionIds: string[]): string | undefined {
    if (event.intersectionId && knownIntersectionIds.includes(event.intersectionId)) {
      return event.intersectionId;
    }
    const reg = registrations.get(event.detectorId);
    if (reg) return reg.intersectionId;
    const sorted = [...knownIntersectionIds].sort((a, b) => b.length - a.length);
    for (const id of sorted) {
      if (event.detectorId === id || event.detectorId.startsWith(`${id}:`) || event.detectorId.startsWith(`${id}-`)) {
        return id;
      }
    }
    return undefined;
  }

  // knownIntersectionIds 由宿主注入，模块本身不依赖路口主数据
  let knownIntersectionIds: string[] = [];
  function bindIntersections(ids: string[]) {
    knownIntersectionIds = [...ids];
  }

  function median(values: number[]): number | null {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = sorted.length >> 1;
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  function flowReference(detectorId: string): number | null {
    const reg = registrations.get(detectorId);
    if (reg?.expectedFlow !== undefined) return reg.expectedFlow;
    return median(flowSamples.get(detectorId) ?? []);
  }

  function isEventAnomalous(event: IngestEvent): boolean {
    if (Number.isFinite(event.occupancy) && event.occupancy >= config.occupancyThreshold) return true;
    if (Number.isFinite(event.speed) && event.speed > 0 && event.speed <= config.speedThresholdKmh) return true;
    const base = flowReference(event.detectorId);
    if (base !== null && Number.isFinite(event.flow) &&
        event.flow <= base * (1 - config.flowDropRatio)) return true;
    return false;
  }

  function ensureContext(intersectionId: string): IntersectionContext {
    let ctx = contexts.get(intersectionId);
    if (!ctx) {
      ctx = { windows: new Map(), watermarkMs: -Infinity };
      contexts.set(intersectionId, ctx);
    }
    return ctx;
  }

  function windowStartOf(timeMs: number): number {
    const w = config.windowSeconds * 1000;
    return Math.floor(timeMs / w) * w;
  }

  function recomputeWindow(window: WindowState): void {
    const abnormalDetectors: string[] = [];
    for (const [detectorId, s] of window.detectors) {
      if (s.n > 0 && s.abnormalCount / s.n >= config.detectorAnomalyRatio) {
        abnormalDetectors.push(detectorId);
      }
    }
    window.abnormalDetectors = abnormalDetectors.sort();
    window.abnormal = abnormalDetectors.length >= config.requiredAbnormalDetectors;
  }

  function activeIncidentIds(intersectionId: string): string[] {
    return (incidentProvider?.(intersectionId) ?? []).map((i) => i.id);
  }

  function summarizeWindow(window: WindowState): AlertWindow {
    let peak: number | null = null;
    let minSpeed: number | null = null;
    let flowSum = 0;
    let flowN = 0;
    for (const id of window.abnormalDetectors) {
      const s = window.detectors.get(id)!;
      peak = peak === null ? s.occMax : Math.max(peak, s.occMax);
      if (s.speedN > 0) minSpeed = minSpeed === null ? s.speedMin : Math.min(minSpeed, s.speedMin);
      flowSum += s.flowSum;
      flowN += s.n;
    }
    return {
      start: iso(window.startMs),
      end: iso(window.endMs),
      detectorIds: [...window.abnormalDetectors],
      eventCount: window.count,
      peakOccupancy: peak === null ? null : r2(peak),
      minSpeed: minSpeed === null ? null : r2(minSpeed),
      avgFlow: flowN === 0 ? null : r2(flowSum / flowN),
    };
  }

  function recomputeAlertAggregates(alert: CongestionAlert): void {
    const detectorSet = new Set<string>();
    let peak: number | null = null;
    let minSpeed: number | null = null;
    for (const w of alert.abnormalWindows) {
      w.detectorIds.forEach((d) => detectorSet.add(d));
      if (w.peakOccupancy !== null) peak = peak === null ? w.peakOccupancy : Math.max(peak, w.peakOccupancy);
      if (w.minSpeed !== null) minSpeed = minSpeed === null ? w.minSpeed : Math.min(minSpeed, w.minSpeed);
    }
    alert.detectorIds = [...detectorSet].sort();
    alert.peakOccupancy = peak;
    alert.minSpeed = minSpeed;
    alert.severity = (peak !== null && peak >= 0.85) || (minSpeed !== null && minSpeed <= 10)
      ? 'critical' : 'warning';
    alert.abnormalWindowCount = alert.abnormalWindows.length;
  }

  function attachWindow(alert: CongestionAlert, window: WindowState, isOpeningStreak: boolean): void {
    const existingIdx = alert.abnormalWindows.findIndex((w) => w.start === iso(window.startMs));
    const summary = summarizeWindow(window);
    if (existingIdx >= 0) alert.abnormalWindows[existingIdx] = summary;
    else alert.abnormalWindows.push(summary);
    alert.abnormalWindows.sort((a, b) => a.start.localeCompare(b.start));
    alert.lastEventAt = iso(window.lastEventMs);
    recomputeAlertAggregates(alert);
    alert.linkedIncidentIds = activeIncidentIds(alert.intersectionId);
    alert.incidentRelated = alert.linkedIncidentIds.length > 0;
    if (existingIdx < 0 && !isOpeningStreak) {
      alert.updates += 1;
      alert.history.push({ type: 'updated', at: nowIso(), windowStart: summary.start });
    }
  }

  function openAlert(intersectionId: string, streak: WindowState[]): CongestionAlert {
    const first = streak[0];
    const alert: CongestionAlert = {
      id: randomUUID(),
      kind: 'congestion',
      source: 'detector-auto',
      intersectionId,
      status: 'active',
      severity: 'warning',
      startedAt: iso(first.startMs),
      firstEventAt: iso(first.firstEventMs),
      lastEventAt: iso(first.lastEventMs),
      detectorIds: [],
      peakOccupancy: null,
      minSpeed: null,
      incidentRelated: false,
      linkedIncidentIds: [],
      abnormalWindowCount: 0,
      updates: 0,
      reopenCount: 0,
      abnormalWindows: [],
      history: [{ type: 'opened', at: nowIso(), windowStart: iso(first.startMs) }],
      createdAt: nowIso(),
    };
    streak.forEach((w) => attachWindow(alert, w, true));
    alerts.push(alert);
    return alert;
  }

  /**
   * 从 fromStartMs 对应的窗口开始，按事件时间重放状态机。
   * 迟到数据可能把旧恢复窗口翻回异常：若上次自动解除点落在重放区间内，则重开原告警；
   * 人工解除的告警不自动重开，再次连续异常会开启新的事件段。
   */
  function evaluate(intersectionId: string, fromStartMs: number): CongestionAlert | null {
    const ctx = ensureContext(intersectionId);
    const starts = [...ctx.windows.keys()].sort((a, b) => a - b);
    if (!starts.length) return null;

    let idx = starts.findIndex((s) => s >= fromStartMs);
    if (idx < 0) idx = starts.length;

    let alert = [...alerts].reverse().find((a) => a.intersectionId === intersectionId && a.status === 'active') ?? null;

    // 跨过重放起点时，把起点之前已形成的连续异常/恢复计数接续上
    let abnormalStreak = 0;
    let recoveryStreak = 0;
    // 跨过重放起点时，把起点之前已形成的连续异常/恢复计数接续上；
    // 只有时间上相邻的窗口才算“连续”，缺数据窗口（静默）会打断计数。
    for (let k = idx - 1; k >= 0; k--) {
      const w = ctx.windows.get(starts[k])!;
      const wNext = ctx.windows.get(starts[k + 1]);
      if (wNext && w.endMs !== wNext.startMs) break;
      if (w.abnormal) {
        if (recoveryStreak === 0) abnormalStreak++;
        else break;
      } else if (alert) {
        if (abnormalStreak === 0) recoveryStreak++;
        else break;
      } else break;
    }

    // 仅当解除点位于本次重放区间内、且为自动解除时，才允许重开同一告警
    let reopenCandidate: CongestionAlert | null = null;
    if (!alert) {
      const latest = [...alerts].reverse().find((a) => a.intersectionId === intersectionId);
      if (latest && latest.status === 'resolved' &&
          latest.resolutionSource === 'auto' &&
          latest.resolutionWindowStartMs !== undefined &&
          latest.resolutionWindowStartMs >= fromStartMs) {
        reopenCandidate = latest;
      }
    }

    let prevStartMs = idx > 0 ? starts[idx - 1] : -Infinity;
    for (let i = idx; i < starts.length; i++) {
      const window = ctx.windows.get(starts[i])!;
      if (window.startMs !== prevStartMs + config.windowSeconds * 1000) {
        // 存在无数据窗口：连续计数从零重新开始
        abnormalStreak = 0;
        recoveryStreak = 0;
      }
      prevStartMs = window.startMs;
      if (window.abnormal) {
        abnormalStreak++;
        recoveryStreak = 0;
        if (!alert) {
          if (abnormalStreak >= config.abnormalWindowsToAlert) {
            if (reopenCandidate) {
              alert = reopenCandidate;
              reopenCandidate = null;
              alert.status = 'active';
              alert.resolvedAt = undefined;
              alert.resolutionSource = undefined;
              alert.resolutionWindowStartMs = undefined;
              alert.reopenCount += 1;
              alert.history.push({ type: 'reopened', at: nowIso(), windowStart: iso(window.startMs) });
              const streakWindows = [];
              for (let k = i; k >= 0 && streakWindows.length < abnormalStreak; k--) {
                const w = ctx.windows.get(starts[k])!;
                if (!w.abnormal) break;
                streakWindows.unshift(w);
              }
              streakWindows.forEach((w) => attachWindow(alert!, w, true));
            } else {
              const streakWindows = [];
              for (let k = i; k >= 0 && streakWindows.length < abnormalStreak; k--) {
                const w = ctx.windows.get(starts[k])!;
                if (!w.abnormal) break;
                streakWindows.unshift(w);
              }
              alert = openAlert(intersectionId, streakWindows);
            }
          }
        } else {
          attachWindow(alert, window, false);
        }
      } else {
        abnormalStreak = 0;
        if (alert) {
          recoveryStreak++;
          if (recoveryStreak >= config.recoveryWindowsToClear) {
            alert.status = 'resolved';
            alert.resolutionSource = 'auto';
            alert.resolvedAt = iso(window.endMs); // 解除时间按事件时间窗口边界
            alert.resolutionWindowStartMs = window.startMs;
            alert.history.push({ type: 'resolved', at: nowIso(), windowStart: iso(window.startMs), detail: 'auto-recovery' });
            alert = null;
            recoveryStreak = 0;
            reopenCandidate = null;
          }
        }
      }
    }
    return alert;
  }

  function ingest(event: IngestEvent): IngestResult {
    const timeMs = event.timeMs !== undefined && Number.isFinite(event.timeMs)
      ? event.timeMs
      : Date.parse(event.time ?? '') ;
    const effectiveMs = Number.isFinite(timeMs) ? timeMs : Date.now();

    // 1) 重复投递去重：优先使用上游 eventId，否则用内容指纹
    const fingerprint = [
      event.eventId ?? '',
      event.detectorId,
      iso(effectiveMs),
      event.flow, event.occupancy, event.speed,
    ].join('|');
    const existing = seenEvents.get(fingerprint);
    const eventId = event.eventId ?? existing ?? randomUUID();
    if (existing) {
      return { outcome: 'duplicate', detectorId: event.detectorId, eventId: existing };
    }
    seenEvents.set(fingerprint, eventId);
    if (seenEvents.size > MAX_SEEN) {
      const oldest = seenEvents.keys().next().value;
      if (oldest !== undefined) seenEvents.delete(oldest);
    }

    // 2) 归属路口；无法归属时与旧行为兼容（检测器事件照常入库），仅不产生告警
    const intersectionId = resolveIntersection(event, knownIntersectionIds);
    if (!intersectionId) {
      return { outcome: 'unassigned', detectorId: event.detectorId, eventId };
    }

    const ctx = ensureContext(intersectionId);
    const startMs = windowStartOf(effectiveMs);

    // 3) 乱序：超过最大允许迟到的事件只记录指纹（已去重），不再改变窗口状态
    const watermark = ctx.watermarkMs;
    if (Number.isFinite(watermark) && effectiveMs < watermark - config.allowedLatenessSeconds * 1000) {
      return {
        outcome: 'late-ignored', detectorId: event.detectorId, eventId,
        intersectionId, windowStart: iso(startMs), windowStartMs: startMs,
      };
    }
    if (effectiveMs > ctx.watermarkMs) ctx.watermarkMs = effectiveMs;

    // 4) 落窗（事件在进入时即固化异常判定，避免基准漂移改写历史）
    const normalized: IngestEvent = {
      ...event,
      detectorId: event.detectorId,
      flow: Number(event.flow) || 0,
      occupancy: Number(event.occupancy) || 0,
      speed: Number(event.speed) || 0,
    };
    const anomalous = isEventAnomalous(normalized);

    let window = ctx.windows.get(startMs);
    if (!window) {
      window = {
        intersectionId, startMs,
        endMs: startMs + config.windowSeconds * 1000,
        count: 0, detectors: new Map(),
        abnormal: false, abnormalDetectors: [],
        firstEventMs: effectiveMs, lastEventMs: effectiveMs,
      };
      ctx.windows.set(startMs, window);
    }
    let ds = window.detectors.get(event.detectorId);
    if (!ds) {
      ds = { n: 0, abnormalCount: 0, occSum: 0, occMax: 0, speedSum: 0, speedN: 0,
             speedMin: Infinity, flowSum: 0, flowMin: Infinity };
      window.detectors.set(event.detectorId, ds);
    }
    ds.n++;
    if (anomalous) ds.abnormalCount++;
    ds.occSum += normalized.occupancy;
    ds.occMax = Math.max(ds.occMax, normalized.occupancy);
    if (normalized.speed > 0) { ds.speedSum += normalized.speed; ds.speedN++; ds.speedMin = Math.min(ds.speedMin, normalized.speed); }
    ds.flowSum += normalized.flow;
    ds.flowMin = Math.min(ds.flowMin, normalized.flow);
    window.count++;
    window.firstEventMs = Math.min(window.firstEventMs, effectiveMs);
    window.lastEventMs = Math.max(window.lastEventMs, effectiveMs);

    // 流量基准样本（用于无登记 expectedFlow 时的骤降判定）
    const samples = flowSamples.get(event.detectorId) ?? [];
    samples.push(normalized.flow);
    if (samples.length > MAX_SAMPLES) samples.shift();
    flowSamples.set(event.detectorId, samples);

    recomputeWindow(window);

    // 5) 从受影响窗口起按事件时间重放状态机
    const active = evaluate(intersectionId, startMs);

    return {
      outcome: 'processed',
      detectorId: event.detectorId,
      eventId,
      intersectionId,
      windowStart: iso(startMs),
      windowStartMs: startMs,
      alertId: active?.id ?? null,
      alertStatus: active?.status,
    };
  }

  function listAlerts(filter: { intersectionId?: string; status?: 'active' | 'resolved' } = {}): CongestionAlert[] {
    return alerts
      .filter((a) => (!filter.intersectionId || a.intersectionId === filter.intersectionId))
      .filter((a) => (!filter.status || a.status === filter.status))
      .sort((a, b) => b.startedAt.localeCompare(a.startedAt))
      .map((a) => ({ ...a }));
  }

  function getAlert(id: string): CongestionAlert | undefined {
    const a = alerts.find((x) => x.id === id);
    return a ? { ...a } : undefined;
  }

  function acknowledgeAlert(id: string, by: string, note?: string): CongestionAlert | undefined {
    const a = alerts.find((x) => x.id === id);
    if (!a) return undefined;
    a.acknowledged = { at: nowIso(), by: by || 'operator', ...(note ? { note } : {}) };
    a.history.push({ type: 'acknowledged', at: nowIso(), detail: a.acknowledged.by });
    return { ...a };
  }

  /** 人工解除：与自动恢复区分，后续异常不再自动重开该告警。 */
  function closeAlert(id: string, by: string, note?: string): CongestionAlert | undefined {
    const a = alerts.find((x) => x.id === id);
    if (!a) return undefined;
    if (a.status === 'resolved') return { ...a };
    a.status = 'resolved';
    a.resolutionSource = 'operator';
    a.resolvedAt = nowIso();
    a.resolutionWindowStartMs = undefined;
    a.history.push({ type: 'closed', at: nowIso(), detail: `${by || 'operator'}${note ? `: ${note}` : ''}` });
    return { ...a };
  }

  function listWindows(intersectionId?: string): Array<AlertWindow & {
    intersectionId: string; abnormal: boolean; firstEventAt: string; lastEventAt: string;
    detectorCount: number;
  }> {
    const out: Array<any> = [];
    for (const [id, ctx] of contexts) {
      if (intersectionId && id !== intersectionId) continue;
      for (const w of ctx.windows.values()) {
        const summary = summarizeWindow(w);
        out.push({
          intersectionId: id,
          abnormal: w.abnormal,
          firstEventAt: iso(w.firstEventMs),
          lastEventAt: iso(w.lastEventMs),
          detectorCount: w.detectors.size,
          ...summary,
          detectorIds: w.abnormal ? summary.detectorIds : [],
        });
      }
    }
    return out.sort((a, b) => a.start.localeCompare(b.start));
  }

  return {
    ingest,
    registerDetector,
    listDetectors,
    bindIntersections,
    resolveIntersection: (e: IngestEvent) => resolveIntersection(e, knownIntersectionIds),
    listAlerts,
    getAlert,
    acknowledgeAlert,
    closeAlert,
    listWindows,
    getConfig,
    updateConfig,
    setIncidentProvider,
  };
}

export type CongestionAlertService = ReturnType<typeof createCongestionAlertService>;
