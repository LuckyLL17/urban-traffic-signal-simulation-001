import {randomUUID} from 'node:crypto';
import type {FastifyInstance} from 'fastify';

// 拥堵自动告警模块：按路口 + 事件时间划分滚动窗口，对流量/占有率/速度做连续异常判定，
// 达到阈值时创建或更新告警，连续恢复后自动解除。重复投递幂等去重，乱序与跨窗口事件
// 通过"按事件时间归档 + 每次摄入后重算该路口窗口序列"处理；手工事故仅做关联标记，
// 不抑制自动告警（cause 区分 incident-correlated / unexplained-congestion）。

export interface AlertConfig{
 windowSeconds:number;          // 窗口长度（秒）
 openAfterWindows:number;       // 连续多少个异常窗口后创建告警
 clearAfterWindows:number;      // 连续多少个正常窗口后解除告警
 flowHigh:number;               // 流量过高阈值（veh/窗口均值）
 occupancyHigh:number;          // 占有率过高阈值（%）
 speedLow:number;               // 速度过低阈值（km/h）
 minMetricAnomalies:number;     // 判定窗口异常所需的最少越限指标数（1-3）
 lateToleranceSeconds:number;   // 允许的最大乱序迟到（秒），超出则忽略
 retentionSeconds:number;       // 每路口窗口保留时长（秒）
 maxSeenEvents:number;          // 去重指纹缓存上限
}
export const defaultAlertConfig=():AlertConfig=>({windowSeconds:60,openAfterWindows:2,clearAfterWindows:2,flowHigh:45,occupancyHigh:30,speedLow:15,minMetricAnomalies:2,lateToleranceSeconds:600,retentionSeconds:7200,maxSeenEvents:10000});

interface WindowAgg{start:number;samples:number;flowSum:number;occSum:number;occN:number;speedSum:number;speedN:number;maxFlow:number;maxOccupancy:number;minSpeed:number;detectorIds:Set<string>}
interface IntersectionState{windows:Map<number,WindowAgg>;newestTs:number;sequence:number}
interface IngestMeta{hasOccupancy:boolean;hasSpeed:boolean;explicitIntersection:string|null}
interface Deps{intersections:{id:string}[];incidents:any[];log:(action:string,payload:any)=>void;now:()=>string}

export function createAlertModule(deps:Deps){
 const config=defaultAlertConfig();
 const alerts:any[]=[];
 const states=new Map<string,IntersectionState>();
 const bindings=new Map<string,string>();
 const seenEvents=new Map<string,any>();
 let skippedEvents=0;
 const W=()=>config.windowSeconds*1000;
 const iso=(ms:number)=>new Date(ms).toISOString();

 // 检测器 -> 路口：显式 intersectionId > 绑定表 > 按路口 id 前缀推断（如 J-01-D1 -> J-01）
 function resolveIntersection(detectorId:string,explicit:string|null):string|null{
  if(explicit&&deps.intersections.some(j=>j.id===explicit))return explicit;
  const bound=bindings.get(detectorId);if(bound)return bound;
  for(const j of deps.intersections)if(detectorId===j.id||detectorId.startsWith(j.id+'-')||detectorId.startsWith(j.id+'_')||detectorId.startsWith(j.id+':'))return j.id;
  return null;
 }
 // 重复投递识别：客户端提供 eventId 时按 eventId 去重；否则按 检测器+时间+指标 指纹去重。
 // 未提供 time/eventId 的事件无法安全识别重投，不做去重（保持原有逐条入账行为）。
 function dedupKey(body:any):string|null{
  if(body.eventId!=null)return`ext:${body.eventId}`;
  if(body.time!=null)return['fp',body.detectorId,body.time,body.flow,body.occupancy??'',body.speed??''].join('|');
  return null;
 }
 function stateFor(iid:string):IntersectionState{
  let st=states.get(iid);if(!st){st={windows:new Map(),newestTs:0,sequence:0};states.set(iid,st);}return st;
 }
 function isAnomalous(w:WindowAgg):boolean{
  let votes=0;
  if(w.flowSum/w.samples>=config.flowHigh)votes++;
  if(w.occN&&w.occSum/w.occN>=config.occupancyHigh)votes++;
  if(w.speedN&&w.speedSum/w.speedN<=config.speedLow)votes++;
  return votes>=config.minMetricAnomalies;
 }
 const windowView=(w:WindowAgg)=>({start:iso(w.start),samples:w.samples,detectorIds:[...w.detectorIds].sort(),avgFlow:Number((w.flowSum/w.samples).toFixed(2)),avgOccupancy:w.occN?Number((w.occSum/w.occN).toFixed(2)):null,avgSpeed:w.speedN?Number((w.speedSum/w.speedN).toFixed(2)):null,anomalous:isAnomalous(w)});

 // 与告警时间窗重叠的手工事故（含已解除的）关联到告警上，用于区分事故型/非事故型拥堵
 function linkIncidents(alert:any){
  const open=Date.parse(alert.openedAt),end=Date.parse(alert.resolvedAt||alert.updatedAt);
  alert.relatedIncidents=deps.incidents.filter(i=>i.intersectionId===alert.intersectionId&&Date.parse(i.startedAt)<=end&&Date.parse(i.clearedAt||deps.now())>=open).map(i=>i.id);
  const cause=alert.relatedIncidents.length?'incident-correlated':'unexplained-congestion';
  if(cause!==alert.cause){alert.cause=cause;pushHistory(alert,'cause',`cause 更新为 ${cause}`);}
 }
 function pushHistory(alert:any,action:string,detail:string){
  alert.history.push({time:deps.now(),action,detail});
  if(alert.history.length>50)alert.history.splice(0,alert.history.length-50);
 }
 // 用告警 openedAt 以来的全部窗口刷新指标、触发窗口列表与事故关联
 function refreshAlert(alert:any){
  const st=states.get(alert.intersectionId);if(!st)return;
  const openedMs=Date.parse(alert.openedAt);
  const wins=[...st.windows.values()].filter(w=>w.samples>0&&w.start>=openedMs).sort((a,b)=>a.start-b.start);
  const anomalous=wins.filter(isAnomalous);
  const sum=(f:(w:WindowAgg)=>number)=>wins.reduce((s,w)=>s+f(w),0);
  const occN=sum(w=>w.occN),speedN=sum(w=>w.speedN),samples=sum(w=>w.samples);
  alert.triggerWindows=anomalous.map(w=>iso(w.start));
  alert.detectorIds=[...new Set(wins.flatMap(w=>[...w.detectorIds]))].sort();
  alert.metrics={windows:wins.length,anomalousWindows:anomalous.length,samples,
   avgFlow:samples?Number((sum(w=>w.flowSum)/samples).toFixed(2)):0,
   avgOccupancy:occN?Number((sum(w=>w.occSum)/occN).toFixed(2)):null,
   avgSpeed:speedN?Number((sum(w=>w.speedSum)/speedN).toFixed(2)):null,
   maxOccupancy:wins.length?Math.max(...wins.map(w=>w.maxOccupancy)):0,
   minSpeed:speedN?Math.min(...wins.filter(w=>w.speedN).map(w=>w.minSpeed)):null,
   maxFlow:wins.length?Math.max(...wins.map(w=>w.maxFlow)):0};
  alert.updatedAt=deps.now();
  linkIncidents(alert);
 }
 function openAlert(iid:string,runStartMs:number){
  const st=stateFor(iid);
  const alert={id:randomUUID(),intersectionId:iid,sequence:++st.sequence,status:'active',openedAt:iso(runStartMs),updatedAt:deps.now(),resolvedAt:null,windowSeconds:config.windowSeconds,triggerWindows:[],detectorIds:[],metrics:{},cause:'unexplained-congestion',relatedIncidents:[],history:[] as any[]};
  pushHistory(alert,'open',`连续 ${config.openAfterWindows} 个异常窗口，触发自动拥堵告警`);
  alerts.push(alert);refreshAlert(alert);deps.log('alert.open',alert);
  return alert;
 }
 function resolveAlert(alert:any,resolvedMs:number){
  refreshAlert(alert);
  alert.status='resolved';alert.resolvedAt=iso(resolvedMs);alert.updatedAt=deps.now();
  pushHistory(alert,'resolve',`连续 ${config.clearAfterWindows} 个正常窗口，告警自动解除`);
  deps.log('alert.resolve',alert);
 }
 // 状态机：按事件时间排序的窗口序列上取尾部连续段，决定 创建/更新/解除。
 // 乱序事件落入历史窗口后重算，openedAt/resolvedAt 均可按事件时间回填修正。
 function reevaluate(iid:string){
  const st=stateFor(iid);
  const wins=[...st.windows.values()].filter(w=>w.samples>0).sort((a,b)=>a.start-b.start);
  if(!wins.length)return;
  let runType:'A'|'N'|null=null,runStart=0,runLen=0;
  for(const w of wins){const t=isAnomalous(w)?'A':'N';if(t===runType)runLen++;else{runType=t;runStart=w.start;runLen=1;}}
  const active=alerts.find(a=>a.intersectionId===iid&&a.status==='active');
  if(runType==='A'&&runLen>=config.openAfterWindows){
   const alert=active||openAlert(iid,runStart);
   refreshAlert(alert);
  }else if(active&&runType==='N'&&runLen>=config.clearAfterWindows){
   resolveAlert(active,runStart+config.clearAfterWindows*W());
  }else if(active){
   refreshAlert(active); // 恢复未确认前保持活动，仅刷新指标（含乱序补数）
  }
 }
 // 事件进入窗口（按事件时间），随后重算该路口告警状态
 function ingest(event:any,meta:IngestMeta){
  const iid=resolveIntersection(event.detectorId,meta.explicitIntersection);
  if(!iid){skippedEvents++;return;}
  const parsed=Date.parse(event.time);const ts=Number.isFinite(parsed)?parsed:Date.now();
  const st=stateFor(iid);
  if(st.newestTs&&ts<st.newestTs-config.lateToleranceSeconds*1000){skippedEvents++;return;}
  st.newestTs=Math.max(st.newestTs,ts);
  const ws=Math.floor(ts/W())*W();
  let w=st.windows.get(ws);
  if(!w){w={start:ws,samples:0,flowSum:0,occSum:0,occN:0,speedSum:0,speedN:0,maxFlow:0,maxOccupancy:0,minSpeed:Infinity,detectorIds:new Set()};st.windows.set(ws,w);}
  w.samples++;w.flowSum+=event.flow;w.maxFlow=Math.max(w.maxFlow,event.flow);
  if(meta.hasOccupancy){w.occSum+=event.occupancy;w.occN++;w.maxOccupancy=Math.max(w.maxOccupancy,event.occupancy);}
  if(meta.hasSpeed){w.speedSum+=event.speed;w.speedN++;w.minSpeed=Math.min(w.minSpeed,event.speed);}
  w.detectorIds.add(event.detectorId);
  const cutoff=st.newestTs-config.retentionSeconds*1000;
  for(const k of[...st.windows.keys()])if(k<cutoff)st.windows.delete(k);
  reevaluate(iid);
 }
 // 检测器事件入口：幂等去重 + 入库 + 告警摄入。返回（可能已存在的）事件对象。
 function ingestDetectorEvent(body:any,store:(e:any)=>void){
  const key=dedupKey(body);
  if(key&&seenEvents.has(key))return seenEvents.get(key);
  const e={id:randomUUID(),time:body.time||deps.now(),detectorId:body.detectorId,flow:Number(body.flow),occupancy:Number(body.occupancy||0),speed:Number(body.speed||0)};
  store(e);
  if(key){seenEvents.set(key,e);if(seenEvents.size>config.maxSeenEvents){const oldest=seenEvents.keys().next().value;if(oldest!==undefined)seenEvents.delete(oldest);}}
  ingest(e,{hasOccupancy:body.occupancy!=null,hasSpeed:body.speed!=null,explicitIntersection:typeof body.intersectionId==='string'?body.intersectionId:null});
  return e;
 }

 function registerRoutes(app:FastifyInstance){
  app.get('/api/alerts',async(req:any)=>{const q=req.query as any;let out=alerts.slice().sort((a,b)=>b.openedAt.localeCompare(a.openedAt));if(q.status)out=out.filter(a=>a.status===q.status);if(q.intersectionId)out=out.filter(a=>a.intersectionId===q.intersectionId);return out;});
  app.get('/api/alerts/config',async()=>({...config,stats:{alerts:alerts.length,active:alerts.filter(a=>a.status==='active').length,skippedEvents,trackedIntersections:states.size}}));
  app.put('/api/alerts/config',async(req:any,res:any)=>{
   const b=req.body as any;const next:AlertConfig={...config};const errs:string[]=[];
   const num=(k:keyof AlertConfig,min:number,max:number)=>{if(b[k]!=null){const v=Number(b[k]);if(!Number.isFinite(v)||v<min||v>max)errs.push(`${k} must be ${min}-${max}`);else next[k]=v;}};
   num('windowSeconds',10,3600);num('openAfterWindows',1,20);num('clearAfterWindows',1,20);num('flowHigh',0,100000);num('occupancyHigh',0,100);num('speedLow',0,200);num('minMetricAnomalies',1,3);num('lateToleranceSeconds',0,86400);num('retentionSeconds',60,604800);num('maxSeenEvents',100,1000000);
   if(errs.length)return res.code(400).send({error:'invalid config',details:errs});
   Object.assign(config,next);deps.log('alert.config',config);return config;
  });
  app.get('/api/alerts/:id',async(req:any,res:any)=>{
   const a=alerts.find(x=>x.id===(req.params as any).id);if(!a)return res.code(404).send({error:'alert not found'});
   const st=states.get(a.intersectionId);const openedMs=Date.parse(a.openedAt);const endMs=a.resolvedAt?Date.parse(a.resolvedAt):Date.now();
   const windows=st?[...st.windows.values()].filter(w=>w.start>=openedMs&&w.start<=endMs).sort((x,y)=>x.start-y.start).map(windowView):[];
   return{...a,windows};
  });
  app.post('/api/detectors/bindings',async(req:any,res:any)=>{
   const b=req.body as any;if(!b.detectorId||!b.intersectionId)return res.code(400).send({error:'detectorId 和 intersectionId 必填'});
   if(!deps.intersections.some(j=>j.id===b.intersectionId))return res.code(404).send({error:'intersection not found'});
   bindings.set(b.detectorId,b.intersectionId);deps.log('detector.bind',{detectorId:b.detectorId,intersectionId:b.intersectionId});
   return{detectorId:b.detectorId,intersectionId:b.intersectionId};
  });
  app.get('/api/detectors/bindings',async()=>[...bindings.entries()].map(([detectorId,intersectionId])=>({detectorId,intersectionId})));
 }
 return{registerRoutes,ingestDetectorEvent};
}
