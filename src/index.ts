import Fastify from 'fastify';
import { randomUUID } from 'node:crypto';
import { createAlertModule } from './alerts.js';

type Phase={id:string;green:number;yellow:number;red:number}; type Intersection={id:string;name:string;phases:Phase[];cycle:number;queues:Record<string,number>;capacity:number;offset?:number};
const app=Fastify({logger:true}); const now=()=>new Date().toISOString();
const intersections:Intersection[]=[
{id:'J-01',name:'人民路/中山路',phases:[{id:'NS',green:38,yellow:3,red:39},{id:'EW',green:32,yellow:3,red:45}],cycle:80,queues:{N:12,S:8,E:15,W:11},capacity:42},
{id:'J-02',name:'中山路/解放路',phases:[{id:'NS',green:30,yellow:3,red:37},{id:'EW',green:30,yellow:3,red:37}],cycle:70,queues:{N:9,S:12,E:18,W:10},capacity:36},
{id:'J-03',name:'解放路/滨江路',phases:[{id:'NS',green:28,yellow:3,red:35},{id:'EW',green:28,yellow:3,red:35}],cycle:66,queues:{N:7,S:5,E:13,W:9},capacity:34}
];
const detectors:any[]=[];const incidents:any[]=[];const plans:any[]=[];const runs:any[]=[];const audit:any[]=[];
const log=(action:string,payload:any)=>audit.push({id:randomUUID(),time:now(),action,payload});
const alertModule=createAlertModule({intersections,incidents,log,now});
function simulate(input:any){
 let totalDelay=0,maxQueue=0,spillbacks=0; const snapshots:any[]=[]; const horizon=Number(input.horizon||900); const demand=input.demand||{};
 const queues:Record<string,Record<string,number>>=Object.fromEntries(intersections.map(j=>[j.id,{...j.queues}]));
 for(let tick=0;tick<horizon;tick+=5){
  for(const j of intersections){
   const d=Number(demand[j.id]??(j.capacity*.72)); const greenRatio=j.phases[0].green/j.cycle; const served=d*greenRatio*.18; const blocked=incidents.some(x=>x.status==='active'&&x.intersectionId===j.id)?.55:1;
   for(const lane of Object.keys(queues[j.id])){const arrival=d*.04*(lane==='E'||lane==='W'?1.05:.95); queues[j.id][lane]=Math.max(0,queues[j.id][lane]+arrival-served*blocked/4); if(queues[j.id][lane]>j.capacity){spillbacks++;queues[j.id][lane]=j.capacity*1.15;}}
   totalDelay+=Object.values(queues[j.id]).reduce((a,b)=>a+b,0)*5; maxQueue=Math.max(maxQueue,...Object.values(queues[j.id]));
  }
  if(tick%60===0)snapshots.push({second:tick,queues:intersections.map(j=>({id:j.id,...queues[j.id]}))});
 }
 return{horizon,totalDelay:Number(totalDelay.toFixed(1)),maxQueue:Number(maxQueue.toFixed(1)),spillbacks,averageSpeed:Number(Math.max(8,42-totalDelay/(horizon*10)).toFixed(2)),snapshots};
}
app.get('/health',async()=>({ok:true,service:'urban-traffic-signal-simulation',time:now()}));
app.get('/api/network/state',async()=>({at:now(),intersections,incidents,detectors,metrics:{averageSpeed:31.4,totalQueue:intersections.reduce((s,j)=>s+Object.values(j.queues).reduce((a,b)=>a+Number(b),0),0)}}));
app.post('/api/detectors/events',async(req:any,res)=>{const b=req.body as any;if(!b.detectorId||b.flow==null)return res.code(400).send({error:'detectorId 和 flow 必填'});return alertModule.ingestDetectorEvent(b,(e:any)=>detectors.push(e));});
app.post('/api/incidents',async(req:any,res)=>{const b=req.body as any;if(!b.intersectionId||!b.type)return res.code(400).send({error:'intersectionId 和 type 必填'});const i={id:randomUUID(),intersectionId:b.intersectionId,type:b.type,lane:b.lane||'unknown',severity:b.severity||'medium',status:'active',startedAt:now(),estimatedClearMinutes:Number(b.estimatedClearMinutes||20)};incidents.push(i);log('incident.open',i);return i;});
app.post('/api/incidents/:id/clear',async(req:any,res)=>{const i=incidents.find(x=>x.id===(req.params as any).id);if(!i)return res.code(404).send({error:'incident not found'});i.status='cleared';i.clearedAt=now();return i;});
app.post('/api/plans',async(req:any,res)=>{const b=req.body as any;if(!b.name||!b.changes)return res.code(400).send({error:'name 和 changes 必填'});const p={id:randomUUID(),name:b.name,basePlan:intersections.map(j=>({id:j.id,cycle:j.cycle,phases:j.phases})),changes:b.changes,priority:b.priority||'normal',status:'draft',createdAt:now()};plans.push(p);return p;});
app.post('/api/plans/:id/validate',async(req:any,res)=>{const p=plans.find(x=>x.id===(req.params as any).id);if(!p)return res.code(404).send({error:'plan not found'});const errors:string[]=[];for(const [id,change] of Object.entries(p.changes as Record<string,any>)){const j=intersections.find(x=>x.id===id);if(!j)errors.push(`${id}: intersection not found`);else{const cycle=Number((change as any).cycle??j.cycle);const phases=(change as any).phases||j.phases;if(cycle<30||cycle>180)errors.push(`${id}: cycle out of [30,180]`);if(phases.reduce((s:any,x:any)=>s+Number(x.green||0)+Number(x.yellow||0),0)>cycle)errors.push(`${id}: phase budget exceeded`);}}p.validation={valid:errors.length===0,errors};return p;});
app.post('/api/plans/:id/apply',async(req:any,res)=>{const p=plans.find(x=>x.id===(req.params as any).id);if(!p)return res.code(404).send({error:'plan not found'});if(!p.validation?.valid)return res.code(409).send({error:'plan must pass validation',validation:p.validation});for(const [id,change] of Object.entries(p.changes as Record<string,any>)){const j=intersections.find(x=>x.id===id);if(j){j.cycle=Number((change as any).cycle??j.cycle);if((change as any).phases)j.phases=(change as any).phases;}}p.status='applied';p.appliedAt=now();log('plan.apply',p);return p;});
app.post('/api/simulations',async(req:any,res)=>{const b=req.body as any;const plan=b.planId?plans.find(x=>x.id===b.planId):null;if(b.planId&&!plan)return res.code(404).send({error:'plan not found'});const baseline=simulate({horizon:b.horizon,demand:b.demand});let candidate=null;if(plan){const saved=intersections.map(j=>({j,cycle:j.cycle,phases:j.phases}));for(const [id,c] of Object.entries(plan.changes as Record<string,any>)){const j=intersections.find(x=>x.id===id);if(j){j.cycle=Number((c as any).cycle??j.cycle);if((c as any).phases)j.phases=(c as any).phases}}candidate=simulate({horizon:b.horizon,demand:b.demand});saved.forEach(x=>{x.j.cycle=x.cycle;x.j.phases=x.phases});}const r={id:randomUUID(),createdAt:now(),planId:b.planId||null,baseline,candidate,delta:candidate?{delay:Number((candidate.totalDelay-baseline.totalDelay).toFixed(1)),maxQueue:Number((candidate.maxQueue-baseline.maxQueue).toFixed(1)),speed:Number((candidate.averageSpeed-baseline.averageSpeed).toFixed(2))}:null};runs.push(r);return r;});
app.post('/api/optimization',async(req:any)=>{
 const b=req.body as any;
 const options=[{cycleBias:-8,greenShift:4},{cycleBias:0,greenShift:0},{cycleBias:10,greenShift:-3}].map((o,i)=>{
  const changes:any={};
  for(const j of intersections) changes[j.id]={cycle:Math.max(30,Math.min(180,j.cycle+o.cycleBias)),phases:j.phases.map((phase,k)=>({...phase,green:Math.max(10,phase.green+(k===0?o.greenShift:-o.greenShift))}))};
  return {id:randomUUID(),label:`候选方案 ${i+1}`,changes,result:simulate({horizon:b.horizon||600,demand:b.demand})};
 });
 options.sort((a,b)=>a.result.totalDelay-b.result.totalDelay);
 return {objective:b.objective||'minimize-delay',candidates:options,best:options[0]};
});
app.get('/api/simulations/:id',async(req:any,res)=>{const r=runs.find(x=>x.id===(req.params as any).id);if(!r)return res.code(404).send({error:'simulation not found'});return r;});
app.get('/api/audit',async()=>audit.slice(-100));

// Advanced operations: corridor coordination, transit priority, pedestrian safety and emissions.
const priorityRequests:any[]=[]; const corridors:any[]=[{id:'C-01',name:'中山路绿波走廊',intersectionIds:['J-01','J-02','J-03'],lengthMeters:2400,targetSpeedKph:36}]; const scenarioPolicies:any[]=[];
app.get('/api/corridors',async()=>corridors);
app.post('/api/corridors/:id/coordinate',async(req:any,res)=>{const c=corridors.find(x=>x.id===(req.params as any).id);if(!c)return res.code(404).send({error:'corridor not found'});const b=req.body as any;const speedKph=Number(b.speedKph||c.targetSpeedKph);if(speedKph<10||speedKph>70)return res.code(400).send({error:'speedKph must be 10-70'});const cycle=Math.max(...c.intersectionIds.map((id:string)=>intersections.find(x=>x.id===id)?.cycle||60));const offsets:any={};let distanceSeconds=0;for(const id of c.intersectionIds){const j=intersections.find(x=>x.id===id)!;distanceSeconds+=c.lengthMeters/c.intersectionIds.length/(speedKph/3.6);offsets[id]=Math.round(distanceSeconds%cycle);j.offset=offsets[id];}const plan={id:randomUUID(),corridorId:c.id,cycleSeconds:cycle,speedKph,offsets,createdAt:now(),status:'coordinated'};plans.push(plan);log('corridor.coordinate',plan);return plan;});
app.post('/api/priority-requests',async(req:any,res)=>{const b=req.body as any;if(!b.intersectionId||!b.routeId||b.etaSeconds==null)return res.code(400).send({error:'intersectionId、routeId、etaSeconds 必填'});const j=intersections.find(x=>x.id===b.intersectionId);if(!j)return res.code(404).send({error:'intersection not found'});const r:any={id:randomUUID(),intersectionId:b.intersectionId,routeId:b.routeId,etaSeconds:Number(b.etaSeconds),passengers:Number(b.passengers||0),requestType:b.requestType||'transit',status:'requested',requestedAt:now()};r.approved=r.passengers>=20||r.requestType==='emergency';r.status=r.approved?'approved':'deferred';priorityRequests.push(r);log('priority.request',r);return r;});
app.post('/api/pedestrian-phase',async(req:any,res)=>{const b=req.body as any;if(!b.intersectionId||b.crossingSeconds==null)return res.code(400).send({error:'intersectionId 和 crossingSeconds 必填'});const crossing=Number(b.crossingSeconds);if(crossing<7||crossing>60)return res.code(400).send({error:'crossingSeconds must be 7-60'});const j=intersections.find(x=>x.id===b.intersectionId);if(!j)return res.code(404).send({error:'intersection not found'});const p={id:randomUUID(),intersectionId:j.id,crossingSeconds:crossing,leadingInterval:Number(b.leadingInterval||3),accessible:Boolean(b.accessible),status:'validated',createdAt:now()};scenarioPolicies.push(p);return p;});
app.post('/api/simulations/:id/emissions',async(req:any,res)=>{const r=runs.find(x=>x.id===(req.params as any).id);if(!r)return res.code(404).send({error:'simulation not found'});const vkt=Number((req.body as any)?.vehicleKm||12000);const stopPenalty=(r.candidate?.totalDelay??r.baseline.totalDelay)/Math.max(1,r.baseline.horizon);return{simulationId:r.id,vehicleKm:vkt,co2Kg:Number((vkt*(.142+stopPenalty*.0008)).toFixed(2)),noxGrams:Number((vkt*(.32+stopPenalty*.0015)).toFixed(2)),idleHours:Number((stopPenalty*vkt/3600).toFixed(2)),method:'speed-delay surrogate model'};});
app.post('/api/evacuation-scenarios',async(req:any)=>{const b=req.body as any;const blocked=(b.blockedIntersections||[]) as string[];const original=incidents.length;for(const id of blocked)incidents.push({id:randomUUID(),intersectionId:id,type:'evacuation-block',severity:'high',status:'active',startedAt:now()});const result=simulate({horizon:b.horizon||1800,demand:b.demand});incidents.splice(original);return{id:randomUUID(),type:'evacuation',blockedIntersections:blocked,result,recommendation:result.spillbacks>3?'open reversible lanes and meter upstream inflow':'maintain coordinated offsets'};});
app.get('/api/priority-requests',async()=>priorityRequests);

// Congestion alert module: windowed anomaly detection over detector events.
alertModule.registerRoutes(app);

app.listen({port:Number(process.env.PORT||6302),host:'127.0.0.1'}).catch(err=>{app.log.error(err);process.exit(1)});
