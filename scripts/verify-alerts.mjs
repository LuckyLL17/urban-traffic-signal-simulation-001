// 告警模块端到端验证：正常流量 / 突发拥堵 / 重复投递 / 恢复解除 / 乱序 / 多检测器 / 手工事故区分 / 接口兼容
import {spawn} from 'node:child_process';

const PORT=6399, BASE=`http://127.0.0.1:${PORT}`;
const server=spawn('npx',['tsx','src/index.ts'],{env:{...process.env,PORT:String(PORT)},stdio:['ignore','pipe','pipe']});
let serverLog='';server.stdout.on('data',d=>serverLog+=d);server.stderr.on('data',d=>serverLog+=d);
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

let passed=0,failed=0;
function check(name,cond,extra){if(cond){passed++;console.log('  ✔',name);}else{failed++;console.log('  ✘',name,extra===undefined?'':JSON.stringify(extra));}}
async function get(p){const r=await fetch(BASE+p);return{status:r.status,body:await r.json()};}
async function post(p,b){const r=await fetch(BASE+p,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b??{})});return{status:r.status,body:await r.json()};}
async function put(p,b){const r=await fetch(BASE+p,{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(b)});return{status:r.status,body:await r.json()};}

// 事件时间基准：1 小时前、分钟对齐（窗口=60s，窗口起点即分钟起点）
const T0=Math.floor((Date.now()-3600_000)/60000)*60000;
const T=(min,sec=0)=>new Date(T0+min*60000+sec*1000).toISOString();
const ev=(det,min,flow,occupancy,speed)=>post('/api/detectors/events',{detectorId:det,time:T(min),flow,occupancy,speed});
const JAM=[65,48,7], OK=[30,12,38]; // 拥堵: 高流量+高占有率+低速度；正常流量

async function main(){
 for(let i=0;i<60;i++){try{const r=await fetch(BASE+'/health');if(r.ok)break;}catch{}await sleep(500);}

 console.log('\n[A] 正常流量不触发告警');
 for(const m of [0,1,2,3,4])await ev('J-01-D1',m,...OK);
 let r=await get('/api/alerts?intersectionId=J-01');
 check('A1 连续正常窗口无告警',r.status===200&&r.body.length===0,r.body);

 console.log('\n[B] 突发拥堵：连续异常窗口创建并更新告警');
 await ev('J-01-D1',10,...JAM);
 r=await get('/api/alerts?intersectionId=J-01');
 check('B1 单个异常窗口不触发',r.body.length===0,r.body);
 await ev('J-01-D1',11,...JAM);
 r=await get('/api/alerts?intersectionId=J-01&status=active');
 check('B2 连续2个异常窗口创建告警',r.body.length===1,r.body);
 const alertB=r.body[0]||{};
 check('B3 openedAt=首个异常窗口起点',alertB.openedAt===T(10),alertB.openedAt);
 check('B4 无手工事故时 cause=unexplained-congestion',alertB.cause==='unexplained-congestion',alertB.cause);
 await ev('J-01-D1',12,...JAM);
 r=await get('/api/alerts?intersectionId=J-01');
 check('B5 持续异常更新同一告警而非新建',r.body.length===1&&r.body[0].triggerWindows.length===3,r.body.map(a=>({id:a.id,n:a.triggerWindows.length})));

 console.log('\n[C] 重复投递幂等');
 const detBefore=(await get('/api/network/state')).body.detectors.length;
 const metricsBefore=(await get('/api/alerts/'+alertB.id)).body.metrics.samples;
 await ev('J-01-D1',10,...JAM);await ev('J-01-D1',11,...JAM);await ev('J-01-D1',12,...JAM);
 const detAfter=(await get('/api/network/state')).body.detectors.length;
 check('C1 重复事件被去重不再入账',detAfter===detBefore,{before:detBefore,after:detAfter});
 const metricsAfter=(await get('/api/alerts/'+alertB.id)).body.metrics.samples;
 check('C2 告警样本数不受重投影响',metricsAfter===metricsBefore,{before:metricsBefore,after:metricsAfter});

 console.log('\n[D] 连续恢复后自动解除');
 await ev('J-01-D1',13,...OK);
 r=await get('/api/alerts/'+alertB.id);
 check('D1 单个恢复窗口未解除',r.body.status==='active',r.body.status);
 await ev('J-01-D1',14,...OK);
 r=await get('/api/alerts/'+alertB.id);
 check('D2 连续2个正常窗口解除',r.body.status==='resolved',r.body.status);
 check('D3 resolvedAt=恢复确认时刻',r.body.resolvedAt===T(15),r.body.resolvedAt);
 check('D4 详情含逐窗口可追溯序列',Array.isArray(r.body.windows)&&r.body.windows.length===5&&r.body.windows.filter(w=>w.anomalous).length===3,r.body.windows);

 console.log('\n[E] 乱序事件按事件时间归窗');
 await ev('J-01-D1',21,...JAM);
 r=await get('/api/alerts?intersectionId=J-01&status=active');
 check('E1 乱序首条(后到的窗口)不触发',r.body.length===0,r.body);
 await ev('J-01-D1',20,...JAM);
 r=await get('/api/alerts?intersectionId=J-01&status=active');
 check('E2 乱序补齐后按事件时间开窗',r.body.length===1&&r.body[0].openedAt===T(20),r.body.map(a=>a.openedAt));
 const alertE=r.body[0];
 await ev('J-01-D1',22,...OK);await ev('J-01-D1',23,...OK);
 r=await get('/api/alerts/'+alertE.id);
 check('E3 乱序场景告警正常解除',r.body.status==='resolved',r.body.status);

 console.log('\n[F] 多个检测器同时异常');
 await ev('J-02-D1',30,...JAM);await ev('J-02-D2',30,66,47,8);
 r=await get('/api/alerts?intersectionId=J-02');
 check('F1 单窗口多检测器不触发',r.body.length===0,r.body);
 await ev('J-02-D1',31,...JAM);await ev('J-02-D2',31,66,47,8);
 r=await get('/api/alerts?intersectionId=J-02&status=active');
 check('F2 同路口多检测器聚合为单告警',r.body.length===1,r.body);
 const alertF=r.body[0]||{};
 check('F3 告警覆盖两个检测器、样本聚合',(alertF.detectorIds||[]).length===2&&alertF.metrics?.samples===4,{det:alertF.detectorIds,samples:alertF.metrics?.samples});

 console.log('\n[G] 手工事故区分');
 const inc=await post('/api/incidents',{intersectionId:'J-03',type:'accident',lane:'N'});
 check('G1 手工事故创建成功',inc.status===200&&!!inc.body.id,inc.body);
 await ev('J-03-D1',30,...JAM);await ev('J-03-D1',31,...JAM);
 r=await get('/api/alerts?intersectionId=J-03&status=active');
 check('G2 事故路口仍自动形成告警',r.body.length===1,r.body);
 const alertG=r.body[0]||{};
 check('G3 告警关联手工事故并标记 cause',alertG.cause==='incident-correlated'&&(alertG.relatedIncidents||[]).includes(inc.body.id),{cause:alertG.cause,rel:alertG.relatedIncidents});
 r=await get('/api/alerts/'+alertF.id);
 check('G4 无事故路口保持 unexplained-congestion',r.body.cause==='unexplained-congestion',r.body.cause);
 r=await get('/api/alerts?status=active');
 check('G5 多路口活动告警并存',r.body.length===2,r.body.map(a=>a.intersectionId));
 await post(`/api/incidents/${inc.body.id}/clear`);
 await ev('J-02-D1',32,...OK);await ev('J-02-D1',33,...OK);
 await ev('J-03-D1',32,...OK);await ev('J-03-D1',33,...OK);
 r=await get('/api/alerts?status=active');
 check('G6 全部恢复后无活动告警',r.body.length===0,r.body);

 console.log('\n[H] 现有接口兼容与查询');
 const legacy=await post('/api/detectors/events',{detectorId:'J-01-D1',time:T(40),flow:22});
 check('H1 旧版事件(仅flow)仍被接受且形状不变',legacy.status===200&&!!legacy.body.id&&legacy.body.occupancy===0&&legacy.body.speed===0,legacy.body);
 const unbound=await post('/api/detectors/events',{detectorId:'X-99',time:T(40),flow:99,occupancy:90,speed:2});
 check('H2 未绑定检测器事件正常存储',unbound.status===200&&!!unbound.body.id,unbound.body);
 r=await get('/api/alerts?intersectionId=J-01');
 check('H3 未绑定/正常事件不产生新告警',r.body.length===2&&r.body.every(a=>a.status==='resolved'),r.body.map(a=>a.status));
 check('H4 未知告警返回404',(await get('/api/alerts/does-not-exist')).status===404);
 r=await get('/api/alerts/config');
 check('H5 告警配置可查询',r.status===200&&r.body.windowSeconds===60,r.body);
 check('H6 非法配置被拒绝',(await put('/api/alerts/config',{openAfterWindows:0})).status===400);
 r=await get('/api/audit');
 check('H7 告警开/解除写入审计轨迹',r.body.some(a=>a.action==='alert.open')&&r.body.some(a=>a.action==='alert.resolve'));
 check('H8 检测器事件必填校验不变',(await post('/api/detectors/events',{flow:10})).status===400);

 console.log(`\n结果: ${passed} 通过, ${failed} 失败`);
 if(failed){console.log('\n--- 服务日志(尾部) ---\n'+serverLog.slice(-3000));process.exitCode=1;}
}

main().catch(e=>{console.error(e);console.log(serverLog.slice(-3000));process.exitCode=1;})
 .finally(()=>{server.kill('SIGTERM');setTimeout(()=>process.exit(),500);});
