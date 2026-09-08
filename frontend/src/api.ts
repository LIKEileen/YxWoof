import type {Conversation} from './types';
const base='/api/v1';
export const SERVICE_MESSAGE='抱歉，服务暂时不可用，请稍后重试。';
export const uuid=()=>crypto.randomUUID();
export const saved=()=>{try{return JSON.parse(localStorage.getItem('yx.session')||'null') as {cid:string;csrf:string}|null}catch{return null}};
export function saveSession(value:{cid:string;csrf:string}){localStorage.setItem('yx.session',JSON.stringify(value))}
export class ApiError extends Error{code:string;constructor(message:string,code:string){super(message);this.code=code}}
export function consumerError(e:unknown){return e instanceof ApiError?e.message:SERVICE_MESSAGE}
function responseError(data:any,status:number){
 if(status>=500||status===429||!data?.message||typeof data.message!=='string')return new ApiError(SERVICE_MESSAGE,'service_unavailable');
 return new ApiError(data.message,data.error||'request_error');
}
async function timed<T>(fn:(signal:AbortSignal)=>Promise<T>,ms=35000):Promise<T>{
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),ms);
 try{return await fn(controller.signal)}catch(e){throw e instanceof ApiError?e:new ApiError(SERVICE_MESSAGE,'service_unavailable')}finally{clearTimeout(timer)}
}
export async function api<T>(path:string,body?:unknown):Promise<T>{
 return timed(async signal=>{
 const clockStart=performance.now(),clockEpoch=performance.timeOrigin;
 const r=await fetch(base+path,{signal,method:body===undefined?'GET':'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':saved()?.csrf||''},body:body===undefined?undefined:JSON.stringify(body)});
 const data=await r.json();if(!r.ok)throw responseError(data,r.status);
 if(data.server_time){const rtt=performance.now()-clockStart;const old=JSON.parse(sessionStorage.getItem('yx.clock')||'null');if(rtt<5000&&(!old||old.origin!==clockEpoch||rtt<old.uncertainty))sessionStorage.setItem('yx.clock',JSON.stringify({origin:clockEpoch,offset:Date.parse(data.server_time)-(clockEpoch+clockStart+rtt/2),uncertainty:rtt}))}
 return data as T;
 });
}
export async function chatStream(cid:string,text:string,interaction_id:string,selected_goal:string|undefined,onState:(s:Conversation)=>void){
 return timed(async signal=>{
 const r=await fetch(base+'/conversations/'+cid+'/chat',{signal,method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':saved()?.csrf||''},body:JSON.stringify({text,interaction_id,selected_goal})});
 if(!r.ok)throw responseError(await r.json(),r.status);
 if(!r.body)throw new ApiError(SERVICE_MESSAGE,'service_unavailable');
 const reader=r.body.getReader(),decoder=new TextDecoder();let buffer='',terminal=false,hasState=false;
 try{
 while(true){const {done,value}=await reader.read();buffer+=decoder.decode(value,{stream:!done});const chunks=buffer.split('\n\n');buffer=chunks.pop()||'';
 for(const block of chunks){
 const event=block.split('\n').find(x=>x.startsWith('event: '))?.slice(7),raw=block.split('\n').find(x=>x.startsWith('data: '))?.slice(6);if(!raw)continue;
 const d=JSON.parse(raw);
 if(event==='state'){onState(d);hasState=true}
 if(event==='done')terminal=true;
 if(event==='error')throw responseError(d,d.error==='service_unavailable'?503:409);
 }
 if(done)break;
 }
 if(!terminal||!hasState)throw new ApiError(SERVICE_MESSAGE,'service_unavailable');
 }finally{await reader.cancel().catch(()=>{});reader.releaseLock()}
 },40000);
}
type Pending={cid:string;body:Record<string,unknown>};
let flushing=false;
function pending():Pending[]{try{return JSON.parse(localStorage.getItem('yx.events')||'[]')}catch{return[]}}
export function track(cid:string,name:string,fields:Record<string,unknown>={}){const rows=pending();rows.push({cid,body:{id:uuid(),name,client_occurred_at:new Date().toISOString(),...fields}});localStorage.setItem('yx.events',JSON.stringify(rows.slice(-500)));void flushEvents()}
export async function flushEvents(){if(flushing)return;flushing=true;try{let rows=pending();while(rows.length){const item=rows[0];try{await api('/conversations/'+item.cid+'/events',item.body)}catch(e){if(e instanceof ApiError&&['not_authorized','event_not_allowed','unauthenticated'].includes(e.code)){rows.shift();localStorage.setItem('yx.events',JSON.stringify(rows));continue}break}const next=pending().filter(x=>x.body.id!==item.body.id);localStorage.setItem('yx.events',JSON.stringify(next));rows=next}}finally{flushing=false}}
