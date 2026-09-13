export type Session = {cid: string; csrf: string; profile?: string};
export type Issue = {id: string; turn_id: string; goal: string; goal_label: string; question: string; order_id: string | null;
 status: string; active: boolean; fields: Record<string,string>; dependencies: string[]; context_version: number;
 clarification?: {message:string; options:string[]}; action_id:string; result: Record<string,unknown>};
export type Order={id:string;merchant_id:string;merchant_name:string;product:string;spec:string;price_cents:number;status:string;delivery_days:number|null;ordered_date:string;logistics:{time:string;text:string}[];updated_at:string;version:number};
export type Message = {id:string; task_id:string|null; turn_id:string|null; role:string; text:string; source_ids:string[];
 cards?:{type:string;[key:string]:unknown}[];
 control_version:number; created_at:string; feedback?:{id:string;helpful:boolean;category:string;comment:string}};
export type Preview = {id:string;action_id:string;content_hash:string;content:Record<string,unknown>;invalidated:boolean;expires_at:string;policy_version:string};
export type Action = {id:string;status:string;application_id:string|null;reconcile_count:number};
export type State = {id:string;api_version:2;consumer:{id:string;name:string};automation_state:'active'|'paused';control_version:number;
 view_version:number;state_version:number;server_time:string;accepting_requests:boolean;order:Order|null;issues:Issue[];
 previews:Record<string,Preview>;actions:Record<string,Action>;messages:Message[];turn:{id:string;status:string;phase:string;deadline_at:string}|null};
export type Source = {id:string;title:string;content:string;revision:number;content_hash:string;policy_version:string;valid_from:string;valid_until:string;time_basis:string;notice:string};

export class APIError extends Error {constructor(public code:string, message:string, public status=0){super(message)}}
export const uuid = () => crypto.randomUUID();
export const saved = ():Session|null => {try{return JSON.parse(localStorage.getItem('yx.v2.session')||'null')}catch{return null}};
export const save = (s:Session) => localStorage.setItem('yx.v2.session', JSON.stringify(s));
export function acceptState(previous:State|null,next:State){
 return !previous || previous.id!==next.id || (next.control_version>=previous.control_version && next.view_version>=previous.view_version);
}
export async function request<T>(path:string, body?:unknown, options:{signal?:AbortSignal;timeout?:number;internal?:boolean;csrf?:string}={}):Promise<T>{
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),options.timeout??20000);
 const abort=()=>controller.abort();options.signal?.addEventListener('abort',abort,{once:true});
 if(options.signal?.aborted)controller.abort();
 try{
  const response=await fetch('/api/v2'+(options.internal?'/internal':'')+path,{method:body===undefined?'GET':'POST',credentials:'same-origin',
   headers:{'Content-Type':'application/json','X-CSRF-Token':options.csrf??saved()?.csrf??''},
   body:body===undefined?undefined:JSON.stringify(body),signal:controller.signal});
  const value=await response.json();if(!response.ok)throw new APIError(value.error,value.message||'操作未完成。',response.status);
  return value;
 }catch(e){if(e instanceof APIError)throw e;throw new APIError('connection_failed','服务暂时不可用。请查看处理状态，或稍后重试。')}
 finally{clearTimeout(timer);options.signal?.removeEventListener('abort',abort)}
}
export async function stream(cid:string,body:unknown,signal:AbortSignal,onState:(s:State)=>void,onProgress:(s:string)=>void){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),35000);
 const abort=()=>controller.abort();signal.addEventListener('abort',abort,{once:true});if(signal.aborted)controller.abort();
 let reader:ReadableStreamDefaultReader<Uint8Array>|undefined;
 try{
  const response=await fetch('/api/v2/conversations/'+cid+'/chat',{method:'POST',credentials:'same-origin',signal:controller.signal,
   headers:{'Content-Type':'application/json','X-CSRF-Token':saved()?.csrf??''},body:JSON.stringify(body)});
  if(!response.ok){const v=await response.json();throw new APIError(v.error,v.message,response.status)}
  reader=response.body?.getReader();if(!reader)throw new Error('No stream');
  const decoder=new TextDecoder();let buffer='',done=false;
  while(true){const chunk=await reader.read();if(chunk.done)break;buffer+=decoder.decode(chunk.value,{stream:true}).replace(/\r\n/g,'\n');
   let boundary:number;while((boundary=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,boundary);buffer=buffer.slice(boundary+2);
    const lines=frame.split('\n'),event=lines.find(l=>l.startsWith('event:'))?.slice(6).trim();
    const payload=lines.filter(l=>l.startsWith('data:')).map(l=>l.slice(5).trim()).join('\n');if(!payload)continue;
    const value=JSON.parse(payload);if(event==='state')onState(value);if(event==='progress')onProgress(value.phase||'processing');
    if(event==='error')throw new APIError(value.error,value.message);if(event==='done')done=true;
   }
  }
  if(!done)throw new APIError('stream_interrupted','连接已中断，请查看处理状态；不要重复提交申请。');
 }catch(e){if(signal.aborted)return;if(e instanceof APIError)throw e;throw new APIError('turn_timeout','本轮等待已结束。可以查看处理状态或稍后明确继续。')}
 finally{clearTimeout(timer);signal.removeEventListener('abort',abort);if(reader)await reader.cancel().catch(()=>{})}
}
