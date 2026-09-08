// @vitest-environment jsdom
import {afterEach,describe,it,expect,vi} from 'vitest';
import {api,chatStream,SERVICE_MESSAGE,consumerError} from './api';
afterEach(()=>{vi.unstubAllGlobals();vi.useRealTimers();localStorage.clear();sessionStorage.clear()});
describe('technical fault boundary',()=>{
 it.each([500,503,429])('hides response cause %s',async status=>{
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(JSON.stringify({error:'sql_secret',message:'internal password'}),{status})));
  await expect(api('/orders')).rejects.toThrow(SERVICE_MESSAGE);
 });
 it.each(['network','invalid-json'])('normalizes %s',async kind=>{
  vi.stubGlobal('fetch',kind==='network'?vi.fn().mockRejectedValue(new TypeError('Failed to fetch')):vi.fn().mockResolvedValue(new Response('<html>SQL secret</html>')));
  await expect(api('/orders')).rejects.toThrow(SERVICE_MESSAGE);
 });
 it('does not retry writes',async()=>{
  const fetch=vi.fn().mockRejectedValue(new TypeError('network'));vi.stubGlobal('fetch',fetch);
  await expect(api('/confirm',{})).rejects.toThrow(SERVICE_MESSAGE);expect(fetch).toHaveBeenCalledTimes(1);
 });
 it('retains user-actionable business validation',async()=>{
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(JSON.stringify({error:'preview_expired',message:'预览已过期'}),{status:409})));
  await expect(api('/confirm',{})).rejects.toThrow('预览已过期');
 });
 it.each(['truncated','invalid-json','error','empty'])('handles SSE %s',async kind=>{
  const body=kind==='truncated'?'event: status\ndata: {}\n\n':kind==='invalid-json'?'event: state\ndata: invalid\n\n':kind==='error'?'event: error\ndata: {"error":"service_unavailable","message":"抱歉，服务暂时不可用，请稍后重试。"}\n\n':'';
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(body)));
  await expect(chatStream('c','text','i',undefined,vi.fn())).rejects.toThrow(SERVICE_MESSAGE);
 });
 it('accepts completed stream',async()=>{
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response('event: state\ndata: {"id":"c"}\n\nevent: done\ndata: {}\n\n')));
  const callback=vi.fn();await chatStream('c','text','i',undefined,callback);expect(callback).toHaveBeenCalledWith({id:'c'});
 });
 it('unknown exceptions never show raw text',()=>expect(consumerError(new Error('SQL password'))).toBe(SERVICE_MESSAGE));
 it.each(['api','sse'])('bounds hung %s requests',async kind=>{
  vi.useFakeTimers();
  vi.stubGlobal('fetch',vi.fn((_url,options)=>new Promise((_resolve,reject)=>{
   options.signal.addEventListener('abort',()=>reject(new DOMException('private timeout','AbortError')));
  })));
  const result=(kind==='api'?api('/orders'):chatStream('c','text','i',undefined,vi.fn())).catch(e=>e);
  await vi.advanceTimersByTimeAsync(41000);
  expect(((await result) as Error).message).toBe(SERVICE_MESSAGE);
 });
});
