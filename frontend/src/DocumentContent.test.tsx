// @vitest-environment jsdom
import {render, screen, cleanup} from '@testing-library/react';
import '@testing-library/jest-dom/vitest';
import {afterEach, describe, it, expect} from 'vitest';
import {EvidenceContent, ServiceSummary} from './DocumentContent';
import {Modal, PreviewCard} from './components';
afterEach(cleanup);
describe('evidence document rendering and plain-text boundaries', () => {
  it('renders nested lists, bold, italic, quotes and code within documents', () => {
    const {container}=render(<EvidenceContent text={'# 证据\n\n1. **规则**\n   - *限制*\n\n> 合成资料\n\n~~~text\n**代码保持原样**\n~~~'}/>);
    expect(container.querySelector('ol > li > ul > li em')).toHaveTextContent('限制');
    expect(container.querySelector('strong')).toHaveTextContent('规则');
    expect(container.querySelector('pre code')).toHaveTextContent('**代码保持原样**');
  });
  it('applies the same security controls to evidence as replies', () => {
    const {container}=render(<EvidenceContent text={'# 安全\n\n<script>alert(1)</script>\n\n<button>伪造确认</button>\n\n[脚本](javascript:alert%281%29) ![追踪](https://bad.test/pixel) [操作](/api/v1/control)'}/>);
    expect(container.querySelector('script,button,a,img,iframe')).toBeNull();
    expect(container).toHaveTextContent('图片未加载：追踪');
  });
  it('keeps summary content literal, including consumer/product symbols', () => {
    const text='YxWoof 服务摘要\n商品：**原始名称**\n订单：<原编号>\n\n此摘要不代表真人接管。';
    const {container}=render(<ServiceSummary text={text}/>);
    expect(container.querySelector('[data-content-format="plain"]')).not.toBeNull();
    expect(container.querySelector('strong')).toBeNull();
    expect(container).toHaveTextContent('**原始名称**');
    expect(container).toHaveTextContent('<原编号>');
    expect(container.querySelectorAll('p')).toHaveLength(5);
  });
  it('keeps editable application reasons literal', () => {
    const preview={id:'p',content_hash:'x',policy_version:'v1',invalidated:false,expires_at:new Date(Date.now()+60000).toISOString(),
      content:{merchant:'店铺',order_id:'O-1',product:'商品',spec:'原款',type:'退货申请',reason:'**不要改写** <原始描述>',condition:'完好未使用',quantity:1,order_amount_cents:100,notice:'仅申请'}};
    render(<PreviewCard preview={preview} onConfirm={()=>{}} onCancel={()=>{}}/>);
    expect(screen.getByText('**不要改写** <原始描述>').tagName).toBe('DD');
  });
  it('handles an empty or plain evidence document without inventing content', () => {
    const {rerender,container}=render(<EvidenceContent text=""/>);
    expect(container.querySelector('.markdown-message')).toBeEmptyDOMElement();
    rerender(<EvidenceContent text={'普通资料第一行\n第二行'}/>);
    expect(container.querySelector('p')?.textContent).toBe('普通资料第一行\n第二行');
  });
  it('includes scrollable evidence tables in the modal focus cycle', () => {
    render(<Modal title="证据" onClose={()=>{}}><EvidenceContent text={'| 项目 | 条件 |\n| --- | --- |\n| 期限 | 7 天 |'}/></Modal>);
    const dialog=screen.getByRole('dialog');
    expect(dialog.querySelectorAll('button,[tabindex]:not([tabindex="-1"])')).toHaveLength(2);
    expect(screen.getByRole('region',{name:'证据表格，可横向滚动'})).toHaveAttribute('tabindex','0');
  });
});
