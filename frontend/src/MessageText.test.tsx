// @vitest-environment jsdom
import {render, screen, cleanup} from '@testing-library/react';
import '@testing-library/jest-dom/vitest';
import {afterEach, describe, it, expect} from 'vitest';
import {MessageText, safeMarkdownUrl} from './MessageText';
afterEach(cleanup);
const sample = '文档排版示例：\n\n1. **第一条规则**：\n   - **适用条件**：示例条件。\n   - **补充要求**：示例内容。\n\n2. **第二条规则**：\n   - 另一个示例项目。\n\n3. **第三条规则**：\n   - 普通正文内容。\n\n*注：这是格式说明。*';
describe('safe assistant Markdown', () => {
  it('renders the reported ordered/nested list, bold and italic without markers', () => {
    const {container} = render(<MessageText text={sample} role="assistant"/>);
    expect(container.querySelectorAll('ol > li')).toHaveLength(3);
    expect(container.querySelectorAll('ol > li > ul')).toHaveLength(3);
    expect(screen.getByText('第一条规则').tagName).toBe('STRONG');
    expect(screen.getByText('注：这是格式说明。').tagName).toBe('EM');
    expect(container).not.toHaveTextContent('**');
  });
  it('keeps consumer messages literal', () => {
    const {container} = render(<MessageText text={'**原文**\n<script>alert(1)</script>'} role="user"/>);
    expect(container).toHaveTextContent('**原文**');
    expect(container).toHaveTextContent('<script>alert(1)</script>');
    expect(container.querySelector('strong,script')).toBeNull();
  });
  it('supports tables, headings, quotes, code and literal escaped Markdown', () => {
    const {container} = render(<MessageText role="assistant" text={'## 说明\n\n> 仅供参考\n\n| 项目 | 内容 |\n| --- | --- |\n| 天数 | 7 |\n\n~~~text\n**代码原文**\n~~~\n\n\\*字面星号\\*'}/>);
    expect(screen.getByRole('heading', {name:'说明'})).toBeInTheDocument();
    expect(screen.getByRole('table')).toBeInTheDocument();
    expect(container.querySelector('blockquote')).toHaveTextContent('仅供参考');
    expect(container.querySelector('pre code')).toHaveTextContent('**代码原文**');
    expect(container).toHaveTextContent('*字面星号*');
  });
  it('does not execute raw HTML or create business action controls', () => {
    const {container} = render(<MessageText role="assistant" text={'<script>window.hacked=1</script>\n\n<img src="https://bad.test/pixel" onerror="alert(1)">\n\n<button>确认提交</button>\n\n<iframe src="https://bad.test"></iframe>\n\n正常 **回答**'}/>);
    expect(container.querySelector('script,img,button,iframe')).toBeNull();
    expect(screen.getByText('回答')).toBeInTheDocument();
  });
  it('blocks dangerous and internal action links', () => {
    const {container} = render(<MessageText role="assistant" text={'[脚本](javascript:alert%281%29) [数据](data:text/html,x) [内网](/api/v1/control) [相对](//bad.test)'}/>);
    expect(container.querySelector('a')).toBeNull();
    expect(container).toHaveTextContent('脚本');
  });
  it.each(['javascript:alert(1)', 'JaVaScRiPt:alert(1)', 'data:text/html,x', 'http://bad.test', '//bad.test', '/api/v1/a', 'https://user:pass@example.com', 'https://example.com/\nfoo'])('rejects URL %s', url => {
    expect(safeMarkdownUrl(url)).toBe('');
  });
  it('allows explicit HTTPS links with safe external-link attributes', () => {
    render(<MessageText role="assistant" text={'[资料](https://example.com/policy)'}/>);
    const link = screen.getByRole('link', {name:'资料'});
    expect(link).toHaveAttribute('href', 'https://example.com/policy');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });
  it('replaces remote Markdown images with text without an image request', () => {
    const {container} = render(<MessageText role="assistant" text={'![示例图片](https://bad.test/pixel.png)'}/>);
    expect(container.querySelector('img')).toBeNull();
    expect(container).toHaveTextContent('图片未加载：示例图片');
  });
  it('renders task list checks as non-interactive text', () => {
    const {container} = render(<MessageText role="assistant" text={'- [x] 已核对\n- [ ] 待确认'}/>);
    expect(container.querySelector('input,button')).toBeNull();
    expect(screen.getByRole('img', {name:'已勾选（仅文本）'})).toBeInTheDocument();
  });
  it('preserves plain text, line breaks and an empty reply', () => {
    const {container,rerender} = render(<MessageText role="assistant" text={'第一行\n第二行\n\n下一段'}/>);
    expect(container.querySelectorAll('p')).toHaveLength(2);
    expect(container.querySelector('p')?.textContent).toBe('第一行\n第二行');
    rerender(<MessageText role="assistant" text=""/>);
    expect(container.querySelector('.markdown-message')).toBeEmptyDOMElement();
  });
});
