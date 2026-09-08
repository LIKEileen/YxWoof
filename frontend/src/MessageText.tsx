import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import './message-text.css';

// Neither model replies nor retrieved documents may create actions, fetch images, or execute HTML.
export function safeMarkdownUrl(value: string): string {
  if (/^#user-content-fn(?:ref)?-[\w-]+$/.test(value)) return value;
  try {
    if (!/^https:\/\//i.test(value) || /[\u0000-\u0020\u007f]/.test(value)) return '';
    const url = new URL(value);
    return url.protocol === 'https:' && !url.username && !url.password ? url.href : '';
  } catch { return ''; }
}

const allowedElements = ['p', 'strong', 'em', 'del', 'ul', 'ol', 'li', 'blockquote',
  'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'pre', 'code', 'hr', 'br', 'a', 'img',
  'table', 'thead', 'tbody', 'tr', 'th', 'td', 'input', 'sup', 'section'];

export function PlainText({text}: {text: string}) {
  return <div className="plain-message" data-content-format="plain">{text.split('\n').map((line, i) => <p key={i}>{line || ' '}</p>)}</div>;
}

export function MessageText({text, role}: {text: string; role: string}) {
  return role === 'assistant' ? <SafeMarkdown text={text}/> : <PlainText text={text}/>;
}

export function SafeMarkdown({text, context = 'reply'}: {text: string; context?: 'reply' | 'evidence'}) {
  return <div className="markdown-message" data-content-format="markdown">
    <Markdown remarkPlugins={[remarkGfm]} skipHtml allowedElements={allowedElements}
      urlTransform={(url, key) => key === 'href' ? safeMarkdownUrl(url) : ''}
      components={{
        a: ({href, children}) => {
          const safe = safeMarkdownUrl(href || '');
          if (!safe) return <span>{children}</span>;
          return safe.startsWith('#')
            ? <a href={safe}>{children}</a>
            : <a href={safe} target="_blank" rel="noopener noreferrer" title="外部链接；业务依据请查看来源卡片">{children}</a>;
        },
        img: ({alt}) => <span className="markdown-image-note">[图片未加载{alt ? '：' + alt : ''}]</span>,
        input: ({checked}) => <span className="markdown-check" role="img" aria-label={checked ? '已勾选（仅文本）' : '未勾选（仅文本）'}>{checked ? '☑' : '☐'}</span>,
        table: ({children}) => <div className="markdown-table-scroll" tabIndex={0} role="region" aria-label={context === 'evidence' ? '证据表格，可横向滚动' : '回复表格，可横向滚动'}><table>{children}</table></div>,
        h1: ({children}) => <h3>{children}</h3>,
        h2: ({children}) => context === 'evidence' ? <h4>{children}</h4> : <h3>{children}</h3>,
      }}>{text}</Markdown>
  </div>;
}
