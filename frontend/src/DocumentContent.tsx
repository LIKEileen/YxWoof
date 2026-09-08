import {PlainText, SafeMarkdown} from './MessageText';

// Explicit formats: policy documents are Markdown; generated business summaries are plain text.
export function EvidenceContent({text}: {text: string}) {
  return <article className="source-content markdown-document" aria-label="证据文档正文">
    <SafeMarkdown text={text} context="evidence"/>
  </article>;
}
export function ServiceSummary({text}: {text: string}) {
  return <section className="summary-text plain-document" aria-label="可复制的服务摘要">
    <PlainText text={text}/>
  </section>;
}
