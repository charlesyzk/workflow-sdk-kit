"""FastAPI 入口。"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from obei_workflow_sdk import create_workflow_router

from .container import get_runtime


runtime = get_runtime()
app = FastAPI(title="Simple Workflow Example", version="0.1.0")
app.include_router(create_workflow_router(runtime))


@app.get("/health")
def health():
    return {"status": "ok", "workflow_types": runtime.registry.types()}


@app.get("/ready")
def ready():
    with runtime.storage.engine.connect() as connection:
        connection.exec_driver_sql("SELECT 1")
    return {"status": "ready"}


DEMO_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>LLM 流式 / 非流式 兼容演示</title>
<style>
  body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; margin: 24px; background: #f5f7fa; }
  h1 { font-size: 20px; }
  .row { margin: 8px 0; }
  input { width: 480px; padding: 6px; border: 1px solid #ccc; border-radius: 4px; }
  button { padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; background: #1f6feb; color: #fff; }
  .panel { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; margin-top: 16px; }
  .panel h2 { font-size: 14px; margin: 0 0 8px 0; color: #334155; }
  #status { font-weight: 600; }
  #stream { white-space: pre-wrap; min-height: 60px; max-height: 200px; overflow-y: auto; }
  #answer { white-space: pre-wrap; min-height: 40px; }
  #log { font-size: 12px; color: #64748b; max-height: 180px; overflow-y: auto; }
</style>
</head>
<body>
<h1>LLM 流式 / 非流式 兼容演示（deepseek-v4-flash）</h1>
<p>四节点：校验输入 → 准备上下文 → <b style="color:#1f6feb">流式生成草稿（逐字）</b> → <b style="color:#16a34a">非流式润色回答（一次性）</b></p>
<div class="row"><input id="question" value="用一句话说明什么是工作流"><button id="submit">提交工作流</button></div>

<div class="panel"><h2>任务状态</h2><div id="status">尚未提交</div></div>

<div class="panel"><h2>流式输出：generate_draft（llm_token 逐字出现）</h2><div id="stream"></div></div>

<div class="panel"><h2>最终回答：polish_answer（非流式，llm_end 一次性出现）</h2><div id="answer"></div></div>

<div class="panel"><h2>事件日志</h2><div id="log"></div></div>

<script>
let taskId = null;
let source = null;

function log(text) {
  const el = document.getElementById('log');
  const line = document.createElement('div');
  line.textContent = '[' + new Date().toLocaleTimeString() + '] ' + text;
  el.prepend(line);
}

function setStatus(text) {
  document.getElementById('status').textContent = text;
  log('状态 → ' + text);
}

async function submitTask() {
  document.getElementById('stream').textContent = '';
  document.getElementById('answer').textContent = '';
  const r = await fetch('/api/v1/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      workflow_type: 'simple_llm_workflow',
      actor_id: 'demo-user',
      input: { question: document.getElementById('question').value },
    }),
  });
  const t = await r.json();
  taskId = t.task_id;
  setStatus('已提交 ' + taskId + '，状态 ' + t.status);
  openStream(taskId);
}

function openStream(id) {
  if (source) source.close();
  source = new EventSource('/api/v1/tasks/' + id + '/events');
  const types = ['workflow_started', 'node_started', 'node_succeeded', 'node_failed',
    'llm_start', 'llm_end', 'llm_token', 'llm_reasoning_token', 'context_token',
    'artifact_created', 'workflow_finished'];
  types.forEach(t => source.addEventListener(t, e => handle(t, JSON.parse(e.data))));
  source.onerror = () => log('SSE 连接中断（任务结束时会自动关闭）');
}

function handle(type, d) {
  log(type + (d.node_name ? ' @' + d.node_name : ''));
  if (type === 'llm_token' || type === 'llm_reasoning_token') {
    // 只有流式节点 generate_draft 会走到这里；非流式 polish_answer 不会有 token。
    document.getElementById('stream').textContent += d.content || '';
  } else if (type === 'context_token') {
    document.getElementById('stream').textContent += '\\n[' + (d.content || '') + ']';
  } else if (type === 'node_succeeded') {
    setStatus('节点完成：' + d.node_name);
  } else if (type === 'llm_end') {
    log('llm_end @' + d.node_name + '（该节点本次是否逐字输出，见上方 llm_token 日志）');
  } else if (type === 'workflow_finished') {
    setStatus('工作流结束：' + d.status);
    loadAnswer();
  } else if (type === 'node_failed') {
    setStatus('节点失败：' + d.node_name + ' ' + JSON.stringify(d.payload || {}));
  }
}

async function loadAnswer() {
  const t = await (await fetch('/api/v1/tasks/' + taskId)).json();
  if (!t.final_artifact_id) return;
  const a = await (await fetch('/api/v1/artifacts/' + t.final_artifact_id)).json();
  document.getElementById('answer').textContent = a.content || '';
}

document.getElementById('submit').addEventListener('click', submitTask);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def demo_page():
    return DEMO_PAGE
