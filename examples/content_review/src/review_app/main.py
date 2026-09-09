"""FastAPI 入口。

根路径 ``/`` 提供零依赖浏览器演示页：提交主题 → 实时观看流式生成 →
出现人工决策按钮（通过/修订/驳回）→ 观看分支执行与最终产物。
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from obei_workflow_sdk import create_workflow_router

from .container import get_runtime


runtime = get_runtime()
app = FastAPI(title="Content Review Workflow Example", version="0.1.0")
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
<title>Obei 工作流 SDK — 内容复核 Demo</title>
<style>
  body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; margin: 24px; background: #f5f7fa; }
  h1 { font-size: 20px; }
  .row { margin: 8px 0; }
  label { display: inline-block; width: 90px; }
  input, textarea { width: 480px; padding: 6px; border: 1px solid #ccc; border-radius: 4px; }
  button { padding: 8px 16px; margin-right: 8px; border: none; border-radius: 4px; cursor: pointer; }
  #submit { background: #1f6feb; color: #fff; }
  #confirm { background: #16a34a; color: #fff; }
  #revise { background: #d97706; color: #fff; }
  #reject { background: #dc2626; color: #fff; }
  .panel { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; margin-top: 16px; }
  .panel h2 { font-size: 14px; margin: 0 0 8px 0; color: #334155; }
  #status { font-weight: 600; }
  #stream { white-space: pre-wrap; min-height: 60px; max-height: 220px; overflow-y: auto; }
  #log { font-size: 12px; color: #64748b; max-height: 160px; overflow-y: auto; }
  #decision { display: none; }
  #result { white-space: pre-wrap; }
  .hidden { display: none; }
</style>
</head>
<body>
<h1>Obei 工作流 SDK — 内容生成 + 人工复核 Demo</h1>
<div class="row"><label>内容主题：</label><input id="topic" value="给新员工写一段工作流 SDK 学习建议"></div>
<div class="row"><label>附加要求：</label><textarea id="requirements" rows="2">控制在 200 字以内，语气亲切</textarea></div>
<div class="row"><button id="submit">提交工作流</button></div>

<div class="panel">
  <h2>任务状态</h2>
  <div id="status">尚未提交</div>
</div>

<div class="panel">
  <h2>实时事件流（节点生命周期 + LLM 逐字输出）</h2>
  <div id="stream"></div>
</div>

<div class="panel" id="decision">
  <h2>人工复核：请对初稿做出决策</h2>
  <div class="row"><label>反馈意见：</label><textarea id="feedback" rows="2" placeholder="驳回/修订时填写"></textarea></div>
  <div class="row">
    <button id="confirm">✅ 通过（CONFIRM）</button>
    <button id="revise">📝 修订（REVISE）</button>
    <button id="reject">⛔ 驳回（REJECT）</button>
  </div>
</div>

<div class="panel hidden" id="resultPanel">
  <h2>最终产物</h2>
  <div id="result"></div>
</div>

<div class="panel">
  <h2>事件日志</h2>
  <div id="log"></div>
</div>

<script>
let taskId = null;
let decisionKey = null;
let artifactRef = null;
let artifactVersion = null;
let contentHash = null;
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
  document.getElementById('decision').style.display = 'none';
  document.getElementById('resultPanel').classList.add('hidden');
  const body = {
    workflow_type: 'content_review',
    actor_id: 'demo-user',
    input: {
      topic: document.getElementById('topic').value,
      requirements: document.getElementById('requirements').value,
    },
  };
  const r = await fetch('/api/v1/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
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
    'llm_token', 'llm_start', 'llm_end', 'context_token',
    'workflow_waiting_user', 'workflow_interrupt', 'artifact_created', 'workflow_finished'];
  types.forEach(t => source.addEventListener(t, e => handle(t, JSON.parse(e.data))));
  source.onmessage = e => handle('message', JSON.parse(e.data));
  source.onerror = () => log('SSE 连接中断（任务已结束时会自动关闭）');
}

function handle(type, d) {
  log(type + (d.node_name ? ' @' + d.node_name : ''));
  if (type === 'llm_token') {
    document.getElementById('stream').textContent += d.content || '';
  } else if (type === 'context_token') {
    document.getElementById('stream').textContent += '\\n[' + (d.content || '') + ']';
  } else if (type === 'workflow_waiting_user' || type === 'workflow_interrupt') {
    decisionKey = d.payload && d.payload.decision_key;
    artifactRef = d.payload && d.payload.artifact_ref;
    document.getElementById('decision').style.display = 'block';
    setStatus('等待人工决策（decision_key=' + decisionKey + '）');
    loadDraftForReview();
  } else if (type === 'node_succeeded') {
    setStatus('节点完成：' + d.node_name);
  } else if (type === 'workflow_finished') {
    setStatus('工作流结束：' + d.status);
    loadResult();
  } else if (type === 'node_failed') {
    setStatus('节点失败：' + d.node_name + ' ' + JSON.stringify(d.payload || {}));
  }
}

async function loadDraftForReview() {
  if (!artifactRef) return;
  const a = await (await fetch('/api/v1/artifacts/' + artifactRef)).json();
  artifactVersion = a.version;
  contentHash = a.content_hash;
  const el = document.getElementById('stream');
  el.textContent += '\\n\\n════ 待复核初稿全文（version=' + a.version + '）════\\n' + (a.content || '') + '\\n════════════════════════════════\\n';
  log('已加载初稿 Artifact：' + artifactRef + ' v' + a.version);
}

async function decide(decision) {
  const body = {
    decision_key: decisionKey,
    decision: decision,
    feedback: document.getElementById('feedback').value || '',
    actor_id: 'demo-user',
    artifact_id: artifactRef,
    artifact_version: artifactVersion,
    content_hash: contentHash,
  };
  const r = await fetch('/api/v1/tasks/' + taskId + '/decisions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const t = await r.json();
  document.getElementById('decision').style.display = 'none';
  setStatus('决策已提交：' + decision + ' → ' + t.status);
}

async function loadResult() {
  const t = await (await fetch('/api/v1/tasks/' + taskId)).json();
  if (!t.final_artifact_id) return;
  const a = await (await fetch('/api/v1/artifacts/' + t.final_artifact_id)).json();
  document.getElementById('result').textContent = '[' + a.artifact_type + ']\\n' + (a.content || '');
  document.getElementById('resultPanel').classList.remove('hidden');
}

document.getElementById('submit').addEventListener('click', submitTask);
document.getElementById('confirm').addEventListener('click', () => decide('CONFIRM'));
document.getElementById('revise').addEventListener('click', () => decide('REVISE'));
document.getElementById('reject').addEventListener('click', () => decide('REJECT'));
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def demo_page():
    return DEMO_PAGE
