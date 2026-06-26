let allTopics = [];
let selectedTopic = '';

function initTopicDropdown() {
  const input = document.getElementById('topicSearch');
  const list = document.getElementById('topicList');

  input.addEventListener('click', () => {
    if (allTopics.length === 0) return;
    input.readOnly = false;
    renderTopicList(input.value);
    list.classList.add('open');
  });

  input.addEventListener('input', () => {
    renderTopicList(input.value);
    list.classList.add('open');
    selectedTopic = '';
  });

  document.addEventListener('click', (e) => {
    if (!document.getElementById('topicDropdown').contains(e.target)) {
      list.classList.remove('open');
      if (selectedTopic) input.value = selectedTopic;
      else if (!allTopics.includes(input.value)) input.value = selectedTopic;
    }
  });
}

function renderTopicList(filter) {
  const list = document.getElementById('topicList');
  const filtered = filter
    ? allTopics.filter(t => t.toLowerCase().includes(filter.toLowerCase()))
    : allTopics;

  list.innerHTML = filtered.map(t =>
    `<div class="topic-item${t === selectedTopic ? ' selected' : ''}" data-value="${t}">${t}</div>`
  ).join('') || '<div class="topic-item" style="color:#585b70">No matches</div>';

  list.querySelectorAll('.topic-item[data-value]').forEach(el => {
    el.addEventListener('click', () => {
      selectedTopic = el.dataset.value;
      document.getElementById('topicSearch').value = selectedTopic;
      document.getElementById('topicSearch').readOnly = true;
      list.classList.remove('open');
    });
  });
}

async function loadTopics() {
  const brokers = document.getElementById('brokers').value.trim();
  if (!brokers) return alert('Enter brokers first');

  const input = document.getElementById('topicSearch');
  input.value = 'Loading...';
  input.readOnly = true;

  try {
    const resp = await fetch('/api/topics?brokers=' + encodeURIComponent(brokers));
    const data = await resp.json();
    if (data.error) throw new Error(data.error);

    allTopics = data.topics;
    selectedTopic = '';
    input.value = '';
    input.placeholder = `Search among ${allTopics.length} topics…`;
    input.readOnly = false;
    renderTopicList('');
  } catch (e) {
    input.value = '';
    input.placeholder = '— error loading —';
    showError(e.message);
  }
}

async function search() {
  const brokers = document.getElementById('brokers').value.trim();
  const topic = selectedTopic;
  const query = document.getElementById('query').value.trim();
  const field = document.getElementById('field').value.trim();
  const fromTs = document.getElementById('from_ts').value;
  const limit = parseInt(document.getElementById('limit').value) || 100;

  if (!brokers) return alert('Enter brokers');
  if (!topic) return alert('Select a topic');

  setLoading(true);
  hideError();
  document.getElementById('results').style.display = 'none';

  const overridesRaw = document.getElementById('overrides').value.trim();
  const broker_overrides = {};
  if (overridesRaw) {
    overridesRaw.split(',').forEach(pair => {
      const [from, to] = pair.trim().split('=');
      if (from && to) broker_overrides[from.trim()] = to.trim();
    });
  }

  const body = {
    brokers: brokers.split(',').map(b => b.trim()),
    broker_overrides,
    topic,
    query,
    field,
    from_timestamp: fromTs ? new Date(fromTs).getTime() : 0,
    limit,
  };

  try {
    const resp = await fetch('/api/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (data.error) throw new Error(data.error);

    renderResults(data);
  } catch (e) {
    showError(e.message);
  } finally {
    setLoading(false);
  }
}

function renderResults(data) {
  const tbody = document.getElementById('tbody');
  const statusText = document.getElementById('status-text');

  statusText.innerHTML =
    `Found <span class="badge green">${data.messages.length}</span> &nbsp;` +
    `Scanned <span class="badge blue">${data.scanned.toLocaleString()}</span> &nbsp;` +
    `Partitions <span class="badge ${data.partitions_searched < data.partitions_total ? 'red' : 'blue'}">${data.partitions_searched}/${data.partitions_total}</span>`;

  const warnEl = document.getElementById('error');
  if (data.warning) {
    warnEl.style.background = '#2d2a1a';
    warnEl.style.borderColor = '#f9e2af';
    warnEl.style.color = '#f9e2af';
    warnEl.textContent = '⚠ ' + data.warning;
    warnEl.style.display = 'block';
  } else {
    warnEl.style.display = 'none';
  }

  tbody.innerHTML = '';

  if (data.messages.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty">No messages found</td></tr>';
  } else {
    data.messages.forEach(msg => {
      const tr = document.createElement('tr');
      const ts = new Date(msg.timestamp).toLocaleString();
      const keyStr = msg.key || '—';
      let valueObj;
      try { valueObj = JSON.parse(msg.value); } catch { valueObj = msg.value; }
      const preview = JSON.stringify(valueObj).slice(0, 120);
      const pretty = syntaxHighlight(JSON.stringify(valueObj, null, 2));

      tr.innerHTML = `
        <td class="td-partition">${msg.partition}</td>
        <td class="td-offset">${msg.offset}</td>
        <td class="td-ts">${ts}</td>
        <td class="td-key" title="${keyStr}">${keyStr}</td>
        <td class="td-value">
          <div class="value-preview">${escapeHtml(preview)}${preview.length >= 120 ? '…' : ''}</div>
          <div class="value-full">${pretty}</div>
        </td>`;

      tr.addEventListener('click', () => tr.classList.toggle('expanded'));
      tbody.appendChild(tr);
    });
  }

  document.getElementById('results').style.display = 'block';
}

function syntaxHighlight(json) {
  return escapeHtml(json).replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
    match => {
      if (/^"/.test(match)) {
        if (/:$/.test(match)) return `<span class="json-key">${match}</span>`;
        return `<span class="json-string">${match}</span>`;
      }
      if (/true|false/.test(match)) return `<span class="json-bool">${match}</span>`;
      if (/null/.test(match)) return `<span class="json-null">${match}</span>`;
      return `<span class="json-number">${match}</span>`;
    }
  );
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function setLoading(on) {
  document.getElementById('status').style.display = 'flex';
  document.getElementById('spinner').style.display = on ? 'block' : 'none';
  if (on) document.getElementById('status-text').textContent = 'Searching…';
}

function showError(msg) {
  const el = document.getElementById('error');
  el.textContent = msg;
  el.style.display = 'block';
}

function hideError() {
  document.getElementById('error').style.display = 'none';
}

// Pre-fill brokers from URL params
const params = new URLSearchParams(location.search);
if (params.get('brokers')) document.getElementById('brokers').value = params.get('brokers');

initTopicDropdown();
