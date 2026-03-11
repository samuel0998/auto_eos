function setStatus(msg) {
  document.getElementById('status').textContent = msg;
}

async function loadMetrics() {
  const res = await fetch('/api/metrics');
  const data = await res.json();
  const items = data.items || [];

  const tbody = document.getElementById('reportTableBody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty-state">Nenhum dado carregado.</td></tr>';
    return;
  }

  const latest = items[0];
  document.getElementById('sumFunctions').textContent = latest.functions_count ?? 0;
  document.getElementById('sumAssociates').textContent = latest.associates_count ?? 0;
  document.getElementById('sumDpmo').textContent = latest.dpmo ?? 0;
  document.getElementById('sumRei').textContent = latest.receive_error_indicator ?? 0;
  document.getElementById('sumHourStart').textContent = `${latest.hour_start ?? '-'}h`;
  document.getElementById('sumHourEnd').textContent = `${latest.hour_end ?? '-'}h`;
  document.getElementById('infoData').textContent = latest.metric_date || '-';
  document.getElementById('infoProcesso').textContent = 'Receive';

  document.getElementById('justificativa').value = latest.justificativa || '';
  document.getElementById('apollo').value = latest.apollo || '';
  document.getElementById('diveDeepText').value = latest.dive_deep || '';
  document.getElementById('callOutsText').value = latest.call_outs || '';

  tbody.innerHTML = items.map(item => `
    <tr>
      <td>${item.metric_date || ''}</td>
      <td>${item.hour_start ?? ''}h-${item.hour_end ?? ''}h</td>
      <td>${item.functions_count ?? '-'}</td>
      <td>${item.associates_count ?? '-'}</td>
      <td>${item.dpmo ?? '-'}</td>
      <td>${item.receive_error_indicator ?? '-'}</td>
      <td>${item.triggered_by || '-'}</td>
      <td>${item.collected_at || '-'}</td>
    </tr>
  `).join('');
}

async function pullNow() {
  setStatus('Executando coleta...');
  const res = await fetch('/api/pull-now', { method: 'POST' });
  const data = await res.json();
  setStatus(data.message || JSON.stringify(data));
  await loadMetrics();
}

async function saveManual() {
  const payload = {
    metric_date: document.getElementById('manualDate').value,
    hour_start: Number(document.getElementById('manualStart').value),
    hour_end: Number(document.getElementById('manualEnd').value),
    justificativa: document.getElementById('justificativa').value,
    apollo: document.getElementById('apollo').value,
    dive_deep: document.getElementById('diveDeepText').value,
    call_outs: document.getElementById('callOutsText').value,
  };

  const res = await fetch('/api/manual-fields', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  setStatus(data.ok ? 'Campos manuais salvos.' : `Falha: ${data.message || 'sem atualização'}`);
  await loadMetrics();
}

async function checkSession() {
  const res = await fetch('/fclm/session/status');
  const data = await res.json();
  setStatus(data.session_ready ? 'Sessão Midway pronta.' : 'Sessão Midway ausente. Use /fclm/session/init ou /fclm/session/upload.');
}

document.getElementById('btnBuscar').addEventListener('click', pullNow);
document.getElementById('btnSaveManual').addEventListener('click', saveManual);
document.getElementById('btnSession').addEventListener('click', checkSession);

loadMetrics();
