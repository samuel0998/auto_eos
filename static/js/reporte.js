async function fetchMetrics() {
  const res = await fetch('/api/metrics');
  const data = await res.json();
  const tbody = document.getElementById('metrics-body');
  tbody.innerHTML = '';

  (data.items || []).forEach((item) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${item.metric_date || ''}</td>
      <td>${item.hour_start ?? ''}h - ${item.hour_end ?? ''}h</td>
      <td>${item.functions_count ?? '-'}</td>
      <td>${item.associates_count ?? '-'}</td>
      <td>${item.dpmo ?? '-'} / ${item.receive_error_indicator ?? '-'}</td>
      <td>${item.collected_at || ''}</td>
    `;
    tbody.appendChild(tr);
  });
}

async function pullNow() {
  const status = document.getElementById('pull-status');
  status.textContent = 'Executando coleta...';
  const res = await fetch('/api/pull-now', { method: 'POST' });
  const data = await res.json();
  status.textContent = data.message || JSON.stringify(data);
  await fetchMetrics();
}

async function saveManual(event) {
  event.preventDefault();
  const status = document.getElementById('manual-status');
  const form = event.target;
  const payload = Object.fromEntries(new FormData(form).entries());
  payload.hour_start = Number(payload.hour_start);
  payload.hour_end = Number(payload.hour_end);

  const res = await fetch('/api/manual-fields', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  const data = await res.json();
  status.textContent = data.ok ? 'Campos manuais salvos.' : `Falha: ${data.message || 'sem atualização'}`;
  await fetchMetrics();
}

document.getElementById('btn-pull').addEventListener('click', pullNow);
document.getElementById('manual-form').addEventListener('submit', saveManual);
fetchMetrics();
