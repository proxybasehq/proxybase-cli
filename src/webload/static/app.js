// ==========================================================================
// ProxyBase Webload — Reactive Single Page Application Logic
// ==========================================================================

(() => {
  'use strict';

  // Session & Local Storage Management (1-Hour Session Timeout)
  const savedToken = localStorage.getItem('webload_token') || '';
  const savedUsername = localStorage.getItem('webload_username') || '';
  const savedExpiresAt = parseInt(localStorage.getItem('webload_expires_at') || '0', 10);
  const isSessionFresh = savedToken && savedExpiresAt > Date.now();

  let sessionTimeoutTimer = null;

  function scheduleSessionTimeout(expiresAt) {
    if (sessionTimeoutTimer) {
      clearTimeout(sessionTimeoutTimer);
      sessionTimeoutTimer = null;
    }
    const remainingMs = expiresAt - Date.now();
    if (remainingMs <= 0) {
      showLoginScreen('Session timed out after 1 hour. Please log in again.');
      return;
    }
    sessionTimeoutTimer = setTimeout(() => {
      showToast('Session timed out after 1 hour. Logging out...', 'warning');
      handleLogout();
    }, remainingMs);
  }

  // Application State
  const state = {
    page: 1,
    limit: 50,
    search: '',
    status: 'all',
    country: 'all',
    category: 'all',
    sortBy: 'id',
    sortDir: 'asc',
    totalPages: 1,
    totalRecords: 0,
    filteredRecords: 0,
    selectedIds: new Set(),
    activeJobId: null,
    isRelayRunning: false,
    uploadedFile: null,
    ingestMode: 'local',
    token: isSessionFresh ? savedToken : '',
    username: isSessionFresh ? savedUsername : '',
    expiresAt: isSessionFresh ? savedExpiresAt : 0,
    eventSource: null,
    backendUrl: 'https://api.proxybase.xyz',
    activeUpstreamPaths: 0,
    activeStreams: 0,
    bytesPerSec: 0,
    // Wallet & Funds State
    walletAddress: '',
    sellerAvailable: 0,
    sellerPending: 0,
    sellerPayoutLocked: 0,
    buyerSpendable: 0,
    buyerReserved: 0,
    buyerSpent: 0,
    activeWalletTab: 'funds',
  };

  // DOM Elements Cache
  const els = {
    // Auth & Gateway
    loginScreen: document.getElementById('login-screen'),
    loginForm: document.getElementById('login-form'),
    loginUsername: document.getElementById('login-username'),
    loginPassword: document.getElementById('login-password'),
    btnLoginSubmit: document.getElementById('btn-login-submit'),
    loginSubmitSpinner: document.getElementById('login-submit-spinner'),
    loginSubmitText: document.getElementById('login-submit-text'),
    loginError: document.getElementById('login-error'),
    loginErrorText: document.getElementById('login-error-text'),
    btnTogglePassVisibility: document.getElementById('btn-toggle-pass-visibility'),
    headerUserPill: document.getElementById('header-user-pill'),
    headerUsername: document.getElementById('header-username'),
    btnLogout: document.getElementById('btn-logout'),

    // Stats
    statTotal: document.getElementById('stat-total'),
    statActive: document.getElementById('stat-active'),
    statPaused: document.getElementById('stat-paused'),
    statError: document.getElementById('stat-error'),
    statStreams: document.getElementById('stat-streams'),
    statThroughput: document.getElementById('stat-throughput'),
    relayStatusDot: document.getElementById('relay-status-dot'),
    relayBtnText: document.getElementById('relay-btn-text'),
    btnToggleRelay: document.getElementById('btn-toggle-relay'),

    // Production Gateway Status Banner
    backendStatusCard: document.getElementById('backend-status-card'),
    backendPulseDot: document.getElementById('backend-pulse-dot'),
    backendStatusHeadline: document.getElementById('backend-status-headline'),
    backendTagBadge: document.getElementById('backend-tag-badge'),
    backendStatusSub: document.getElementById('backend-status-sub'),
    backendMetricGateway: document.getElementById('backend-metric-gateway'),
    backendMetricPaths: document.getElementById('backend-metric-paths'),
    backendMetricActivity: document.getElementById('backend-metric-activity'),

    // Toolbar & Filters
    searchInput: document.getElementById('search-input'),
    searchClear: document.getElementById('search-clear'),
    statusPills: document.getElementById('status-filter-pills'),
    countrySelect: document.getElementById('country-select'),
    categorySelect: document.getElementById('category-select'),
    limitSelect: document.getElementById('limit-select'),
    btnRefresh: document.getElementById('btn-refresh'),

    // Table
    tbody: document.getElementById('proxy-tbody'),
    thCheckbox: document.getElementById('th-checkbox'),
    tableEmpty: document.getElementById('table-empty'),
    tableLoading: document.getElementById('table-loading'),
    sortHeaders: document.querySelectorAll('.proxy-table th.sortable'),

    // Bulk Bar
    bulkBar: document.getElementById('bulk-bar'),
    bulkCount: document.getElementById('bulk-selected-count'),
    btnBulkPause: document.getElementById('btn-bulk-pause'),
    btnBulkResume: document.getElementById('btn-bulk-resume'),
    btnBulkDelete: document.getElementById('btn-bulk-delete'),
    btnBulkClear: document.getElementById('btn-bulk-clear'),
    btnSelectAllFiltered: document.getElementById('btn-select-all-filtered'),

    // Pagination
    paginationInfo: document.getElementById('pagination-info'),
    pageInput: document.getElementById('page-input'),
    pageTotalText: document.getElementById('page-total-text'),
    btnPageFirst: document.getElementById('btn-page-first'),
    btnPagePrev: document.getElementById('btn-page-prev'),
    btnPageNext: document.getElementById('btn-page-next'),
    btnPageLast: document.getElementById('btn-page-last'),

    // Ingestion Banner
    ingestBanner: document.getElementById('ingest-banner'),
    ingestRateText: document.getElementById('ingest-rate-text'),
    ingestProgressBar: document.getElementById('ingest-progress-bar'),
    ingestLinesRead: document.getElementById('ingest-lines-read'),
    ingestValidCount: document.getElementById('ingest-valid-count'),
    ingestDupCount: document.getElementById('ingest-dup-count'),
    ingestWarnCount: document.getElementById('ingest-warn-count'),
    btnCancelIngest: document.getElementById('btn-cancel-ingest'),

    // Ingestion Modal
    btnOpenIngest: document.getElementById('btn-open-ingest'),
    modalIngest: document.getElementById('modal-ingest'),
    btnModalClose: document.getElementById('btn-modal-close'),
    btnModalCancel: document.getElementById('btn-modal-cancel'),
    btnStartLoad: document.getElementById('btn-start-load'),
    inputFilePath: document.getElementById('input-file-path'),
    modalTabs: document.querySelectorAll('#modal-ingest .modal-tab'),
    tabLocal: document.getElementById('tab-local'),
    tabUpload: document.getElementById('tab-upload'),
    dropZone: document.getElementById('drop-zone'),
    fileUploadInput: document.getElementById('file-upload-input'),
    uploadFileName: document.getElementById('upload-file-name'),

    // Wallet Modal & Payouts
    btnOpenWallet: document.getElementById('btn-open-wallet'),
    headerWalletBadge: document.getElementById('header-wallet-badge'),
    modalWallet: document.getElementById('modal-wallet'),
    btnWalletModalClose: document.getElementById('btn-wallet-modal-close'),
    btnWalletModalDone: document.getElementById('btn-wallet-modal-done'),
    walletStatusDot: document.getElementById('wallet-status-dot'),
    walletAddressDisplay: document.getElementById('wallet-address-display'),
    btnCopyWalletAddress: document.getElementById('btn-copy-wallet-address'),
    btnCopyText: document.getElementById('btn-copy-text'),
    walletTabs: document.querySelectorAll('.wallet-tabs .modal-tab'),
    walletTabFunds: document.getElementById('wallet-tab-funds'),
    walletTabWithdraw: document.getElementById('wallet-tab-withdraw'),
    walletTabHistory: document.getElementById('wallet-tab-history'),
    fundSellerAvailable: document.getElementById('fund-seller-available'),
    fundSellerAvailableUsd: document.getElementById('fund-seller-available-usd'),
    fundSellerPending: document.getElementById('fund-seller-pending'),
    fundSellerPendingUsd: document.getElementById('fund-seller-pending-usd'),
    fundSellerLocked: document.getElementById('fund-seller-locked'),
    fundSellerLockedUsd: document.getElementById('fund-seller-locked-usd'),
    fundBuyerSpendable: document.getElementById('fund-buyer-spendable'),
    fundBuyerSpendableUsd: document.getElementById('fund-buyer-spendable-usd'),
    fundBuyerReserved: document.getElementById('fund-buyer-reserved'),
    fundBuyerReservedUsd: document.getElementById('fund-buyer-reserved-usd'),
    fundBuyerSpent: document.getElementById('fund-buyer-spent'),
    fundBuyerSpentUsd: document.getElementById('fund-buyer-spent-usd'),
    btnQuickWithdraw: document.getElementById('btn-quick-withdraw'),
    withdrawAvailableHeadline: document.getElementById('withdraw-available-headline'),
    payoutForm: document.getElementById('payout-form'),
    payoutAddress: document.getElementById('payout-address'),
    payoutAddressStatus: document.getElementById('payout-address-status'),
    payoutAddressHint: document.getElementById('payout-address-hint'),
    btnUseOwnWallet: document.getElementById('btn-use-own-wallet'),
    payoutAmount: document.getElementById('payout-amount'),
    payoutAmountUsd: document.getElementById('payout-amount-usd'),
    btnPresets: document.querySelectorAll('.btn-preset'),
    payoutError: document.getElementById('payout-error'),
    payoutErrorText: document.getElementById('payout-error-text'),
    payoutSuccess: document.getElementById('payout-success'),
    payoutSuccessText: document.getElementById('payout-success-text'),
    btnSubmitPayout: document.getElementById('btn-submit-payout'),
    payoutSubmitSpinner: document.getElementById('payout-submit-spinner'),
    payoutSubmitText: document.getElementById('payout-submit-text'),
    historyLoading: document.getElementById('history-loading'),
    historyEmpty: document.getElementById('history-empty'),
    historyTableWrapper: document.getElementById('history-table-wrapper'),
    historyTbody: document.getElementById('history-tbody'),

    // Toasts
    toastContainer: document.getElementById('toast-container'),
  };

  // Authenticated Fetch Wrapper
  async function apiFetch(url, options = {}) {
    options.headers = options.headers || {};
    if (state.token) {
      if (options.headers instanceof Headers) {
        options.headers.set('Authorization', `Bearer ${state.token}`);
      } else {
        options.headers['Authorization'] = `Bearer ${state.token}`;
      }
    }
    const res = await fetch(url, options);
    if (res.status === 401 && !url.includes('/api/auth/login')) {
      showLoginScreen('Session expired or unauthorized. Please authenticate.');
      throw new Error('Unauthorized');
    }
    return res;
  }

  // Dashboard & Authentication State Management
  function enterDashboard() {
    document.documentElement.classList.add('has-active-session');
    els.loginScreen.classList.add('hidden');
    els.headerUsername.textContent = state.username || 'admin';
    els.headerUserPill.classList.remove('hidden');
    fetchProxies();
    fetchStats();
    fetchWalletInfo();
    connectEventSource();
  }

  function showLoginScreen(errorMsg = '') {
    if (sessionTimeoutTimer) {
      clearTimeout(sessionTimeoutTimer);
      sessionTimeoutTimer = null;
    }
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    state.token = '';
    state.username = '';
    state.expiresAt = 0;
    localStorage.removeItem('webload_token');
    localStorage.removeItem('webload_username');
    localStorage.removeItem('webload_expires_at');
    document.documentElement.classList.remove('has-active-session');
    els.headerUserPill.classList.add('hidden');
    els.loginScreen.classList.remove('hidden');
    if (errorMsg) {
      els.loginErrorText.textContent = errorMsg;
      els.loginError.classList.remove('hidden');
    } else {
      els.loginError.classList.add('hidden');
    }
    els.loginPassword.value = '';
    setTimeout(() => els.loginPassword.focus(), 50);
  }

  // Helper formatting functions
  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  function formatNumber(num) {
    return (num || 0).toLocaleString();
  }

  function formatMicrocredits(amount) {
    return (Number(amount) || 0).toLocaleString() + ' µcr';
  }

  function microcreditsToUsd(amount) {
    const usd = ((Number(amount) || 0) / 1000000).toFixed(2);
    return `≈ $${usd} AlphaUSD`;
  }

  function isValidEthAddress(addr) {
    return /^0x[0-9a-fA-F]{40}$/.test(String(addr || '').trim());
  }

  function getCountryFlag(cc) {
    if (!cc || cc === 'WW' || cc.toLowerCase() === 'worldwide') {
      return '🌐 Worldwide';
    }
    const code = cc.toUpperCase();
    if (code.length === 2) {
      const codePoints = [...code].map(c => 127397 + c.charCodeAt(0));
      return String.fromCodePoint(...codePoints) + ' ' + code;
    }
    return code;
  }

  // Toast Notification
  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `<span>${message}</span>`;
    els.toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      setTimeout(() => toast.remove(), 200);
    }, 3500);
  }

  // Fetch Proxies
  async function fetchProxies() {
    els.tableLoading.classList.remove('hidden');

    try {
      const params = new URLSearchParams({
        page: state.page,
        limit: state.limit,
        status: state.status,
        country: state.country,
        category: state.category,
        sort_by: state.sortBy,
        sort_dir: state.sortDir,
      });

      if (state.search.trim()) {
        params.append('search', state.search.trim());
      }

      const res = await apiFetch(`/api/proxies?${params.toString()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const data = await res.json();
      state.totalRecords = data.total;
      state.filteredRecords = data.filtered;
      state.totalPages = Math.max(1, data.total_pages);
      state.page = data.page;

      renderTable(data.items);
      renderPagination();
      updateStats();
    } catch (err) {
      console.error('Failed to fetch proxies:', err);
      showToast('Error loading proxies: ' + err.message, 'error');
    } finally {
      els.tableLoading.classList.add('hidden');
    }
  }

  // Render Table Rows
  function renderTable(items) {
    els.tbody.innerHTML = '';
    els.thCheckbox.checked = false;

    if (!items || items.length === 0) {
      els.tableEmpty.classList.remove('hidden');
      return;
    }
    els.tableEmpty.classList.add('hidden');

    const fragment = document.createDocumentFragment();

    items.forEach(p => {
      const tr = document.createElement('tr');
      tr.dataset.id = p.id;
      tr.dataset.pathId = p.path_id;

      // Checkbox
      const isSelected = state.selectedIds.has(p.id);

      // Latency badge
      let latBadge = `<span class="latency-badge latency-none">—</span>`;
      if (p.last_latency_ms !== null && p.last_latency_ms !== undefined) {
        let latClass = 'latency-good';
        if (p.last_latency_ms > 500) latClass = 'latency-poor';
        else if (p.last_latency_ms > 150) latClass = 'latency-fair';
        latBadge = `<span class="latency-badge ${latClass}">${p.last_latency_ms}ms</span>`;
      }

      // Status badge
      const statusClass = p.status === 'active' ? 'active' : (p.status === 'paused' ? 'paused' : 'error');
      const statusDot = `<span class="badge-status ${statusClass}"><span class="badge-status-dot"></span>${p.status}</span>`;

      // Upstream Toggle Button
      const isUpstreamActive = p.status === 'active';
      const toggleBtn = isUpstreamActive
        ? `<button class="toggle-btn btn-stop" data-action="toggle" data-id="${p.id}" title="Stop upstream relaying for this proxy">Stop</button>`
        : `<button class="toggle-btn btn-resume" data-action="toggle" data-id="${p.id}" title="Resume upstream relaying for this proxy">Resume</button>`;

      // Auth masked
      const authDisplay = p.username ? `${escapeHtml(p.username)}:••••••` : `<span class="text-muted">none</span>`;

      // Category
      const catDisplay = p.category ? `<span class="badge-category">${escapeHtml(p.category)}</span>` : `<span class="text-muted">standard</span>`;

      tr.innerHTML = `
        <td class="col-checkbox">
          <input type="checkbox" class="row-checkbox" data-id="${p.id}" ${isSelected ? 'checked' : ''}>
        </td>
        <td class="col-status">${statusDot}</td>
        <td class="col-toggle">${toggleBtn}</td>
        <td class="col-address">
          <span>${escapeHtml(p.address)}</span>
          <button class="btn-icon btn-xs" data-action="copy-addr" data-addr="${escapeHtml(p.address)}" title="Copy address">
            <svg viewBox="0 0 24 24" width="12" height="12" stroke="currentColor" stroke-width="2" fill="none">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
            </svg>
          </button>
        </td>
        <td class="col-auth">${authDisplay}</td>
        <td class="col-country">${getCountryFlag(p.country)}</td>
        <td class="col-category">${catDisplay}</td>
        <td class="col-latency">${latBadge}</td>
        <td class="col-streams">
          <div>${formatNumber(p.total_streams)} streams</div>
          <div style="font-size: 0.75rem; color: var(--text-muted)">${formatBytes(p.total_bytes_relayed)}</div>
        </td>
        <td class="col-actions">
          <button class="btn btn-xs btn-secondary" data-action="test" data-id="${p.id}" title="Test handshake">Test</button>
          <button class="btn btn-xs btn-danger" data-action="delete" data-id="${p.id}" title="Delete proxy">Delete</button>
        </td>
      `;

      fragment.appendChild(tr);
    });

    els.tbody.appendChild(fragment);
    updateBulkBar();
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  // Render Pagination
  function renderPagination() {
    const start = state.totalRecords === 0 ? 0 : (state.page - 1) * state.limit + 1;
    const end = Math.min(state.page * state.limit, state.filteredRecords);

    els.paginationInfo.textContent = `Showing ${formatNumber(start)} - ${formatNumber(end)} of ${formatNumber(state.filteredRecords)} matching (${formatNumber(state.totalRecords)} total)`;
    els.pageInput.value = state.page;
    els.pageTotalText.textContent = formatNumber(state.totalPages);

    els.btnPageFirst.disabled = state.page <= 1;
    els.btnPagePrev.disabled = state.page <= 1;
    els.btnPageNext.disabled = state.page >= state.totalPages;
    els.btnPageLast.disabled = state.page >= state.totalPages;
  }

  // Update Stats & Aggregate Data
  async function updateStats() {
    try {
      const res = await apiFetch('/api/stats');
      if (!res.ok) return;
      const data = await res.json();

      if (data.backend_url) state.backendUrl = data.backend_url;
      if (data.active_upstream_paths !== undefined) state.activeUpstreamPaths = data.active_upstream_paths;
      if (data.active_streams !== undefined) state.activeStreams = data.active_streams;
      if (data.bytes_per_sec !== undefined) state.bytesPerSec = data.bytes_per_sec;

      if (els.statTotal) els.statTotal.textContent = formatNumber(data.total_proxies);
      if (els.statActive) els.statActive.textContent = formatNumber(data.active_proxies);
      if (els.statPaused) els.statPaused.textContent = formatNumber(data.paused_proxies);
      if (els.statError) els.statError.textContent = formatNumber(data.error_proxies);

      if (els.statStreams && data.active_streams !== undefined) {
        els.statStreams.textContent = formatNumber(data.active_streams);
      }
      if (els.statThroughput) {
        els.statThroughput.textContent = `${formatBytes(data.bytes_per_sec || 0)}/s`;
      }

      if (data.is_relay_running !== undefined) {
        state.isRelayRunning = !!data.is_relay_running;
        updateRelayUi();
      }

      // Populate countries dropdown if options only has default
      if (els.countrySelect.options.length <= 2 && data.top_countries) {
        data.top_countries.forEach(([cName, count]) => {
          if (cName !== 'Worldwide') {
            const opt = document.createElement('option');
            opt.value = cName;
            opt.textContent = `${getCountryFlag(cName)} (${formatNumber(count)})`;
            els.countrySelect.appendChild(opt);
          }
        });
      }
      fetchWalletInfo();
    } catch (e) {
      console.warn('Stats fetch error:', e);
    }
  }

  const fetchStats = updateStats;

  // Bulk Bar Logic
  function updateBulkBar() {
    const count = state.selectedIds.size;
    els.bulkCount.textContent = formatNumber(count);
    if (count > 0) {
      els.bulkBar.classList.remove('hidden');
    } else {
      els.bulkBar.classList.add('hidden');
    }
  }

  // Toggle Single Proxy Upstream Status
  async function toggleProxy(id) {
    try {
      const res = await apiFetch(`/api/proxies/${id}/toggle`, { method: 'POST' });
      if (!res.ok) throw new Error('Toggle failed');
      const data = await res.json();

      showToast(`Proxy ${data.new_status === 'active' ? 'resumed as upstream' : 'stopped from upstream'}`, 'success');
      fetchProxies();
    } catch (e) {
      showToast('Error toggling proxy: ' + e.message, 'error');
    }
  }

  // Test Single Proxy Handshake
  async function testProxy(id) {
    try {
      showToast(`Testing proxy #${id} handshake...`, 'info');
      const res = await apiFetch(`/api/proxies/${id}/test`, { method: 'POST' });
      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(errText || `HTTP ${res.status}`);
      }
      const data = await res.json();

      if (data.is_success) {
        showToast(`Proxy #${id} responded in ${data.latency_ms}ms`, 'success');
      } else {
        showToast(`Proxy #${id} failed: ${data.error_message || 'Connection error'}`, 'error');
      }
      fetchProxies();
    } catch (e) {
      showToast(`Probe error (Proxy #${id}): ${e.message}`, 'error');
    }
  }

  // Delete Single Proxy
  async function deleteProxy(id) {
    if (!confirm('Are you sure you want to delete this proxy?')) return;
    try {
      const res = await apiFetch(`/api/proxies/${id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error('Delete failed');
      showToast('Proxy removed from storage', 'success');
      state.selectedIds.delete(id);
      fetchProxies();
    } catch (e) {
      showToast('Delete error: ' + e.message, 'error');
    }
  }

  // Bulk Operations
  async function executeBulkAction(action) {
    const ids = Array.from(state.selectedIds);
    if (ids.length === 0) return;

    if (action === 'delete' && !confirm(`Delete ${ids.length} selected proxies?`)) {
      return;
    }

    try {
      const res = await apiFetch('/api/proxies/bulk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, ids }),
      });
      if (!res.ok) throw new Error('Bulk action failed');
      const data = await res.json();

      showToast(`Updated ${data.affected} proxies successfully`, 'success');
      state.selectedIds.clear();
      updateBulkBar();
      fetchProxies();
    } catch (e) {
      showToast('Bulk action error: ' + e.message, 'error');
    }
  }

  // Toggle Global Seller Relay
  async function toggleSellerRelay() {
    try {
      const res = await apiFetch('/api/seller/toggle', { method: 'POST' });
      if (!res.ok) throw new Error('Failed to toggle seller relay');
      const data = await res.json();
      state.isRelayRunning = data.is_running;
      updateRelayUi();
      showToast(state.isRelayRunning ? 'Seller relay started' : 'Seller relay stopped', 'success');
    } catch (e) {
      showToast('Relay error: ' + e.message, 'error');
    }
  }

  function updateRelayUi() {
    const cleanGw = (state.backendUrl || 'api.proxybase.xyz').replace(/^https?:\/\//, '');

    if (state.isRelayRunning) {
      els.relayStatusDot.className = 'relay-indicator online';
      els.relayBtnText.textContent = 'Seller Online';
      els.btnToggleRelay.className = 'btn btn-success';

      if (els.backendStatusCard) {
        els.backendStatusCard.className = 'backend-status-card online';
        els.backendStatusHeadline.textContent = `Connected to Production Gateway (${cleanGw})`;
        els.backendTagBadge.textContent = 'Seller Online';
        els.backendTagBadge.className = 'backend-tag-badge';

        const pathsCount = state.activeUpstreamPaths || (state.proxies ? state.proxies.length : 0);
        if (state.activeStreams > 0) {
          els.backendStatusSub.innerHTML = `<strong>Relaying Active Traffic:</strong> ${formatNumber(state.activeStreams)} active stream(s) currently routing through your upstream nodes.`;
          els.backendMetricActivity.textContent = `Relaying (${formatBytes(state.bytesPerSec)}/s)`;
        } else {
          els.backendStatusSub.innerHTML = `<strong>Connected & Verified:</strong> Upstream proxies are registered on the ProxyBase marketplace. Waiting for incoming buyer crawler streams.`;
          els.backendMetricActivity.textContent = 'Awaiting Buyer Requests';
        }

        els.backendMetricGateway.textContent = cleanGw;
        els.backendMetricPaths.textContent = `${pathsCount} active`;
      }
    } else {
      els.relayStatusDot.className = 'relay-indicator';
      els.relayBtnText.textContent = 'Seller Offline';
      els.btnToggleRelay.className = 'btn btn-secondary';

      if (els.backendStatusCard) {
        els.backendStatusCard.className = 'backend-status-card standby';
        els.backendStatusHeadline.textContent = 'Seller Relay Standby (Offline)';
        els.backendTagBadge.textContent = 'Standby';
        els.backendTagBadge.className = 'backend-tag-badge standby';

        const totalActive = els.statActive ? els.statActive.textContent : '0';
        els.backendStatusSub.innerHTML = `Proxies are loaded locally. Click <strong>[Seller Offline]</strong> in the top header to connect your proxies to ${cleanGw}.`;
        els.backendMetricGateway.textContent = cleanGw;
        els.backendMetricPaths.textContent = `${totalActive} loaded`;
        els.backendMetricActivity.textContent = 'Offline (Standby)';
      }
    }
  }

  // Start File Ingestion
  async function startIngestion() {
    // 1. Direct check: Did user choose an upload file?
    const file = state.uploadedFile || (els.fileUploadInput && els.fileUploadInput.files && els.fileUploadInput.files[0]);
    const isUploadTab = !els.tabUpload.classList.contains('hidden') || state.ingestMode === 'upload';
    const filePath = els.inputFilePath ? els.inputFilePath.value.trim() : '';

    let body = {};

    if (file && (isUploadTab || !filePath)) {
      // User intends to upload file
      try {
        els.btnStartLoad.disabled = true;
        els.btnStartLoad.textContent = 'Reading file...';
        const text = await file.text();
        body = { raw_content: text };
      } catch (err) {
        showToast('Failed to read file: ' + err.message, 'error');
        els.btnStartLoad.disabled = false;
        els.btnStartLoad.textContent = 'Start Streaming Ingestion';
        return;
      }
    } else if (isUploadTab && !file) {
      showToast('Please click or drag a .txt proxy file to upload', 'error');
      return;
    } else if (filePath) {
      body = { file_path: filePath };
    } else {
      showToast('Please select a file to upload or enter a server file path', 'error');
      return;
    }

    try {
      els.btnStartLoad.disabled = true;
      els.btnStartLoad.textContent = 'Starting ingestion...';

      const res = await apiFetch('/api/proxies/load', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(errText || `HTTP ${res.status}`);
      }

      const data = await res.json();
      state.activeJobId = data.job_id;
      showToast('Streaming ingestion started in background', 'success');
      closeModal();
      els.ingestBanner.classList.remove('hidden');

      // Immediate refresh attempt after 600ms (for fast/small files)
      setTimeout(() => fetchProxies(), 600);

      // Active polling fallback in case SSE message is delayed or dropped
      if (state.activePollTimer) clearInterval(state.activePollTimer);
      state.activePollTimer = setInterval(async () => {
        await fetchProxies();
      }, 1500);

      // Failsafe: stop active poll timer after 45s
      setTimeout(() => {
        if (state.activePollTimer) {
          clearInterval(state.activePollTimer);
          state.activePollTimer = null;
          els.ingestBanner.classList.add('hidden');
        }
      }, 45000);
    } catch (e) {
      showToast('Failed to start ingestion: ' + e.message, 'error');
    } finally {
      els.btnStartLoad.disabled = false;
      els.btnStartLoad.textContent = 'Start Streaming Ingestion';
    }
  }

  // Cancel Ingestion
  async function cancelIngestion() {
    if (!state.activeJobId) return;
    try {
      await apiFetch(`/api/jobs/${state.activeJobId}/cancel`, { method: 'POST' });
      showToast('Cancelling ingestion...', 'info');
    } catch (e) {
      showToast('Cancel failed: ' + e.message, 'error');
    }
  }

  // Connect to Real-Time SSE Stream
  function connectEventSource() {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    if (!state.token) return;

    const sse = new EventSource(`/api/events?token=${encodeURIComponent(state.token)}`);
    state.eventSource = sse;

    sse.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);

        // Ingestion Progress Event
        if (msg.type === 'ingest_progress' || msg.job_id || msg.lines_read !== undefined) {
          els.ingestBanner.classList.remove('hidden');
          els.ingestProgressBar.style.width = `${(msg.progress_percent || 0).toFixed(1)}%`;
          els.ingestRateText.textContent = `${formatNumber(msg.lines_per_second || 0)} lines/sec`;
          els.ingestLinesRead.textContent = formatNumber(msg.lines_read || 0);
          els.ingestValidCount.textContent = formatNumber(msg.valid_count || 0);
          els.ingestDupCount.textContent = formatNumber(msg.duplicate_count || 0);
          els.ingestWarnCount.textContent = formatNumber(msg.warning_count || 0);

          if (msg.status === 'completed') {
            if (state.activePollTimer) {
              clearInterval(state.activePollTimer);
              state.activePollTimer = null;
            }
            if (msg.valid_count === 0 && msg.warning_count > 0) {
              showToast(`Ingestion finished, but 0 valid proxies were parsed (${formatNumber(msg.warning_count)} invalid format lines)`, 'warning');
            } else {
              showToast(`Ingestion completed! Added ${formatNumber(msg.valid_count)} proxies.`, 'success');
            }
            setTimeout(() => els.ingestBanner.classList.add('hidden'), 3500);
            fetchProxies();
          } else if (msg.status === 'cancelled') {
            if (state.activePollTimer) {
              clearInterval(state.activePollTimer);
              state.activePollTimer = null;
            }
            showToast('Ingestion cancelled by operator.', 'info');
            setTimeout(() => els.ingestBanner.classList.add('hidden'), 3000);
            fetchProxies();
          } else if (msg.status === 'failed') {
            if (state.activePollTimer) {
              clearInterval(state.activePollTimer);
              state.activePollTimer = null;
            }
            showToast(`Ingestion error: ${msg.error_message || 'Unknown error'}`, 'error');
            setTimeout(() => els.ingestBanner.classList.add('hidden'), 5000);
          }
        }

        // Live Relay Telemetry Event
        if (msg.type === 'telemetry') {
          state.activeStreams = msg.active_streams || 0;
          state.bytesPerSec = msg.bytes_per_sec || 0;
          if (msg.active_upstream_paths !== undefined) {
            state.activeUpstreamPaths = msg.active_upstream_paths;
          }
          if (els.statStreams) els.statStreams.textContent = formatNumber(msg.active_streams);
          if (els.statThroughput) els.statThroughput.textContent = `${formatBytes(msg.bytes_per_sec)}/s`;
          state.isRelayRunning = !!msg.is_relay_running;
          updateRelayUi();
        }
      } catch (err) {
        console.warn('SSE parse error:', err);
      }
    };

    sse.onerror = () => {
      if (!state.token && state.eventSource) {
        state.eventSource.close();
        state.eventSource = null;
      }
    };
  }

  // Modal Handlers
  function openModal() {
    els.modalIngest.classList.remove('hidden');
    const activeTab = document.querySelector('.modal-tab.active');
    if (activeTab && activeTab.dataset.tab) {
      state.ingestMode = activeTab.dataset.tab;
    }
  }

  function closeModal() {
    els.modalIngest.classList.add('hidden');
    state.uploadedFile = null;
    if (els.fileUploadInput) els.fileUploadInput.value = '';
    els.uploadFileName.textContent = '';
    els.uploadFileName.classList.add('hidden');
  }

  // ---------------------------------------------------------------------------
  // Wallet & Payout Operations
  // ---------------------------------------------------------------------------

  function openWalletModal(tab = 'funds') {
    if (!els.modalWallet) return;
    els.modalWallet.classList.remove('hidden');
    switchWalletTab(tab);
    fetchWalletInfo();
  }

  function closeWalletModal() {
    if (!els.modalWallet) return;
    els.modalWallet.classList.add('hidden');
    if (els.payoutError) els.payoutError.classList.add('hidden');
    if (els.payoutSuccess) els.payoutSuccess.classList.add('hidden');
  }

  function copyWalletAddress() {
    if (!state.walletAddress) {
      showToast('No wallet address available to copy', 'warning');
      return;
    }
    navigator.clipboard.writeText(state.walletAddress).then(() => {
      if (els.btnCopyText) els.btnCopyText.textContent = 'Copied!';
      setTimeout(() => {
        if (els.btnCopyText) els.btnCopyText.textContent = 'Copy';
      }, 2000);
      showToast('Wallet address copied to clipboard', 'info');
    }).catch(() => {
      showToast('Failed to copy to clipboard', 'error');
    });
  }

  function switchWalletTab(tabName) {
    state.activeWalletTab = tabName;
    els.walletTabs.forEach(t => {
      t.classList.toggle('active', t.dataset.walletTab === tabName);
    });

    if (els.walletTabFunds) {
      els.walletTabFunds.classList.toggle('hidden', tabName !== 'funds');
      els.walletTabFunds.classList.toggle('active', tabName === 'funds');
    }
    if (els.walletTabWithdraw) {
      els.walletTabWithdraw.classList.toggle('hidden', tabName !== 'withdraw');
      els.walletTabWithdraw.classList.toggle('active', tabName === 'withdraw');
    }
    if (els.walletTabHistory) {
      els.walletTabHistory.classList.toggle('hidden', tabName !== 'history');
      els.walletTabHistory.classList.toggle('active', tabName === 'history');
    }

    if (tabName === 'history') {
      fetchPayouts();
    } else if (tabName === 'withdraw') {
      if (els.payoutError) els.payoutError.classList.add('hidden');
      if (els.payoutSuccess) els.payoutSuccess.classList.add('hidden');
      validatePayoutAddress();
      if (els.payoutAmount && els.payoutAmountUsd) {
        els.payoutAmountUsd.textContent = microcreditsToUsd(els.payoutAmount.value || 0);
      }
    }
  }

  function validatePayoutAddress() {
    if (!els.payoutAddress || !els.payoutAddressStatus) return false;
    const val = (els.payoutAddress.value || '').trim();
    if (!val) {
      els.payoutAddressStatus.className = 'validation-status-badge hidden';
      els.payoutAddressStatus.textContent = '';
      return false;
    }
    if (isValidEthAddress(val)) {
      els.payoutAddressStatus.className = 'validation-status-badge valid';
      els.payoutAddressStatus.textContent = '✓ Valid ETH Address';
      return true;
    } else {
      els.payoutAddressStatus.className = 'validation-status-badge invalid';
      els.payoutAddressStatus.textContent = '✗ Invalid (0x + 40 hex)';
      return false;
    }
  }

  function setPayoutPreset(pct) {
    if (!els.payoutAmount || !els.payoutAmountUsd) return;
    const avail = Math.max(0, state.sellerAvailable || 0);
    const amount = Math.floor(avail * pct);
    els.payoutAmount.value = amount > 0 ? amount : '';
    els.payoutAmountUsd.textContent = microcreditsToUsd(amount);
  }

  async function fetchWalletInfo() {
    if (!state.token) return;
    try {
      const res = await apiFetch('/api/wallet');
      if (!res.ok) throw new Error('Failed to fetch wallet status');
      const data = await res.json();

      state.walletAddress = data.wallet_address || '';
      state.sellerAvailable = data.seller_available || 0;
      state.sellerPending = data.seller_pending || 0;
      state.sellerPayoutLocked = data.seller_payout_locked || 0;
      state.buyerSpendable = data.buyer_available || 0;
      state.buyerReserved = data.buyer_reserved || 0;
      state.buyerSpent = data.buyer_spent || 0;

      // Update Header badge with seller earnings
      if (els.headerWalletBadge) {
        els.headerWalletBadge.textContent = formatMicrocredits(state.sellerAvailable);
      }

      // Update Address Display
      if (els.walletAddressDisplay) {
        if (state.walletAddress) {
          els.walletAddressDisplay.textContent = state.walletAddress;
          els.walletAddressDisplay.title = state.walletAddress;
          if (els.walletStatusDot) els.walletStatusDot.className = 'wallet-indicator-dot online';
        } else {
          els.walletAddressDisplay.textContent = 'No node wallet detected';
          if (els.walletStatusDot) els.walletStatusDot.className = 'wallet-indicator-dot offline';
        }
      }

      // Update Funds Cards
      if (els.fundSellerAvailable) {
        els.fundSellerAvailable.textContent = formatMicrocredits(state.sellerAvailable);
        els.fundSellerAvailableUsd.textContent = microcreditsToUsd(state.sellerAvailable);
        els.fundSellerPending.textContent = formatMicrocredits(state.sellerPending);
        els.fundSellerPendingUsd.textContent = microcreditsToUsd(state.sellerPending);
        els.fundSellerLocked.textContent = formatMicrocredits(state.sellerPayoutLocked);
        els.fundSellerLockedUsd.textContent = microcreditsToUsd(state.sellerPayoutLocked);
        els.fundBuyerSpendable.textContent = formatMicrocredits(state.buyerSpendable);
        els.fundBuyerSpendableUsd.textContent = microcreditsToUsd(state.buyerSpendable);
        els.fundBuyerReserved.textContent = formatMicrocredits(state.buyerReserved);
        els.fundBuyerReservedUsd.textContent = microcreditsToUsd(state.buyerReserved);
        els.fundBuyerSpent.textContent = formatMicrocredits(state.buyerSpent);
        els.fundBuyerSpentUsd.textContent = microcreditsToUsd(state.buyerSpent);
      }

      // Update Withdraw Banner headline
      if (els.withdrawAvailableHeadline) {
        els.withdrawAvailableHeadline.textContent = `${formatMicrocredits(state.sellerAvailable)} (${microcreditsToUsd(state.sellerAvailable)})`;
      }
    } catch (e) {
      console.warn('Wallet fetch error:', e);
    }
  }

  async function fetchPayouts() {
    if (!state.token) return;
    if (els.historyLoading) els.historyLoading.classList.remove('hidden');
    if (els.historyEmpty) els.historyEmpty.classList.add('hidden');
    if (els.historyTableWrapper) els.historyTableWrapper.classList.add('hidden');
    if (els.historyTbody) els.historyTbody.innerHTML = '';

    try {
      const res = await apiFetch('/api/wallet/payouts');
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to fetch payouts');
      }
      const payouts = await res.json();
      if (els.historyLoading) els.historyLoading.classList.add('hidden');

      const items = Array.isArray(payouts) ? payouts : (payouts.payouts || payouts.items || []);
      if (!items || items.length === 0) {
        if (els.historyEmpty) els.historyEmpty.classList.remove('hidden');
        return;
      }

      if (els.historyTableWrapper) els.historyTableWrapper.classList.remove('hidden');
      const rowsHtml = items.map(p => {
        const amount = p.amount_microcredits || p.amount || 0;
        const address = p.tempo_address || p.destination_address || p.address || '—';
        const truncatedAddr = address.length > 18 ? `${address.slice(0, 8)}...${address.slice(-6)}` : address;
        const status = (p.status || 'pending').toLowerCase();
        let statusBadgeClass = 'badge-warning';
        if (status === 'completed' || status === 'success' || status === 'confirmed') {
          statusBadgeClass = 'badge-active';
        } else if (status === 'failed' || status === 'rejected') {
          statusBadgeClass = 'badge-error';
        }

        const dateStr = p.created_at ? new Date(p.created_at).toLocaleString() : '—';
        const txHash = p.tx_hash || p.transaction_hash || p.tx || '';
        const txDisplay = txHash ? `<code class="font-mono text-xs" title="${escapeHtml(txHash)}">${escapeHtml(txHash.slice(0, 10))}...</code>` : '<span class="text-muted">—</span>';

        return `<tr>
          <td><span class="text-muted font-mono">${escapeHtml(dateStr)}</span></td>
          <td><strong>${formatMicrocredits(amount)}</strong><br><span class="text-muted text-xs">${microcreditsToUsd(amount)}</span></td>
          <td><code class="font-mono" title="${escapeHtml(address)}">${escapeHtml(truncatedAddr)}</code></td>
          <td><span class="badge ${statusBadgeClass}">${escapeHtml(status)}</span></td>
          <td>${txDisplay}</td>
        </tr>`;
      }).join('');

      if (els.historyTbody) els.historyTbody.innerHTML = rowsHtml;
    } catch (e) {
      if (els.historyLoading) els.historyLoading.classList.add('hidden');
      if (els.historyEmpty) {
        els.historyEmpty.classList.remove('hidden');
        const p = els.historyEmpty.querySelector('p');
        if (p) p.textContent = `Could not load payout history: ${e.message}`;
      }
    }
  }

  async function handlePayoutSubmit(e) {
    if (e) e.preventDefault();
    if (els.payoutError) els.payoutError.classList.add('hidden');
    if (els.payoutSuccess) els.payoutSuccess.classList.add('hidden');

    const address = (els.payoutAddress.value || '').trim();
    const amount = parseInt(els.payoutAmount.value, 10);

    // Strict Ethereum-style address check
    if (!isValidEthAddress(address)) {
      els.payoutErrorText.textContent = 'Invalid address: Must be a valid 42-character Ethereum-style wallet address starting with 0x (e.g. 0x71C...).';
      els.payoutError.classList.remove('hidden');
      els.payoutAddress.focus();
      return;
    }

    if (isNaN(amount) || amount <= 0) {
      els.payoutErrorText.textContent = 'Please enter a valid payout amount in microcredits (> 0).';
      els.payoutError.classList.remove('hidden');
      els.payoutAmount.focus();
      return;
    }

    if (amount > state.sellerAvailable) {
      els.payoutErrorText.textContent = `Insufficient funds: Requested ${formatMicrocredits(amount)}, but only ${formatMicrocredits(state.sellerAvailable)} is available for withdrawal.`;
      els.payoutError.classList.remove('hidden');
      return;
    }

    els.btnSubmitPayout.disabled = true;
    els.payoutSubmitSpinner.classList.remove('hidden');
    els.payoutSubmitText.textContent = 'Submitting Payout Request...';

    try {
      const res = await apiFetch('/api/wallet/payout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          amount_microcredits: amount,
          tempo_address: address,
        }),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(data.error || 'Failed to submit payout request');
      }

      els.payoutSuccessText.innerHTML = `<strong>Payout requested!</strong> ${formatMicrocredits(amount)} queued for <code class="font-mono">${escapeHtml(address.slice(0, 10))}...</code>. Track status under Payout History.`;
      els.payoutSuccess.classList.remove('hidden');
      els.payoutAmount.value = '';
      els.payoutAmountUsd.textContent = '≈ $0.00 AlphaUSD';

      showToast(`Payout request for ${formatMicrocredits(amount)} submitted!`, 'success');
      fetchWalletInfo();
    } catch (err) {
      els.payoutErrorText.textContent = err.message || 'Payout request failed';
      els.payoutError.classList.remove('hidden');
    } finally {
      els.btnSubmitPayout.disabled = false;
      els.payoutSubmitSpinner.classList.add('hidden');
      els.payoutSubmitText.textContent = 'Confirm & Submit Payout Request';
    }
  }

  // Event Listeners Initialization
  function initEvents() {
    // Search input with debounce
    let debounceTimer;
    els.searchInput.addEventListener('input', (e) => {
      clearTimeout(debounceTimer);
      const val = e.target.value;
      els.searchClear.classList.toggle('hidden', !val);

      debounceTimer = setTimeout(() => {
        state.search = val;
        state.page = 1;
        fetchProxies();
      }, 250);
    });

    els.searchClear.addEventListener('click', () => {
      els.searchInput.value = '';
      els.searchClear.classList.add('hidden');
      state.search = '';
      state.page = 1;
      fetchProxies();
    });

    // Status Filter Pills
    els.statusPills.addEventListener('click', (e) => {
      const btn = e.target.closest('.pill');
      if (!btn) return;
      els.statusPills.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      state.status = btn.dataset.status;
      state.page = 1;
      fetchProxies();
    });

    // Country Select
    els.countrySelect.addEventListener('change', (e) => {
      state.country = e.target.value;
      state.page = 1;
      fetchProxies();
    });

    // Category Select
    els.categorySelect.addEventListener('change', (e) => {
      state.category = e.target.value;
      state.page = 1;
      fetchProxies();
    });

    // Limit Select
    els.limitSelect.addEventListener('change', (e) => {
      state.limit = parseInt(e.target.value, 10);
      state.page = 1;
      fetchProxies();
    });

    // Refresh button
    els.btnRefresh.addEventListener('click', fetchProxies);

    // Relay Toggle
    els.btnToggleRelay.addEventListener('click', toggleSellerRelay);

    // Table Header Sort
    els.sortHeaders.forEach(th => {
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (state.sortBy === col) {
          state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          state.sortBy = col;
          state.sortDir = 'asc';
        }

        els.sortHeaders.forEach(h => h.classList.remove('asc', 'desc'));
        th.classList.add(state.sortDir);
        state.page = 1;
        fetchProxies();
      });
    });

    // Table Row Checkboxes & Actions (Delegated)
    els.tbody.addEventListener('click', (e) => {
      const btn = e.target.closest('button');
      if (btn) {
        const action = btn.dataset.action;
        const id = parseInt(btn.dataset.id, 10);

        if (action === 'toggle') {
          toggleProxy(id);
        } else if (action === 'test') {
          testProxy(id);
        } else if (action === 'delete') {
          deleteProxy(id);
        } else if (action === 'copy-addr') {
          navigator.clipboard.writeText(btn.dataset.addr);
          showToast('Copied address to clipboard', 'info');
        }
        return;
      }

      const cb = e.target.closest('.row-checkbox');
      if (cb) {
        const id = parseInt(cb.dataset.id, 10);
        if (cb.checked) {
          state.selectedIds.add(id);
        } else {
          state.selectedIds.delete(id);
        }
        updateBulkBar();
      }
    });

    // Header Checkbox (Select all on current page)
    els.thCheckbox.addEventListener('change', (e) => {
      const checked = e.target.checked;
      const rowCheckboxes = els.tbody.querySelectorAll('.row-checkbox');
      rowCheckboxes.forEach(cb => {
        cb.checked = checked;
        const id = parseInt(cb.dataset.id, 10);
        if (checked) {
          state.selectedIds.add(id);
        } else {
          state.selectedIds.delete(id);
        }
      });
      updateBulkBar();
    });

    // Bulk Buttons
    els.btnBulkPause.addEventListener('click', () => executeBulkAction('pause'));
    els.btnBulkResume.addEventListener('click', () => executeBulkAction('resume'));
    els.btnBulkDelete.addEventListener('click', () => executeBulkAction('delete'));
    els.btnBulkClear.addEventListener('click', () => {
      state.selectedIds.clear();
      els.tbody.querySelectorAll('.row-checkbox').forEach(cb => cb.checked = false);
      els.thCheckbox.checked = false;
      updateBulkBar();
    });

    // Pagination Controls
    els.btnPageFirst.addEventListener('click', () => { if (state.page > 1) { state.page = 1; fetchProxies(); } });
    els.btnPagePrev.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchProxies(); } });
    els.btnPageNext.addEventListener('click', () => { if (state.page < state.totalPages) { state.page++; fetchProxies(); } });
    els.btnPageLast.addEventListener('click', () => { if (state.page < state.totalPages) { state.page = state.totalPages; fetchProxies(); } });

    els.pageInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        const p = parseInt(e.target.value, 10);
        if (!isNaN(p) && p >= 1 && p <= state.totalPages) {
          state.page = p;
          fetchProxies();
        } else {
          e.target.value = state.page;
        }
      }
    });

    // Auth & Gateway Events
    els.loginForm.addEventListener('submit', handleLoginSubmit);
    els.btnLogout.addEventListener('click', handleLogout);
    els.btnTogglePassVisibility.addEventListener('click', () => {
      const isPass = els.loginPassword.type === 'password';
      els.loginPassword.type = isPass ? 'text' : 'password';
    });

    // Ingestion Modal Events
    els.btnOpenIngest.addEventListener('click', openModal);
    els.btnModalClose.addEventListener('click', closeModal);
    els.btnModalCancel.addEventListener('click', closeModal);
    els.btnStartLoad.addEventListener('click', startIngestion);
    els.btnCancelIngest.addEventListener('click', cancelIngestion);

    // Modal Tabs
    els.modalTabs.forEach(tab => {
      tab.addEventListener('click', () => {
        els.modalTabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        const mode = tab.dataset.tab;
        state.ingestMode = mode;
        if (mode === 'local') {
          els.tabLocal.classList.remove('hidden');
          els.tabLocal.classList.add('active');
          els.tabUpload.classList.add('hidden');
          els.tabUpload.classList.remove('active');
        } else {
          els.tabLocal.classList.add('hidden');
          els.tabLocal.classList.remove('active');
          els.tabUpload.classList.remove('hidden');
          els.tabUpload.classList.add('active');
        }
      });
    });

    // File Drag & Drop
    els.dropZone.addEventListener('click', () => els.fileUploadInput.click());
    els.dropZone.addEventListener('dragover', (e) => { e.preventDefault(); els.dropZone.classList.add('dragover'); });
    els.dropZone.addEventListener('dragleave', () => els.dropZone.classList.remove('dragover'));
    els.dropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      els.dropZone.classList.remove('dragover');
      if (e.dataTransfer.files.length > 0) {
        handleFileSelect(e.dataTransfer.files[0]);
      }
    });

    els.fileUploadInput.addEventListener('change', (e) => {
      if (e.target.files.length > 0) {
        handleFileSelect(e.target.files[0]);
      }
    });

    function handleFileSelect(file) {
      state.uploadedFile = file;
      state.ingestMode = 'upload';
      els.modalTabs.forEach(t => t.classList.toggle('active', t.dataset.tab === 'upload'));
      els.tabLocal.classList.add('hidden');
      els.tabLocal.classList.remove('active');
      els.tabUpload.classList.remove('hidden');
      els.tabUpload.classList.add('active');
      els.uploadFileName.textContent = `Selected: ${file.name} (${formatBytes(file.size)})`;
      els.uploadFileName.classList.remove('hidden');
    }

    // Wallet Modal Events
    if (els.btnOpenWallet) {
      els.btnOpenWallet.addEventListener('click', () => openWalletModal('funds'));
    }
    if (els.btnWalletModalClose) {
      els.btnWalletModalClose.addEventListener('click', closeWalletModal);
    }
    if (els.btnWalletModalDone) {
      els.btnWalletModalDone.addEventListener('click', closeWalletModal);
    }
    if (els.modalWallet) {
      els.modalWallet.addEventListener('click', (e) => {
        if (e.target === els.modalWallet) closeWalletModal();
      });
    }
    if (els.btnCopyWalletAddress) {
      els.btnCopyWalletAddress.addEventListener('click', copyWalletAddress);
    }
    if (els.btnQuickWithdraw) {
      els.btnQuickWithdraw.addEventListener('click', () => switchWalletTab('withdraw'));
    }

    // Wallet Tabs
    els.walletTabs.forEach(tab => {
      tab.addEventListener('click', () => {
        const tabName = tab.dataset.walletTab;
        if (tabName) switchWalletTab(tabName);
      });
    });

    // Payout Form & Validation
    if (els.payoutAddress) {
      els.payoutAddress.addEventListener('input', validatePayoutAddress);
    }
    if (els.btnUseOwnWallet) {
      els.btnUseOwnWallet.addEventListener('click', () => {
        if (state.walletAddress) {
          els.payoutAddress.value = state.walletAddress;
          validatePayoutAddress();
        } else {
          showToast('No node wallet address loaded yet', 'warning');
        }
      });
    }
    if (els.payoutAmount) {
      els.payoutAmount.addEventListener('input', () => {
        const val = parseInt(els.payoutAmount.value, 10) || 0;
        els.payoutAmountUsd.textContent = microcreditsToUsd(val);
      });
    }
    els.btnPresets.forEach(btn => {
      btn.addEventListener('click', () => {
        const pct = parseFloat(btn.dataset.pct) || 0;
        setPayoutPreset(pct);
      });
    });
    if (els.payoutForm) {
      els.payoutForm.addEventListener('submit', handlePayoutSubmit);
    }

    // Keyboard shortcut (Escape to close modals)
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        if (els.modalWallet && !els.modalWallet.classList.contains('hidden')) {
          closeWalletModal();
        } else if (els.modalIngest && !els.modalIngest.classList.contains('hidden')) {
          closeModal();
        }
      }
    });
  }

  // Auth Handlers
  async function handleLoginSubmit(e) {
    if (e) e.preventDefault();
    const username = els.loginUsername.value.trim();
    const password = els.loginPassword.value;

    if (!username || !password) {
      els.loginErrorText.textContent = 'Please enter both username and password';
      els.loginError.classList.remove('hidden');
      return;
    }

    els.btnLoginSubmit.disabled = true;
    els.loginSubmitSpinner.classList.remove('hidden');
    els.loginSubmitText.textContent = 'Authenticating...';
    els.loginError.classList.add('hidden');

    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || 'Invalid credentials');
      }

      const data = await res.json();
      state.token = data.token;
      state.username = data.username || username;
      const ttlSecs = data.expires_in_seconds || 3600;
      const expiresAt = Date.now() + (ttlSecs * 1000);
      state.expiresAt = expiresAt;

      localStorage.setItem('webload_token', data.token);
      localStorage.setItem('webload_username', state.username);
      localStorage.setItem('webload_expires_at', expiresAt.toString());

      scheduleSessionTimeout(expiresAt);
      showToast(`Welcome back, ${state.username}!`, 'success');
      enterDashboard();
    } catch (err) {
      els.loginErrorText.textContent = err.message || 'Authentication failed';
      els.loginError.classList.remove('hidden');
      els.loginPassword.focus();
    } finally {
      els.btnLoginSubmit.disabled = false;
      els.loginSubmitSpinner.classList.add('hidden');
      els.loginSubmitText.textContent = 'Unlock Dashboard';
    }
  }

  async function handleLogout() {
    try {
      await apiFetch('/api/auth/logout', { method: 'POST' });
    } catch (_) {}
    showToast('Logged out of session', 'info');
    showLoginScreen();
  }

  // Application Bootstrap
  document.addEventListener('DOMContentLoaded', async () => {
    initEvents();

    const savedToken = localStorage.getItem('webload_token');
    const savedExpiresAt = parseInt(localStorage.getItem('webload_expires_at') || '0', 10);
    const now = Date.now();

    if (savedToken && savedExpiresAt > now) {
      state.token = savedToken;
      state.username = localStorage.getItem('webload_username') || 'admin';
      state.expiresAt = savedExpiresAt;
      document.documentElement.classList.add('has-active-session');
      els.loginScreen.classList.add('hidden');

      try {
        const res = await fetch('/api/auth/status', {
          headers: { 'Authorization': `Bearer ${state.token}` }
        });
        if (res.ok) {
          const data = await res.json();
          state.username = data.username || state.username;
          const remainingSecs = data.expires_in_seconds || Math.max(1, Math.floor((savedExpiresAt - Date.now()) / 1000));
          const effectiveExpiresAt = Date.now() + (remainingSecs * 1000);
          state.expiresAt = effectiveExpiresAt;
          localStorage.setItem('webload_expires_at', effectiveExpiresAt.toString());
          scheduleSessionTimeout(effectiveExpiresAt);
          enterDashboard();
          return;
        }
      } catch (err) {
        console.warn('Session verification error:', err);
      }
    } else if (savedToken && savedExpiresAt <= now) {
      showLoginScreen('Your session timed out after 1 hour. Please log in again.');
      return;
    }

    showLoginScreen();
  });
})();
