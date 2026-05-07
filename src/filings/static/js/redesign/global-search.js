/* ============================================================================
 * PaperPanda Redesign — Global fuzzy search (Fuse.js)
 *
 * Extracted from templates/base.html line 2264-2591 so the redesign can
 * load it via <script src> without depending on the OLD base.html.
 *
 * Public API (all on window):
 *   ppGlobalSearch(query, containerId)      — render results into containerId
 *   ppSearchKeydown(event, containerId)     — arrow nav + enter + esc
 *   tickerSearchGo(ticker, type, cik, ...)  — navigate to result destination
 *   escHtml(s)                              — XSS-safe HTML escape
 *
 * Result containerId can be ANY element on the page (e.g. the redesign's
 * `pp-hero-search-results` or the legacy `ticker-search-results`); state
 * is per-container so multiple search inputs can coexist.
 *
 * Loads the search corpus from /api/ticker-search-index with a 30-min
 * sessionStorage cache.  Builds three Fuse instances (stocks / investors
 * / politicians) and surfaces them through a single dispatcher function.
 *
 * Required globals at load time:  Fuse  (from cdn.jsdelivr.net)
 * ==========================================================================*/

(function() {
  'use strict';

  /* ─── Fuse instances + ready flag ─── */
  var _fuseStocks = null;
  var _fuseInvestors = null;
  var _fusePoliticians = null;
  var _indexReady = false;

  function _initFuse(data) {
    var stocks = [];
    var investors = [];
    var politicians = [];
    for (var i = 0; i < data.length; i++) {
      if (data[i].type === 'investor') {
        investors.push(data[i]);
      } else if (data[i].type === 'politician') {
        politicians.push(data[i]);
      } else {
        stocks.push(data[i]);
      }
    }
    _fuseStocks = new Fuse(stocks, {
      keys: [
        { name: 'ticker', weight: 1.0 },
        { name: 'name', weight: 0.5 }
      ],
      threshold: 0.3,
      distance: 100,
      includeScore: true,
      minMatchCharLength: 1,
      shouldSortByScore: true,
      sortFn: function(a, b) {
        var scoreDiff = a.score - b.score;
        if (Math.abs(scoreDiff) > 0.05) return scoreDiff;
        var aItem = a.item || {};
        var bItem = b.item || {};
        var aBoost = (aItem.held_by_super ? 0 : 1) * 10 + (aItem.in_sp500 ? 0 : 1);
        var bBoost = (bItem.held_by_super ? 0 : 1) * 10 + (bItem.in_sp500 ? 0 : 1);
        return aBoost - bBoost;
      }
    });
    _fuseInvestors = new Fuse(investors, {
      keys: [{ name: 'ticker', weight: 1.0 }, { name: 'name', weight: 0.7 }],
      threshold: 0.35, distance: 100, includeScore: true, minMatchCharLength: 1,
    });
    _fusePoliticians = new Fuse(politicians, {
      keys: [{ name: 'ticker', weight: 1.0 }, { name: 'name', weight: 0.7 }],
      threshold: 0.35, distance: 100, includeScore: true, minMatchCharLength: 1,
    });
    _indexReady = true;
  }

  /* ─── Index load — sessionStorage 30-min cache ─── */
  var _SS_KEY = 'pp_ticker_index';
  var _SS_TTL = 1800000; // 30 min
  var _cacheHit = false;
  try {
    var raw = sessionStorage.getItem(_SS_KEY);
    if (raw) {
      var cached = JSON.parse(raw);
      if (cached && cached.ts && (Date.now() - cached.ts) < _SS_TTL && cached.data) {
        _initFuse(cached.data);
        _cacheHit = true;
      }
    }
  } catch (e) { /* fall through to fetch */ }

  if (!_cacheHit) {
    fetch('/api/ticker-search-index')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        _initFuse(data);
        try {
          sessionStorage.setItem(_SS_KEY, JSON.stringify({ ts: Date.now(), data: data }));
        } catch (e) {}
      })
      .catch(function() { _indexReady = true; });
  }

  /* ─── XSS-safe escape ─── */
  window.escHtml = function(s) {
    if (!s) return '';
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  };
  var escHtml = window.escHtml;

  /* ─── Per-container state for keyboard nav ─── */
  var _containerState = {};
  function _getState(id) {
    if (!_containerState[id]) {
      _containerState[id] = { selectedIdx: -1, flatItems: [], debounceTimer: null };
    }
    return _containerState[id];
  }

  /* ─── Public: ppGlobalSearch(query, containerId) ─── */
  window.ppGlobalSearch = function(query, containerId) {
    var state = _getState(containerId);
    clearTimeout(state.debounceTimer);
    state.debounceTimer = setTimeout(function() { _doFilter(query, containerId); }, 150);
  };

  function _doFilter(query, containerId) {
    var container = document.getElementById(containerId);
    var state = _getState(containerId);
    if (!container || !_indexReady) return;

    query = (query || '').trim();
    if (query.length < 1) {
      container.style.display = 'none';
      state.flatItems = [];
      return;
    }

    var stockResults = _fuseStocks ? _fuseStocks.search(query, { limit: 10 }) : [];
    var investorResults = _fuseInvestors ? _fuseInvestors.search(query, { limit: 5 }) : [];
    var politicianResults = _fusePoliticians ? _fusePoliticians.search(query, { limit: 5 }) : [];

    var stocks = stockResults.map(function(r) { return r.item; });
    var investors = investorResults.map(function(r) { return r.item; });
    var politicians = politicianResults.map(function(r) { return r.item; });

    /* Boost exact ticker prefix matches */
    var qUpper = query.toUpperCase();
    stocks.sort(function(a, b) {
      var aExact = a.ticker.toUpperCase().indexOf(qUpper) === 0 ? 0 : 1;
      var bExact = b.ticker.toUpperCase().indexOf(qUpper) === 0 ? 0 : 1;
      if (aExact !== bExact) return aExact - bExact;
      return (a.held_by_super ? 0 : 1) - (b.held_by_super ? 0 : 1);
    });

    if (stocks.length === 0 && investors.length === 0 && politicians.length === 0) {
      container.innerHTML = '<div class="ts-empty">No results for &ldquo;' + escHtml(query) + '&rdquo;</div>';
      container.style.display = 'block';
      state.flatItems = [];
      state.selectedIdx = -1;
      return;
    }

    var html = '';
    state.flatItems = [];
    var globalIdx = 0;

    if (stocks.length > 0) {
      html += '<div class="ts-category">Stocks</div>';
      for (var s = 0; s < stocks.length; s++) {
        var st = stocks[s];
        var exchange = st.exchange || '';
        var superStar = st.held_by_super
          ? '<span class="ts-star" title="Held by superinvestors">&#9733;</span>'
          : '';
        var metaParts = [];
        if (exchange) metaParts.push(exchange);
        if (st.in_sp500) metaParts.push('S&P 500');
        var metaStr = metaParts.join(' &middot; ');
        html += '<div class="ts-item" data-gidx="' + globalIdx + '"'
          + ' data-ticker="' + escHtml(st.ticker) + '"'
          + ' data-type="ticker" data-cik="">'
          + superStar
          + '<span class="ts-item-ticker">' + escHtml(st.ticker) + '</span>'
          + '<span class="ts-item-name">' + escHtml(st.name) + '</span>'
          + (metaStr ? '<span class="ts-item-meta">' + metaStr + '</span>' : '')
          + '</div>';
        state.flatItems.push({ ticker: st.ticker, type: 'ticker', cik: '' });
        globalIdx++;
      }
    }

    if (investors.length > 0) {
      html += '<div class="ts-category">Investors</div>';
      for (var v = 0; v < investors.length; v++) {
        var inv = investors[v];
        html += '<div class="ts-item" data-gidx="' + globalIdx + '"'
          + ' data-ticker="' + escHtml(inv.ticker) + '"'
          + ' data-type="investor" data-cik="' + escHtml(inv.cik || '') + '">'
          + '<span class="ts-icon">&#128100;</span>'
          + '<span class="ts-item-ticker">' + escHtml(inv.ticker) + '</span>'
          + '<span class="ts-item-name">' + escHtml(inv.name) + '</span>'
          + '<span class="ts-item-meta">Fund</span>'
          + '</div>';
        state.flatItems.push({ ticker: inv.ticker, type: 'investor', cik: inv.cik || '' });
        globalIdx++;
      }
    }

    if (politicians.length > 0) {
      html += '<div class="ts-category">Congress</div>';
      for (var p = 0; p < politicians.length; p++) {
        var pol = politicians[p];
        var partyClass = (pol.party === 'Democrat') ? 'pp-party-d'
          : (pol.party === 'Republican') ? 'pp-party-r' : '';
        html += '<div class="ts-item" data-gidx="' + globalIdx + '"'
          + ' data-ticker="' + escHtml(pol.ticker) + '"'
          + ' data-type="politician" data-member-id="' + escHtml(pol.member_id || '') + '">'
          + '<span class="ts-icon ' + partyClass + '">&#9679;</span>'
          + '<span class="ts-item-ticker">' + escHtml(pol.ticker) + '</span>'
          + '<span class="ts-item-name">' + escHtml(pol.name) + '</span>'
          + '<span class="ts-item-meta">Congress</span>'
          + '</div>';
        state.flatItems.push({ ticker: pol.ticker, type: 'politician', member_id: pol.member_id || '' });
        globalIdx++;
      }
    }

    state.selectedIdx = -1;
    container.innerHTML = html;
    container.style.display = 'block';

    /* Click handlers — attach AFTER innerHTML write, share state */
    container.querySelectorAll('.ts-item').forEach(function(el) {
      el.addEventListener('click', function() {
        var idx = parseInt(el.dataset.gidx, 10);
        if (state.flatItems[idx]) {
          window.tickerSearchGo(
            state.flatItems[idx].ticker,
            state.flatItems[idx].type,
            state.flatItems[idx].cik,
            state.flatItems[idx].member_id
          );
        }
      });
    });
  }

  /* ─── Public: ppSearchKeydown(event, containerId) ─── */
  window.ppSearchKeydown = function(e, containerId) {
    var container = document.getElementById(containerId);
    var state = _getState(containerId);
    if (!container || container.style.display === 'none' || state.flatItems.length === 0) return;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      state.selectedIdx = Math.min(state.selectedIdx + 1, state.flatItems.length - 1);
      _highlight(container, state);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      state.selectedIdx = Math.max(state.selectedIdx - 1, 0);
      _highlight(container, state);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      var idx = state.selectedIdx >= 0 ? state.selectedIdx : 0;
      if (state.flatItems[idx]) {
        window.tickerSearchGo(
          state.flatItems[idx].ticker,
          state.flatItems[idx].type,
          state.flatItems[idx].cik,
          state.flatItems[idx].member_id
        );
      }
    } else if (e.key === 'Escape') {
      container.style.display = 'none';
      e.target.blur();
    }
  };

  function _highlight(container, state) {
    container.querySelectorAll('.ts-item').forEach(function(el) {
      var idx = parseInt(el.dataset.gidx, 10);
      if (idx === state.selectedIdx) {
        el.classList.add('ts-active');
        el.scrollIntoView({ block: 'nearest' });
      } else {
        el.classList.remove('ts-active');
      }
    });
  }

  /* ─── Public: tickerSearchGo(ticker, type, cik, memberId) ─── */
  window.tickerSearchGo = function(ticker, type, cik, memberId) {
    if (window.ppCloseSearchPalette) ppCloseSearchPalette();
    if (window.posthog) {
      posthog.capture('stock_search', {
        ticker: ticker,
        search_type: type,
        cik: cik || null,
        member_id: memberId || null
      });
    }
    if (type === 'investor' && cik) {
      // v2 funds page reads ?cik=…; falls back to Berkshire if omitted.
      window.location.href = '/_v2/funds/' + encodeURIComponent(cik);
    } else if (type === 'politician' && memberId) {
      // No v2 politician profile yet — keep v1 destination for now.
      window.location.href = '/politician/' + encodeURIComponent(memberId);
    } else {
      window.location.href = '/_v2/stock/' + encodeURIComponent(ticker);
    }
  };

  /* ─── Outside-click handler — close any open results dropdown ─── */
  if (!window._ppSearchClickBound) {
    window._ppSearchClickBound = true;
    document.addEventListener('click', function(e) {
      /* For each registered container, hide if click is outside its parent */
      Object.keys(_containerState).forEach(function(id) {
        var container = document.getElementById(id);
        if (!container) return;
        /* The "wrap" is the closest pp-hero-search ancestor (or, fallback,
           the parent element). */
        var wrap = container.closest('.pp-hero-search')
          || (container.parentElement || container);
        if (!wrap.contains(e.target)) {
          container.style.display = 'none';
        }
      });
    });
  }
})();
