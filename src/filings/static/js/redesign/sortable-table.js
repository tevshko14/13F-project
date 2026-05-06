/* ─────────────────────────────────────────────────────────────────────────
 * pp-sortable-table.js — auto-attach click-to-sort to every .pp-table
 *
 * Conventions:
 *   • Reads the first row's cells to detect column type (numeric vs string).
 *   • Each <th> becomes clickable, toggles asc/desc on repeat click.
 *   • Add `data-sortable="false"` to a <th> to opt that column out.
 *   • Add `data-sort-value="…"` to a <td> to override what's compared
 *     (useful when displayed text is decorated, e.g. "★ 9 Gurus" → 9).
 *   • Numeric parser handles +/- prefix, $, commas, K/M/B suffix, and %.
 *   • Empty / "—" cells always sort to the bottom regardless of direction.
 *   • Re-runs on `htmx:afterSettle` so HTMX-injected tables also wire up.
 * ─────────────────────────────────────────────────────────────────────────*/

(function () {
  /** Parse a cell's display string into a number, or null if non-numeric. */
  function parseNum(s) {
    if (s == null) return null;
    var t = String(s).trim();
    if (!t || t === '—' || t === '-') return null;
    // Capture optional leading sign + numeric body, plus optional unit suffix.
    var m = t.match(/(-?\+?\$?[\d,]+\.?\d*)\s*([KMB])?/i);
    if (!m) return null;
    var n = parseFloat(m[1].replace(/[,$+]/g, ''));
    if (isNaN(n)) return null;
    var unit = (m[2] || '').toUpperCase();
    if (unit === 'K') n *= 1e3;
    else if (unit === 'M') n *= 1e6;
    else if (unit === 'B') n *= 1e9;
    return n;
  }

  function cellValue(row, idx) {
    var cell = row.cells && row.cells[idx];
    if (!cell) return '';
    if (cell.dataset && cell.dataset.sortValue !== undefined) {
      return cell.dataset.sortValue;
    }
    return (cell.textContent || '').trim();
  }

  function detectNumeric(rows, idx, sampleSize) {
    var n = Math.min(sampleSize || 5, rows.length);
    var hits = 0, total = 0;
    for (var i = 0; i < n; i++) {
      var v = cellValue(rows[i], idx);
      if (v === '' || v === '—' || v === '-') continue;
      total++;
      if (parseNum(v) !== null) hits++;
    }
    return total > 0 && hits >= Math.ceil(total / 2);
  }

  function sortColumn(table, idx, th) {
    var tbody = table.tBodies && table.tBodies[0];
    if (!tbody) return;
    var rows = Array.prototype.slice.call(tbody.rows);
    if (rows.length < 2) return;

    // Use the existing v2 `aria-sort` convention (matches the Screener CSS
    // in primitives.css so the chevrons render without page-specific rules).
    var dir = (th.getAttribute('aria-sort') === 'ascending') ? 'descending' : 'ascending';

    // Reset siblings.
    var ths = table.querySelectorAll('th');
    for (var i = 0; i < ths.length; i++) {
      ths[i].removeAttribute('aria-sort');
    }
    th.setAttribute('aria-sort', dir);
    dir = (dir === 'ascending') ? 'asc' : 'desc';

    var numeric = detectNumeric(rows, idx);

    rows.sort(function (a, b) {
      var va = cellValue(a, idx);
      var vb = cellValue(b, idx);
      // Empties always sink to the bottom.
      var aEmpty = (va === '' || va === '—' || va === '-');
      var bEmpty = (vb === '' || vb === '—' || vb === '-');
      if (aEmpty && bEmpty) return 0;
      if (aEmpty) return 1;
      if (bEmpty) return -1;
      if (numeric) {
        var na = parseNum(va);
        var nb = parseNum(vb);
        if (na === null && nb === null) return 0;
        if (na === null) return 1;
        if (nb === null) return -1;
        return dir === 'asc' ? na - nb : nb - na;
      }
      var cmp = va.localeCompare(vb, undefined, {numeric: true, sensitivity: 'base'});
      return dir === 'asc' ? cmp : -cmp;
    });

    // Re-append in sorted order; appendChild moves nodes without cloning.
    var frag = document.createDocumentFragment();
    for (var j = 0; j < rows.length; j++) frag.appendChild(rows[j]);
    tbody.appendChild(frag);
  }

  function initTable(table) {
    if (!table || table.dataset.ppSortableInit === 'true') return;
    table.dataset.ppSortableInit = 'true';
    var thead = table.tHead;
    if (!thead || !thead.rows.length) return;
    var ths = thead.rows[0].cells;
    for (var i = 0; i < ths.length; i++) {
      var th = ths[i];
      if (th.getAttribute('data-sortable') === 'false') continue;
      th.classList.add('pp-sortable');
      (function (idx, hdr) {
        hdr.addEventListener('click', function () { sortColumn(table, idx, hdr); });
      })(i, th);
    }
  }

  function initAll(root) {
    var scope = root || document;
    var tables = scope.querySelectorAll ? scope.querySelectorAll('.pp-table') : [];
    for (var i = 0; i < tables.length; i++) initTable(tables[i]);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { initAll(); });
  } else {
    initAll();
  }
  // HTMX-injected tables: re-run after every settle.
  document.addEventListener('htmx:afterSettle', function (ev) { initAll(ev.target); });
})();
