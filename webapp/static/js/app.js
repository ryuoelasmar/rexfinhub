// ETP Filing Tracker - Dashboard JavaScript

// Toggle trust accordion
function toggleTrust(el) {
  el.parentElement.classList.toggle('open');
}

// Toggle download group
function toggleDl(el) {
  el.parentElement.classList.toggle('open');
}

// Jump to trust from dropdown
function jumpToTrust(id) {
  if (!id) return;
  var el = document.getElementById('trust-' + id);
  if (el) {
    el.classList.add('open');
    el.scrollIntoView({behavior: 'smooth', block: 'start'});
  }
}

// Filter table rows
function filterTable(tableId, query, statusFilter) {
  var table = document.getElementById(tableId);
  if (!table) return;
  var rows = table.querySelectorAll('tbody tr');
  var q = (query || '').toLowerCase();
  var shown = 0;
  rows.forEach(function(row) {
    var name = (row.getAttribute('data-name') || '').toLowerCase();
    var ticker = (row.getAttribute('data-ticker') || '').toLowerCase();
    var status = row.getAttribute('data-status') || '';
    var matchText = !q || name.indexOf(q) >= 0 || ticker.indexOf(q) >= 0;
    var matchStatus = !statusFilter || statusFilter === 'ALL' || status === statusFilter;
    if (matchText && matchStatus) {
      row.style.display = '';
      shown++;
    } else {
      row.style.display = 'none';
    }
  });
  var countEl = table.parentElement.querySelector('.filter-count');
  if (countEl) countEl.textContent = shown + ' of ' + rows.length + ' funds';
}

// Status pill click
function setStatusFilter(btn, tableId) {
  var bar = btn.closest('.filter-bar');
  bar.querySelectorAll('.pill').forEach(function(p) { p.classList.remove('active'); });
  btn.classList.add('active');
  var search = bar.querySelector('input');
  filterTable(tableId, search ? search.value : '', btn.getAttribute('data-status'));
}

// Global search across all trust blocks
function globalSearch(query) {
  var q = query.toLowerCase();
  document.querySelectorAll('.trust-block').forEach(function(block) {
    var table = block.querySelector('table');
    if (!table) return;
    var rows = table.querySelectorAll('tbody tr');
    var anyMatch = false;
    rows.forEach(function(row) {
      var name = (row.getAttribute('data-name') || '').toLowerCase();
      var ticker = (row.getAttribute('data-ticker') || '').toLowerCase();
      if (!q || name.indexOf(q) >= 0 || ticker.indexOf(q) >= 0) {
        row.style.display = '';
        anyMatch = true;
      } else {
        row.style.display = 'none';
      }
    });
    if (q && anyMatch) {
      block.classList.add('open');
    }
  });
}

// Column sorting
function sortTable(tableId, colIdx) {
  var table = document.getElementById(tableId);
  if (!table) return;
  var tbody = table.querySelector('tbody');
  var rows = Array.from(tbody.querySelectorAll('tr'));
  var asc = table.getAttribute('data-sort-dir') !== 'asc';
  table.setAttribute('data-sort-dir', asc ? 'asc' : 'desc');
  rows.sort(function(a, b) {
    var aVal = a.cells[colIdx] ? a.cells[colIdx].textContent.trim() : '';
    var bVal = b.cells[colIdx] ? b.cells[colIdx].textContent.trim() : '';
    var aNum = parseFloat(aVal);
    var bNum = parseFloat(bVal);
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return asc ? aNum - bNum : bNum - aNum;
    }
    return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
  });
  rows.forEach(function(row) { tbody.appendChild(row); });
}

// Back to top visibility
window.addEventListener('scroll', function() {
  var btn = document.getElementById('backTop');
  if (btn) btn.classList.toggle('visible', window.scrollY > 300);
});
