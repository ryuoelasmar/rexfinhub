/* ===== Scrolling Ticker Bar ===== */
(function() {
  var track = document.getElementById('tickerTrack');
  if (!track) return;

  function buildTicker(products) {
    var html = '';
    products.forEach(function(p) {
      var color = p.change_pct >= 0 ? 'var(--green)' : 'var(--red)';
      var arrow = p.change_pct >= 0 ? '\u25B2' : '\u25BC';
      var pct = p.change_pct !== null ? (p.change_pct >= 0 ? '+' : '') + p.change_pct.toFixed(1) + '%' : '--';
      html += '<span class="ticker-item">';
      html += '<span class="ticker-symbol">' + p.ticker + '</span> ';
      html += '<span class="ticker-value">' + p.value + '</span> ';
      html += '<span class="ticker-change" style="color:' + color + '">' + arrow + ' ' + pct + '</span>';
      html += '</span>';
    });
    // Duplicate for seamless loop
    track.innerHTML = html + html;

    // Calculate animation duration based on width
    var itemWidth = track.scrollWidth / 2;
    var duration = Math.max(itemWidth / 60, 15); // ~60px/s, min 15s
    track.style.animationDuration = duration + 's';
    track.classList.add('ticker-animate');
  }

  // Try to load from API
  fetch('/api/v1/ticker-strip')
    .then(function(r) { return r.ok ? r.json() : Promise.reject(); })
    .then(function(data) { buildTicker(data.products); })
    .catch(function() {
      // Fallback: static REX products
      buildTicker([
        {ticker: 'SOXL', value: '$3.8B AUM', change_pct: 6.4},
        {ticker: 'FNGS', value: '$1.2B AUM', change_pct: 3.1},
        {ticker: 'FEPI', value: '$890M AUM', change_pct: 12.5},
        {ticker: 'NVDX', value: '$73M Flow', change_pct: null},
        {ticker: 'TSLT', value: '$45M AUM', change_pct: -2.1},
        {ticker: 'MSTU', value: '$41M Flow', change_pct: null},
        {ticker: 'OBTC', value: '$32M AUM', change_pct: 8.3},
        {ticker: 'NAIL', value: '$28M AUM', change_pct: -1.4},
      ]);
    });

  // Refresh every 5 minutes
  setInterval(function() {
    fetch('/api/v1/ticker-strip')
      .then(function(r) { return r.ok ? r.json() : Promise.reject(); })
      .then(function(data) { buildTicker(data.products); })
      .catch(function() {});
  }, 300000);

  // Pause on hover
  track.addEventListener('mouseenter', function() { track.style.animationPlayState = 'paused'; });
  track.addEventListener('mouseleave', function() { track.style.animationPlayState = 'running'; });
})();
