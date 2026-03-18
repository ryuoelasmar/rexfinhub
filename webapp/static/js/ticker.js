/* Scrolling Ticker Bar */
(function() {
  var track = document.getElementById('tickerTrack');
  if (!track) return;

  function buildTicker(products) {
    if (!products || products.length === 0) {
      track.parentElement.style.display = 'none';
      return;
    }
    var html = '';
    products.forEach(function(p) {
      var pct = p.change_pct;
      var color = pct >= 0 ? '#00C853' : '#F44336';
      var arrow = pct >= 0 ? '\u25B2' : '\u25BC';
      var sign = pct >= 0 ? '+' : '';
      html += '<span class="ticker-item">';
      html += '<span class="ticker-sym">' + p.ticker + '</span>';
      html += '<span class="ticker-val">' + p.value + '</span>';
      html += '<span style="color:' + color + ';font-weight:600;font-size:11px;">' + arrow + ' ' + sign + pct.toFixed(1) + '%</span>';
      html += '</span>';
    });
    track.innerHTML = html + html;
    var w = track.scrollWidth / 2;
    var dur = Math.max(w / 50, 20);
    track.style.animation = 'tickerScroll ' + dur + 's linear infinite';
  }

  fetch('/api/v1/ticker-strip')
    .then(function(r) { return r.ok ? r.json() : Promise.reject(); })
    .then(function(d) { buildTicker(d.products); })
    .catch(function() { track.parentElement.style.display = 'none'; });
})();
