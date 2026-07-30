from __future__ import annotations

PRIVATE_HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Private Predictor — PREDICT2</title>
<style>
:root{--bg:#061017;--panel:#0b1d27;--panel2:#102632;--line:#244451;
--text:#eef8f7;--muted:#8eb6c4;--teal:#45dbc2;--warn:#f4d77b;--bad:#ff7f94}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#0b2d2d 0,transparent 30%),var(--bg);color:var(--text);font-family:Inter,system-ui,sans-serif}
button,input{font:inherit}.shell{max-width:1280px;margin:auto;padding:34px 24px 80px}
.card{background:rgba(11,29,39,.94);border:1px solid #1d5b5a;border-radius:24px;padding:28px}
.login{max-width:480px;margin:10vh auto}.brand{width:58px;height:58px;border-radius:17px;background:var(--teal);display:grid;place-items:center;color:#062225;font-weight:900;font-size:25px}
h1{margin:16px 0 6px;font-size:34px}h2{margin:0 0 8px}.muted{color:var(--muted)}
label{display:block;margin-top:24px}input{width:100%;margin-top:8px;background:#081720;border:1px solid #294652;color:var(--text);padding:15px;border-radius:14px}
button{border:0;border-radius:14px;padding:13px 18px;font-weight:800;cursor:pointer}.primary{background:var(--teal);color:#062225}.secondary{background:#102632;color:var(--text);border:1px solid #294652}.danger{color:var(--bad);margin-top:12px}
.top{display:flex;gap:18px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap}.actions{display:flex;gap:10px;flex-wrap:wrap}
.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:24px 0 14px}.tab{background:#102632;color:#b7d2dc;border:1px solid #294652}.tab.active{color:var(--teal);border-color:#2da895}
.searchrow{display:grid;grid-template-columns:1fr auto;gap:10px}.status{margin:14px 0;padding:12px 14px;border:1px solid #445138;background:#1b231f;border-radius:14px;color:var(--warn)}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:16px}.fixture{background:var(--panel);border:1px solid #203e4a;border-radius:20px;padding:18px}.meta{display:flex;justify-content:space-between;gap:10px;color:#79aec1;font-size:13px}.teams{display:grid;grid-template-columns:1fr auto 1fr;gap:10px;align-items:center;margin:18px 0;font-weight:900}.teams div:last-child{text-align:right}.venue{background:#071821;padding:13px;border-radius:13px;min-height:72px}.badge{display:inline-block;margin-top:10px;padding:5px 9px;border:1px solid #544f2d;border-radius:999px;color:var(--warn);font-size:12px}.disabled{width:100%;margin-top:14px;background:#18313a;color:#70909b;cursor:not-allowed}
.small{font-size:12px}.hidden{display:none!important}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:620px){.shell{padding:18px 12px 60px}.card{padding:22px}.grid{grid-template-columns:1fr}.searchrow{grid-template-columns:1fr}.top{display:block}.actions{margin-top:16px}}
</style>
</head>
<body>
<div class="shell">
<section id="login" class="card login">
  <div class="brand">R</div>
  <h1>Private Predictor</h1>
  <p class="muted">Clean PREDICT2 fixture foundation with 10-day provider windows.</p>
  <form id="loginForm">
    <label>Private password
      <input id="password" type="password" autocomplete="current-password" required>
    </label>
    <button class="primary" style="width:100%;margin-top:14px">Enter dashboard</button>
    <div id="loginError" class="danger"></div>
  </form>
</section>

<section id="app" class="hidden">
  <div class="top">
    <div>
      <div class="small" style="color:var(--teal);font-weight:900;letter-spacing:2px">PREDICT2 FOUNDATION</div>
      <h1>Upcoming matches</h1>
      <p class="muted">Fixtures come from football-data.org and are read from PostgreSQL. Venue-local kickoff is shown only after the venue timezone is verified.</p>
    </div>
    <div class="actions">
      <button id="sync" class="primary">Sync 90 days</button>
      <button id="logout" class="secondary">Log out</button>
    </div>
  </div>
  <div class="tabs">
    <button class="tab active" data-window="today">Today</button>
    <button class="tab" data-window="tomorrow">Tomorrow</button>
    <button class="tab" data-window="3days">Next 3 days</button>
    <button class="tab" data-window="3months">Next 3 months</button>
  </div>
  <div class="searchrow">
    <input id="search" placeholder="Search team, competition, country or venue">
    <button id="refresh" class="secondary">Refresh</button>
  </div>
  <div id="status" class="status">Loading stored fixtures…</div>
  <div id="grid" class="grid"></div>
</section>
</div>
<script>
const $=s=>document.querySelector(s);
let currentWindow="today";
async function api(url,opts={}){
  const response=await fetch(url,{credentials:"same-origin",headers:{"Content-Type":"application/json",...(opts.headers||{})},...opts});
  const text=await response.text();let data={};
  if(text){try{data=JSON.parse(text)}catch{data={detail:text.slice(0,250)}}}
  if(!response.ok)throw new Error(typeof data.detail==="string"?data.detail:(data.detail?.message||data.message||`HTTP ${response.status}`));
  return data;
}
function showLogin(){ $("#login").classList.remove("hidden");$("#app").classList.add("hidden")}
function showApp(){ $("#login").classList.add("hidden");$("#app").classList.remove("hidden");loadFixtures()}
function esc(value){return String(value??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#039;"}[c]))}
function render(rows){
  $("#grid").innerHTML=rows.map(row=>`
    <article class="fixture">
      <div class="meta"><span>${esc(row.competition_name)}</span><span>${esc(row.competition_country||"")}</span></div>
      <div class="teams"><div>${esc(row.home_team)}</div><span class="muted">vs</span><div>${esc(row.away_team)}</div></div>
      <div class="venue">
        <strong>${esc(row.kickoff_local.display)}</strong><br>
        <span class="muted">${esc(row.venue_name||"Venue pending")}${row.venue_city?` · ${esc(row.venue_city)}`:""}</span>
      </div>
      <span class="badge">${esc(row.provider)}</span>
      <button class="disabled" disabled>Prediction engine migration pending</button>
    </article>`).join("");
}
async function loadFixtures(){
  $("#status").textContent="Loading stored fixtures…";
  try{
    const search=encodeURIComponent($("#search").value.trim());
    const data=await api(`/private/api/fixtures?window=${currentWindow}&search=${search}`);
    render(data.fixtures);
    $("#status").textContent=`${data.count} stored fixture(s). Loading is database-only; no provider call was made.`;
  }catch(err){$("#status").textContent=err.message}
}
$("#loginForm").onsubmit=async event=>{
  event.preventDefault();$("#loginError").textContent="";
  try{
    await api("/private/api/login",{method:"POST",body:JSON.stringify({password:$("#password").value})});
    $("#password").value="";showApp();
  }catch(err){$("#loginError").textContent=err.message}
};
$("#logout").onclick=async()=>{try{await api("/private/api/logout",{method:"POST"})}finally{showLogin()}};
$("#refresh").onclick=loadFixtures;
$("#search").addEventListener("input",()=>{clearTimeout(window._searchTimer);window._searchTimer=setTimeout(loadFixtures,350)});
document.querySelectorAll(".tab").forEach(button=>button.onclick=()=>{
  document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));
  button.classList.add("active");currentWindow=button.dataset.window;loadFixtures();
});
async function pollSync(){
  const button=$("#sync");
  for(let attempt=0;attempt<180;attempt++){
    const data=await api("/private/api/sync-status");
    const job=data.current_job||{};
    const progress=job.chunks_total?` ${job.chunks_completed||0}/${job.chunks_total} chunks · ${job.imported||0} imported.`:"";
    $("#status").textContent=`Sync ${job.status||"unknown"}.${progress} ${job.message||""}`.trim();
    if(["ok","error","provider_error","busy"].includes(job.status)){
      button.disabled=false;
      if(job.status==="ok")await loadFixtures();
      if(job.status==="error"||job.status==="provider_error"){
        const detail=[job.error_type,job.message,job.provider_message,job.provider_http_status?`HTTP ${job.provider_http_status}`:""].filter(Boolean).join(" · ");
        $("#status").textContent=`Sync failed: ${detail}`;
      }
      return;
    }
    await new Promise(resolve=>setTimeout(resolve,2000));
  }
  button.disabled=false;
  $("#status").textContent="Sync is still running. Refresh the page to check its status.";
}
$("#sync").onclick=async()=>{
  const button=$("#sync");button.disabled=true;$("#status").textContent="Starting background fixture sync…";
  try{
    const data=await api("/private/api/sync-football-data",{method:"POST"});
    if(data.status==="busy"){$("#status").textContent="A fixture sync is already running."}
    await pollSync();
  }catch(err){button.disabled=false;$("#status").textContent=`Could not start sync: ${err.message}`}
};
(async()=>{try{await api("/private/api/session");showApp()}catch{showLogin()}})();
</script>
</body>
</html>
"""
