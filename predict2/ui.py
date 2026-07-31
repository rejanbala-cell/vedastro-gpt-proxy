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
--text:#eef8f7;--muted:#8eb6c4;--teal:#45dbc2;--warn:#f4d77b;
--bad:#ff7f94;--good:#8ce8c9}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#0b2d2d 0,transparent 30%),var(--bg);color:var(--text);font-family:Inter,system-ui,sans-serif}
button,input{font:inherit}.shell{max-width:1280px;margin:auto;padding:34px 24px 80px}
.card{background:rgba(11,29,39,.94);border:1px solid #1d5b5a;border-radius:24px;padding:28px}
.login{max-width:480px;margin:10vh auto}.brand{width:58px;height:58px;border-radius:17px;background:var(--teal);display:grid;place-items:center;color:#062225;font-weight:900;font-size:25px}
h1{margin:16px 0 6px;font-size:34px}.muted{color:var(--muted)}
label{display:block;margin-top:24px}input{width:100%;margin-top:8px;background:#081720;border:1px solid #294652;color:var(--text);padding:15px;border-radius:14px}
button{border:0;border-radius:14px;padding:13px 18px;font-weight:800;cursor:pointer}
.primary{background:var(--teal);color:#062225}.secondary{background:#102632;color:var(--text);border:1px solid #294652}.danger{color:var(--bad);margin-top:12px}
.top{display:flex;gap:18px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap}.actions{display:flex;gap:10px;flex-wrap:wrap}
.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:24px 0 14px}.tab{background:#102632;color:#b7d2dc;border:1px solid #294652}.tab.active{color:var(--teal);border-color:#2da895}
.searchrow{display:grid;grid-template-columns:1fr auto;gap:10px}.status{margin:14px 0;padding:12px 14px;border:1px solid #445138;background:#1b231f;border-radius:14px;color:var(--warn)}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:16px}.fixture{background:var(--panel);border:1px solid #203e4a;border-radius:20px;padding:18px}.meta{display:flex;justify-content:space-between;gap:10px;color:#79aec1;font-size:13px}.teams{display:grid;grid-template-columns:1fr auto 1fr;gap:10px;align-items:center;margin:18px 0;font-weight:900}.teams div:last-child{text-align:right}.venue{background:#071821;padding:13px;border-radius:13px;min-height:82px}.badge{display:inline-block;margin-top:10px;padding:5px 9px;border:1px solid #544f2d;border-radius:999px;color:var(--warn);font-size:12px}.badge.good{border-color:#286d5b;color:var(--good)}.cardactions{display:grid;gap:8px;margin-top:14px}.verify{background:#163d42;color:var(--teal);border:1px solid #2c7775}.disabled{background:#18313a;color:#70909b;cursor:not-allowed}.small{font-size:12px}.hidden{display:none!important}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:620px){.shell{padding:18px 12px 60px}.card{padding:22px}.grid{grid-template-columns:1fr}.searchrow{grid-template-columns:1fr}.top{display:block}.actions{margin-top:16px}}
</style>
</head>
<body>
<div class="shell">
<section id="login" class="card login">
  <div class="brand">R</div>
  <h1>Private Predictor</h1>
  <p class="muted">PREDICT2 venue-enrichment phase.</p>
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
      <div class="small" style="color:var(--teal);font-weight:900;letter-spacing:2px">PREDICT2 · FINAL ONE-CLICK</div>
      <h1>Upcoming matches</h1>
      <p class="muted">Fixtures load from PostgreSQL. Predict automatically verifies evidence, attempts one valid chart, and freezes one exact 90-minute outcome.</p>
    </div>
    <div class="actions">
      <button id="sync" class="secondary">Sync fixtures</button>
      
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

  <div id="predictionModal" class="hidden" style="position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:20;padding:20px;overflow:auto">
    <div class="card" style="max-width:900px;margin:5vh auto">
      <div style="display:flex;justify-content:space-between;gap:12px">
        <div><div class="small" style="color:var(--teal);font-weight:900;letter-spacing:2px">PREDICTION WORKFLOW</div><h1 id="predictionTitle">Preparing prediction</h1></div>
        <button id="closePrediction" class="secondary">×</button>
      </div>
      <div id="predictionBody" class="status">Verifying fixture, market, performance and venue…</div>
    </div>
  </div>
</section>
</div>

<script>
const $=selector=>document.querySelector(selector);
let currentWindow="today";
let activeVenueJobId=null;

async function api(url,options={}){
  const response=await fetch(url,{
    credentials:"same-origin",
    headers:{"Content-Type":"application/json",...(options.headers||{})},
    ...options
  });
  const text=await response.text();
  let data={};
  if(text){try{data=JSON.parse(text)}catch{data={detail:text.slice(0,300)}}}
  if(!response.ok){
    const detail=typeof data.detail==="string"
      ? data.detail
      : data.detail?.message||data.message||`HTTP ${response.status}`;
    throw new Error(detail);
  }
  return data;
}

function escapeHtml(value){
  return String(value??"").replace(/[&<>"']/g,char=>({
    "&":"&amp;","<":"&lt;",">":"&gt;",
    "\"":"&quot;","'":"&#039;"
  }[char]));
}

function showLogin(){
  $("#login").classList.remove("hidden");
  $("#app").classList.add("hidden");
}

function showApp(){
  $("#login").classList.add("hidden");
  $("#app").classList.remove("hidden");
  loadFixtures();
}

function venueBadge(row){
  if(row.venue_ready){
    return `<span class="badge good">Venue verified</span>`;
  }
  const status=row.venue_attempt_status||"not_attempted";
  return `<span class="badge">${escapeHtml(status.replaceAll("_"," "))}</span>`;
}

function render(rows){
  $("#grid").innerHTML=rows.map(row=>`
    <article class="fixture">
      <div class="meta">
        <span>${escapeHtml(row.competition_name)}</span>
        <span>${escapeHtml(row.competition_country||"")}</span>
      </div>
      <div class="teams">
        <div>${escapeHtml(row.home_team)}</div>
        <span class="muted">vs</span>
        <div>${escapeHtml(row.away_team)}</div>
      </div>
      <div class="venue">
        <strong>${escapeHtml(row.kickoff_local.display)}</strong><br>
        <span class="muted">
          ${escapeHtml(row.venue_name||"Venue pending")}
          ${row.venue_city?` · ${escapeHtml(row.venue_city)}`:""}
        </span>
      </div>
      ${venueBadge(row)}
      <span class="badge">${escapeHtml(row.provider)}</span>
      <div class="cardactions">
        ${row.prediction_exists
          ? `<button class="primary predict" data-fixture-id="${row.id}">${escapeHtml(row.prediction_outcome_label||"View prediction")}</button>`
          : `<button class="primary predict" data-fixture-id="${row.id}">Predict</button>`}
      </div>
    </article>
  `).join("");

  document.querySelectorAll(".predict").forEach(button=>{
    button.onclick=()=>predictOne(Number(button.dataset.fixtureId));
  });
}

async function loadFixtures(){
  $("#status").textContent="Loading stored fixtures…";
  try{
    const search=encodeURIComponent($("#search").value.trim());
    const data=await api(`/private/api/fixtures?window=${currentWindow}&search=${search}`);
    render(data.fixtures);
    const verified=data.fixtures.filter(row=>row.venue_ready).length;
    $("#status").textContent=`${data.count} fixture(s) · ${verified} venue(s) verified · database-only page load.`;
  }catch(error){
    $("#status").textContent=error.message;
  }
}

async function pollFixtureSync(){
  const button=$("#sync");
  for(let attempt=0;attempt<180;attempt++){
    const data=await api("/private/api/sync-status");
    const job=data.current_job||{};
    const progress=job.chunks_total
      ? ` ${job.chunks_completed||0}/${job.chunks_total} chunks · ${job.imported||0} imported.`
      : "";
    $("#status").textContent=`Fixture sync ${job.status||"unknown"}.${progress} ${job.message||""}`.trim();
    if(["ok","error","provider_error","busy"].includes(job.status)){
      button.disabled=false;
      if(job.status==="ok")await loadFixtures();
      return;
    }
    await new Promise(resolve=>setTimeout(resolve,2000));
  }
  button.disabled=false;
}

async function pollVenueJob(jobId){
  const bulk=$("#venues");
  for(let attempt=0;attempt<180;attempt++){
    const data=await api(`/private/api/venue-status?job_id=${encodeURIComponent(jobId)}`);
    const job=data.current_job||{};
    const progress=job.fixtures_total
      ? ` ${job.fixtures_completed||0}/${job.fixtures_total} · ${job.verified||0} verified · ${job.unresolved||0} unresolved.`
      : "";
    const stage=job.current_stage
      ? ` Stage: ${job.current_stage.replaceAll("_"," ")}.`
      : "";
    const elapsed=Number.isFinite(job.elapsed_seconds)
      ? ` Elapsed: ${job.elapsed_seconds}s.`
      : "";
    $("#status").textContent=`Venue job ${job.status||"unknown"}.${progress}${stage}${elapsed} ${job.message||""}`.trim();
    if(["ok","error","busy"].includes(job.status)){
      bulk.disabled=false;
      const finalText=job.status==="error"
        ? `Venue job failed: ${job.error_type||""} · ${job.message||""}`
        : `Venue job ${job.status}. ${job.fixtures_completed||0}/${job.fixtures_total||0} processed · ${job.verified||0} verified · ${job.unresolved||0} unresolved · ${job.skipped||0} skipped · ${job.errors||0} errors. ${job.message||""}`;
      await loadFixtures();
      $("#status").textContent=finalText.trim();
      return;
    }
    await new Promise(resolve=>setTimeout(resolve,2000));
  }
  bulk.disabled=false;
  $("#status").textContent="Venue job is still running. Refresh to check progress.";
}

async function verifyOne(fixtureId){
  $("#status").textContent="Starting venue verification…";
  try{
    const data=await api(`/private/api/fixtures/${fixtureId}/enrich-venue`,{method:"POST"});
    const job=data.job||{};
    activeVenueJobId=data.job_id||job.job_id;
    if(data.status==="busy"){
      activeVenueJobId=job.job_id;
      $("#status").textContent="Another venue job is already running; showing its progress.";
    }
    if(!activeVenueJobId){
      throw new Error("The server did not return a venue job id.");
    }
    await pollVenueJob(activeVenueJobId);
  }catch(error){
    $("#status").textContent=`Could not start venue verification: ${error.message}`;
  }
}


function formatPrediction(prediction){
  if(!prediction)return "No stored prediction.";
  const market=prediction.market_json||{};
  const performance=prediction.performance_json||{};
  const decision=prediction.decision_json||{};
  const diagnostic=prediction.diagnostic_json||{};
  return `
    <div style="font-size:28px;font-weight:900;color:var(--teal);margin-bottom:14px">${escapeHtml(prediction.outcome_label)}</div>
    <div><strong>Confidence:</strong> ${escapeHtml(prediction.confidence)} · <strong>Eligibility:</strong> ${escapeHtml(prediction.eligibility)}</div>
    <div><strong>Method:</strong> ${escapeHtml(prediction.method)}</div>
    <div><strong>Reason:</strong> ${escapeHtml(decision.reason||"")}</div>
    <hr style="border-color:#294652;margin:18px 0">
    <div><strong>Market favourite:</strong> ${escapeHtml(prediction.favourite_team||"Not verified")}</div>
    <div><strong>Market domains:</strong> ${escapeHtml(market.distinct_domains??0)}</div>
    <div><strong>Performance baseline:</strong> ${escapeHtml(performance.baseline_outcome||"")}</div>
    <div><strong>Astrology call made:</strong> ${decision.chart_call_made?"Yes — exactly one":"No — performance-only fallback"}</div>
    <div><strong>Immutable event ID:</strong> ${escapeHtml(prediction.event_id)}</div>
    <p class="muted">Astrology is not scientifically validated and sports outcomes remain uncertain.</p>
  `;
}

async function predictOne(fixtureId){
  $("#predictionModal").classList.remove("hidden");
  $("#predictionTitle").textContent="Preparing deep prediction";
  $("#predictionBody").textContent="Automatically verifying venue, market, performance and chart eligibility…";
  try{
    const existing=await api(`/private/api/fixtures/${fixtureId}/prediction`);
    let prediction=existing.prediction;
    if(!prediction){
      const result=await api(`/private/api/fixtures/${fixtureId}/predict`,{method:"POST"});
      prediction=result.prediction;
    }
    $("#predictionTitle").textContent="Prediction frozen";
    $("#predictionBody").innerHTML=formatPrediction(prediction);
    await loadFixtures();
  }catch(error){
    $("#predictionTitle").textContent="Prediction workflow stopped";
    $("#predictionBody").textContent=error.message;
  }
}
$("#closePrediction").onclick=()=>$("#predictionModal").classList.add("hidden");

$("#loginForm").onsubmit=async event=>{
  event.preventDefault();
  $("#loginError").textContent="";
  try{
    await api("/private/api/login",{
      method:"POST",
      body:JSON.stringify({password:$("#password").value})
    });
    $("#password").value="";
    showApp();
  }catch(error){
    $("#loginError").textContent=error.message;
  }
};

$("#logout").onclick=async()=>{
  try{await api("/private/api/logout",{method:"POST"})}
  finally{showLogin()}
};

$("#refresh").onclick=loadFixtures;
$("#search").addEventListener("input",()=>{
  clearTimeout(window._searchTimer);
  window._searchTimer=setTimeout(loadFixtures,350);
});

document.querySelectorAll(".tab").forEach(button=>{
  button.onclick=()=>{
    document.querySelectorAll(".tab").forEach(item=>item.classList.remove("active"));
    button.classList.add("active");
    currentWindow=button.dataset.window;
    loadFixtures();
  };
});

$("#sync").onclick=async()=>{
  const button=$("#sync");
  button.disabled=true;
  $("#status").textContent="Starting fixture sync…";
  try{
    await api("/private/api/sync-football-data",{method:"POST"});
    await pollFixtureSync();
  }catch(error){
    button.disabled=false;
    $("#status").textContent=`Could not start fixture sync: ${error.message}`;
  }
};



(async()=>{
  try{
    await api("/private/api/session");
    showApp();
    const data=await api("/private/api/venue-status");
    const job=data.current_job||{};
    if(["queued","running"].includes(job.status)&&job.job_id){
      activeVenueJobId=job.job_id;
      pollVenueJob(activeVenueJobId);
    }
  }catch{
    showLogin();
  }
})();
</script>
</body>
</html>
"""
