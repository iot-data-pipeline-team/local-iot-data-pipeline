"""
CNC Factory — Industrial Gateway API
=====================================
Simulates a Modbus TCP / OPC-UA industrial gateway.

One endpoint per machine — returns latest sensor reading as JSON.
Visit http://localhost:8000 to see all 4 machines live, auto-refreshing every second.

Usage:
    pip install fastapi uvicorn
    python gateway_api.py

Endpoints:
    GET /machines/CNC_01   → latest reading
    GET /machines/ROB_01   → latest reading
    GET /machines/CNV_01   → latest reading
    GET /machines/PMP_01   → latest reading

Fault injection (for demo — via query param):
    GET /machines/CNC_01?fault=overheat
    GET /machines/CNC_01?fault=none        ← reset
"""

import time
import math
import random
import threading
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import uvicorn


# ─────────────────────────────────────────────────────────────────────────────
#  MACHINE STATE
# ─────────────────────────────────────────────────────────────────────────────

class MachineState:
    def __init__(self, machine_id: str, machine_type: str):
        self.machine_id   = machine_id
        self.machine_type = machine_type
        self.lock         = threading.Lock()

        # Slow degradation over time
        self.oil_level    = random.uniform(70, 95)   # starts high, drains slowly
        self.bearing_wear = random.uniform(0.0, 0.1) # 0.0 = new, 1.0 = failed
        self.oil_drain    = random.uniform(0.003, 0.006)
        self.wear_rate    = random.uniform(0.0001, 0.0002)

        # Fault state — None means normal operation
        self.fault: Optional[str] = None


    def tick(self):
        """Called on every reading — advance degradation."""
        with self.lock:
            self.oil_level    = max(0.0, self.oil_level - self.oil_drain)
            self.bearing_wear = min(1.0, self.bearing_wear + self.wear_rate)



    def set_fault(self, fault: Optional[str]):
        with self.lock:
            if fault == "none":
                self.fault        = None
                self.oil_level    = random.uniform(70, 95)
                self.bearing_wear = 0.02
            else:
                self.fault = fault

    def _noise(self, scale=1.0):
        return random.gauss(0, scale)

    def _lerp(self, a, b, t):
        return a + (b - a) * t

    def reading(self) -> dict:
        self.tick()

        with self.lock:
            wear  = self.bearing_wear
            oil   = self.oil_level
            fault = self.fault

        # ── Causal chain: oil low → temp rises → vibration rises ─────────
        oil_temp_penalty = max(0, (40 - oil) / 40 * 8)   if oil < 40 else 0
        oil_vib_penalty  = max(0, (40 - oil) / 40 * 0.5) if oil < 40 else 0

        # ── Base sensor values ────────────────────────────────────────────
        temp      = self._lerp(70, 86, wear) + oil_temp_penalty + self._noise(0.4)
        vibration = self._lerp(2.0, 4.2, wear) + oil_vib_penalty + self._noise(0.1)
        rpm       = self._lerp(1490, 1380, wear) + self._noise(5)
        power_kw  = self._lerp(4.0, 5.8, wear) + self._noise(0.1)

        # ── Apply injected fault — guaranteed to breach threshold ──────────
        if fault == "overheat":
            temp      = max(91.0, temp + random.uniform(15, 22))
            power_kw += random.uniform(1.0, 1.5)

        elif fault == "vibration":
            vibration = max(5.1, vibration + random.uniform(2.0, 3.5))
            temp     += random.uniform(3, 7)

        elif fault == "rpm_drop":
            rpm       = min(1190, rpm - random.uniform(350, 500))
            power_kw += random.uniform(0.5, 1.0)

        elif fault == "oil_leak":
            self.oil_level = max(0, self.oil_level - 5)  # fast drain
            oil = min(oil, 18.0)  # guarantee below E005 threshold (20%)
            temp      += max(0, (40 - oil) / 40 * 12)
            vibration += max(0, (40 - oil) / 40 * 0.8)

        # ── Round values ──────────────────────────────────────────────────
        temp      = round(temp, 1)
        vibration = round(max(0.1, vibration), 2)
        rpm       = round(max(100, rpm))
        power_kw  = round(max(0.1, power_kw), 2)
        oil_r     = round(max(0.0, oil), 1)

        # ── Detect errors — ONLY populated when threshold breached ────────
        errors = []
        if temp      >  90:    errors.append("E001")  # overheat
        if vibration >  5.0:   errors.append("E002")  # vibration
        if rpm       < 1200:   errors.append("E003")  # rpm drop
        if oil_r     <  20.0:  errors.append("E005")  # oil critical

        # ── Build output ──────────────────────────────────────────────────
        metrics = {
            "temperature":  temp,
            "vibration":    vibration,
            "rpm":          rpm,
            "power_kw":     power_kw,
        }

        # Machine-type specific sensors
        type_sensors = {}
        mtype = self.machine_type

        if mtype == "cnc_machine":
            coolant = self._lerp(2.5, 1.6, wear) + self._noise(0.05)
            if fault == "coolant_low":
                coolant = min(coolant, 1.0)  # guarantee below E004 threshold (1.2 bar)
                errors.append("E004")
            type_sensors = {
                "oil_level_pct":          oil_r,
                "coolant_pressure_bar":   round(max(0.1, coolant), 2),
            }

        elif mtype == "robot_arm":
            torque = self._lerp(40, 82, wear)
            if fault == "overload":
                torque = max(101.0, torque + random.uniform(25, 40))  # guarantee above 100 Nm
                errors.append("E006")
            type_sensors = {
                "joint_torque_nm":       round(max(0, torque + self._noise(1)), 1),
                "end_effector_force_n":  round(max(0, random.uniform(10, 45) + self._noise(2)), 1),
            }

        elif mtype == "conveyor_belt":
            tension = self._lerp(300, 620, wear)
            if fault == "belt_tension":
                tension = max(801.0, tension + random.uniform(200, 350))  # guarantee above 800 N
                errors.append("E007")
            type_sensors = {
                "belt_tension_n":  round(max(0, tension + self._noise(5)), 1),
                "load_weight_kg":  round(max(0, random.uniform(20, 140) + self._noise(3)), 1),
            }

        elif mtype == "pump":
            flow  = self._lerp(55, 20, wear)
            inlet = self._lerp(3.5, 1.6, wear)
            if fault == "flow_drop":
                flow = min(9.0, flow - random.uniform(15, 25))  # guarantee below 10 lpm
                errors.append("E008")
            type_sensors = {
                "oil_level_pct":       oil_r,
                "flow_rate_lpm":       round(max(0, flow + self._noise(1)), 1),
                "inlet_pressure_bar":  round(max(0.1, inlet + self._noise(0.1)), 2),
            }

        # ── Build final document ─────────────────────────────────────────
        now        = datetime.now(tz=timezone.utc)
        ts_iso     = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        event_id   = f"{self.machine_id}_{int(now.timestamp())}"
        floor      = MACHINE_META[self.machine_id]["floor"]
        hour       = now.hour
        shift      = "morning" if 6 <= hour < 14 else ("evening" if 14 <= hour < 22 else "night")
        error_code = errors[0] if errors else None
        is_fault   = bool(errors)
        status     = "fault" if is_fault else ("warning" if wear > 0.5 else "running")

        # Sensor key per machine type
        sensor_key_map = {
            "cnc_machine":   "cnc_sensors",
            "robot_arm":     "robot_sensors",
            "conveyor_belt": "conveyor_sensors",
            "pump":          "pump_sensors",
        }
        sensor_key = sensor_key_map.get(mtype, mtype + "_sensors")

        return {
            "event_id":    event_id,
            "timestamp":   ts_iso,
            "machine_id":  self.machine_id,
            "machine_type": mtype,
            "floor":       floor,
            "shift":       shift,
            "status":      status,
            "error_code":  error_code,
            "is_fault":    is_fault,
            "metrics":     metrics,
            sensor_key:    type_sensors,
        }


# ─────────────────────────────────────────────────────────────────────────────
#  MACHINE REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

MACHINE_META = {
    "CNC_01": {"floor": "A"},
    "ROB_01": {"floor": "B"},
    "CNV_01": {"floor": "C"},
    "PMP_01": {"floor": "C"},
}

MACHINES = {
    "CNC_01": MachineState("CNC_01", "cnc_machine"),
    "ROB_01": MachineState("ROB_01", "robot_arm"),
    "CNV_01": MachineState("CNV_01", "conveyor_belt"),
    "PMP_01": MachineState("PMP_01", "pump"),
}


# ─────────────────────────────────────────────────────────────────────────────
#  FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="CNC Factory Gateway API", version="1.0.0", docs_url=None, redoc_url=None)


# ── Live dashboard at / ───────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>CNC Factory — Live Gateway</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
:root{
  --bg:#0a0c12;--surf:#111620;--card:#161e2e;
  --bdr:#1e2d48;--bdr2:#253650;
  --or:#FF9900;--tl:#14b8a6;--red:#ef4444;--amber:#f59e0b;--green:#22c55e;
  --text:#c8d8f0;--sub:#4a6080;
  --mono:'IBM Plex Mono',monospace;--sans:'IBM Plex Sans',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;}

header{
  background:#070910;border-bottom:1px solid var(--bdr);
  padding:14px 28px;display:flex;align-items:center;justify-content:space-between;
}
.logo{font-family:var(--mono);font-size:14px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#fff;}
.logo em{color:var(--or);font-style:normal;}
.status-bar{display:flex;align-items:center;gap:16px;font-family:var(--mono);font-size:10px;color:var(--sub);}
.pulse{width:7px;height:7px;border-radius:50%;background:var(--green);animation:pulse 1.5s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

.grid{
  display:grid;grid-template-columns:repeat(2,1fr);
  gap:16px;padding:20px 24px;
  max-width:1200px;margin:0 auto;
}

.card{
  background:var(--card);border:1px solid var(--bdr2);
  border-radius:6px;overflow:hidden;
  animation:fadeIn .3s ease;
}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}

.card-head{
  padding:10px 14px;
  display:flex;align-items:center;justify-content:space-between;
  border-bottom:1px solid var(--bdr);
  background:var(--surf);
}
.machine-id{font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:2px;color:var(--or);}
.machine-type{font-family:var(--mono);font-size:9px;color:var(--sub);letter-spacing:1px;text-transform:uppercase;}

.badge{
  font-family:var(--mono);font-size:8px;font-weight:700;
  padding:2px 8px;border-radius:2px;letter-spacing:1px;text-transform:uppercase;
}
.b-run{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.3);}
.b-warn{background:rgba(245,158,11,.1);color:var(--amber);border:1px solid rgba(245,158,11,.3);}
.b-fault{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.3);}

.json-body{
  padding:12px 14px;
  font-family:var(--mono);font-size:11px;line-height:1.7;
  white-space:pre-wrap;word-break:break-all;
}

/* JSON syntax highlight */
.k{color:#94b4d8;}         /* key */
.s{color:#86efac;}         /* string value */
.n{color:#fbbf24;}         /* number */
.er{color:var(--red);font-weight:700;} /* error code */
.b{color:#a78bfa;}         /* boolean */

.errors-row{
  margin:8px 14px 10px;
  padding:6px 10px;
  background:rgba(239,68,68,.08);
  border:1px solid rgba(239,68,68,.25);
  border-radius:3px;
  font-family:var(--mono);font-size:10px;color:var(--red);
  display:none;
}

.fault-bar{
  padding:8px 14px;
  background:var(--surf);
  border-top:1px solid var(--bdr);
  display:flex;align-items:center;gap:8px;
  flex-wrap:wrap;
}
.fault-label{font-family:var(--mono);font-size:8px;color:var(--sub);letter-spacing:1px;text-transform:uppercase;}
.fault-btn{
  font-family:var(--mono);font-size:8px;font-weight:700;
  padding:2px 9px;border-radius:2px;cursor:pointer;
  border:1px solid var(--bdr2);background:transparent;
  color:var(--sub);letter-spacing:.5px;text-transform:uppercase;
  transition:all .15s;
}
.fault-btn:hover{background:rgba(239,68,68,.1);color:var(--red);border-color:rgba(239,68,68,.4);}
.fault-btn.reset{color:var(--green);}
.fault-btn.reset:hover{background:rgba(34,197,94,.1);border-color:rgba(34,197,94,.4);}
.fault-btn.active{background:rgba(239,68,68,.15);color:var(--red);border-color:rgba(239,68,68,.5);}

.ts{font-family:var(--mono);font-size:9px;color:var(--sub);}

footer{
  text-align:center;padding:16px;
  font-family:var(--mono);font-size:9px;color:var(--sub);
  border-top:1px solid var(--bdr);margin-top:8px;
}
</style>
</head>
<body>

<header>
  <div class="logo">🏭 CNC Factory <em>Gateway</em></div>
  <div class="status-bar">
    <div class="pulse"></div>
    <span>LIVE · 1s REFRESH</span>
    <span id="tick" style="color:var(--text);">—</span>
  </div>
</header>

<div class="grid" id="grid">
  <!-- Cards injected by JS -->
</div>

<footer>
  Simulates Modbus TCP / OPC-UA industrial gateway &nbsp;·&nbsp;
  Kafka producer polls GET /machines/{id} every 1 second &nbsp;·&nbsp;
  Use fault buttons to inject faults for demo
</footer>

<script>
const MACHINES = ['CNC_01','ROB_01','CNV_01','PMP_01'];
const FAULTS = {
  CNC_01: ['overheat','vibration','rpm_drop','oil_leak','coolant_low'],
  ROB_01: ['overheat','vibration','rpm_drop','overload'],
  CNV_01: ['overheat','vibration','rpm_drop','belt_tension'],
  PMP_01: ['overheat','vibration','rpm_drop','oil_leak','flow_drop'],
};
const activeFaults = {};

// Build initial cards
const grid = document.getElementById('grid');
MACHINES.forEach(mid => {
  grid.innerHTML += `
  <div class="card" id="card-${mid}">
    <div class="card-head">
      <div>
        <div class="machine-id">${mid}</div>
        <div class="machine-type" id="type-${mid}">—</div>
      </div>
      <div>
        <span class="badge b-run" id="badge-${mid}">—</span>
      </div>
    </div>
    <div class="errors-row" id="err-${mid}"></div>
    <div class="json-body" id="json-${mid}">Loading...</div>
    <div class="fault-bar">
      <span class="fault-label">Inject fault →</span>
      ${(FAULTS[mid]||[]).map(f =>
        `<button class="fault-btn" id="btn-${mid}-${f}" onclick="setFault('${mid}','${f}')">${f}</button>`
      ).join('')}
      <button class="fault-btn reset" onclick="setFault('${mid}','none')">✕ reset</button>
    </div>
  </div>`;
});

function syntaxHL(obj) {
  const json = JSON.stringify(obj, null, 2);
  return json
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"([^"]+)":/g, '<span class="k">"$1"</span>:')
    .replace(/: "(.*?)"/g, (m,v) => {
      if (['E001','E002','E003','E004','E005','E006','E007','E008'].includes(v))
        return `: <span class="er">"${v}"</span>`;
      return `: <span class="s">"${v}"</span>`;
    })
    .replace(/[.]/g,'&period;')
    .replace(/: (true|false)/g,':<span class="b"> $1</span>');
}

async function setFault(mid, fault) {
  // Always send the fault/reset param directly to the server first
  await fetch(`/machines/${mid}?fault=${fault}`);
  activeFaults[mid] = fault === 'none' ? null : fault;
  // Update button styles
  (FAULTS[mid]||[]).forEach(f => {
    const btn = document.getElementById(`btn-${mid}-${f}`);
    if(btn) btn.classList.toggle('active', f === fault);
  });
  // Trigger immediate refresh
  await fetchMachine(mid);
}

async function fetchMachine(mid) {
  const fault = activeFaults[mid];
  const url = `/machines/${mid}${fault ? `?fault=${fault}` : ''}`;
  try {
    const r = await fetch(url);
    const data = await r.json();

    // Update badge
    const badge = document.getElementById(`badge-${mid}`);
    badge.className = 'badge';
    if (data.status === 'fault')   badge.classList.add('b-fault');
    else if(data.status==='warning') badge.classList.add('b-warn');
    else                           badge.classList.add('b-run');
    badge.textContent = data.status.toUpperCase();

    // Update type
    document.getElementById(`type-${mid}`).textContent =
      data.machine_type.replace(/_/g,' ');

    // Errors row
    const errRow = document.getElementById(`err-${mid}`);
    if (data.is_fault && data.error_code) {
      errRow.style.display = 'block';
      errRow.textContent = '⚠ FAULT CODE: ' + data.error_code;
    } else {
      errRow.style.display = 'none';
    }

    // JSON body
    document.getElementById(`json-${mid}`).innerHTML = syntaxHL(data);
  } catch(e) {
    document.getElementById(`json-${mid}`).textContent = 'Connection error';
  }
}

async function tick() {
  const now = new Date().toLocaleTimeString();
  document.getElementById('tick').textContent = now;
  await Promise.all(MACHINES.map(fetchMachine));
}

tick();
setInterval(tick, 1000);
</script>
</body>
</html>"""


# ── One endpoint per machine ──────────────────────────────────────────────────

@app.get("/machines/{machine_id}")
def get_machine(
    machine_id: str,
    fault: Optional[str] = Query(default=None),
):
    if machine_id not in MACHINES:
        return {"error": f"Unknown machine: {machine_id}"}

    state = MACHINES[machine_id]

    # Apply fault if passed
    if fault is not None:
        state.set_fault(fault)

    return state.reading()


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  CNC Factory — Gateway API")
    print("  Dashboard : http://localhost:8000")
    print("  Endpoints :")
    for mid in MACHINES:
        print(f"    GET /machines/{mid}")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
