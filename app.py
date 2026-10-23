import os
import hmac
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from flask import Flask, request, jsonify, Response, render_template_string

app = Flask(__name__)

# ---- Secrets come from Render "Environment Variables" ----
DB_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["API_KEY"]                          # used by the ESP32
DASHBOARD_PASSWORD = os.environ["DASHBOARD_PASSWORD"]    # used to open the dashboard

# Times on the dashboard are shown in this time zone
TZ = ZoneInfo(os.environ.get("TIMEZONE", "Asia/Kolkata"))


def db():
    return psycopg2.connect(DB_URL)


def log_event(level, message):
    """Save a line in the events table (shown on the dashboard)."""
    try:
        conn = db()
        with conn, conn.cursor() as cur:
            cur.execute("INSERT INTO events (level, message) VALUES (%s, %s)",
                        (level, message))
        conn.close()
    except Exception:
        pass  # logging must never crash the API


# =====================================================================
#  1. API used by the ESP32
# =====================================================================

@app.route("/")
def home():
    return "API is running"


@app.route("/data", methods=["POST"])
def data():
    # Check the API key
    if request.headers.get("x-api-key") != API_KEY:
        log_event("warning", "Rejected a request: wrong API key")
        return jsonify(error="unauthorized"), 401

    # Read the JSON sent by the ESP32
    try:
        body = request.get_json(force=True)
        rows = [(int(r["ts"]), float(r["temperature"]), float(r["humidity"]))
                for r in body["readings"]]
        status = body.get("status", {})
    except Exception:
        log_event("error", "Received data in a wrong format")
        return jsonify(error="bad format"), 400

    # Save into the database
    try:
        conn = db()
        with conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO sensor_data (recorded_at, temperature, humidity)
                   VALUES (to_timestamp(%s), %s, %s)
                   ON CONFLICT (recorded_at) DO NOTHING""",
                rows)

            if status:
                cur.execute(
                    """INSERT INTO device_status
                         (id, updated_at, uptime_s, wifi_drops,
                          send_failures, sensor_errors, queued)
                       VALUES (1, now(), %s, %s, %s, %s, %s)
                       ON CONFLICT (id) DO UPDATE SET
                         updated_at = now(),
                         uptime_s = EXCLUDED.uptime_s,
                         wifi_drops = EXCLUDED.wifi_drops,
                         send_failures = EXCLUDED.send_failures,
                         sensor_errors = EXCLUDED.sensor_errors,
                         queued = EXCLUDED.queued""",
                    (status.get("uptime_s", 0), status.get("wifi_drops", 0),
                     status.get("send_failures", 0), status.get("sensor_errors", 0),
                     status.get("queued", 0)))
        conn.close()
    except Exception as e:
        log_event("error", "Database error: " + str(e)[:200])
        return jsonify(error="database error"), 500

    # If the oldest reading is old, the ESP32 was offline and is catching up
    if rows:
        age = time.time() - min(r[0] for r in rows)
        if age > 120:
            log_event("info",
                      f"Received {len(rows)} stored readings "
                      f"(oldest is {int(age // 60)} min old) after an offline period")

    return jsonify(ok=True, saved=len(rows)), 201


# =====================================================================
#  2. Dashboard (browser asks for a password)
# =====================================================================

def password_ok():
    auth = request.authorization
    return bool(auth) and hmac.compare_digest(auth.password or "", DASHBOARD_PASSWORD)


def ask_for_password():
    return Response("Password required", 401,
                    {"WWW-Authenticate": 'Basic realm="Sensor Dashboard"'})


def fmt(t):
    """Format a database time in the dashboard time zone."""
    return t.astimezone(TZ).strftime("%d %b %Y, %H:%M:%S") if t else "-"


PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="15">
  <title>Sensor Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; padding: 16px; background: #f4f6f8; color: #222; }
    h1 { margin: 0 0 12px; font-size: 22px; }
    h2 { font-size: 16px; margin: 0 0 8px; }
    .cards { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; }
    .card { background: #fff; border-radius: 8px; padding: 12px 16px; min-width: 150px; flex: 1; box-shadow: 0 1px 3px rgba(0,0,0,.12); }
    .big { font-size: 26px; font-weight: bold; }
    .label { font-size: 12px; color: #666; }
    .online { color: #1a8f3c; } .offline { color: #c62828; }
    .panel { background: #fff; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.12); overflow-x: auto; }
    table { border-collapse: collapse; width: 100%; font-size: 14px; }
    th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #eee; white-space: nowrap; }
    .error { color: #c62828; font-weight: bold; } .warning { color: #ef6c00; font-weight: bold; } .info { color: #1565c0; }
  </style>
</head>
<body>
  <h1>Sensor Dashboard</h1>

  <div class="cards">
    <div class="card">
      <div class="label">Device</div>
      <div class="big {{ 'online' if online else 'offline' }}">{{ 'Online' if online else 'Offline' }}</div>
      <div class="label">Last report: {{ last_seen }}</div>
    </div>
    <div class="card">
      <div class="label">Latest temperature</div>
      <div class="big">{{ latest_temp }}</div>
    </div>
    <div class="card">
      <div class="label">Latest humidity</div>
      <div class="big">{{ latest_hum }}</div>
    </div>
    <div class="card">
      <div class="label">Total readings saved</div>
      <div class="big">{{ total }}</div>
    </div>
  </div>

  <div class="cards">
    <div class="card"><div class="label">Wi-Fi drops</div><div class="big">{{ st.wifi_drops }}</div></div>
    <div class="card"><div class="label">Send failures</div><div class="big">{{ st.send_failures }}</div></div>
    <div class="card"><div class="label">Sensor errors</div><div class="big">{{ st.sensor_errors }}</div></div>
    <div class="card"><div class="label">Waiting to send</div><div class="big">{{ st.queued }}</div></div>
    <div class="card"><div class="label">Device uptime</div><div class="big">{{ uptime }}</div></div>
  </div>

  <div class="panel">
    <h2>Last 50 readings</h2>
    <canvas id="chart" height="110"></canvas>
  </div>

  <div class="panel">
    <h2>Events and errors</h2>
    <table>
      <tr><th>Time</th><th>Level</th><th>Message</th></tr>
      {% for e in events %}
      <tr><td>{{ e.time }}</td><td class="{{ e.level }}">{{ e.level }}</td><td>{{ e.message }}</td></tr>
      {% else %}
      <tr><td colspan="3">No events yet</td></tr>
      {% endfor %}
    </table>
  </div>

  <div class="panel">
    <h2>Recent readings</h2>
    <table>
      <tr><th>Recorded at</th><th>Temperature (C)</th><th>Humidity (%RH)</th></tr>
      {% for r in readings %}
      <tr><td>{{ r.time }}</td><td>{{ r.temp }}</td><td>{{ r.hum }}</td></tr>
      {% endfor %}
    </table>
  </div>

  <script>
    const labels = {{ chart_labels | tojson }};
    new Chart(document.getElementById("chart"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          { label: "Temperature (C)", data: {{ chart_temp | tojson }}, borderColor: "#e53935", tension: 0.3, pointRadius: 0 },
          { label: "Humidity (%RH)", data: {{ chart_hum | tojson }}, borderColor: "#1e88e5", tension: 0.3, pointRadius: 0 }
        ]
      },
      options: { animation: false }
    });
  </script>
</body>
</html>
"""


@app.route("/dashboard")
def dashboard():
    if not password_ok():
        return ask_for_password()

    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sensor_data")
        total = cur.fetchone()[0]

        cur.execute("""SELECT COALESCE(recorded_at, created_at) AS t, temperature, humidity
                       FROM sensor_data ORDER BY t DESC LIMIT 50""")
        rows = cur.fetchall()

        cur.execute("""SELECT updated_at, uptime_s, wifi_drops, send_failures,
                              sensor_errors, queued
                       FROM device_status WHERE id = 1""")
        s = cur.fetchone()

        cur.execute("SELECT created_at, level, message FROM events ORDER BY id DESC LIMIT 30")
        ev = cur.fetchall()
    conn.close()

    # Device status
    last_seen_time = s[0] if s else None
    online = bool(last_seen_time) and (
        datetime.now(last_seen_time.tzinfo) - last_seen_time).total_seconds() < 60
    st = {"wifi_drops": s[2], "send_failures": s[3], "sensor_errors": s[4], "queued": s[5]} \
        if s else {"wifi_drops": "-", "send_failures": "-", "sensor_errors": "-", "queued": "-"}
    uptime_s = s[1] if s else 0
    uptime = f"{uptime_s // 3600}h {(uptime_s % 3600) // 60}m" if s else "-"

    readings = [{"time": fmt(r[0]), "temp": f"{r[1]:.2f}", "hum": f"{r[2]:.2f}"} for r in rows]
    events = [{"time": fmt(e[0]), "level": e[1], "message": e[2]} for e in ev]

    # Chart wants oldest first
    chart_rows = list(reversed(rows))
    return render_template_string(
        PAGE,
        online=online, last_seen=fmt(last_seen_time), total=total,
        latest_temp=f"{rows[0][1]:.2f} C" if rows else "-",
        latest_hum=f"{rows[0][2]:.2f} %" if rows else "-",
        st=st, uptime=uptime, readings=readings, events=events,
        chart_labels=[r[0].astimezone(TZ).strftime("%H:%M:%S") for r in chart_rows],
        chart_temp=[round(r[1], 2) for r in chart_rows],
        chart_hum=[round(r[2], 2) for r in chart_rows],
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
