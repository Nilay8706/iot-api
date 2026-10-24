"""
Sensor server: receives ESP32 data over MQTT, saves it in PostgreSQL,
and shows a password-protected dashboard.

Render settings:
  Build command : pip install -r requirements.txt
  Start command : gunicorn app:app --workers 1 --threads 4 --timeout 60
  (keep it at 1 worker: the MQTT listener runs inside it)
"""
import os
import hmac
import json
import time
import secrets
from datetime import timedelta
from functools import wraps
from zoneinfo import ZoneInfo

import psycopg2
import paho.mqtt.client as mqtt
from flask import (Flask, request, redirect, url_for, session, flash,
                   render_template_string)

# =====================================================================
#  1. SETTINGS (secrets come from Render "Environment Variables")
# =====================================================================

DB_URL             = os.environ["DATABASE_URL"]
DASHBOARD_PASSWORD = os.environ["DASHBOARD_PASSWORD"]
SECRET_KEY         = os.environ["SECRET_KEY"]          # any long random text
MQTT_HOST          = os.environ["MQTT_HOST"]           # e.g. abc123.s1.eu.hivemq.cloud
MQTT_PORT          = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER          = os.environ["MQTT_USER"]
MQTT_PASS          = os.environ["MQTT_PASS"]
TZ                 = ZoneInfo(os.environ.get("TIMEZONE", "Asia/Kolkata"))

DASHBOARD_USER = "esp32-iot"

T_READINGS = "esp32-iot/readings"     # device -> server  (batches of readings)
T_STATUS   = "esp32-iot/status"       # device -> server  (counters, signal)
T_STATE    = "esp32-iot/state"        # device -> server  (online / sleeping / offline)
T_EVENTS   = "esp32-iot/events"       # device -> server  ("level|message")
T_CONFIG   = "esp32-iot/config"       # server -> device  (sleep mode, interval)
T_WIFI     = "esp32-iot/wifi_cmd"     # server -> device  (new Wi-Fi name and password)

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)

# =====================================================================
#  2. DATABASE HELPERS
# =====================================================================

def run_sql(sql, params=(), many=False, fetch=None):
    """Run one SQL command. fetch = None | 'one' | 'all'."""
    conn = psycopg2.connect(DB_URL)
    try:
        with conn, conn.cursor() as cur:
            if many:
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
    finally:
        conn.close()


def log_event(level, message):
    """Save a line in the events table (shown on the dashboard)."""
    try:
        run_sql("INSERT INTO events (level, message) VALUES (%s, %s)", (level, message))
        if secrets.randbelow(50) == 0:      # now and then, keep only the newest 1000
            run_sql("DELETE FROM events WHERE id < (SELECT MAX(id) - 1000 FROM events)")
    except Exception:
        pass                                 # logging must never crash anything


# =====================================================================
#  3. MQTT: messages coming from the ESP32
# =====================================================================

def save_readings(payload):
    data = json.loads(payload)
    rows = [(int(r["ts"]), float(r["temperature"]), float(r["humidity"]))
            for r in data["readings"]]
    if not rows:
        return

    for attempt in range(3):                 # try up to 3 times if the database is busy
        try:
            run_sql("""INSERT INTO sensor_data (recorded_at, temperature, humidity)
                       VALUES (to_timestamp(%s), %s, %s)
                       ON CONFLICT (recorded_at) DO NOTHING""", rows, many=True)
            break
        except Exception as e:
            if attempt == 2:
                log_event("error", "Database error: " + str(e)[:200])
                return
            time.sleep(2)

    # If the oldest reading is old, the device was offline and is catching up
    age = time.time() - min(r[0] for r in rows)
    if age > 120:
        log_event("info", f"Received {len(rows)} stored readings "
                          f"(oldest is {int(age // 60)} min old)")


def save_status(payload):
    s = json.loads(payload)
    run_sql("""INSERT INTO device_status
                 (id, updated_at, uptime_s, wifi_drops, send_failures,
                  sensor_errors, queued, rssi, sleep_mode, interval_s)
               VALUES (1, now(), %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO UPDATE SET
                 updated_at = now(), uptime_s = EXCLUDED.uptime_s,
                 wifi_drops = EXCLUDED.wifi_drops, send_failures = EXCLUDED.send_failures,
                 sensor_errors = EXCLUDED.sensor_errors, queued = EXCLUDED.queued,
                 rssi = EXCLUDED.rssi, sleep_mode = EXCLUDED.sleep_mode,
                 interval_s = EXCLUDED.interval_s""",
            (s.get("uptime_s", 0), s.get("wifi_drops", 0), s.get("send_failures", 0),
             s.get("sensor_errors", 0), s.get("queued", 0), s.get("rssi", 0),
             s.get("sleep", 0), s.get("interval_s", 0)))


def save_state(payload):
    state = payload.strip()
    if state not in ("online", "sleeping", "offline"):
        return
    run_sql("""INSERT INTO device_status (id, state) VALUES (1, %s)
               ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state""", (state,))
    if state == "offline":
        log_event("warning", "Device went offline (connection lost)")


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        log_event("error", f"Server could not connect to MQTT broker: {reason_code}")
        return
    client.subscribe([(T_READINGS, 1), (T_STATUS, 0), (T_STATE, 1), (T_EVENTS, 1)])


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    try:
        if msg.topic == T_READINGS:
            save_readings(payload)
        elif msg.topic == T_STATUS:
            save_status(payload)
        elif msg.topic == T_STATE:
            save_state(payload)
        elif msg.topic == T_EVENTS:
            level, _, text = payload.partition("|")
            log_event(level if level in ("info", "warning", "error") else "info", text[:300])
    except Exception as e:
        log_event("error", f"Could not process {msg.topic}: {str(e)[:150]}")


def start_mqtt():
    # clean_session=False + fixed client id: the broker keeps our messages
    # while this server is asleep or restarting.
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id="render-dashboard-server",
                         protocol=mqtt.MQTTv311, clean_session=False)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


mqtt_client = start_mqtt()


def mqtt_send(topic, payload, retain=False):
    """Send a message to the device. Returns True if it was handed to the broker."""
    if not mqtt_client.is_connected():
        return False
    info = mqtt_client.publish(topic, payload, qos=1, retain=retain)
    return info.rc == mqtt.MQTT_ERR_SUCCESS


# =====================================================================
#  4. LOGIN, CSRF AND HELPERS
# =====================================================================

FAILS = {}      # ip -> [number of failed logins, time of first failure]


def client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    return forwarded.split(",")[0].strip() or request.remote_addr or "?"


def too_many_attempts(ip):
    rec = FAILS.get(ip)
    if not rec:
        return False
    if time.time() - rec[1] > 600:           # forget after 10 minutes
        FAILS.pop(ip, None)
        return False
    return rec[0] >= 5


def record_fail(ip):
    rec = FAILS.setdefault(ip, [0, time.time()])
    rec[0] += 1


def same(a, b):
    return hmac.compare_digest(a.encode(), b.encode())


def csrf_token():
    if "csrf" not in session:
        session["csrf"] = secrets.token_hex(16)
    return session["csrf"]


def csrf_ok():
    token = session.get("csrf", "")
    return bool(token) and same(request.form.get("csrf", ""), token)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def fmt(t):
    """Show a database time in the dashboard time zone."""
    return t.astimezone(TZ).strftime("%d %b %Y, %H:%M:%S") if t else "-"


# =====================================================================
#  5. PAGES (HTML)
# =====================================================================

CSS = """
body{font-family:Arial,sans-serif;margin:0;background:#f4f6f8;color:#222}
.wrap{padding:16px;max-width:1100px;margin:auto}
.nav{background:#263238;color:#fff;padding:12px 16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
.nav a{color:#fff;margin-left:14px;text-decoration:none}
h1{font-size:22px;margin:0 0 12px} h2{font-size:16px;margin:0 0 8px}
.cards{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:16px}
.card{background:#fff;border-radius:8px;padding:12px 16px;min-width:140px;flex:1;box-shadow:0 1px 3px rgba(0,0,0,.12)}
.big{font-size:24px;font-weight:bold} .label{font-size:12px;color:#666}
.online{color:#1a8f3c}.sleeping{color:#1565c0}.offline{color:#c62828}
.panel{background:#fff;border-radius:8px;padding:12px 16px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.12);overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #eee;white-space:nowrap}
.error{color:#c62828;font-weight:bold}.warning{color:#ef6c00;font-weight:bold}.info{color:#1565c0}
.msg{padding:10px 14px;border-radius:6px;margin-bottom:12px}
.msg.ok{background:#e3f4e8;color:#1a6b34}.msg.error{background:#fdecea;color:#b71c1c;font-weight:normal}
label{display:block;margin:10px 0 4px;font-size:14px}
input[type=text],input[type=password],input[type=number]{width:100%;max-width:360px;padding:8px;box-sizing:border-box}
button{padding:9px 16px;background:#1565c0;color:#fff;border:0;border-radius:6px;cursor:pointer;margin-top:12px}
.login{max-width:340px;margin:60px auto;background:#fff;padding:24px;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.15)}
.note{font-size:13px;color:#555}
"""

HEAD = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
{% if refresh %}<meta http-equiv="refresh" content="15">{% endif %}
<title>{{ title }}</title><style>""" + CSS + """</style></head><body>
{% if nav %}<div class="nav"><b>Sensor Dashboard</b><span>
<a href="/dashboard">Dashboard</a><a href="/settings">Settings</a><a href="/logout">Log out</a></span></div>{% endif %}
<div class="wrap">
{% with msgs = get_flashed_messages(with_categories=true) %}
{% for cat, m in msgs %}<div class="msg {{ cat }}">{{ m }}</div>{% endfor %}{% endwith %}
"""
TAIL = "</div></body></html>"

LOGIN_PAGE = HEAD + """
<div class="login">
  <h1>Sensor Dashboard</h1>
  <form method="post" action="/login">
    <input type="hidden" name="csrf" value="{{ csrf }}">
    <label>Username</label>
    <input type="text" name="username" autocomplete="username" required>
    <label>Password</label>
    <input type="password" name="password" autocomplete="current-password" required>
    <button type="submit">Log in</button>
  </form>
</div>""" + TAIL

DASHBOARD_PAGE = HEAD + """
<div class="cards">
  <div class="card"><div class="label">Device</div>
    <div class="big {{ state_class }}">{{ state_label }}</div>
    <div class="label">Last report: {{ last_seen }}</div></div>
  <div class="card"><div class="label">Latest temperature</div><div class="big">{{ latest_temp }}</div></div>
  <div class="card"><div class="label">Latest humidity</div><div class="big">{{ latest_hum }}</div></div>
  <div class="card"><div class="label">Total readings saved</div><div class="big">{{ total }}</div></div>
</div>
<div class="cards">
  <div class="card"><div class="label">Mode</div><div class="big">{{ mode }}</div><div class="label">{{ interval }}</div></div>
  <div class="card"><div class="label">Wi-Fi drops</div><div class="big">{{ st.wifi_drops }}</div></div>
  <div class="card"><div class="label">Send failures</div><div class="big">{{ st.send_failures }}</div></div>
  <div class="card"><div class="label">Sensor errors</div><div class="big">{{ st.sensor_errors }}</div></div>
  <div class="card"><div class="label">Waiting to send</div><div class="big">{{ st.queued }}</div></div>
  <div class="card"><div class="label">Wi-Fi signal</div><div class="big">{{ st.rssi }}</div></div>
</div>
<div class="panel"><h2>Last 50 readings</h2><canvas id="chart" height="110"></canvas></div>
<div class="panel"><h2>Events and errors</h2>
  <table><tr><th>Time</th><th>Level</th><th>Message</th></tr>
  {% for e in events %}<tr><td>{{ e.time }}</td><td class="{{ e.level }}">{{ e.level }}</td><td>{{ e.message }}</td></tr>
  {% else %}<tr><td colspan="3">No events yet</td></tr>{% endfor %}</table></div>
<div class="panel"><h2>Recent readings</h2>
  <table><tr><th>Recorded at</th><th>Temperature (C)</th><th>Humidity (%RH)</th></tr>
  {% for r in readings %}<tr><td>{{ r.time }}</td><td>{{ r.temp }}</td><td>{{ r.hum }}</td></tr>{% endfor %}</table></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
new Chart(document.getElementById("chart"), {
  type: "line",
  data: { labels: {{ chart_labels | tojson }},
    datasets: [
      { label: "Temperature (C)", data: {{ chart_temp | tojson }}, borderColor: "#e53935", tension: 0.3, pointRadius: 0 },
      { label: "Humidity (%RH)", data: {{ chart_hum | tojson }}, borderColor: "#1e88e5", tension: 0.3, pointRadius: 0 } ] },
  options: { animation: false }
});
</script>""" + TAIL

SETTINGS_PAGE = HEAD + """
<h1>Device settings</h1>

<div class="panel">
  <h2>Mode and reading interval</h2>
  <form method="post" action="/settings/device">
    <input type="hidden" name="csrf" value="{{ csrf }}">
    <label><input type="checkbox" name="sleep" {{ 'checked' if cfg.sleep_mode else '' }}> Deep sleep mode (saves power)</label>
    <label>Seconds between readings (5 or more; 30 or more in deep sleep)</label>
    <input type="number" name="interval" value="{{ cfg.interval_s }}" min="5" max="86400" required>
    <button type="submit">Save settings</button>
  </form>
  <p class="note">The device reports: {{ reported }}.<br>
  In normal mode the change arrives within seconds. In deep sleep mode the device only connects every few
  readings, so a change can take a few wake-ups.</p>
</div>

<div class="panel">
  <h2>Change Wi-Fi network</h2>
  <form method="post" action="/settings/wifi" autocomplete="off">
    <input type="hidden" name="csrf" value="{{ csrf }}">
    <label>New Wi-Fi name (2.4 GHz)</label>
    <input type="text" name="ssid" maxlength="32" required>
    <label>New Wi-Fi password (leave empty for an open network)</label>
    <input type="password" name="password" maxlength="64">
    <button type="submit">Send to device</button>
  </form>
  <p class="note">The device tries the new network. If it cannot connect within about 25 seconds it goes back
  to the old one and logs an error event. The password is sent over an encrypted connection and is not stored here.<br>
  First-time setup, or a device that is offline: connect your phone to the device's own Wi-Fi
  <b>ESP32-IoT-Setup</b> and open <b>http://192.168.4.1</b>.</p>
</div>

<div class="panel"><h2>Latest events</h2>
  <table><tr><th>Time</th><th>Level</th><th>Message</th></tr>
  {% for e in events %}<tr><td>{{ e.time }}</td><td class="{{ e.level }}">{{ e.level }}</td><td>{{ e.message }}</td></tr>
  {% else %}<tr><td colspan="3">No events yet</td></tr>{% endfor %}</table></div>
""" + TAIL


# =====================================================================
#  6. ROUTES
# =====================================================================

@app.route("/")
def home():
    return "OK"          # also used by an uptime pinger to keep Render awake


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        ip = client_ip()
        if too_many_attempts(ip):
            flash("Too many wrong attempts. Try again in 10 minutes.", "error")
        elif not csrf_ok():
            flash("Page expired. Please try again.", "error")
        else:
            user_ok = same(request.form.get("username", ""), DASHBOARD_USER)
            pass_ok = same(request.form.get("password", ""), DASHBOARD_PASSWORD)
            if user_ok and pass_ok:
                FAILS.pop(ip, None)
                session.clear()
                session["user"] = DASHBOARD_USER
                session.permanent = True
                return redirect(url_for("dashboard"))
            record_fail(ip)
            log_event("warning", f"Failed dashboard login from {ip}")
            flash("Wrong username or password.", "error")
    return render_template_string(LOGIN_PAGE, title="Login", nav=False,
                                  refresh=False, csrf=csrf_token())


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    total = run_sql("SELECT COUNT(*) FROM sensor_data", fetch="one")[0]
    rows = run_sql("""SELECT COALESCE(recorded_at, created_at) AS t, temperature, humidity
                      FROM sensor_data ORDER BY t DESC LIMIT 50""", fetch="all")
    s = run_sql("""SELECT updated_at, uptime_s, wifi_drops, send_failures, sensor_errors,
                          queued, rssi, sleep_mode, interval_s, state
                   FROM device_status WHERE id = 1""", fetch="one")
    ev = run_sql("SELECT created_at, level, message FROM events ORDER BY id DESC LIMIT 30",
                 fetch="all")

    state = (s[9] if s else None) or "unknown"
    st = {"wifi_drops": "-", "send_failures": "-", "sensor_errors": "-", "queued": "-", "rssi": "-"}
    mode, interval = "-", ""
    if s:
        st = {"wifi_drops": s[2], "send_failures": s[3], "sensor_errors": s[4],
              "queued": s[5], "rssi": f"{s[6]} dBm" if s[6] else "-"}
        mode = "Deep sleep" if s[7] else "Always on"
        interval = f"every {s[8]} s" if s[8] else ""

    chart_rows = list(reversed(rows))        # chart wants oldest first
    return render_template_string(
        DASHBOARD_PAGE, title="Sensor Dashboard", nav=True, refresh=True,
        state_label={"online": "Online", "sleeping": "Sleeping", "offline": "Offline"}.get(state, "Unknown"),
        state_class={"online": "online", "sleeping": "sleeping"}.get(state, "offline"),
        last_seen=fmt(s[0]) if s else "-", total=total,
        latest_temp=f"{rows[0][1]:.2f} C" if rows else "-",
        latest_hum=f"{rows[0][2]:.2f} %" if rows else "-",
        st=st, mode=mode, interval=interval,
        events=[{"time": fmt(e[0]), "level": e[1], "message": e[2]} for e in ev],
        readings=[{"time": fmt(r[0]), "temp": f"{r[1]:.2f}", "hum": f"{r[2]:.2f}"} for r in rows],
        chart_labels=[r[0].astimezone(TZ).strftime("%H:%M:%S") for r in chart_rows],
        chart_temp=[round(r[1], 2) for r in chart_rows],
        chart_hum=[round(r[2], 2) for r in chart_rows],
    )


@app.route("/settings")
@login_required
def settings():
    c = run_sql("SELECT sleep_mode, interval_s FROM device_config WHERE id = 1", fetch="one")
    s = run_sql("SELECT sleep_mode, interval_s FROM device_status WHERE id = 1", fetch="one")
    ev = run_sql("SELECT created_at, level, message FROM events ORDER BY id DESC LIMIT 10",
                 fetch="all")
    cfg = {"sleep_mode": bool(c[0]) if c else False, "interval_s": c[1] if c else 15}
    reported = "nothing yet"
    if s and s[1]:
        reported = f"{'deep sleep' if s[0] else 'always on'}, every {s[1]} s"
    return render_template_string(
        SETTINGS_PAGE, title="Settings", nav=True, refresh=False, csrf=csrf_token(),
        cfg=cfg, reported=reported,
        events=[{"time": fmt(e[0]), "level": e[1], "message": e[2]} for e in ev])


@app.route("/settings/device", methods=["POST"])
@login_required
def settings_device():
    if not csrf_ok():
        flash("Page expired. Please try again.", "error")
        return redirect(url_for("settings"))
    sleep_mode = request.form.get("sleep") == "on"
    try:
        interval = int(request.form.get("interval", ""))
    except ValueError:
        interval = 0
    minimum = 30 if sleep_mode else 5
    if not (minimum <= interval <= 86400):
        flash(f"Interval must be between {minimum} and 86400 seconds.", "error")
        return redirect(url_for("settings"))

    run_sql("""INSERT INTO device_config (id, sleep_mode, interval_s) VALUES (1, %s, %s)
               ON CONFLICT (id) DO UPDATE SET sleep_mode = EXCLUDED.sleep_mode,
                                              interval_s = EXCLUDED.interval_s""",
            (sleep_mode, interval))
    # retained = the broker keeps it, so a sleeping device gets it when it wakes up
    if mqtt_send(T_CONFIG, f"sleep={1 if sleep_mode else 0} interval={interval}", retain=True):
        flash("Saved and sent to the device.", "ok")
        log_event("info", f"Settings changed from dashboard: "
                          f"{'deep sleep' if sleep_mode else 'always on'}, every {interval} s")
    else:
        flash("Saved, but the server is not connected to the MQTT broker right now. Save again in a minute.", "error")
    return redirect(url_for("settings"))


@app.route("/settings/wifi", methods=["POST"])
@login_required
def settings_wifi():
    if not csrf_ok():
        flash("Page expired. Please try again.", "error")
        return redirect(url_for("settings"))
    ssid = request.form.get("ssid", "")
    password = request.form.get("password", "")
    if not (1 <= len(ssid.encode()) <= 32) or len(password.encode()) > 64 \
            or (password and len(password) < 8) or "\n" in ssid + password:
        flash("Wi-Fi name must be 1-32 characters. The password must be empty or 8-64 characters.", "error")
        return redirect(url_for("settings"))

    if mqtt_send(T_WIFI, f"{ssid}\n{password}", retain=False):
        flash("Sent to the device. Watch the events list for the result.", "ok")
        log_event("info", f"Wi-Fi change requested from dashboard (network: {ssid[:32]})")
    else:
        flash("The server is not connected to the MQTT broker right now. Try again in a minute.", "error")
    return redirect(url_for("settings"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
