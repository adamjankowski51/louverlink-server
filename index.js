// ─────────────────────────────────────────────────────────────────────────────
// LouverLink MQTT Server — index.js
// Replaces the Render/Express HTTP polling API with an MQTT event-driven model.
//
// MQTT topic structure:
//   louverlink/{device_id}/status      ← device publishes its state (JSON)
//   louverlink/{device_id}/command     ← server publishes commands to device (JSON)
//   louverlink/{device_id}/online      ← device publishes "1" on connect (LWT "0")
//   louverlink/register                ← device publishes registration request (JSON)
//   louverlink/{device_id}/credentials ← server publishes unique credentials to device (JSON, QoS1, retain)
//
// Per-device credential flow:
//   1. New device connects with shared REGISTRATION credential (read-only, register topic only)
//   2. Device publishes { device_id } to louverlink/register
//   3. Server generates unique username + strong random password for the device
//   4. Server writes credentials to /etc/mosquitto/passwd via mosquitto_passwd
//   5. Server publishes credentials to louverlink/{device_id}/credentials (QoS1, retain)
//   6. Mosquitto is reloaded 2s AFTER publish so the device session is not killed before delivery
//   7. Device receives credentials, stores in Preferences, reconnects with own credentials
//   8. Shared registration credential cannot access any other topics (ACL enforced)
//
// HTTP API (for the app):
//   GET  /functions/getDevices
//   GET  /functions/getDevice/:device_id
//   POST /functions/setTarget        { device_id, target_position_pct }
//   POST /functions/claimDevice      { device_id, name, servo_angle_min, servo_angle_max }
//   POST /functions/unclaimDevice    { device_id }
//   POST /functions/setOta           { device_id, ota_version, ota_url }
// ─────────────────────────────────────────────────────────────────────────────

const express        = require('express');
const mqtt           = require('mqtt');
const { Pool }       = require('pg');
const crypto         = require('crypto');
const { execSync }   = require('child_process');
const fs             = require('fs');

const app = express();
app.use(express.json());

// ── CORS ──────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

// ── Credential helpers ────────────────────────────────────────────────────────
const PASSWD_FILE = '/etc/mosquitto/passwd';

// Generate a cryptographically strong random password
function generatePassword(length = 32) {
  return crypto.randomBytes(length).toString('base64url').slice(0, length);
}

// Write or update a Mosquitto credential using mosquitto_passwd
function setMosquittoCredential(username, password) {
  try {
    execSync(`mosquitto_passwd -b ${PASSWD_FILE} "${username}" "${password}"`, {
      stdio: 'pipe'
    });
    execSync(`chown root:mosquitto ${PASSWD_FILE} && chmod 640 ${PASSWD_FILE}`, {
      stdio: 'pipe'
    });
    console.log(`[Credentials] Set Mosquitto credential for ${username}`);
    return true;
  } catch (err) {
    console.error(`[Credentials] Failed to set credential for ${username}:`, err.message);
    return false;
  }
}

// Delete a Mosquitto credential
function deleteMosquittoCredential(username) {
  try {
    execSync(`mosquitto_passwd -D ${PASSWD_FILE} "${username}"`, { stdio: 'pipe' });
    execSync(`chown root:mosquitto ${PASSWD_FILE} && chmod 640 ${PASSWD_FILE}`, { stdio: 'pipe' });
    console.log(`[Credentials] Deleted Mosquitto credential for ${username}`);
  } catch (err) {
    console.error(`[Credentials] Failed to delete credential for ${username}:`, err.message);
  }
}

const ACL_FILE = '/etc/mosquitto/acl';

// Append per-device ACL entry so device can only access its own topics
function addDeviceAcl(mqttUsername, deviceId) {
  try {
    const entry = `\nuser ${mqttUsername}\ntopic readwrite louverlink/${deviceId}/#\n`;
    fs.appendFileSync(ACL_FILE, entry, 'utf8');
    console.log(`[ACL] Added entry for ${mqttUsername}`);
  } catch (err) {
    console.error(`[ACL] Failed to add entry for ${mqttUsername}:`, err.message);
  }
}

// Remove per-device ACL entry on unclaim
function removeDeviceAcl(mqttUsername) {
  try {
    if (!fs.existsSync(ACL_FILE)) return;
    let content = fs.readFileSync(ACL_FILE, 'utf8');
    // Remove the user block — matches "user <name>\ntopic readwrite ...\n"
    const regex = new RegExp(`\\nuser ${mqttUsername}\\ntopic readwrite louverlink/[^\\n]+\\n`, 'g');
    content = content.replace(regex, '');
    fs.writeFileSync(ACL_FILE, content, 'utf8');
    console.log(`[ACL] Removed entry for ${mqttUsername}`);
  } catch (err) {
    console.error(`[ACL] Failed to remove entry for ${mqttUsername}:`, err.message);
  }
}

// Reload Mosquitto so it picks up passwd and ACL changes without full restart
function reloadMosquitto() {
  try {
    execSync('systemctl kill -s HUP mosquitto.service', { stdio: 'pipe' });
    console.log('[Credentials] Mosquitto reloaded');
  } catch (err) {
    console.error('[Credentials] Failed to reload Mosquitto:', err.message);
  }
}

// ── Database init ─────────────────────────────────────────────────────────────
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS devices (
      device_id           TEXT PRIMARY KEY,
      ip                  TEXT,
      firmware_version    TEXT,
      current_position    INTEGER DEFAULT -1,
      target_position     INTEGER DEFAULT 0,
      target_position_pct INTEGER DEFAULT 0,
      current_position_pct INTEGER DEFAULT 0,
      target_state        TEXT DEFAULT 'closed',
      is_moving           BOOLEAN DEFAULT false,
      is_online           BOOLEAN DEFAULT false,
      battery_voltage     REAL DEFAULT 0,
      battery_pct         INTEGER DEFAULT 0,
      usb_powered         BOOLEAN DEFAULT false,
      poll_interval_ms    INTEGER DEFAULT 30000,
      servo_angle_min     INTEGER DEFAULT 0,
      servo_angle_max     INTEGER DEFAULT 180,
      gpio_pin            INTEGER DEFAULT 0,
      ota_version         TEXT,
      ota_url             TEXT,
      claimed             BOOLEAN DEFAULT false,
      name                TEXT,
      mqtt_username       TEXT,
      mqtt_password       TEXT,
      last_seen           TIMESTAMPTZ DEFAULT NOW(),
      created_at          TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  // Add columns if upgrading from older schema
  await pool.query(`ALTER TABLE devices ADD COLUMN IF NOT EXISTS mqtt_username TEXT`);
  await pool.query(`ALTER TABLE devices ADD COLUMN IF NOT EXISTS mqtt_password TEXT`);
  console.log('[DB] Tables ready');
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function pctToAngle(pct, servoMin, servoMax) {
  return Math.round(servoMin + (pct / 100) * (servoMax - servoMin));
}

function angleToPct(angle, servoMin, servoMax) {
  const range = servoMax - servoMin;
  if (range <= 0) return 0;
  return Math.max(0, Math.min(100, Math.round(((angle - servoMin) / range) * 100)));
}

// ── MQTT client ───────────────────────────────────────────────────────────────
const mqttClient = mqtt.connect('mqtts://mqtt.scshutters.com:8883', {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  capath:   '/etc/ssl/certs',
  rejectUnauthorized: true,
  clientId: 'louverlink-server',
  clean:    true,
  reconnectPeriod: 3000,
});

mqttClient.on('connect', () => {
  console.log('[MQTT] Connected to broker');
  mqttClient.subscribe('louverlink/+/status',  { qos: 1 });
  mqttClient.subscribe('louverlink/+/online',  { qos: 1 });
  mqttClient.subscribe('louverlink/register',  { qos: 1 });
  console.log('[MQTT] Subscribed to status, online, and register topics');
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Error:', err.message);
});

mqttClient.on('reconnect', () => {
  console.log('[MQTT] Reconnecting...');
});

// ── MQTT message handler ──────────────────────────────────────────────────────
mqttClient.on('message', async (topic, payload) => {
  const parts    = topic.split('/');
  const deviceId = parts[1];
  const msgType  = parts[2];

  // ── Device credential registration ───────────────────────────────────────
  // Device connects with shared registration credential and publishes here
  // to request its own unique credentials.
  if (topic === 'louverlink/register') {
    let data;
    try { data = JSON.parse(payload.toString()); } catch { return; }
    const regDeviceId = data.device_id;
    if (!regDeviceId) return;

    console.log(`[Register] Credential request from ${regDeviceId}`);

    try {
      // Check if device already has credentials
      const result = await pool.query(
        'SELECT mqtt_username, mqtt_password FROM devices WHERE device_id = $1',
        [regDeviceId]
      );

      let mqttUsername, mqttPassword;

      if (result.rows[0]?.mqtt_password) {
        // Already has credentials — resend them (device may have lost them)
        mqttUsername = result.rows[0].mqtt_username;
        mqttPassword = result.rows[0].mqtt_password;
        console.log(`[Register] Resending existing credentials to ${regDeviceId}`);
      } else {
        // Generate new unique credentials for this device
        mqttUsername = `device_${regDeviceId.toLowerCase().replace(/[^a-z0-9]/g, '_')}`;
        mqttPassword = generatePassword(32);

        // Write to Mosquitto passwd file
        const ok = setMosquittoCredential(mqttUsername, mqttPassword);
        if (!ok) {
          console.error(`[Register] Failed to write Mosquitto credential for ${regDeviceId}`);
          return;
        }

        // Add ACL entry so device can only access its own topics
        addDeviceAcl(mqttUsername, regDeviceId);

        // NOTE: Do NOT reload Mosquitto here — the HUP would kill the device's
        // active subscription before credentials are delivered. Reload happens
        // 2 seconds after publish confirms delivery (see publish callback below).

        // Store in database
        await pool.query(`
          INSERT INTO devices (device_id, mqtt_username, mqtt_password, last_seen)
          VALUES ($1, $2, $3, NOW())
          ON CONFLICT (device_id) DO UPDATE SET
            mqtt_username = $2,
            mqtt_password = $3,
            last_seen     = NOW()
        `, [regDeviceId, mqttUsername, mqttPassword]);

        console.log(`[Register] Generated credentials for ${regDeviceId}: user=${mqttUsername}`);
      }

      // Publish credentials back to device — QoS 1, retain so device gets it
      // even if it reconnects after a brief drop.
      // IMPORTANT: Mosquitto is reloaded AFTER publish confirms delivery (2s delay)
      // so the HUP signal does not kill the device's active subscription mid-flight.
      const credTopic = `louverlink/${regDeviceId}/credentials`;
      const credPayload = JSON.stringify({
        mqtt_username: mqttUsername,
        mqtt_password: mqttPassword,
      });

      mqttClient.publish(credTopic, credPayload, { qos: 1, retain: true }, (err) => {
        if (err) {
          console.error(`[Register] Failed to publish credentials to ${regDeviceId}:`, err.message);
        } else {
          console.log(`[Register] Credentials sent to ${regDeviceId}`);
          // Reload Mosquitto 2s after delivery — gives device time to receive
          // the retained message before the HUP resets active sessions.
          setTimeout(() => reloadMosquitto(), 2000);
        }
      });

    } catch (err) {
      console.error('[Register] Error:', err.message);
    }
    return;
  }

  if (!deviceId || !msgType) return;

  // ── Online/offline (Last Will Testament) ──────────────────────────────────
  if (msgType === 'online') {
    const online = payload.toString() === '1';
    console.log(`[MQTT] Device ${deviceId} is ${online ? 'ONLINE' : 'OFFLINE'}`);
    try {
      await pool.query(
        'UPDATE devices SET is_online = $1, last_seen = NOW() WHERE device_id = $2',
        [online, deviceId]
      );
    } catch (err) {
      console.error('[MQTT] online update error:', err.message);
    }
    return;
  }

  // ── Device status update ──────────────────────────────────────────────────
  if (msgType === 'status') {
    let data;
    try {
      data = JSON.parse(payload.toString());
    } catch {
      console.error(`[MQTT] Bad JSON from ${deviceId}:`, payload.toString());
      return;
    }

    const {
      firmware_version,
      current_position,
      is_moving,
      battery_voltage,
      battery_pct,
      usb_powered,
      poll_interval_ms,
      ip,
    } = data;

    console.log(`[MQTT] Status from ${deviceId}: pos=${current_position} moving=${is_moving} batt=${battery_voltage}V`);

    try {
      // Get current device record (for servo limits and target)
      const result = await pool.query(
        'SELECT * FROM devices WHERE device_id = $1',
        [deviceId]
      );
      const device = result.rows[0];

      const servoMin = device?.servo_angle_min ?? 0;
      const servoMax = device?.servo_angle_max ?? 180;

      let currentPct = device?.current_position_pct ?? 0;
      if (current_position !== undefined && current_position >= 0 && !is_moving) {
        currentPct = angleToPct(current_position, servoMin, servoMax);
      }

      // Upsert device record
      await pool.query(`
        INSERT INTO devices (
          device_id, ip, firmware_version, current_position,
          current_position_pct, is_moving, is_online, battery_voltage,
          battery_pct, usb_powered, poll_interval_ms, last_seen
        ) VALUES ($1,$2,$3,$4,$5,$6,true,$7,$8,$9,$10,NOW())
        ON CONFLICT (device_id) DO UPDATE SET
          ip                  = EXCLUDED.ip,
          firmware_version    = EXCLUDED.firmware_version,
          current_position    = EXCLUDED.current_position,
          current_position_pct = $5,
          is_moving           = EXCLUDED.is_moving,
          is_online           = true,
          battery_voltage     = EXCLUDED.battery_voltage,
          battery_pct         = EXCLUDED.battery_pct,
          usb_powered         = EXCLUDED.usb_powered,
          poll_interval_ms    = EXCLUDED.poll_interval_ms,
          last_seen           = NOW()
      `, [
        deviceId,
        ip ?? null,
        firmware_version ?? null,
        current_position ?? -1,
        currentPct,
        is_moving ?? false,
        battery_voltage ?? 0,
        battery_pct ?? 0,
        usb_powered ?? false,
        poll_interval_ms ?? 30000,
      ]);

      // If device is unclaimed, nothing more to do
      if (!device || !device.claimed) {
        console.log(`[MQTT] Unclaimed device: ${deviceId}`);
        return;
      }

      // Send command back to device with current target and OTA info
      const targetAngle = device.target_position ?? 0;
      const command = {
        target_position: targetAngle,
        status: 'ok',
      };

      // Include OTA info if pending
      if (device.ota_version && device.ota_url) {
        command.ota_version = device.ota_version;
        command.ota_url     = device.ota_url;
        // Clear OTA after sending
        await pool.query(
          'UPDATE devices SET ota_version = NULL, ota_url = NULL WHERE device_id = $1',
          [deviceId]
        );
      }

      mqttClient.publish(
        `louverlink/${deviceId}/command`,
        JSON.stringify(command),
        { qos: 1, retain: true }   // retain so device gets it on reconnect
      );

      console.log(`[MQTT] Command sent to ${deviceId}: target=${targetAngle}°`);

    } catch (err) {
      console.error('[MQTT] status handler error:', err.message);
    }
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// HTTP API — used by the app (same contract as the old Render API)
// ─────────────────────────────────────────────────────────────────────────────

// ── GET /functions/getDevices ─────────────────────────────────────────────────
app.get('/functions/getDevices', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM devices ORDER BY created_at ASC');
    res.json(result.rows);
  } catch (err) {
    console.error('[getDevices] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /functions/getDevice/:device_id ───────────────────────────────────────
app.get('/functions/getDevice/:device_id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM devices WHERE device_id = $1',
      [req.params.device_id]
    );
    if (!result.rows[0]) return res.status(404).json({ error: 'Device not found' });
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[getDevice] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /functions/setTarget ─────────────────────────────────────────────────
// App sets a new target position. Server updates DB and immediately
// publishes command to the device topic (instant delivery vs waiting for poll).
app.post('/functions/setTarget', async (req, res) => {
  const { device_id, target_position_pct, target_state } = req.body;

  if (!device_id || target_position_pct === undefined)
    return res.status(400).json({ error: 'device_id and target_position_pct required' });

  if (target_position_pct < 0 || target_position_pct > 100)
    return res.status(400).json({ error: 'target_position_pct must be 0-100' });

  try {
    const result = await pool.query(
      'SELECT * FROM devices WHERE device_id = $1', [device_id]
    );
    const device = result.rows[0];
    if (!device) return res.status(404).json({ error: 'Device not found' });

    const servoMin    = device.servo_angle_min ?? 0;
    const servoMax    = device.servo_angle_max ?? 180;
    const targetAngle = pctToAngle(target_position_pct, servoMin, servoMax);
    const state       = target_state ?? (target_position_pct > 50 ? 'open' : 'closed');

    await pool.query(`
      UPDATE devices
      SET target_position_pct = $1,
          target_position     = $2,
          target_state        = $3
      WHERE device_id = $4
    `, [target_position_pct, targetAngle, state, device_id]);

    // Immediately push command to device via MQTT (no waiting for next poll)
    const command = {
      target_position: targetAngle,
      status: 'ok',
    };
    mqttClient.publish(
      `louverlink/${device_id}/command`,
      JSON.stringify(command),
      { qos: 1, retain: true }
    );

    console.log(`[setTarget] ${device_id} → ${target_position_pct}% (${targetAngle}°)`);
    res.json({ ok: true, target_position: targetAngle, target_position_pct });
  } catch (err) {
    console.error('[setTarget] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /functions/claimDevice ───────────────────────────────────────────────
app.post('/functions/claimDevice', async (req, res) => {
  const { device_id, name, servo_angle_min, servo_angle_max, gpio_pin } = req.body;
  if (!device_id) return res.status(400).json({ error: 'device_id required' });

  try {
    await pool.query(`
      UPDATE devices
      SET name            = $1,
          servo_angle_min = $2,
          servo_angle_max = $3,
          gpio_pin        = $4,
          claimed         = true
      WHERE device_id = $5
    `, [name ?? device_id, servo_angle_min ?? 0, servo_angle_max ?? 180,
        gpio_pin ?? 0, device_id]);

    console.log(`[claimDevice] Claimed: ${device_id} as "${name}"`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[claimDevice] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /functions/unclaimDevice ─────────────────────────────────────────────
app.post('/functions/unclaimDevice', async (req, res) => {
  const { device_id } = req.body;
  if (!device_id) return res.status(400).json({ error: 'device_id required' });

  try {
    // Get existing mqtt_username before clearing
    const result = await pool.query(
      'SELECT mqtt_username FROM devices WHERE device_id = $1', [device_id]
    );
    const mqttUsername = result.rows[0]?.mqtt_username;

    await pool.query(`
      UPDATE devices
      SET claimed             = false,
          name                = NULL,
          target_position     = 0,
          target_position_pct = 0,
          current_position    = -1,
          mqtt_username       = NULL,
          mqtt_password       = NULL
      WHERE device_id = $1
    `, [device_id]);

    // Delete device's unique Mosquitto credential and ACL entry
    if (mqttUsername) {
      deleteMosquittoCredential(mqttUsername);
      removeDeviceAcl(mqttUsername);
      reloadMosquitto();
    }

    // Clear any retained command on the broker
    mqttClient.publish(`louverlink/${device_id}/command`, '', { retain: true });

    console.log(`[unclaimDevice] Unclaimed: ${device_id}`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[unclaimDevice] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /functions/setOta ────────────────────────────────────────────────────
app.post('/functions/setOta', async (req, res) => {
  const { device_id, ota_version, ota_url } = req.body;
  if (!device_id || !ota_version || !ota_url)
    return res.status(400).json({ error: 'device_id, ota_version and ota_url required' });

  try {
    await pool.query(
      'UPDATE devices SET ota_version = $1, ota_url = $2 WHERE device_id = $3',
      [ota_version, ota_url, device_id]
    );
    console.log(`[setOta] OTA queued for ${device_id}: v${ota_version}`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[setOta] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    mqtt: mqttClient.connected ? 'connected' : 'disconnected',
    uptime: process.uptime(),
  });
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
initDb().then(() => {
  app.listen(PORT, () => {
    console.log(`[HTTP] LouverLink API listening on port ${PORT}`);
  });
}).catch(err => {
  console.error('[DB] Init failed:', err.message);
  process.exit(1);
});
