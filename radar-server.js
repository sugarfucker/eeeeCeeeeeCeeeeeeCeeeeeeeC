#!/usr/bin/env node
'use strict';
// scp "C:\Users\qq157\Desktop\radar-server.js" root@156.245.247.233:/root/radar/
// thTxeGdVW0YU
// ssh root@156.245.247.233
// pm2 restart radar
// 自动安装 ws 依赖
try { require('ws'); } catch (e) {
  console.log('[*] 正在安装 ws 依赖...');
  require('child_process').execSync('npm install ws', { stdio: 'inherit' });
}

const http = require('http');
const https = require('https');
const fs = require('fs');
const WebSocket = require('ws');

// 重写 console.log / console.warn，自动添加 UTC+8 时间戳
(function () {
  const _origLog = console.log;
  const _origWarn = console.warn;
  function ts() {
    const d = new Date(Date.now() + 8 * 3600000);
    const pad = n => String(n).padStart(2, '0');
    return `[${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}]`;
  }
  console.log = (...a) => _origLog(ts(), ...a);
  console.warn = (...a) => _origWarn(ts(), ...a);
})();

const HTTPS_PORT = parseInt(process.env.HTTPS_PORT) || 443;
const HTTP_PORT = parseInt(process.env.HTTP_PORT) || 80;
const DOMAIN = process.env.DOMAIN || 'xxxdxxx.cc.cd';
const CERT_PATH = process.env.CERT_PATH || `/etc/letsencrypt/live/${DOMAIN}`;

// 加载 TLS 证书
let tlsOptions = null;
try {
  tlsOptions = {
    cert: fs.readFileSync(`${CERT_PATH}/fullchain.pem`),
    key: fs.readFileSync(`${CERT_PATH}/privkey.pem`),
  };
  console.log(`[*] TLS 证书已加载: ${CERT_PATH}`);
} catch (e) {
  console.warn(`[!] TLS 证书加载失败: ${e.message}`);
  console.warn('[!] 将以 HTTP 明文模式运行 (不安全)');
}
const ROOM_TIMEOUT = 60000;  // 房间超时 60s
const MAX_ROOMS = 200;       // 最大房间数
const MAX_VIEWERS = 10;      // 每房间最多观看者
const RATE_LIMIT = 10;       // 每IP每分钟最大连接数
const MIN_FRAME_INTERVAL = 15; // 推送端最小帧间隔 ms (限制 ~60fps)

// ==================== 配置上传相关 ====================
const CONFIG_DIR = '/root/user_configs';          // 配置文件存储目录
const MAX_LOGIN_HISTORY = 100;                     // 最多保留登录历史条数
const MAX_UPLOAD_BODY = 1024 * 512;                // 上传body最大512KB
const path = require('path');

// 启动时确保目录存在
if (!fs.existsSync(CONFIG_DIR)) {
  fs.mkdirSync(CONFIG_DIR, { recursive: true });
  console.log(`[*] 已创建配置目录: ${CONFIG_DIR}`);
}

// ==================== 地图ID映射 ====================
const MAP_NAMES = [
  'unknown', 'de_dust2', 'de_mirage', 'de_inferno', 'de_nuke', 'de_overpass',
  'de_vertigo', 'de_ancient', 'de_anubis', 'cs_italy', 'cs_office', 'cs_agency',
  'de_grail', 'de_jura', 'de_mills', 'de_thera', 'de_train', 'de_cache'
];

// ==================== 武器ID → 图标字符映射 ====================
const WEAPON_ICONS = [
  '', '\uE001', '\uE002', '\uE003', '\uE004', '\uE007', '\uE008', '\uE009', '\uE00a', '\uE00b', '\uE00d', '\uE03c', '\uE00e', '\uE011', '\uE024', 'mp5sd', '\uE018', '\uE019', '\uE01a', '\uE01b', '\uE01c', '\uE01d', '\uE01e', '\uE01f', '\uE013', '\uE021', '\uE022', '\uE023', '\uE020', 'shield', '\uE026', '\uE027', '\uE028', 'knife_gg', '\uE02a', '\uE02b', '\uE02c', '\uE02d', '\uE02e', '\uE02f', '\uE030', '\uE031', '\uE03b', '\uE010', '\uE03d', '\uE03f', '\uE040', '\uE1f4', '\uE02a', '\uE1f9', '\uE1fa', '\uE1fb', '\uE1fc', '\uE1fd', '\uE200', '\uE202', '\uE203', '\uE204', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '', '', ''
];

// ==================== 房间管理 ====================
class Room {
  constructor(id) {
    this.id = id;
    this.pusher = null;
    this.viewers = new Set();
    this.lastFrame = null;
    this.lastActivity = Date.now();
  }
}

const rooms = new Map();
const ipConnections = new Map();

// 定期清理过期房间
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of rooms) {
    // 清理条件: 推送端已断开且超时, 或推送端虽连着但长时间无数据
    const pusherDead = !room.pusher || room.pusher.readyState !== WebSocket.OPEN;
    const zombieRoom = now - room.lastActivity > ROOM_TIMEOUT;
    if (pusherDead && zombieRoom) {
      for (const v of room.viewers) v.close(1000, 'room_expired');
      rooms.delete(id);
    } else if (zombieRoom && room.pusher) {
      // zombie pusher: 连着但长时间不发数据, 主动断开
      room.pusher.close(1000, 'inactive');
      room.pusher = null;
    }
  }
  ipConnections.clear();
}, 30000);

// 速率限制检查
function checkRateLimit(ip) {
  const count = ipConnections.get(ip) || 0;
  if (count >= RATE_LIMIT) return false;
  ipConnections.set(ip, count + 1);
  return true;
}

// ==================== 雷达前端 HTML ====================
function getRadarHTML(roomId) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
<title>Cloud Radar</title>
<script src="https://cdn.jsdelivr.net/npm/nosleep.js@0.12.0/dist/NoSleep.min.js"><\/script>
<style>
:root{--color-t:#f0c941;--color-ct:#5ab8f4;--color-bg:#1a1a1a;--color-text:#e0e0e0;--color-text-dark:#000;--health-full:#60d060;--health-medium:#d0d040;--health-low:#d04040;--color-bomb:#ff4040}
html,body{margin:0;padding:0;overflow:hidden;width:100%;height:100%;background:var(--color-bg);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;color:var(--color-text);user-select:none}
#radar-container{position:relative;width:100%;height:100%;display:flex;justify-content:center;align-items:center}
#radar-overlay{position:absolute;top:0;left:0;width:100%;height:100%;z-index:2;will-change:transform}
.stroked-text{paint-order:stroke;stroke:var(--color-text-dark);stroke-width:3px;stroke-linecap:butt;stroke-linejoin:miter;font-weight:bold}
.player-info-text{stroke-width:2.5px}
.weapon-icon-font{font-family:'cs_icons';font-weight:normal}
.bomb-icon{font-size:18px;fill:var(--color-bomb);stroke:var(--color-text-dark);stroke-width:2px}
#loading-screen{position:fixed;top:0;left:0;width:100%;height:100%;background:#111;z-index:99;transition:opacity .5s;overflow:hidden}
#loading-screen.hidden{opacity:0;pointer-events:none}
#loading-screen canvas{position:absolute;top:0;left:0;width:100%;height:100%;opacity:0.6}
/* 扫描线 */
#loading-screen .scanlines{position:absolute;top:0;left:0;width:100%;height:100%;background:repeating-linear-gradient(0deg,rgba(0,0,0,0) 0px,rgba(0,0,0,0) 1px,rgba(0,0,0,0.15) 1px,rgba(0,0,0,0.15) 2px);pointer-events:none;z-index:2}
/* CRT 暗角 */
#loading-screen .vignette{position:absolute;top:0;left:0;width:100%;height:100%;background:radial-gradient(ellipse at center,rgba(0,0,0,0) 50%,rgba(0,0,0,0.7) 100%);pointer-events:none;z-index:3}
/* 文字层 */
#loading-screen .tv-text{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center;z-index:4;font-family:'Courier New',monospace}
#loading-screen .no-signal{font-size:clamp(24px,5vw,48px);font-weight:bold;color:#eee;text-shadow:0 0 10px rgba(255,255,255,0.5),2px 0 #ff0055,-2px 0 #00ffaa;letter-spacing:8px;animation:tv-flicker 3s infinite}
#loading-screen .tv-room{font-size:clamp(11px,2vw,16px);color:#777;margin-top:16px;letter-spacing:3px}
@keyframes tv-flicker{0%,93%,95%,97%,100%{opacity:1}94%{opacity:0.7}96%{opacity:0.4}98%{opacity:0.9}}
/* 浮动小圆点 */
#fab-dot{position:fixed;top:16px;left:16px;width:36px;height:36px;border-radius:50%;background:rgba(255,255,255,0.15);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);border:1.5px solid rgba(255,255,255,0.25);z-index:100;cursor:grab;display:none;align-items:center;justify-content:center;transition:box-shadow .2s,background .2s;touch-action:none}
#fab-dot:active{cursor:grabbing}
#fab-dot:hover{background:rgba(255,255,255,0.25)}
#fab-dot.open{background:rgba(90,184,244,0.35);border-color:rgba(90,184,244,0.6);box-shadow:0 0 12px rgba(90,184,244,0.3)}
#fab-dot .dot-inner{width:10px;height:10px;border-radius:50%;background:rgba(255,255,255,0.7);transition:background .2s}
#fab-dot.open .dot-inner{background:#5ab8f4}
#fab-dot.disconnected .dot-inner{background:#ff6b6b}
/* 展开面板 */
#settings-panel{position:fixed;top:60px;left:16px;background:rgba(20,20,25,0.92);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border:1px solid rgba(255,255,255,0.12);border-radius:12px;padding:14px 16px;z-index:99;min-width:190px;opacity:0;transform:scale(0.9) translateY(-8px);pointer-events:none;transition:opacity .2s ease,transform .2s ease;font-size:13px;color:#ccc}
#settings-panel.open{opacity:1;transform:scale(1) translateY(0);pointer-events:auto}
.panel-row{display:flex;align-items:center;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(255,255,255,0.06)}
.panel-row:last-child{border-bottom:none}
.panel-label{font-size:13px;color:#aaa}
.panel-value{font-size:13px;color:#eee;font-weight:500}
/* iOS风格 Switch */
.switch{position:relative;width:42px;height:24px;flex-shrink:0}
.switch input{opacity:0;width:0;height:0}
.switch .slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:rgba(255,255,255,0.15);border-radius:24px;transition:background .25s}
.switch .slider::before{content:'';position:absolute;height:18px;width:18px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:transform .25s cubic-bezier(.4,.0,.2,1),box-shadow .25s}
.switch input:checked+.slider{background:#5ab8f4}
.switch input:checked+.slider::before{transform:translateX(18px);box-shadow:0 0 4px rgba(90,184,244,0.5)}
/* 电视切台动画 */
@keyframes tv-switch-out{0%{transform:scaleY(1);opacity:1;filter:brightness(1)}40%{transform:scaleY(1);opacity:1;filter:brightness(2.5)}60%{transform:scaleY(0.005);opacity:1;filter:brightness(3)}100%{transform:scaleY(0);opacity:0;filter:brightness(0)}}
@keyframes tv-switch-in{0%{transform:scaleY(0);opacity:0;filter:brightness(0)}40%{transform:scaleY(0.005);opacity:1;filter:brightness(3)}60%{transform:scaleY(1);opacity:1;filter:brightness(2.5)}100%{transform:scaleY(1);opacity:1;filter:brightness(1)}}
.tv-out{animation:tv-switch-out .35s ease-in forwards}
.tv-in{animation:tv-switch-in .35s ease-out forwards}
/* 自定义 Combobox */
.panel-select{appearance:none;-webkit-appearance:none;background:rgba(255,255,255,0.1);border:1px solid rgba(255,255,255,0.15);border-radius:8px;color:#eee;font-size:13px;padding:4px 24px 4px 8px;outline:none;cursor:pointer;min-width:60px;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%23aaa' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 6px center;background-size:12px;transition:border-color .2s,background .2s}
.panel-select:hover{border-color:rgba(90,184,244,0.5)}
.panel-select:focus{border-color:#5ab8f4;background:rgba(90,184,244,0.1)}
.panel-select option{background:#1a1a1e;color:#eee}
</style>
</head>
<body>
<div id="radar-container">
  <svg id="radar-overlay" viewBox="0 0 1024 1024" preserveAspectRatio="xMidYMid meet">
    <defs><filter id="dead-drop-shadow" x="-50%" y="-50%" width="200%" height="200%"><feDropShadow dx="0" dy="0" stdDeviation="1" flood-color="black" flood-opacity="0.5"/></filter></defs>
    <g id="world-group">
      <image id="map-image-lower" x="0" y="0" width="1024" height="1024" style="opacity:0;"/>
      <image id="map-image" x="0" y="0" width="1024" height="1024" style="opacity:0;"/>
      <!-- 默认占位图: 暗色方块+问号 -->
      <g id="map-placeholder">
        <rect x="12" y="12" width="1000" height="1000" rx="16" ry="16" fill="#1e1e22" stroke="rgba(255,255,255,0.08)" stroke-width="2"/>
        <text x="512" y="512" text-anchor="middle" dominant-baseline="central" font-size="200" font-weight="bold" fill="rgba(255,255,255,0.08)" font-family="sans-serif">?</text>
      </g>
      <g id="players-layer"></g>
      <g id="bomb-layer"></g>
    </g>
  </svg>
</div>
<!-- 浮动小圆点 -->
<div id="fab-dot" class="disconnected"><div class="dot-inner"></div></div>
<!-- 设置面板 -->
<div id="settings-panel">
  <div class="panel-row"><span class="panel-label">地图</span><span id="panel-map" class="panel-value">...</span></div>
  <div class="panel-row"><span class="panel-label">房间人数</span><span id="panel-viewers" class="panel-value">0</span></div>
  <div class="panel-row"><span class="panel-label">小地图模式</span><label class="switch"><input type="checkbox" id="sw-minimap" checked><span class="slider"></span></label></div>
  <div class="panel-row"><span class="panel-label">焦点玩家</span><select id="sel-focus-player" class="panel-select"><option value="-1">自动</option></select></div>
</div>
<div id="loading-screen">
  <canvas id="tv-static"></canvas>
  <div class="scanlines"></div>
  <div class="vignette"></div>
  <div class="tv-text">
    <div class="no-signal">NO SIGNAL</div>
    <div class="tv-room">Channel: ${roomId}</div>
  </div>
</div>
<script>
(function(){
const ROOM_ID = '${roomId}';
const SCALE = 1.6;
const FONT_URL = 'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/Icons.ttf';
const CACHE_NAME = 'radar-assets-v2';
const STALE_MS = 200;
const MAX_FRAMES = 20;
const INTERP_DELAY = 300;       // 插值延迟 ms (越大抗抖越强, 但显示延迟越高)
const TARGET_FPS = 60;
const FRAME_TIME = 1000 / TARGET_FPS;
const EXTRAP_MAX = 500;         // 最大外推时长 ms (覆盖500ms丢包)
const SNAP_DIST_SQ = 250000;    // 超过此距离²直接跳转(复活/传送)
const CORRECTION_DECAY = 0.75;  // 外推修正偏移衰减系数 (越小回弹越快)
const VEL_EMA_ALPHA = 0.6;      // 速度EMA平滑系数 (0→完全旧速度, 1→完全新速度)

// 地图名称表
const MAP_NAMES = ['unknown','de_dust2','de_mirage','de_inferno','de_nuke','de_overpass','de_vertigo','de_ancient','de_anubis','cs_italy','cs_office','cs_agency','de_grail','de_jura','de_mills','de_thera','de_train','de_cache'];

// 武器图标表
const WEAPON_ICONS = [
  '', '\uE001', '\uE002', '\uE003', '\uE004', '\uE007', '\uE008', '\uE009', '\uE00a', '\uE00b', '\uE00d', '\uE03c', '\uE00e', '\uE011', '\uE024', 'mp5sd', '\uE018', '\uE019', '\uE01a', '\uE01b', '\uE01c', '\uE01d', '\uE01e', '\uE01f', '\uE013', '\uE021', '\uE022', '\uE023', '\uE020', 'shield', '\uE026', '\uE027', '\uE028', 'knife_gg', '\uE02a', '\uE02b', '\uE02c', '\uE02d', '\uE02e', '\uE02f', '\uE030', '\uE031', '\uE03b', '\uE010', '\uE03d', '\uE03f', '\uE040', '\uE1f4', '\uE02a', '\uE1f9', '\uE1fa', '\uE1fb', '\uE1fc', '\uE1fd', '\uE200', '\uE202', '\uE203', '\uE204', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '\uE02a', '', '', ''
];

// 地图配置
const MAP_DATA = {
  de_dust2:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_dust2.png',pos_x:-2476,pos_y:3239,scale:4.4},
  de_mirage:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_mirage.png',pos_x:-3230,pos_y:1713,scale:5},
  de_inferno:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_inferno.png',pos_x:-2087,pos_y:3870,scale:4.9},
  de_nuke:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_nuke_upper.png',imageUrlLower:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_nuke_lower.png',splitZ:-480,pos_x:-3453,pos_y:2887,scale:7},
  de_overpass:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_overpass.png',pos_x:-4831,pos_y:1781,scale:5.2},
  de_vertigo:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_vertigo_up.png',imageUrlLower:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_vertigo_low.png',splitZ:11680,pos_x:-3168,pos_y:1762,scale:4},
  de_ancient:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_ancient.png',pos_x:-2953,pos_y:2164,scale:5},
  de_anubis:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_anubis.png',pos_x:-2796,pos_y:3328,scale:5.22},
  cs_italy:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/cs_italy.png',pos_x:-2647,pos_y:2592,scale:4.6},
  cs_office:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/cs_office.png',pos_x:-1838,pos_y:1858,scale:4.1},
  cs_agency:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/cs_agency.png',pos_x:-2597.7368,pos_y:2079.3687,scale:4.1817436},
  de_grail:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_grail.png',pos_x:-4395.903,pos_y:4203.903,scale:4.3513728},
  de_jura:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_jura.png',pos_x:-2126.9092,pos_y:2389.8,scale:5.008376},
  de_mills:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_mills.png',pos_x:-4810,pos_y:-320,scale:5.148437},
  de_thera:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_thera.png',pos_x:-85.609764,pos_y:2261.8025,scale:4.846961},
  de_train:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_train.png',pos_x:-2308,pos_y:2078,scale:4.082077},
  de_cache:{imageUrl:'https://gh.llkk.cc/https://raw.githubusercontent.com/zacharyadcock800-glitch/bb/master/de_cache.png',pos_x:-2000,pos_y:3250,scale:5.5}
};

// DOM 引用
const worldGroup = document.getElementById('world-group');
const mapImage = document.getElementById('map-image');
const mapImageLower = document.getElementById('map-image-lower');
const playersLayer = document.getElementById('players-layer');
const bombLayer = document.getElementById('bomb-layer');
const loadingScreen = document.getElementById('loading-screen');
const fabDot = document.getElementById('fab-dot');
const settingsPanel = document.getElementById('settings-panel');
const panelMap = document.getElementById('panel-map');
const panelViewers = document.getElementById('panel-viewers');
const swMinimap = document.getElementById('sw-minimap');
const selFocusPlayer = document.getElementById('sel-focus-player');

// 状态
let currentMapConfig = null;
let currentMapName = '';
let viewerCount = 0;
let lastDataTime = 0;
let frameBuffer = [];
let lastFrameTime = 0;
let isFollowMode = localStorage.getItem('isFollowMode') !== 'false'; // 默认 ON
let focusPlayerIndex = -1; // 焦点玩家 index (-1=自动, 使用服务端 is_me)
const playerElements = new Map();
const bombElement = { element: null, visible: false };
const blobUrlCache = new Map();
const blobToOriginal = new Map();

// 插值用缓冲
let interpPlayers = [];
let interpResult = [];
let interpState = { players: null, bomb: null, current_map: '', timestamp: 0, _bombInterp: { bomb_position: { x:0, y:0, z:0 } } };
const playerVelocityMap = new Map();
const playerCorrectionMap = new Map();
let wasExtrapolating = false;
let serverTimeOffset = null;

// 更新状态栏显示
function updateStatus() {
  panelMap.textContent = currentMapName || '...';
  panelViewers.textContent = String(viewerCount);
  fabDot.classList.remove('disconnected');
}

// ==================== 浮动圆点 + 拖拽 + 面板 ====================
let panelOpen = false;
let isDragging = false;
let dragStartX = 0, dragStartY = 0, dotStartX = 0, dotStartY = 0;
const DRAG_THRESHOLD = 6; // px, 超过才算拖拽

function setDotPos(x, y) {
  const maxX = window.innerWidth - fabDot.offsetWidth;
  const maxY = window.innerHeight - fabDot.offsetHeight;
  fabDot.style.left = Math.max(0, Math.min(maxX, x)) + 'px';
  fabDot.style.top  = Math.max(0, Math.min(maxY, y)) + 'px';
}

function onPointerDown(e) {
  isDragging = false;
  const t = e.touches ? e.touches[0] : e;
  dragStartX = t.clientX; dragStartY = t.clientY;
  dotStartX = fabDot.offsetLeft; dotStartY = fabDot.offsetTop;
  document.addEventListener('mousemove', onPointerMove);
  document.addEventListener('mouseup', onPointerUp);
  document.addEventListener('touchmove', onPointerMove, { passive: false });
  document.addEventListener('touchend', onPointerUp);
}
function onPointerMove(e) {
  const t = e.touches ? e.touches[0] : e;
  const dx = t.clientX - dragStartX, dy = t.clientY - dragStartY;
  if (!isDragging && (Math.abs(dx) + Math.abs(dy)) > DRAG_THRESHOLD) isDragging = true;
  if (isDragging) {
    e.preventDefault();
    setDotPos(dotStartX + dx, dotStartY + dy);
    // 面板跟随圆点
    settingsPanel.style.left = fabDot.style.left;
    settingsPanel.style.top = (fabDot.offsetTop + fabDot.offsetHeight + 8) + 'px';
  }
}
function onPointerUp(e) {
  document.removeEventListener('mousemove', onPointerMove);
  document.removeEventListener('mouseup', onPointerUp);
  document.removeEventListener('touchmove', onPointerMove);
  document.removeEventListener('touchend', onPointerUp);
  if (!isDragging) {
    // 点击切换面板
    panelOpen = !panelOpen;
    fabDot.classList.toggle('open', panelOpen);
    settingsPanel.classList.toggle('open', panelOpen);
    settingsPanel.style.left = fabDot.style.left;
    settingsPanel.style.top = (fabDot.offsetTop + fabDot.offsetHeight + 8) + 'px';
  }
}
fabDot.addEventListener('mousedown', onPointerDown);
fabDot.addEventListener('touchstart', onPointerDown, { passive: true });

// 点击面板外关闭
document.addEventListener('mousedown', function(e) {
  if (panelOpen && !settingsPanel.contains(e.target) && !fabDot.contains(e.target)) {
    panelOpen = false; fabDot.classList.remove('open'); settingsPanel.classList.remove('open');
  }
});

// 小地图模式开关
swMinimap.checked = isFollowMode;
swMinimap.addEventListener('change', function() {
  isFollowMode = swMinimap.checked;
  localStorage.setItem('isFollowMode', isFollowMode);
});

// 焦点玩家选择
selFocusPlayer.addEventListener('change', function() {
  focusPlayerIndex = parseInt(this.value, 10);
});

// 更新焦点玩家下拉选项
function updateFocusPlayerOptions(players) {
  var indices = players.map(function(p) { return p.player_index; }).sort(function(a,b) { return a - b; });
  var currentOpts = [];
  for (var i = 1; i < selFocusPlayer.options.length; i++) currentOpts.push(parseInt(selFocusPlayer.options[i].value));
  if (JSON.stringify(indices) === JSON.stringify(currentOpts)) return;
  var oldVal = selFocusPlayer.value;
  selFocusPlayer.innerHTML = '<option value="-1">自动</option>';
  for (var j = 0; j < indices.length; j++) {
    var opt = document.createElement('option');
    opt.value = indices[j];
    opt.textContent = 'Player ' + indices[j];
    selFocusPlayer.appendChild(opt);
  }
  if (indices.indexOf(parseInt(oldVal)) !== -1) {
    selFocusPlayer.value = oldVal;
  } else {
    selFocusPlayer.value = '-1';
    focusPlayerIndex = -1;
  }
}

// NoSleep
let noSleep = null;
if (typeof NoSleep !== 'undefined') noSleep = new NoSleep();
async function enableNoSleep() { if (noSleep) try { await noSleep.enable(); } catch(e){} }
document.addEventListener('click', enableNoSleep, { once: true });
document.addEventListener('touchend', enableNoSleep, { once: true });

// ==================== TV \u9759\u6001\u566a\u58f0 ====================
(function initTVStatic() {
  const cvs = document.getElementById('tv-static');
  if (!cvs) return;
  const ctx = cvs.getContext('2d');
  // \u4f4e\u5206\u8fa8\u7387\u7ed8\u5236\u63d0\u5347\u6027\u80fd, CSS \u62c9\u4f38\u5230\u5168\u5c4f
  const W = 256, H = 192;
  cvs.width = W; cvs.height = H;
  const imgData = ctx.createImageData(W, H);
  const data = imgData.data;
  let staticRAF = 0;
  function drawNoise() {
    // \u68c0\u6d4b loading-screen \u662f\u5426\u5df2\u9690\u85cf
    if (loadingScreen.classList.contains('hidden')) {
      cancelAnimationFrame(staticRAF);
      return;
    }
    for (let i = 0; i < data.length; i += 4) {
      const v = (Math.random() * 255) | 0;
      data[i] = data[i+1] = data[i+2] = v;
      data[i+3] = 255;
    }
    ctx.putImageData(imgData, 0, 0);
    staticRAF = requestAnimationFrame(drawNoise);
  }
  staticRAF = requestAnimationFrame(drawNoise);
})();

// 图片缓存
async function getCachedUrl(url) {
  if (blobUrlCache.has(url)) return blobUrlCache.get(url);
  if (!('caches' in window)) return url;
  try {
    const cache = await caches.open(CACHE_NAME);
    let resp = await cache.match(url);
    if (!resp) {
      const r = await fetch(url);
      if (!r.ok) return url;
      await cache.put(url, r.clone());
      resp = r;
    }
    const blob = await resp.blob();
    const blobUrl = URL.createObjectURL(blob);
    blobUrlCache.set(url, blobUrl);
    blobToOriginal.set(blobUrl, url);
    return blobUrl;
  } catch(e) { return url; }
}

// 加载武器图标字体
(async function loadFont() {
  const fontUrl = await getCachedUrl(FONT_URL);
  const s = document.createElement('style');
  s.textContent = "@font-face{font-family:'cs_icons';src:url('" + fontUrl + "') format('truetype');}";
  document.head.appendChild(s);
})();

// SVG 辅助
function svgEl(tag, attrs) {
  const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const k in attrs) if (attrs.hasOwnProperty(k)) el.setAttribute(k, attrs[k]);
  return el;
}

// 坐标转换
function gameToRadar(gx, gy, cfg) {
  if (!cfg) return null;
  const rx = (gx - cfg.pos_x) / cfg.scale;
  const ry = (cfg.pos_y - gy) / cfg.scale;
  if (!isFinite(rx) || !isFinite(ry)) return null;
  return { x: rx, y: ry };
}

// 线性插值
function lerp(a, b, t) { return a + (b - a) * t; }
function lerpAngle(a, b, t) {
  if (!isFinite(a) || !isFinite(b)) return isFinite(b) ? b : isFinite(a) ? a : 0;
  let d = ((b - a) % 360 + 540) % 360 - 180;
  return a + d * t;
}

// 血量颜色
function healthColor(hp) {
  if (hp > 60) return 'var(--health-full)';
  if (hp > 30) return 'var(--health-medium)';
  return 'var(--health-low)';
}

// 创建玩家 SVG 元素 (boltobserv 风格: 圆角方块 + 尖角朝向)
function createPlayerEl(isMe) {
  const S = SCALE;
  const DOT = 11.25 * S;         // 方块边长 (原9 × 1.25倍)
  const HALF = DOT / 2;
  const CORNER = DOT * 0.42;     // 大圆角半径
  const TIP = DOT * 0.02;        // 尖角处的小圆角

  const g = svgEl('g', {});

  // ---- 可旋转层: 方块本体 ----
  const rot = svgEl('g', {});
  g.appendChild(rot);

  function dotPath() {
    const r = CORNER, t = TIP;
    return 'M ' + HALF + ',' + (-HALF + t) +
      ' Q ' + HALF + ',' + (-HALF) + ' ' + (HALF - t) + ',' + (-HALF) +
      ' L ' + (-HALF + r) + ',' + (-HALF) +
      ' Q ' + (-HALF) + ',' + (-HALF) + ' ' + (-HALF) + ',' + (-HALF + r) +
      ' L ' + (-HALF) + ',' + (HALF - r) +
      ' Q ' + (-HALF) + ',' + HALF + ' ' + (-HALF + r) + ',' + HALF +
      ' L ' + (HALF - r) + ',' + HALF +
      ' Q ' + HALF + ',' + HALF + ' ' + HALF + ',' + (HALF - r) +
      ' Z';
  }

  // 方块主体 (所有玩家统一样式, Me 玩家通过描边区分)
  const dotShape = svgEl('path', {
    d: dotPath(),
    'stroke-linejoin': 'round',
    style: 'filter:drop-shadow(0 0 2px rgba(0,0,0,0.4))'
  });
  rot.appendChild(dotShape);

  // ---- 阵亡 X 形覆盖层 (默认隐藏) ----
  const deadX = svgEl('path', {
    d: dotPath(),
    fill: 'inherit',
    'clip-path': 'polygon(20% 0%, 0% 20%, 30% 50%, 0% 80%, 20% 100%, 50% 70%, 80% 100%, 100% 80%, 70% 50%, 100% 20%, 80% 0%, 50% 30%)',
    style: 'display:none;',
    filter: 'url(#dead-drop-shadow)'
  });
  rot.appendChild(deadX);

  // ---- UI层 (不跟随旋转) ----
  const ui = svgEl('g', {});
  g.appendChild(ui);

  // ---- 右侧信息面板: 武器图标 + 血条 ----
  const infoOffsetX = HALF + 3 * S;  // 右侧偏移起点

  // 武器图标 (右侧, 垂直居中偏上)
  const weapText = svgEl('text', {
    x: infoOffsetX, y: -2 * S,
    fill: 'white',
    class: 'stroked-text player-info-text weapon-icon-font',
    'text-anchor': 'start',
    'dominant-baseline': 'central',
    'font-size': (8 * S) + 'px'
  });
  ui.appendChild(weapText);

  // 血条 (右侧, 武器图标下方)
  const hbW = 20 * S, hbH = 3 * S;
  const hbX = infoOffsetX;
  const hbY = 2 * S;

  // 血条背景: 深色圆角药丸
  const hpBg = svgEl('rect', {
    x: hbX, y: hbY, width: hbW, height: hbH,
    fill: 'rgba(0,0,0,0.6)',
    stroke: 'rgba(255,255,255,0.1)', 'stroke-width': '0.5px',
    rx: hbH / 2, ry: hbH / 2
  });
  ui.appendChild(hpBg);

  // 血条掉血残影层 (红色, 延迟缩减制造掉血动画)
  const hpGhost = svgEl('rect', {
    x: hbX, y: hbY, width: hbW, height: hbH,
    fill: 'rgba(255,60,60,0.7)',
    rx: hbH / 2, ry: hbH / 2
  });
  ui.appendChild(hpGhost);

  // 血条前景: 渐变色, 带 transition 动画
  const hpFg = svgEl('rect', {
    x: hbX, y: hbY, width: hbW, height: hbH,
    fill: 'var(--health-full)',
    rx: hbH / 2, ry: hbH / 2,
    style: 'transition: width 0.15s ease-out;'
  });
  ui.appendChild(hpFg);

  // 拆弹器图标 (左侧)
  const kitIcon = svgEl('text', { x: -HALF - 6 * S, y: 5 * S, fill: 'var(--color-ct)', class: 'bomb-icon', 'font-size': 14 * S + 'px', display: 'none' });
  kitIcon.textContent = 'D';
  ui.appendChild(kitIcon);

  // 玩家 index 文字 (居中显示, 仅面板展开时可见)
  const idxText = svgEl('text', {
    x: 0, y: 0,
    fill: 'white',
    stroke: 'rgba(0,0,0,0.8)',
    'stroke-width': '2px',
    'paint-order': 'stroke',
    'text-anchor': 'middle',
    'dominant-baseline': 'central',
    'font-size': (6 * S) + 'px',
    'font-weight': 'bold',
    style: 'display:none;pointer-events:none;'
  });
  ui.appendChild(idxText);

  return {
    group: g, rotatable: rot, dotShape: dotShape, deadX: deadX, path: dotShape,
    ui: ui, weaponText: weapText, healthBg: hpBg, healthFg: hpFg, hpGhost: hpGhost, kitIcon: kitIcon, idxText: idxText,
    healthBarMaxWidth: hbW, lastSeen: Date.now(), _isDead: false,
    _ghostTimer: null, _lastGhostWidth: hbW,
    _lastColor:'',_lastStroke:'',_lastTransform:'',_lastRotation:'',_lastUiRotation:'',
    _lastWeapon:'',_lastHpWidth:-1,_lastHpColor:'',_lastOpacity:'',
    _lastDisplay:'',_lastKitDisplay:'',_lastWeaponDisplay:'',_lastHealthDisplay:'',
    _lastIdxDisplay:'',_lastIdxText:''
  };
}

// 创建炸弹 SVG 元素 (使用武器字体图标)
function createBombEl() {
  const g = svgEl('g', {});
  const t = svgEl('text', {
    x: 0, y: 0,
    fill: 'white',
    'text-anchor': 'middle',
    'dominant-baseline': 'central',
    'font-size': (10 * SCALE) + 'px',
    class: 'stroked-text weapon-icon-font',
    style: 'pointer-events:none;'
  });
  t.textContent = '\uE031'; // C4 武器字体图标
  g.appendChild(t);
  return g;
}

// ==================== 解码二进制帧 ====================
function decodeBinaryFrame(buffer) {
  const dv = new DataView(buffer);
  if (buffer.byteLength < 8) return null;

  const version = dv.getUint8(0);
  const msgType = dv.getUint8(1);
  const mapId = dv.getUint8(2);
  const playerCount = dv.getUint8(3);
  const serverTick = dv.getUint32(4, true);

  const mapName = MAP_NAMES[mapId] || 'unknown';
  const players = [];
  let offset = 8;

  for (let i = 0; i < playerCount; i++) {
    if (offset + 19 > buffer.byteLength) break;
    const idx = dv.getUint8(offset);
    const team = dv.getUint8(offset + 1);
    const health = dv.getUint8(offset + 2);
    const flags = dv.getUint8(offset + 3);
    const weapId = dv.getUint8(offset + 4);
    const px = dv.getFloat32(offset + 5, true);
    const py = dv.getFloat32(offset + 9, true);
    const pz = dv.getFloat32(offset + 13, true);
    const yawRaw = dv.getInt16(offset + 17, true);
    const yaw = yawRaw / 100.0;

    players.push({
      player_index: idx,
      player_team: team,
      player_health: health,
      player_has_defuser: !!(flags & 0x01),
      is_me: !!(flags & 0x02),
      weapon_id: weapId,
      player_view_angle_yaw: yaw,
      player_position: { x: px, y: py, z: pz }
    });
    offset += 19;
  }

  // 炸弹数据
  let bomb = null;
  if (offset + 13 <= buffer.byteLength) {
    const bflags = dv.getUint8(offset);
    bomb = {
      is_planted: !!(bflags & 0x01),
      has_data: !!(bflags & 0x02),
      bomb_position: {
        x: dv.getFloat32(offset + 1, true),
        y: dv.getFloat32(offset + 5, true),
        z: dv.getFloat32(offset + 9, true)
      }
    };
  }

  return { current_map: mapName, players: players, bomb: bomb, serverTick: serverTick, timestamp: 0 };
}

// ==================== 渲染 ====================
function renderFrame(state) {
  if (!state || !state.players || !currentMapConfig) return;

  const mePlayer = focusPlayerIndex >= 0 ? state.players.find(p => p.player_index === focusPlayerIndex) : state.players.find(p => p.is_me);
  const meAlive = mePlayer && mePlayer.player_health > 0;
  const hasSplit = currentMapConfig.splitZ !== undefined;

  let meOnLower = false;
  if (mePlayer && hasSplit && mePlayer.player_position && mePlayer.player_position.z !== undefined)
    meOnLower = mePlayer.player_position.z < currentMapConfig.splitZ;

  // 地图图片
  if (mapImage.getAttribute('href') !== currentMapConfig.imageUrl)
    mapImage.setAttribute('href', currentMapConfig.imageUrl || '');
  if (hasSplit && currentMapConfig.imageUrlLower && mapImageLower.getAttribute('href') !== currentMapConfig.imageUrlLower)
    mapImageLower.setAttribute('href', currentMapConfig.imageUrlLower);

  if (hasSplit) {
    const upperOp = meOnLower ? '0' : '1';
    const lowerOp = meOnLower ? '1' : '0';
    if (mapImage.style.opacity !== upperOp) mapImage.style.opacity = upperOp;
    if (mapImageLower.style.opacity !== lowerOp) mapImageLower.style.opacity = lowerOp;
  } else {
    if (mapImage.style.opacity !== '1') mapImage.style.opacity = '1';
    if (mapImageLower.style.opacity !== '0') mapImageLower.style.opacity = '0';
  }

  // 跟随模式变换
  let globalRotation = 0, worldTransform = '';
  if (isFollowMode && meAlive && mePlayer && mePlayer.player_position) {
    const mp = gameToRadar(mePlayer.player_position.x, mePlayer.player_position.y, currentMapConfig);
    if (mp) {
      const yaw = mePlayer.player_view_angle_yaw || 0;
      worldTransform = 'translate(512, 512) rotate(' + (yaw - 90) + ') translate(' + (-mp.x) + ', ' + (-mp.y) + ')';
      globalRotation = 90 - yaw;
    }
  }
  worldGroup.setAttribute('transform', worldTransform);

  // 玩家渲染
  const activeNames = new Set();
  const now = Date.now();

  for (let i = 0; i < state.players.length; i++) {
    const p = state.players[i];
    const key = p.is_me ? '__ME__' : ('p' + p.player_index + '_' + p.player_team);
    activeNames.add(key);
    const isMe = focusPlayerIndex >= 0 ? (p.player_index === focusPlayerIndex) : p.is_me;


    if (!p.player_position) continue;
    const rp = gameToRadar(p.player_position.x, p.player_position.y, currentMapConfig);
    if (!rp) continue;

    let el = playerElements.get(key);
    if (!el) {
      el = createPlayerEl(isMe);
      playersLayer.appendChild(el.group);
      playerElements.set(key, el);
    }
    el.lastSeen = now;

    if (el._lastDisplay === 'none') { el.group.style.display = ''; el._lastDisplay = ''; }

    // 透明度 (不同层)
    let opacity = '1';
    if (hasSplit && p.player_position.z !== undefined) {
      if ((p.player_position.z < currentMapConfig.splitZ) !== meOnLower) opacity = '0.4';
    }

    // 阵亡状态
    const isDead = p.player_health <= 0;
    if (isDead) opacity = String(0.4);
    if (el._lastOpacity !== opacity) { el.group.style.opacity = opacity; el._lastOpacity = opacity; }

    // 阵亡 X 形切换
    if (isDead !== el._isDead) {
      el._isDead = isDead;
      if (isDead) {
        el.dotShape.style.display = 'none';
        el.deadX.style.display = '';
      } else {
        el.dotShape.style.display = '';
        el.deadX.style.display = 'none';
      }
    }

    // 颜色
    let color = p.player_team === 2 ? 'var(--color-t)' : 'var(--color-ct)';

    el.group.setAttribute('transform', 'translate(' + rp.x + ', ' + rp.y + ')');

    // 方块旋转
    let dotRot = p.player_view_angle_yaw ? (-p.player_view_angle_yaw + 45) : 45;
    if (isDead) dotRot = 45;
    if (!isFinite(dotRot)) dotRot = -45;
    const rotStr = 'rotate(' + dotRot + ')';
    if (el._lastRotation !== rotStr) { el.rotatable.setAttribute('transform', rotStr); el._lastRotation = rotStr; }

    // 填充色
    if (el._lastColor !== color) {
      el.dotShape.setAttribute('fill', color);
      el.deadX.setAttribute('fill', color);
      el._lastColor = color;
    }

    // Me 玩家: 白色描边区分 (统一形状, 仅加描边)
    const meStroke = isMe ? 'rgba(255,255,255,0.9)' : 'none';
    if (el._lastStroke !== meStroke) {
      el.dotShape.setAttribute('stroke', meStroke);
      el.dotShape.setAttribute('stroke-width', isMe ? (1.5 * SCALE) : 0);
      el._lastStroke = meStroke;
    }

    const uiRot = 'rotate(' + globalRotation + ')';
    if (el._lastUiRotation !== uiRot) { el.ui.setAttribute('transform', uiRot); el._lastUiRotation = uiRot; }

    // 阵亡时隐藏所有 UI
    if (isDead) {
      if (el._lastWeaponDisplay !== 'none') { el.weaponText.style.display = 'none'; el._lastWeaponDisplay = 'none'; }
      if (el._lastHealthDisplay !== 'none') { el.healthBg.style.display = 'none'; el.healthFg.style.display = 'none'; el.hpGhost.style.display = 'none'; el._lastHealthDisplay = 'none'; }
      if (el._lastKitDisplay !== 'none') { el.kitIcon.style.display = 'none'; el._lastKitDisplay = 'none'; }
    } else {
      if (el._lastWeaponDisplay !== '') { el.weaponText.style.display = ''; el._lastWeaponDisplay = ''; }
      if (el._lastHealthDisplay !== '') { el.healthBg.style.display = ''; el.healthFg.style.display = ''; el.hpGhost.style.display = ''; el._lastHealthDisplay = ''; }

      const icon = WEAPON_ICONS[p.weapon_id] || '';
      if (el._lastWeapon !== icon) { el.weaponText.textContent = icon; el._lastWeapon = icon; }

      const hp = Number(p.player_health) || 0;
      const hpW = Math.round(Math.max(0, hp / 100 * el.healthBarMaxWidth) * 10) / 10;
      if (el._lastHpWidth !== hpW) {
        // 掉血残影动画: ghost 延迟缩减
        if (hpW < el._lastHpWidth && el._lastHpWidth > 0) {
          // 掉血了 → ghost 保持旧宽度, 延迟 300ms 后平滑缩减
          if (el._ghostTimer) clearTimeout(el._ghostTimer);
          el._ghostTimer = setTimeout(function() {
            el.hpGhost.setAttribute('width', hpW);
          }, 350);
        } else {
          // 回血或初始 → ghost 直接跟随
          el.hpGhost.setAttribute('width', hpW);
          if (el._ghostTimer) { clearTimeout(el._ghostTimer); el._ghostTimer = null; }
        }
        el.healthFg.setAttribute('width', hpW);
        el._lastHpWidth = hpW;
      }
      const hpC = healthColor(hp);
      if (el._lastHpColor !== hpC) { el.healthFg.setAttribute('fill', hpC); el._lastHpColor = hpC; }

      const kitDisp = p.player_has_defuser ? '' : 'none';
      if (el._lastKitDisplay !== kitDisp) { el.kitIcon.style.display = kitDisp; el._lastKitDisplay = kitDisp; }
    }

    // 玩家 index 文字 (面板展开时显示)
    const idxDisp = panelOpen ? '' : 'none';
    if (el._lastIdxDisplay !== idxDisp) { el.idxText.style.display = idxDisp; el._lastIdxDisplay = idxDisp; }
    if (panelOpen) {
      const idxStr = String(p.player_index);
      if (el._lastIdxText !== idxStr) { el.idxText.textContent = idxStr; el._lastIdxText = idxStr; }
    }
  }

  // 清理过期玩家
  for (const [k, el] of playerElements) {
    if (!activeNames.has(k) && now - el.lastSeen > STALE_MS) { el.group.remove(); playerElements.delete(k); }
  }

  // 炸弹
  if (state.bomb && state.bomb.has_data && (state.bomb.bomb_position.x !== 0 || state.bomb.bomb_position.y !== 0)) {
    const bp = gameToRadar(state.bomb.bomb_position.x, state.bomb.bomb_position.y, currentMapConfig);
    if (bp) {
      if (!bombElement.element) { bombElement.element = createBombEl(); bombLayer.appendChild(bombElement.element); }
      if (bombElement.element.style.display === 'none') bombElement.element.style.display = '';
      bombElement.element.setAttribute('transform', 'translate(' + bp.x + ', ' + bp.y + ') rotate(' + globalRotation + ')');
      // 下包后变红色
      const bombColor = state.bomb.is_planted ? '#ff4040' : 'white';
      if (bombElement._lastColor !== bombColor) {
        bombElement.element.querySelector('text').setAttribute('fill', bombColor);
        bombElement._lastColor = bombColor;
      }
      bombElement.visible = true;
    } else if (bombElement.visible && bombElement.element) { bombElement.element.style.display = 'none'; bombElement.visible = false; }
  } else if (bombElement.visible && bombElement.element) { bombElement.element.style.display = 'none'; bombElement.visible = false; }
}

// 帧插值 + 外推预测 (抗网络波动)
function interpolateFrames(renderTime) {
  const len = frameBuffer.length;
  if (len === 0) return null;

  // 查找包含 renderTime 的两帧区间
  let prev = null, next = null;
  for (let i = 0; i < len - 1; i++) {
    if (frameBuffer[i].timestamp <= renderTime && frameBuffer[i + 1].timestamp >= renderTime) {
      prev = frameBuffer[i]; next = frameBuffer[i + 1]; break;
    }
  }

  const lastFrame = frameBuffer[len - 1];

  // ====== 外推模式: renderTime 超出帧缓冲区最后一帧 ======
  if (!prev && renderTime > lastFrame.timestamp) {
    const overshot = Math.min(renderTime - lastFrame.timestamp, EXTRAP_MAX);
    const extDt = overshot / 1000;
    const bf = lastFrame;

    while (interpPlayers.length < bf.players.length)
      interpPlayers.push({ player_position: { x:0,y:0,z:0 }, player_view_angle_yaw: 0 });

    for (let i = 0; i < bf.players.length; i++) {
      const bp = bf.players[i], ip = interpPlayers[i];
      const keys = Object.keys(bp);
      for (let j = 0; j < keys.length; j++) { if (keys[j] !== 'player_position') ip[keys[j]] = bp[keys[j]]; }
      if (!ip.player_position) ip.player_position = { x:0,y:0,z:0 };

      const key = bp.is_me ? '__ME__' : ('p' + bp.player_index + '_' + bp.player_team);
      const vel = playerVelocityMap.get(key);

      if (vel && bp.player_position && bp.player_health > 0) {
        // 活着的玩家: 用速度向量外推位置
        ip.player_position.x = bp.player_position.x + vel.vx * extDt;
        ip.player_position.y = bp.player_position.y + vel.vy * extDt;
        ip.player_position.z = (bp.player_position.z || 0) + vel.vz * extDt;
      } else if (bp.player_position) {
        ip.player_position.x = bp.player_position.x || 0;
        ip.player_position.y = bp.player_position.y || 0;
        ip.player_position.z = bp.player_position.z || 0;
      }
      ip.player_view_angle_yaw = bp.player_view_angle_yaw || 0;

      // 记录外推位置，恢复时计算修正偏移
      playerCorrectionMap.set(key, { ex: ip.player_position.x, ey: ip.player_position.y, ez: ip.player_position.z, active: true });
    }

    interpResult.length = bf.players.length;
    for (let i = 0; i < bf.players.length; i++) interpResult[i] = interpPlayers[i];
    interpState.players = interpResult;
    interpState.current_map = bf.current_map;
    interpState.timestamp = bf.timestamp;
    interpState.bomb = bf.bomb || null;
    wasExtrapolating = true;
    return interpState;
  }

  // ====== renderTime 在所有帧之前 ======
  if (!prev) return frameBuffer[0];

  // ====== 正常插值模式 ======
  const span = next.timestamp - prev.timestamp;
  const t = span > 0 ? Math.min(1, Math.max(0, (renderTime - prev.timestamp) / span)) : 1;

  while (interpPlayers.length < next.players.length)
    interpPlayers.push({ player_position: { x:0,y:0,z:0 }, player_view_angle_yaw: 0 });

  for (let i = 0; i < next.players.length; i++) {
    const np = next.players[i], ip = interpPlayers[i];
    const keys = Object.keys(np);
    for (let j = 0; j < keys.length; j++) { if (keys[j] !== 'player_position') ip[keys[j]] = np[keys[j]]; }
    if (!ip.player_position) ip.player_position = { x:0,y:0,z:0 };

    let pp = null;
    for (let j = 0; j < prev.players.length; j++) {
      if (prev.players[j].player_index === np.player_index && prev.players[j].player_team === np.player_team && prev.players[j].is_me === np.is_me) { pp = prev.players[j]; break; }
    }

    if (pp && pp.player_position && np.player_position) {
      ip.player_position.x = lerp(pp.player_position.x, np.player_position.x, t);
      ip.player_position.y = lerp(pp.player_position.y, np.player_position.y, t);
      ip.player_position.z = (pp.player_position.z !== undefined && np.player_position.z !== undefined) ? lerp(pp.player_position.z, np.player_position.z, t) : (np.player_position.z || 0);
      ip.player_view_angle_yaw = lerpAngle(pp.player_view_angle_yaw || 0, np.player_view_angle_yaw || 0, t);
    } else if (np.player_position) {
      ip.player_position.x = np.player_position.x || 0;
      ip.player_position.y = np.player_position.y || 0;
      ip.player_position.z = np.player_position.z || 0;
    }

    // 外推→正常插值 过渡: 平滑修正偏移
    const key = np.is_me ? '__ME__' : ('p' + np.player_index + '_' + np.player_team);
    const cor = playerCorrectionMap.get(key);
    if (cor && cor.active) {
      // 首次回到正常插值: 外推位置 vs 实际位置 的差值作为修正偏移
      const dx = cor.ex - ip.player_position.x;
      const dy = cor.ey - ip.player_position.y;
      const dz = cor.ez - ip.player_position.z;
      if (dx * dx + dy * dy < SNAP_DIST_SQ) {
        cor.dx = dx; cor.dy = dy; cor.dz = dz;
        cor.active = false;
      } else {
        playerCorrectionMap.delete(key);
      }
    }
    if (cor && !cor.active && cor.dx !== undefined) {
      // 应用并衰减修正偏移 (从外推位置平滑过渡到真实位置)
      ip.player_position.x += cor.dx;
      ip.player_position.y += cor.dy;
      ip.player_position.z += cor.dz;
      cor.dx *= CORRECTION_DECAY;
      cor.dy *= CORRECTION_DECAY;
      cor.dz *= CORRECTION_DECAY;
      if (Math.abs(cor.dx) < 0.5 && Math.abs(cor.dy) < 0.5) playerCorrectionMap.delete(key);
    }
  }

  if (wasExtrapolating) wasExtrapolating = false;

  interpResult.length = next.players.length;
  for (let i = 0; i < next.players.length; i++) interpResult[i] = interpPlayers[i];
  interpState.players = interpResult;
  interpState.current_map = next.current_map;
  interpState.timestamp = next.timestamp;

  // 炸弹插值
  if (prev.bomb && next.bomb && prev.bomb.bomb_position && next.bomb.bomb_position) {
    const bi = interpState._bombInterp;
    const bkeys = Object.keys(next.bomb);
    for (let j = 0; j < bkeys.length; j++) { if (bkeys[j] !== 'bomb_position') bi[bkeys[j]] = next.bomb[bkeys[j]]; }
    bi.bomb_position.x = lerp(prev.bomb.bomb_position.x, next.bomb.bomb_position.x, t);
    bi.bomb_position.y = lerp(prev.bomb.bomb_position.y, next.bomb.bomb_position.y, t);
    bi.bomb_position.z = (prev.bomb.bomb_position.z !== undefined && next.bomb.bomb_position.z !== undefined) ? lerp(prev.bomb.bomb_position.z, next.bomb.bomb_position.z, t) : (next.bomb.bomb_position.z || 0);
    interpState.bomb = bi;
  } else {
    interpState.bomb = next.bomb || null;
  }

  return interpState;
}

// ==================== WebSocket 连接 ====================
let ws = null;
let reconnectDelay = 1000;

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = proto + '//' + location.host + '/view/' + ROOM_ID;

  ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => {
    reconnectDelay = 1000;
    updateStatus();
  };

  ws.onmessage = (ev) => {
    // 文本消息: 控制信息 (观看人数等)
    if (typeof ev.data === 'string') {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === 'info' && msg.viewers !== undefined) {
          viewerCount = msg.viewers;
          updateStatus();
        }
      } catch(e) {}
      return;
    }

    // 二进制消息: 游戏数据
    if (!(ev.data instanceof ArrayBuffer)) return;
    const frame = decodeBinaryFrame(ev.data);
    if (!frame) return;
    updateFocusPlayerOptions(frame.players);

    const currentOffset = Date.now() - frame.serverTick;
    if (serverTimeOffset === null || Math.abs(currentOffset - serverTimeOffset) > 1000) {
      serverTimeOffset = currentOffset;
    } else {
      serverTimeOffset = 0.95 * serverTimeOffset + 0.05 * currentOffset;
    }
    frame.timestamp = frame.serverTick + serverTimeOffset;

    // 地图切换检测
    if (!currentMapConfig || currentMapConfig._name !== frame.current_map) {
      const cfg = MAP_DATA[frame.current_map];
      if (!cfg) {
        // 未知地图: 显示占位图
        currentMapConfig = { _name: frame.current_map, pos_x: 0, pos_y: 0, scale: 1 };
        currentMapName = frame.current_map;
        mapImage.style.opacity = '0';
        mapImageLower.style.opacity = '0';
        document.getElementById('map-placeholder').style.display = '';
        loadingScreen.classList.add('hidden');
        fabDot.style.display = 'flex';
        playersLayer.innerHTML = '';
        playerElements.clear();
        frameBuffer.length = 0;
        updateStatus();
      } else {
        const isFirstLoad = !currentMapConfig;
        currentMapConfig = { ...cfg, _name: frame.current_map };
        currentMapName = frame.current_map;
        loadingScreen.classList.add('hidden');
        fabDot.style.display = 'flex';

        // 电视切台动画
        const overlay = document.getElementById('radar-overlay');
        if (!isFirstLoad) {
          overlay.classList.remove('tv-in');
          overlay.classList.add('tv-out');
          setTimeout(function() {
            // 隐藏占位图, 清空玩家
            document.getElementById('map-placeholder').style.display = 'none';
            playersLayer.innerHTML = '';
            playerElements.clear();
            frameBuffer.length = 0;
            mapImage.style.opacity = '1';
            // 加载新地图
            overlay.classList.remove('tv-out');
            overlay.classList.add('tv-in');
            setTimeout(function() { overlay.classList.remove('tv-in'); }, 400);
          }, 350);
        } else {
          document.getElementById('map-placeholder').style.display = 'none';
          playersLayer.innerHTML = '';
          playerElements.clear();
          frameBuffer.length = 0;
          mapImage.style.opacity = '1';
          overlay.classList.add('tv-in');
          setTimeout(function() { overlay.classList.remove('tv-in'); }, 400);
        }

        getCachedUrl(cfg.imageUrl).then(url => {
          if (currentMapConfig && currentMapConfig._name === frame.current_map) currentMapConfig.imageUrl = url;
        });
        if (cfg.imageUrlLower) {
          getCachedUrl(cfg.imageUrlLower).then(url => {
            if (currentMapConfig && currentMapConfig._name === frame.current_map) currentMapConfig.imageUrlLower = url;
          });
        }
        updateStatus();
      }
    }

    // 计算玩家速度 EMA (新帧 vs 缓冲区最后一帧, 用于外推预测)
    if (frameBuffer.length > 0) {
      const pf = frameBuffer[frameBuffer.length - 1];
      const vdt = (frame.serverTick - pf.serverTick) / 1000;
      if (vdt > 0.02 && vdt < 2) {
        for (let i = 0; i < frame.players.length; i++) {
          const np = frame.players[i];
          const key = np.is_me ? '__ME__' : ('p' + np.player_index + '_' + np.player_team);
          for (let j = 0; j < pf.players.length; j++) {
            const pp = pf.players[j];
            if (pp.player_index === np.player_index && pp.player_team === np.player_team && pp.is_me === np.is_me) {
              if (pp.player_position && np.player_position) {
                const nvx = (np.player_position.x - pp.player_position.x) / vdt;
                const nvy = (np.player_position.y - pp.player_position.y) / vdt;
                const nvz = ((np.player_position.z||0) - (pp.player_position.z||0)) / vdt;
                // 速度合理性检查 (过大说明是传送/复活, 不记录)
                if (nvx * nvx + nvy * nvy < 1000000) {
                  const old = playerVelocityMap.get(key);
                  if (old) {
                    // EMA 平滑: 避免单帧突变导致外推抖动
                    playerVelocityMap.set(key, {
                      vx: VEL_EMA_ALPHA * nvx + (1 - VEL_EMA_ALPHA) * old.vx,
                      vy: VEL_EMA_ALPHA * nvy + (1 - VEL_EMA_ALPHA) * old.vy,
                      vz: VEL_EMA_ALPHA * nvz + (1 - VEL_EMA_ALPHA) * old.vz
                    });
                  } else {
                    playerVelocityMap.set(key, { vx: nvx, vy: nvy, vz: nvz });
                  }
                }
              }
              break;
            }
          }
        }
      }
    }

    frameBuffer.push(frame);
    if (frameBuffer.length > MAX_FRAMES) frameBuffer.shift();
    lastDataTime = Date.now();
  };

  ws.onclose = () => {
    statusDisplay.textContent = 'Disconnected';
    statusDisplay.className = 'disconnected';
    setTimeout(() => {
      reconnectDelay = Math.min(reconnectDelay * 1.5, 10000);
      connectWS();
    }, reconnectDelay);

    if (Date.now() - lastDataTime > 5000) {
      loadingScreen.classList.remove('hidden');
      currentMapConfig = null;
      playersLayer.innerHTML = '';
      playerElements.clear();
      frameBuffer.length = 0;
    }
  };

  ws.onerror = () => {};
}

// ==================== 渲染循环 ====================
function renderLoop() {
  requestAnimationFrame(renderLoop);
  const now = Date.now();
  if (now - lastFrameTime < FRAME_TIME) return;
  lastFrameTime = now - ((now - lastFrameTime) % FRAME_TIME);

  const renderTime = now - INTERP_DELAY;
  const interpolated = interpolateFrames(renderTime);
  if (interpolated) renderFrame(interpolated);
}

// 清理 blob URLs
window.addEventListener('beforeunload', () => {
  blobUrlCache.forEach(url => { if (url.startsWith('blob:')) URL.revokeObjectURL(url); });
  blobUrlCache.clear();
  blobToOriginal.clear();
});

// 启动
connectWS();
requestAnimationFrame(renderLoop);
})();
<\/script>
</body>
</html>`;
}

// ==================== 配置上传处理 ====================
function handleConfigUpload(req, res) {
  // 获取客户端真实IP
  const clientIP = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').replace(/^::ffff:/, '');

  let body = '';
  let bodySize = 0;

  req.on('data', (chunk) => {
    bodySize += chunk.length;
    if (bodySize > MAX_UPLOAD_BODY) {
      res.writeHead(413, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'error', message: 'body too large' }));
      req.destroy();
      return;
    }
    body += chunk.toString();
  });

  req.on('end', () => {
    try {
      const data = JSON.parse(body);
      const username = (data.username || '').trim();
      const configContent = data.config || '';

      if (!username || username.length > 64) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'error', message: 'invalid username' }));
        return;
      }

      // 安全文件名: 替换危险字符
      const safeUsername = username.replace(/[\\/:*?"<>|]/g, '_');
      const filePath = path.join(CONFIG_DIR, safeUsername + '.ini');

      // 获取当前时间 (UTC+8)
      const now = new Date();
      now.setHours(now.getHours() + 8);
      const timeStr = now.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
      const newEntry = `; ${timeStr} | IP: ${clientIP}`;

      // 读取旧的登录历史
      let oldHistory = [];
      if (fs.existsSync(filePath)) {
        try {
          const oldContent = fs.readFileSync(filePath, 'utf-8');
          const historyMatch = oldContent.match(/\[LoginHistory\]\r?\n([\s\S]*?)(?=\r?\n\[|$)/);
          if (historyMatch) {
            oldHistory = historyMatch[1].split('\n')
              .map(l => l.trim())
              .filter(l => l.startsWith(';'));
          }
        } catch (e) { /* 旧文件读取失败，忽略 */ }
      }

      // 追加新记录（新的在最前面），限制最多100条
      oldHistory.unshift(newEntry);
      if (oldHistory.length > MAX_LOGIN_HISTORY) {
        oldHistory = oldHistory.slice(0, MAX_LOGIN_HISTORY);
      }

      // 组装最终文件内容
      const finalContent = '[LoginHistory]\n' + oldHistory.join('\n') + '\n\n' + configContent;
      fs.writeFileSync(filePath, finalContent, 'utf-8');

      console.log(`[Upload] ${safeUsername} | IP: ${clientIP} | ${configContent.length} bytes`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', ip: clientIP }));
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'error', message: 'invalid json' }));
    }
  });
}

// ==================== HTTP(S) 服务器 ====================
function handleRequest(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const urlPath = url.pathname;

  // CORS 头
  res.setHeader('Access-Control-Allow-Origin', '*');

  // 配置上传接口
  if (req.method === 'POST' && urlPath === '/upload') {
    handleConfigUpload(req, res);
    return;
  }

  if (urlPath === '/' || urlPath === '') {
    res.writeHead(404);
    res.end();
    return;
  }

  if (urlPath.startsWith('/push/') || urlPath.startsWith('/view/') || urlPath === '/favicon.ico') {
    res.writeHead(404);
    res.end();
    return;
  }

  const roomId = urlPath.substring(1).replace(/[^a-zA-Z0-9]/g, '');
  if (roomId.length > 0 && roomId.length <= 20) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(getRadarHTML(roomId));
  } else {
    res.writeHead(404);
    res.end('Invalid room ID');
  }
}

// 主服务器 (HTTPS 或 HTTP 回退)
const server = tlsOptions
  ? https.createServer(tlsOptions, handleRequest)
  : http.createServer(handleRequest);

// HTTP → HTTPS 跳转服务器 (仅 TLS 模式)
let httpRedirectServer = null;
if (tlsOptions) {
  httpRedirectServer = http.createServer((req, res) => {
    const host = (req.headers.host || DOMAIN).replace(/:\d+$/, '');
    res.writeHead(301, { Location: `https://${host}${req.url}` });
    res.end();
  });
}

// ==================== WebSocket 服务器 ====================
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  if (!checkRateLimit(ip)) {
    ws.close(1008, 'rate_limited');
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  const parts = url.pathname.split('/').filter(Boolean);

  if (parts[0] === 'push' && parts[1]) {
    handlePusher(ws, parts[1].replace(/[^a-zA-Z0-9]/g, ''));
  } else if (parts[0] === 'view' && parts[1]) {
    handleViewer(ws, parts[1].replace(/[^a-zA-Z0-9]/g, ''));
  } else {
    ws.close(1002, 'invalid_path');
  }
});

function handlePusher(ws, roomId) {
  if (rooms.size >= MAX_ROOMS && !rooms.has(roomId)) {
    ws.close(1013, 'server_full');
    return;
  }

  let room = rooms.get(roomId);
  if (!room) {
    room = new Room(roomId);
    rooms.set(roomId, room);
  }

  // 踢掉旧推送端
  if (room.pusher && room.pusher.readyState === WebSocket.OPEN) {
    room.pusher.close(1000, 'replaced');
  }
  room.pusher = ws;
  room.lastActivity = Date.now();

  console.log(`[+] Pusher connected: room=${roomId}, viewers=${room.viewers.size}`);

  let lastFrameTime = 0; // 帧率限制时间戳

  ws.on('message', (data) => {
    if (!(data instanceof Buffer)) return;

    // 帧率限制: 防止推送端恶意刷屏
    const now = Date.now();
    if (now - lastFrameTime < MIN_FRAME_INTERVAL) return; // 丢弃过快的帧
    lastFrameTime = now;

    room.lastFrame = data;
    room.lastActivity = now;

    // 广播给所有观看者 (零解析直接转发)
    for (const viewer of room.viewers) {
      if (viewer.readyState === WebSocket.OPEN && viewer.bufferedAmount < 65536) {
        viewer.send(data);
      }
    }
  });

  ws.on('close', () => {
    console.log(`[-] Pusher disconnected: room=${roomId}`);
    if (room.pusher === ws) room.pusher = null;
  });

  ws.on('error', () => { });
}

// 广播观看人数给同房间所有观看者
function broadcastViewerCount(room) {
  const msg = JSON.stringify({ type: 'info', viewers: room.viewers.size });
  for (const v of room.viewers) {
    if (v.readyState === WebSocket.OPEN) {
      try { v.send(msg); } catch (e) { }
    }
  }
}

function handleViewer(ws, roomId) {
  let room = rooms.get(roomId);
  if (!room) {
    room = new Room(roomId);
    rooms.set(roomId, room);
  }

  // 观看者人数上限检查
  if (room.viewers.size >= MAX_VIEWERS) {
    ws.close(1013, 'room_full');
    return;
  }

  room.viewers.add(ws);
  room.lastActivity = Date.now();

  console.log(`[+] Viewer connected: room=${roomId}, total_viewers=${room.viewers.size}`);

  // 立即发送最新帧
  if (room.lastFrame && ws.readyState === WebSocket.OPEN) {
    ws.send(room.lastFrame);
  }

  // 广播更新后的观看人数
  broadcastViewerCount(room);

  ws.on('close', () => {
    room.viewers.delete(ws);
    console.log(`[-] Viewer disconnected: room=${roomId}, remaining=${room.viewers.size}`);
    // 广播更新后的观看人数
    broadcastViewerCount(room);
  });

  ws.on('error', () => { });
}

// ==================== 启动服务器 ====================
const mainPort = tlsOptions ? HTTPS_PORT : HTTP_PORT;
server.listen(mainPort, '0.0.0.0', () => {
  const proto = tlsOptions ? 'https' : 'http';
  const wsProto = tlsOptions ? 'wss' : 'ws';
  console.log('============================================');
  console.log('  \u2601  Cloud Radar Server' + (tlsOptions ? ' (TLS \u{1F512})' : ' (NO TLS \u26A0)'));
  console.log(`  Domain: ${DOMAIN}`);
  console.log(`  Port: ${mainPort}`);
  console.log(`  Max viewers/room: ${MAX_VIEWERS}`);
  console.log(`  Push: ${wsProto}://${DOMAIN}:${mainPort}/push/<ROOM_ID>`);
  console.log(`  View: ${proto}://${DOMAIN}/<ROOM_ID>`);
  console.log('============================================');
});

// 启动 HTTP 跳转服务器
if (httpRedirectServer) {
  httpRedirectServer.listen(HTTP_PORT, '0.0.0.0', () => {
    console.log(`[*] HTTP → HTTPS 跳转服务器运行在端口 ${HTTP_PORT}`);
  });
}

// ==================== Graceful Shutdown ====================
function gracefulShutdown(signal) {
  console.log(`\n[*] ${signal} received, shutting down...`);

  // 关闭所有 WebSocket 连接
  for (const [id, room] of rooms) {
    if (room.pusher && room.pusher.readyState === WebSocket.OPEN) {
      room.pusher.close(1001, 'server_shutdown');
    }
    for (const v of room.viewers) {
      if (v.readyState === WebSocket.OPEN) {
        v.close(1001, 'server_shutdown');
      }
    }
  }
  rooms.clear();

  // 关闭 WebSocket 服务器
  wss.close(() => {
    // 关闭 HTTP 服务器
    server.close(() => {
      console.log('[*] Server closed cleanly.');
      process.exit(0);
    });
  });

  // 强制超时退出 (5秒)
  setTimeout(() => {
    console.log('[!] Forced exit after timeout.');
    process.exit(1);
  }, 5000);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
