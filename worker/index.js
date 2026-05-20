// =====================================================
// Snake.io — Cloudflare Workers + Durable Objects
// Each GameRoom DO is an independent game world
// =====================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // API: list rooms
    if (url.pathname === '/api/rooms') {
      if (request.method === 'POST') {
        const body = await request.json();
        const id = 'custom-' + Date.now();
        const name = (body.name || 'Custom Room').substring(0, 24);
        const mode = body.mode || 'solo';
        const teamSize = body.teamSize || 2;
        // Store room config in the DO
        const doId = env.GAME_ROOM.idFromName(id);
        const stub = env.GAME_ROOM.get(doId);
        await stub.fetch(new Request('http://internal/init', {
          method: 'POST',
          body: JSON.stringify({ id, name, mode, teamSize, isCustom: true }),
        }));
        return new Response(JSON.stringify({ id, name }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      // GET — list all rooms
      const rooms = [];
      const defaultRooms = [
        { id: 'room-0', name: 'Free For All', mode: 'solo' },
        { id: 'room-1', name: 'Neon Arena', mode: 'solo' },
        { id: 'room-2', name: 'Team Battle', mode: 'team', teamSize: 2 },
      ];
      for (const r of defaultRooms) {
        const doId = env.GAME_ROOM.idFromName(r.id);
        const stub = env.GAME_ROOM.get(doId);
        try {
          const resp = await stub.fetch(new Request('http://internal/info'));
          const info = await resp.json();
          rooms.push({ ...r, ...info });
        } catch (e) {
          rooms.push({ ...r, players: 0, maxPlayers: 30, teams: null, code: null, isCustom: false });
        }
      }
      return new Response(JSON.stringify(rooms), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Room code lookup
    if (url.pathname.startsWith('/api/rooms/code/')) {
      const code = url.pathname.split('/').pop().toUpperCase();
      // We'd need a KV store for code→room mapping. For now return not found.
      return new Response(JSON.stringify({ error: 'Not implemented' }), { status: 404, headers: corsHeaders });
    }

    // WebSocket upgrade — route to room DO
    if (request.headers.get('Upgrade') === 'websocket') {
      const roomId = url.searchParams.get('room') || 'room-0';
      const doId = env.GAME_ROOM.idFromName(roomId);
      const stub = env.GAME_ROOM.get(doId);
      return stub.fetch(request);
    }

    return new Response('Snake.io API', { headers: corsHeaders });
  },
};

// =====================================================
// GameRoom Durable Object — one per room
// =====================================================

const MAP_SIZE = 14000;
// 修改点 1：大幅提升光球的基础数量与上限数量
const FOOD_COUNT = 3000; // 基础光球数量从 1200 提升至 3000
const MAX_FOOD = 5000;   // 最大光球上限从 2000 提升至 5000
const SNAKE_SPEED = 280;
const BOOST_SPEED = 500;
const SEGMENT_SPACING = 24;
const DOT_RADIUS = 9;
const INITIAL_LENGTH = 10;
const HEAD_RADIUS = 14;
const BOOST_SHRINK_RATE = 2.5;
const MAX_BOTS = 15;
const MEGA_ORB_COUNT = 25; // 顺便把大光球数量也从 12 提升到 25 增加刺激感
const TICK_RATE = 30;
const TICK_MS = 1000 / TICK_RATE;
const BROADCAST_MS = 33;
const MAX_PLAYERS = 30;
const SKINS_COUNT = 43;

const BOT_NAMES = [
  'Viper','Shadow','Blaze','Neon','Ghost','Toxic','Pixel','Glitch',
  'Storm','Bolt','Ember','Frost','Nova','Pulse','Drift','Surge',
  'Zenith','Razor','Flux','Echo','Orbit','Prism','Hex','Chrome',
];

export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.snakes = new Map();
    this.food = [];
    this.megaOrbs = [];
    this.bots = [];
    this.nextId = 1;
    this.clients = new Map(); // ws → snakeId
    this.tickCount = 0;
    this.lastTick = Date.now();
    this.mode = 'solo';
    this.teamSize = 2;
    this.name = 'Room';
    this.isCustom = false;
    this.code = null;
    this.teams = new Map();
    this.initialized = false;
  }

  _init() {
    if (this.initialized) return;
    this.initialized = true;
    this._spawnFood();
    this._spawnMegaOrbs();
    this._spawnBots(MAX_BOTS);
    this.state.storage.setAlarm(Date.now() + TICK_MS);
  }

  async alarm() {
    this._tick();
    this._broadcastState();
    this._broadcastLeaderboard();
    if (this.clients.size > 0 || this.bots.length > 0) {
      this.state.storage.setAlarm(Date.now() + TICK_MS);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/init') {
      const body = await request.json();
      this.name = body.name || 'Room';
      this.mode = body.mode || 'solo';
      this.teamSize = body.teamSize || 2;
      this.isCustom = body.isCustom || false;
      this.code = this._genCode();
      if (this.mode === 'team') this._initTeams();
      this._init();
      return new Response('OK');
    }

    if (url.pathname === '/info') {
      return new Response(JSON.stringify({
        players: this.clients.size,
        maxPlayers: MAX_PLAYERS,
        isCustom: this.isCustom,
        code: this.code,
        teams: this.mode === 'team' ? Array.from(this.teams.entries()).map(([id, t]) => ({
          id, name: t.name, color: t.color, members: t.memberIds.size,
        })) : null,
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    if (request.headers.get('Upgrade') === 'websocket') {
      this._init();
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.state.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response('Not found', { status: 404 });
  }

  webSocketMessage(ws, data) {
    if (typeof data === 'string') return;
    const buf = new Uint8Array(data);
    if (buf.length < 1) return;
    const type = buf[0];

    if (type === 0x03) {
      const skinIdx = buf.length > 1 ? buf[1] : 0;
      const accessory = buf.length > 2 ? buf[2] : 0;
      let teamId = -1, nameStart = 3;
      if (this.mode === 'team' && buf.length > 3) { teamId = buf[3]; nameStart = 4; }
      const name = new TextDecoder().decode(buf.slice(nameStart)).substring(0, 16) || 'Player';
      this._playerJoin(ws, name, skinIdx, teamId, accessory);
      return;
    }

    const playerId = this.clients.get(ws);
    if (playerId === undefined) return;
    const snake = this.snakes.get(playerId);
    if (!snake || !snake.alive) return;

    if (type === 0x01 && buf.length >= 5) {
      const view = new DataView(data);
      snake.targetAngle = view.getFloat32(1, true);
    } else if (type === 0x02 && buf.length >= 2) {
      snake.boosting = buf[1] === 1;
    } else if (type === 0x07 && buf.length >= 2) {
      const emoteId = buf[1];
      const out = new Uint8Array(4);
      out[0] = 0x07;
      new DataView(out.buffer).setUint16(1, playerId, true);
      out[3] = emoteId;
      this._broadcast(out.buffer);
    }
  }

  webSocketClose(ws) {
    const playerId = this.clients.get(ws);
    if (playerId !== undefined) {
      this._killSnake(playerId, null, true);
      this.clients.delete(ws);
      this._adjustBots();
    }
  }

  webSocketError(ws) { this.webSocketClose(ws); }

  _zoned(power = 1.5) {
    const r = Math.pow(Math.random(), power) * (MAP_SIZE / 2 - 100);
    const a = Math.random() * Math.PI * 2;
    return { x: Math.cos(a) * r, y: Math.sin(a) * r };
  }
  _thickness(s) { return 1 + 0.6 * Math.log10(1 + s.score / 50); }
  _genCode() { const c = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'; let s = ''; for (let i = 0; i < 6; i++) s += c[Math.floor(Math.random() * 36)]; return s; }

  _initTeams() {
    const colors = ['#0ff', '#f44', '#0f0', '#ff0', '#f0f', '#f80', '#08f', '#8f0'];
    const names = ['Cyan', 'Red', 'Green', 'Gold', 'Pink', 'Orange', 'Blue', 'Lime'];
    const numTeams = Math.min(Math.floor(MAX_PLAYERS / this.teamSize), 8);
    for (let i = 0; i < numTeams; i++) {
      this.teams.set(i, { name: names[i], color: colors[i], memberIds: new Set() });
    }
  }

  get targetBotCount() { return Math.max(0, MAX_BOTS - this.clients.size); }

  _spawnFood() { while (this.food.length < FOOD_COUNT) this.food.push(this._createFood()); }
  _createFood() {
    const r = Math.random(); let radius, value, tier;
    if (r < 0.35) { radius = 3 + Math.random() * 2; value = 1; tier = 0; }
    else if (r < 0.58) { radius = 5 + Math.random() * 2; value = 2; tier = 1; }
    else if (r < 0.75) { radius = 7 + Math.random() * 2; value = 3; tier = 2; }
    else if (r < 0.87) { radius = 9 + Math.random() * 2; value = 5; tier = 3; }
    else if (r < 0.94) { radius = 11 + Math.random() * 2; value = 8; tier = 4; }
    else if (r < 0.975) { radius = 13 + Math.random() * 2; value = 12; tier = 5; }
    else if (r < 0.99) { radius = 15 + Math.random() * 2; value = 18; tier = 6; }
    else if (r < 0.997) { radius = 18 + Math.random() * 2; value = 26; tier = 7; }
    else { radius = 21 + Math.random() * 3; value = 35; tier = 8; }
    const pos = this._zoned(1.5);
    return { x: pos.x, y: pos.y, color: Math.floor(Math.random() * 8), radius, value, tier };
  }

  _spawnMegaOrbs() { while (this.megaOrbs.length < MEGA_ORB_COUNT) this.megaOrbs.push(this._createMegaOrb()); }
  _createMegaOrb() {
    const a = Math.random() * Math.PI * 2, speed = 25 + Math.random() * 25, pos = this._zoned(1.3);
    return { x: pos.x, y: pos.y, vx: Math.cos(a) * speed, vy: Math.sin(a) * speed, radius: 22 + Math.random() * 8, value: 50 + Math.floor(Math.random() * 31), color: Math.floor(Math.random() * 8), spin: Math.random() * Math.PI * 2 };
  }

  _spawnBots(count) {
    for (let i = 0; i < count; i++) {
      const snake = this._createSnake(BOT_NAMES[i % BOT_NAMES.length], true, Math.floor(Math.random() * SKINS_COUNT), 0);
      if (this.mode === 'team') { const tid = i % this.teams.size; snake.teamId = tid; this.teams.get(tid).memberIds.add(snake.id); }
      this.bots.push(snake.id);
    }
  }

  _createSnake(name, isBot, skinIdx, skill, accessory) {
    const id = this.nextId++;
    const angle = Math.random() * Math.PI * 2;
    const pos = isBot ? this._zoned(1.2) : this._zoned(2.5);
    const segments = [];
    for (let i = 0; i < INITIAL_LENGTH; i++) segments.push({ x: pos.x - Math.cos(angle) * i * SEGMENT_SPACING, y: pos.y - Math.sin(angle) * i * SEGMENT_SPACING });
    // 修改点 2：在生成蛇时，初始化 magnetTimer（磁铁倒计时）和原本的 invincible 属性（此处改为支持累加时间）
    const snake = { 
      id, name, segments, angle, targetAngle: angle, 
      boosting: false, score: 0, skin: skinIdx, accessory: accessory || 0, 
      color: Math.floor(Math.random() * 8), alive: true, isBot, skill: skill || 0, 
      boostAccum: 0, botTimer: 0, botWanderAngle: angle, teamId: -1, 
      invincible: 2, // 初始自带 2 秒无敌保护
      magnetTimer: 0, // 初始磁铁时间 0 秒
      kills: 0 
    };
    this.snakes.set(id, snake);
    return snake;
  }

  _playerJoin(ws, name, skinIdx, teamId, accessory) {
    if (this.clients.has(ws) || this.clients.size >= MAX_PLAYERS) return;
    const snake = this._createSnake(name, false, skinIdx, 0, accessory);
    if (this.mode === 'team' && this.teams.has(teamId)) { snake.teamId = teamId; this.teams.get(teamId).memberIds.add(snake.id); }
    this.clients.set(ws, snake.id);
    const welcome = new Uint8Array(4);
    welcome[0] = 0x02; welcome[1] = 1;
    new DataView(welcome.buffer).setUint16(2, snake.id, true);
    ws.send(welcome.buffer);
    this._adjustBots();
  }

  _adjustBots() {
    const target = this.targetBotCount;
    while (this.bots.length > target) {
      const botId = this.bots.pop();
      const bot = this.snakes.get(botId);
      if (bot) { if (bot.teamId >= 0) { const t = this.teams.get(bot.teamId); if (t) t.memberIds.delete(botId); } bot.alive = false; this.snakes.delete(botId); }
    }
    while (this.bots.length < target) {
      const snake = this._createSnake(BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)], true, Math.floor(Math.random() * SKINS_COUNT), Math.random() < 0.05 ? 2 : Math.random() < 0.3 ? 1 : 0);
      if (this.mode === 'team') { let minT = 0, minS = Infinity; for (const [id, t] of this.teams) { if (t.memberIds.size < minS) { minS = t.memberIds.size; mt = id; } } snake.teamId = minT; this.teams.get(minT).memberIds.add(snake.id); }
      this.bots.push(snake.id);
    }
  }

  _killSnake(id, killerId, noRespawn = false) {
    const snake = this.snakes.get(id);
    if (!snake || !snake.alive) return;
    snake.alive = false;
    const dropStep = Math.max(3, Math.floor(snake.segments.length / 10));
    let count = 0;
    for (let i = 0; i < snake.segments.length && this.food.length < MAX_FOOD && count < 10; i += dropStep) {
      const s = snake.segments[i];
      this.food.push({ x: s.x + (Math.random() - 0.5) * 30, y: s.y + (Math.random() - 0.5) * 30, color: snake.color, radius: 8 + Math.min(snake.score / 50, 8) + Math.random() * 3, value: Math.max(2, Math.floor(snake.score / 20)) + Math.floor(Math.random() * 3), tier: 3 });
      count++;
    }
    if (killerId !== null) {
      const killer = this.snakes.get(killerId);
      if (killer) killer.kills++;
      const buf = new Uint8Array(5);
      buf[0] = 0x04;
      const v = new DataView(buf.buffer);
      v.setUint16(1, killerId, true); v.setUint16(3, id, true);
      this._broadcast(buf.buffer);
    }
    for (const [ws, pid] of this.clients) {
      if (pid === id) { const d = new Uint8Array(3); d[0] = 0x03; new DataView(d.buffer).setUint16(1, id, true); ws.send(d.buffer); this.clients.delete(ws); break; }
    }
    if (snake.teamId >= 0) { const t = this.teams.get(snake.teamId); if (t) t.memberIds.delete(id); }
    if (snake.isBot && !noRespawn) { const idx = this.bots.indexOf(id); if (idx >= 0) { if (this.bots.length > this.targetBotCount) this.bots.splice(idx, 1); else { const ns = this._createSnake(BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)], true, Math.floor(Math.random() * SKINS_COUNT), 0); if (this.mode === 'team') { let mt = 0, ms = Infinity; for (const [tid, t] of this.teams) { if (t.memberIds.size < ms) { ms = t.memberIds.size; mt = tid; } } ns.teamId = mt; this.teams.get(mt).memberIds.add(ns.id); } this.bots[idx] = ns.id; } } }
    this.snakes.delete(id);
  }

  _tick() {
    const now = Date.now();
    const dt = Math.min((now - this.lastTick) / 1000, 0.05);
    this.lastTick = now;
    this.tickCount++;

    const half = MAP_SIZE / 2 - 50;
    for (const m of this.megaOrbs) { m.x += m.vx * dt; m.y += m.vy * dt; m.spin += dt * 1.5; if (m.x < -half) { m.x = -half; m.vx = Math.abs(m.vx); } if (m.x > half) { m.x = half; m.vx = -Math.abs(m.vx); } if (m.y < -half) { m.y = -half; m.vy = Math.abs(m.vy); } if (m.y > half) { m.y = half; m.vy = -Math.abs(m.vy); } }
    for (const [, s] of this.snakes) { if (s.isBot && s.alive) this._botAI(s, dt); }

    // 修改点 3：由于磁铁会改变光球的（x, y）坐标，我们先执行 _updateSnake 更新磁铁拉扯，再重新构建网格或直接判定
    // 为了简单且高性能，我们在 _updateSnake 内部处理磁铁对全局 food 的位置拉扯
    for (const [, s] of this.snakes) { if (s.alive) this._updateSnake(s, dt); }

    this._checkCollisions();
    this._spawnFood(); this._spawnMegaOrbs();
    while (this.food.length > MAX_FOOD) this.food.shift();
  }

  _updateSnake(snake, dt) {
    // 衰减无敌和磁铁时间
    if (snake.invincible > 0) snake.invincible = Math.max(0, snake.invincible - dt);
    if (snake.magnetTimer > 0) snake.magnetTimer = Math.max(0, snake.magnetTimer - dt);

    let ad = snake.targetAngle - snake.angle;
    while (ad > Math.PI) ad -= Math.PI * 2; while (ad < -Math.PI) ad += Math.PI * 2;
    if (Math.abs(ad) < 9 * dt) snake.angle = snake.targetAngle; else snake.angle += Math.sign(ad) * 9 * dt;
    if (snake.boosting && snake.score <= 0) snake.boosting = false;
    
    const speed = snake.boosting ? BOOST_SPEED : SNAKE_SPEED;
    const head = snake.segments[0];
    head.x += Math.cos(snake.angle) * speed * dt; head.y += Math.sin(snake.angle) * speed * dt;
    
    const h = MAP_SIZE / 2;
    if (head.x < -h || head.x > h || head.y < -h || head.y > h) { this._killSnake(snake.id, null); return; }
    
    while (snake.segments.length >= 2) {
      const dx = head.x - snake.segments[1].x, dy = head.y - snake.segments[1].y;
      if (dx * dx + dy * dy < SEGMENT_SPACING ** 2) break;
      const dist = Math.sqrt(dx * dx + dy * dy), t = SEGMENT_SPACING / dist;
      snake.segments.splice(1, 0, { x: snake.segments[1].x + dx * t, y: snake.segments[1].y + dy * t });
    }
    const tl = INITIAL_LENGTH + Math.floor(8 * Math.log(1 + snake.score / 10));
    while (snake.segments.length > tl) snake.segments.pop();
    
    if (snake.boosting && snake.score > 0) {
      snake.boostAccum += BOOST_SHRINK_RATE * dt;
      if (snake.boostAccum >= 1) { 
        const rm = Math.floor(snake.boostAccum); 
        snake.boostAccum -= rm; 
        snake.score = Math.max(0, snake.score - rm); 
        if (snake.segments.length > 0) { 
          const tail = snake.segments[snake.segments.length - 1]; 
          this.food.push({ x: tail.x + (Math.random() - 0.5) * 14, y: tail.y + (Math.random() - 0.5) * 14, color: snake.color, radius: 5 + Math.random() * 2, value: 1, tier: 0 }); 
        } 
      }
    }

    // 计算吞噬半径
    const headR = HEAD_RADIUS * this._thickness(snake);
    
    // 修改点 4：根据是否有磁铁功能，动态调整“磁铁吸附范围”
    // 如果有磁铁道具状态，吸附半径扩大到 450 像素（可根据需要微调）
    const magnetRange = snake.magnetTimer > 0 ? 450 : 0; 
    const eatR = headR + 30;

    // 为了兼容磁铁的实时拉扯，我们遍历食物列表进行距离判定和磁力位移
    for (let i = this.food.length - 1; i >= 0; i--) {
      const f = this.food[i];
      if (!f) continue;
      const dx = f.x - head.x;
      const dy = f.y - head.y;
      const distSq = dx * dx + dy * dy;

      // 1. 磁铁拉扯逻辑：如果在吸附范围内，光球向蛇头加速飞过去
      if (magnetRange > 0 && distSq < magnetRange * magnetRange) {
        const dist = Math.sqrt(distSq);
        if (dist > 5) {
          // 飞向蛇头的速度（离蛇头越近飞得越快）
          const pullSpeed = 800 * (1 - dist / magnetRange) + speed; 
          f.x -= (dx / dist) * pullSpeed * dt;
          f.y -= (dy / dist) * pullSpeed * dt;
        }
      }

      // 2. 正常吞噬逻辑判定（基于更新后的位置）
      const newDx = f.x - head.x;
      const newDy = f.y - head.y;
      if (newDx * newDx + newDy * newDy < (headR + f.radius) ** 2) {
        this.food.splice(i, 1);
        snake.score += f.value || 1;

        // 修改点 5：吃到任何光球，磁铁和无敌倒计时增加 15 秒（可累加）
        snake.magnetTimer += 15;
        snake.invincible += 15;
      }
    }

    // 吞噬大光球（同样享受磁铁和无敌叠加效果）
    for (let i = this.megaOrbs.length - 1; i >= 0; i--) {
      const m = this.megaOrbs[i];
      const dx = head.x - m.x;
      const dy = head.y - m.y;
      const distSq = dx * dx + dy * dy;

      if (magnetRange > 0 && distSq < magnetRange * magnetRange) {
        const dist = Math.sqrt(distSq);
        if (dist > 5) {
          const pullSpeed = 600 * (1 - dist / magnetRange) + speed;
          m.x += (dx / dist) * pullSpeed * dt;
          m.y += (dy / dist) * pullSpeed * dt;
        }
      }

      const newDx = head.x - m.x;
      const newDy = head.y - m.y;
      if (newDx * newDx + newDy * newDy < (headR + m.radius) ** 2) {
        this.megaOrbs.splice(i, 1);
        snake.score += m.value;
        
        // 吃到大光球同样累加 15 秒
        snake.magnetTimer += 15;
        snake.invincible += 15;
      }
    }
  }

  _checkCollisions() {
    const arr = Array.from(this.snakes.values()).filter(s => s.alive);
    for (let i = 0; i < arr.length; i++) {
      const a = arr[i]; 
      // 修改点 6：由于 invincible 现在会长时间累加，这里只要大于 0 就属于绝对无敌，不会触发死亡碰撞
      if (!a.alive || a.invincible > 0) continue; 
      const ahead = a.segments[0], aHeadR = HEAD_RADIUS * this._thickness(a) * 0.75;
      for (let j = 0; j < arr.length; j++) {
        if (i === j) continue;
        const b = arr[j]; if (!b.alive) continue; // 别人有没有无敌无所谓，只要“我”没有无敌，我撞上别人就会死
        if (this.mode === 'team' && a.teamId >= 0 && a.teamId === b.teamId) continue;
        const bDotR = DOT_RADIUS * this._thickness(b) * 0.75;
        const dist = aHeadR + bDotR, distSq = dist * dist;
        for (let k = 1; k < b.segments.length; k++) {
          const seg = b.segments[k], dx = ahead.x - seg.x, dy = ahead.y - seg.y;
          if (dx * dx + dy * dy < distSq) { this._killSnake(a.id, b.id); b.score += Math.floor(a.segments.length / 2 + a.score / 4); break; }
        }
        if (!a.alive) break;
      }
    }
  }

  _botAI(s, dt) {
    s.botTimer -= dt; if (s.botTimer > 0) return;
    s.botTimer = s.skill === 2 ? 0.08 : s.skill === 1 ? 0.3 + Math.random() * 0.4 : 0.5 + Math.random() * 0.6;
    const head = s.segments[0], wall = MAP_SIZE / 2 - 250;
    if (Math.abs(head.x) > wall || Math.abs(head.y) > wall) { s.targetAngle = Math.atan2(-head.y, -head.x); s.boosting = s.skill > 0; return; }
    
    // 如果 Bot 处于无敌状态，它就不需要刻意避开别的蛇身体了，可以直接横冲直撞
    if (s.invincible <= 0) {
      for (const [, o] of this.snakes) {
        if (o.id === s.id || !o.alive) continue;
        for (let k = 0; k < Math.min(o.segments.length, 20); k += 2) {
          const dx = o.segments[k].x - head.x, dy = o.segments[k].y - head.y, d = Math.sqrt(dx * dx + dy * dy);
          if (d < 100) { const aTo = Math.atan2(dy, dx); let diff = aTo - s.angle; while (diff > Math.PI) diff -= Math.PI * 2; while (diff < -Math.PI) diff += Math.PI * 2; if (Math.abs(diff) < Math.PI / 2) { s.targetAngle = s.angle + (diff > 0 ? -1 : 1) * Math.PI / 2; s.boosting = false; return; } }
        }
      }
    }

    const range = s.skill === 2 ? 600 : s.skill === 1 ? 450 : 300;
    let cl = null, cSq = range * range;
    for (const f of this.food) { const dx = f.x - head.x; if (dx > range || dx < -range) continue; const dy = f.y - head.y; if (dy > range || dy < -range) continue; const d2 = dx * dx + dy * dy; if (d2 < cSq) { cSq = d2; cl = f; } }
    if (cl) { s.targetAngle = Math.atan2(cl.y - head.y, cl.x - head.x); s.boosting = false; }
    else { s.botWanderAngle += (Math.random() - 0.5) * 1.5; s.targetAngle = s.botWanderAngle; s.boosting = false; }
  }

  _broadcastState() {
    for (const [ws, playerId] of this.clients) {
      const mySnake = this.snakes.get(playerId);
      if (!mySnake || !mySnake.alive) continue;
      const cx = mySnake.segments[0].x, cy = mySnake.segments[0].y, viewRange = 1800;
      const visSnakes = [], visFood = [], visMega = [];
      for (const [, snake] of this.snakes) {
        if (!snake.alive) continue;
        for (let i = 0; i < snake.segments.length; i += 3) { if (Math.abs(snake.segments[i].x - cx) < viewRange && Math.abs(snake.segments[i].y - cy) < viewRange) { visSnakes.push(snake); break; } }
      }
      for (const f of this.food) { if (Math.abs(f.x - cx) < viewRange && Math.abs(f.y - cy) < viewRange) visFood.push(f); }
      for (const m of this.megaOrbs) { if (Math.abs(m.x - cx) < viewRange && Math.abs(m.y - cy) < viewRange) visMega.push(m); }

      let totalSegs = 0, totalNameBytes = 0;
      for (const s of visSnakes) { totalSegs += s.segments.length; totalNameBytes += new TextEncoder().encode(s.name).length; }
      const bufSize = 1 + 2 + visSnakes.length * (2 + 1 + 1 + 1 + 1 + 1 + 1 + 2 + 1 + 2) + totalNameBytes + totalSegs * 4 + 2 + visFood.length * 7 + 2 + visMega.length * 7;
      const buf = new ArrayBuffer(bufSize);
      const u8 = new Uint8Array(buf);
      const dv = new DataView(buf);
      let off = 0;
      u8[off++] = 0x01;
      dv.setUint16(off, visSnakes.length, true); off += 2;
      for (const snake of visSnakes) {
        dv.setUint16(off, snake.id, true); off += 2;
        u8[off++] = snake.skin; u8[off++] = snake.boosting ? 1 : 0; u8[off++] = snake.isBot ? 1 : 0;
        dv.setInt8(off, snake.teamId); off += 1;
        // 这里的二进制协议里有 invincible 属性下发，前端会自动接收到无敌状态渲染对应的特效
        u8[off++] = snake.invincible > 0 ? 1 : 0;
        u8[off++] = snake.accessory || 0;
        dv.setUint16(off, Math.min(snake.score, 65535), true); off += 2;
        const nameBytes = new TextEncoder().encode(snake.name);
        u8[off++] = nameBytes.length;
        u8.set(nameBytes, off); off += nameBytes.length;
        dv.setUint16(off, snake.segments.length, true); off += 2;
        for (const seg of snake.segments) { dv.setInt16(off, Math.round(seg.x), true); off += 2; dv.setInt16(off, Math.round(seg.y), true); off += 2; }
      }
      dv.setUint16(off, visFood.length, true); off += 2;
      for (const f of visFood) { dv.setInt16(off, Math.round(f.x), true); off += 2; dv.setInt16(off, Math.round(f.y), true); off += 2; u8[off++] = f.color; u8[off++] = Math.round(f.radius); u8[off++] = f.tier; }
      dv.setUint16(off, visMega.length, true); off += 2;
      for (const m of visMega) { dv.setInt16(off, Math.round(m.x), true); off += 2; dv.setInt16(off, Math.round(m.y), true); off += 2; u8[off++] = m.color; u8[off++] = Math.round(m.radius); u8[off++] = Math.min(m.value, 255); }
      try { ws.send(buf); } catch (e) { this.webSocketClose(ws); }
    }
  }

  _broadcastLeaderboard() {
    const sorted = Array.from(this.snakes.values()).filter(s => s.alive).sort((a, b) => b.score - a.score).slice(0, 10);
    let size = 2;
    for (const s of sorted) size += 7 + new TextEncoder().encode(s.name).length;
    const buf = new ArrayBuffer(size);
    const u8 = new Uint8Array(buf);
    const dv = new DataView(buf);
    let off = 0;
    u8[off++] = 0x05; u8[off++] = sorted.length;
    for (const s of sorted) {
      dv.setUint16(off, s.id, true); off += 2;
      dv.setUint16(off, Math.min(s.score, 65535), true); off += 2;
      u8[off++] = s.isBot ? 1 : 0;
      u8[off++] = Math.min(s.kills || 0, 255);
      dv.setInt8(off, s.teamId); off += 1;
      const nb = new TextEncoder().encode(s.name);
      u8[off++] = nb.length; u8.set(nb, off); off += nb.length;
    }
    this._broadcast(buf);
  }

  _broadcast(data) {
    for (const [ws] of this.clients) {
      try { ws.send(data); } catch (e) { this.webSocketClose(ws); }
    }
  }
}
