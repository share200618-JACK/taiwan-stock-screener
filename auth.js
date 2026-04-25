// ══════════════════════════════════════════════════════
// 全站共用認證模組 auth.js
// ══════════════════════════════════════════════════════
const TOKEN_KEY    = 'tw_user_token';
const USERNAME_KEY = 'tw_username';
const _API_BASE    = location.hostname === 'localhost' ? 'http://localhost:5000' : '';

function getToken()    { return localStorage.getItem(TOKEN_KEY)    || ''; }
function getUsername() { return localStorage.getItem(USERNAME_KEY) || ''; }

function authLogout() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USERNAME_KEY);
  location.href = '/login?redirect=' + encodeURIComponent(location.pathname);
}

// 每個頁面呼叫此函式，若未登入則跳轉
async function requireAuth() {
  const token    = getToken();
  const username = getUsername();
  if (!token || !username) {
    location.href = '/login?redirect=' + encodeURIComponent(location.pathname);
    return null;
  }
  // 快速驗證（30秒內不重複驗證）
  const lastVerify = parseInt(sessionStorage.getItem('last_verify') || '0');
  if (Date.now() - lastVerify < 30000) {
    return {token, username};
  }
  try {
    const r = await fetch(`${_API_BASE}/api/auth/verify`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({token})
    });
    const d = await r.json();
    if (!d.valid) {
      authLogout();
      return null;
    }
    sessionStorage.setItem('last_verify', Date.now().toString());
    return {token, username};
  } catch(e) {
    // 網路錯誤時允許繼續使用（離線容錯）
    return {token, username};
  }
}

// 顯示用戶狀態列（呼叫 requireAuth 後使用）
function renderAuthBar(containerId) {
  const el       = document.getElementById(containerId);
  const username = getUsername();
  if (!el || !username) return;
  el.innerHTML = `
    <div style="display:flex;align-items:center;gap:8px;background:#161b25;border-radius:8px;padding:6px 12px;font-size:.78rem;">
      <div style="width:26px;height:26px;background:linear-gradient(135deg,#a855f7,#7c3aed);border-radius:50%;
                  display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;flex-shrink:0;">
        ${username.charAt(0).toUpperCase()}
      </div>
      <span style="color:#a855f7;font-weight:600;">${username}</span>
      <button onclick="authLogout()" style="margin-left:auto;padding:3px 8px;border:1px solid #252a38;
              border-radius:5px;background:none;color:#6a748f;font-size:.7rem;cursor:pointer;">登出</button>
    </div>`;
}
