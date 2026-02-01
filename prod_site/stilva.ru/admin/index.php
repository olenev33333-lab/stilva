<?php
// Безопасные куки для сессии (не ломаемся на HTTP)
$isHttps = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off')
  || (isset($_SERVER['HTTP_X_FORWARDED_PROTO']) && $_SERVER['HTTP_X_FORWARDED_PROTO'] === 'https');

session_set_cookie_params([
  'lifetime' => 0,
  'path'     => '/admin',
  'secure'   => $isHttps,
  'httponly' => true,
  'samesite' => 'Strict',
]);
session_start();

// Логин/пароль (потом заведёшь нормальные)
const ADMIN_USER = 'admin';
const ADMIN_PASS = 'admin123';

// Выход
if (isset($_GET['logout'])) {
  $_SESSION = [];
  if (ini_get('session.use_cookies')) {
    $params = session_get_cookie_params();
    setcookie(session_name(), '', time() - 42000, $params['path'], $params['domain'] ?? '', $params['secure'], $params['httponly']);
  }
  session_destroy();
  header('Location: /admin/?bye');
  exit;
}

// Уже вошли — отдаём админку
if (!empty($_SESSION['ok'])) {
  // Анти-кэш, чтобы после выхода «Назад» не показывал старую страницу
  header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
  header('Pragma: no-cache');
  header('X-Frame-Options: SAMEORIGIN');

  $app = __DIR__ . '/app.html';
  if (!is_file($app)) {
    http_response_code(500);
    echo 'Не найден /admin/app.html';
    exit;
  }
  readfile($app);
  exit;
}

// Пытаемся войти
$err = false;
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
  $u = trim($_POST['u'] ?? '');
  $p = (string)($_POST['p'] ?? '');
  if ($u === ADMIN_USER && $p === ADMIN_PASS) {
    session_regenerate_id(true);
    $_SESSION['ok'] = true;
    header('Location: /admin/');
    exit;
  }
  $err = true;
}

// Анти-кэш для страницы логина
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('Pragma: no-cache');
?>
<!doctype html>
<html lang="ru"><meta charset="utf-8"><title>Вход • Admin</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{color-scheme:dark light}
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0b1020;color:#e6e9f2;margin:0;display:grid;place-items:center;min-height:100svh}
  .card{background:#12172a;border:1px solid #1f2744;border-radius:16px;padding:24px;max-width:320px;width:100%;box-shadow:0 10px 30px rgba(0,0,0,.25)}
  .h{margin:0 0 12px;font-size:18px}
  .inp{width:90%;margin:8px 0 12px;padding:10px 12px;border-radius:10px;border:1px solid #2a355a;background:#0d1326;color:#e6e9f2}
  .btn{width:100%;padding:10px 12px;border-radius:10px;border:0;background:#4f46e5;color:#fff;font-weight:700;cursor:pointer}
  .note{margin-top:10px;font-size:12px;color:#93a1c8}
  .err{color:#f87171;margin:0 0 10px}
</style>
<div class="card">
  <h1 class="h">Вход в админ-панель</h1>
  <?php if ($err): ?><p class="err">Неверный логин или пароль</p><?php endif; ?>
  <form method="post" autocomplete="off">
    <input class="inp" type="text" name="u" placeholder="Логин" required>
    <input class="inp" type="password" name="p" placeholder="Пароль" required>
    <button class="btn" type="submit">Войти</button>
  </form>
</div>
