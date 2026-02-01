<?php
// public/api/config.php
// 1) copy to config.php and set env vars, or just fill constants here.
// By default reads from environment variables.
const DB_HOST = '127.0.0.1';   // если учётка создана как 'user'@'localhost'
const DB_NAME = 'u3294621_user_stilva';
const DB_USER = 'u3294621_user_stilva';
const DB_PASS = 'user_stilva123';

function env($k, $def=null){ $v = getenv($k); return $v === false ? $def : $v; }
$GLOBALS['__DB_CFG'] = [
  'host' => defined('DB_HOST') && DB_HOST ? DB_HOST : env('DB_HOST','127.0.0.1'),
  'name' => defined('DB_NAME') && DB_NAME ? DB_NAME : env('DB_NAME','stilva'),
  'user' => defined('DB_USER') && DB_USER ? DB_USER : env('DB_USER','root'),
  'pass' => defined('DB_PASS') && DB_PASS ? DB_PASS : env('DB_PASS',''),
];
