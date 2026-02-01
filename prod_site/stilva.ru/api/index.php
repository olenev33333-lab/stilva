<?php
// public/api/index.php
declare(strict_types=1);
header('Content-Type: application/json; charset=utf-8');

require __DIR__ . '/config.php';

function db(): PDO {
  static $pdo = null;
  if ($pdo) return $pdo;
  $cfg = $GLOBALS['__DB_CFG'];
  $dsn = "mysql:host={$cfg['host']};dbname={$cfg['name']};charset=utf8mb4";
  $pdo = new PDO($dsn, $cfg['user'], $cfg['pass'], [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
  ]);
  $pdo->exec("SET time_zone = '+00:00'");
  return $pdo;
}

function read_json(){
  $raw = file_get_contents('php://input');
  if ($raw === false || $raw === '') return [];
  $data = json_decode($raw, true);
  return is_array($data) ? $data : [];
}

function out($code, $data=null){
  http_response_code($code);
  if ($data === null) $data = ['ok'=>($code>=200 && $code<300)];
  echo json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  exit;
}

$uri = strtok($_SERVER['REQUEST_URI'], '?');
$method = $_SERVER['REQUEST_METHOD'];
$base = '/api';
$path = substr($uri, strlen($base));
$path = $path === false ? '/' : ($path === '' ? '/' : $path);
$segments = array_values(array_filter(explode('/', $path)));
$pdo = null;

try {

  // Health
  if ($path === '/' || ($segments[0] ?? '') === 'health'){
    out(200, ['ok'=>true, 'ts'=>gmdate('c')]);
  }

  if (($segments[0] ?? '') === 'products'){
    $pdo = db();

    if ($method === 'GET' && count($segments) === 1){
      $published = isset($_GET['published']) ? $_GET['published'] : null;
      if ($published !== null && ($published === 'true' || $published === '1')){
        $stmt = $pdo->query("SELECT * FROM products WHERE published = 1 ORDER BY id ASC");
      } else {
        $stmt = $pdo->query("SELECT * FROM products ORDER BY id ASC");
      }
      $rows = $stmt->fetchAll();
      out(200, $rows);
    }

    if ($method === 'POST' && count($segments) === 1){
      $b = read_json();
      if (!isset($b['name']) || trim((string)$b['name']) === '') out(400, ['error'=>'name']);
      $stmt = $pdo->prepare("INSERT INTO products
        (name, price, published, image_url, shelves, material, construction, perforation, shelf_thickness, description, stock_qty, lead_time_days)
        VALUES (:name, :price, :published, :image_url, :shelves, :material, :construction, :perforation, :shelf_thickness, :description, :stock_qty, :lead_time_days)");
      $stmt->execute([
        ':name' => (string)$b['name'],
        ':price' => (float)($b['price'] ?? 0),
        ':published' => !empty($b['published']) ? 1 : 0,
        ':image_url' => (string)($b['image_url'] ?? ''),
        ':shelves' => (int)($b['shelves'] ?? 0),
        ':material' => (string)($b['material'] ?? ''),
        ':construction' => (string)($b['construction'] ?? ''),
        ':perforation' => (string)($b['perforation'] ?? ''),
        ':shelf_thickness' => (string)($b['shelf_thickness'] ?? ''),
        ':description' => (string)($b['description'] ?? ''),
        ':stock_qty' => (int)($b['stock_qty'] ?? 0),
        ':lead_time_days' => (int)($b['lead_time_days'] ?? 0),
      ]);
      $id = (int)$pdo->lastInsertId();
      out(200, ['id'=>$id]);
    }

    if (count($segments) === 2){
      $id = (int)$segments[1];

      if ($method === 'PUT' || $method === 'PATCH'){
        $b = read_json();
        $fields = ['name','price','published','image_url','shelves','material','construction','perforation','shelf_thickness','description','stock_qty','lead_time_days'];
        $set = [];
        $args = [':id'=>$id];
        foreach($fields as $f){
          if (array_key_exists($f, $b)){
            $set[] = "$f = :$f";
            $args[":$f"] = in_array($f, ['price']) ? (float)$b[$f]
              : (in_array($f, ['shelves','stock_qty','lead_time_days']) ? (int)$b[$f]
              : ($f==='published' ? (!empty($b[$f]) ? 1 : 0) : (string)$b[$f]));
          }
        }
        if (!$set) out(400, ['error'=>'no_fields']);
        $sql = "UPDATE products SET ".implode(',', $set)." WHERE id = :id";
        $stmt = $pdo->prepare($sql);
        $stmt->execute($args);
        out(200, ['ok'=>true]);
      }

      if ($method === 'DELETE'){
        $stmt = $pdo->prepare("DELETE FROM products WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        out(200, ['ok'=>true]);
      }
    }
    out(404, ['error'=>'nf']);
  }

  if (($segments[0] ?? '') === 'orders'){
    $pdo = db();

    if ($method === 'GET' && count($segments) === 1){
      $status = $_GET['status'] ?? null;
      if ($status){
        $stmt = $pdo->prepare("SELECT * FROM orders WHERE status = :st ORDER BY id DESC");
        $stmt->execute([':st'=>$status]);
      } else {
        $stmt = $pdo->query("SELECT * FROM orders ORDER BY id DESC");
      }
      $rows = $stmt->fetchAll();
      out(200, $rows);
    }

    if ($method === 'POST' && count($segments) === 1){
      $b = read_json();
      $name = trim((string)($b['customer_name'] ?? ''));
      $items = $b['items'] ?? [];
      if ($name === '' || !is_array($items) || count($items) === 0){
        out(400, ['error'=>'bad']);
      }
      $pdo->beginTransaction();
      try{
        $stmt = $pdo->prepare("INSERT INTO orders (customer_name, phone, email, note, total, status) VALUES (:n,:p,:e,:note,:tot,'Новый')");
        $stmt->execute([
          ':n'=>$name,
          ':p'=> (string)($b['phone'] ?? ''),
          ':e'=> (string)($b['email'] ?? ''),
          ':note'=> (string)($b['note'] ?? ''),
          ':tot'=> (float)($b['total'] ?? 0),
        ]);
        $oid = (int)$pdo->lastInsertId();

        $ins = $pdo->prepare("INSERT INTO order_items (order_id, product_id, name, price, qty) VALUES (:oid,:pid,:name,:price,:qty)");
        foreach ($items as $it){
          $pid = isset($it['id']) && is_numeric($it['id']) ? (int)$it['id'] : null;
          $ins->execute([
            ':oid'=>$oid,
            ':pid'=>$pid,
            ':name'=>(string)($it['name'] ?? 'Товар'),
            ':price'=>(float)($it['price'] ?? 0),
            ':qty'=>(int)($it['qty'] ?? 1),
          ]);
        }
        $pdo->commit();
        out(200, ['id'=>$oid]);
      }catch(Throwable $e){
        $pdo->rollBack();
        out(500, ['error'=>'fail']);
      }
    }

    if (count($segments) === 2){
      $id = (int)$segments[1];

      if ($method === 'GET'){
        $stmt = $pdo->prepare("SELECT * FROM orders WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        $o = $stmt->fetch();
        if (!$o) out(404, ['error'=>'nf']);
        $it = $pdo->prepare("SELECT id, product_id, name, price, qty FROM order_items WHERE order_id = :id ORDER BY id ASC");
        $it->execute([':id'=>$id]);
        $o['items'] = $it->fetchAll();
        out(200, $o);
      }

      if ($method === 'PATCH'){
        $b = read_json();
        $fields = ['note','cancel_reason','customer_name','phone','email','total','status'];
        $set = [];
        $args = [':id'=>$id];
        foreach($fields as $f){
          if (array_key_exists($f, $b)){
            $set[] = "$f = :$f";
            $args[":$f"] = in_array($f, ['total']) ? (float)$b[$f] : (string)$b[$f];
          }
        }
        if (!$set) out(400, ['error'=>'no_fields']);
        $sql = "UPDATE orders SET ".implode(',', $set)." WHERE id = :id";
        $stmt = $pdo->prepare($sql);
        $stmt->execute($args);
        out(200, ['ok'=>true]);
      }

      if ($method === 'DELETE'){
        $stmt = $pdo->prepare("DELETE FROM orders WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        out(200, ['ok'=>true]);
      }
    }

    if (count($segments) === 3 && $segments[2] === 'status' && $method === 'PATCH'){
      $id = (int)$segments[1];
      $b = read_json();
      $st = $b['status'] ?? null;
      if (!$st) out(400, ['error'=>'no_status']);
      $stmt = $pdo->prepare("UPDATE orders SET status = :s WHERE id = :id");
      $stmt->execute([':s'=>$st, ':id'=>$id]);
      out(200, ['ok'=>true]);
    }

    out(404, ['error'=>'nf']);
  }

  if (($segments[0] ?? '') === 'stats'){
    $pdo = db();

    if (count($segments) === 2 && $segments[1] === 'sales' && $method === 'GET'){
      $from = $_GET['from'] ?? null;
      $to   = $_GET['to'] ?? null;
      $status = $_GET['status'] ?? null;
      $productId = $_GET['productId'] ?? null;

      // Build conditions
      $conds = [];
      $args = [];
      if ($from){ $conds[] = "o.created_at >= :from"; $args[':from'] = $from.' 00:00:00'; }
      if ($to){   $conds[] = "o.created_at <= :to";   $args[':to']   = $to.' 23:59:59'; }
      if ($status){ $conds[] = "o.status = :st"; $args[':st'] = $status; }
      $where = $conds ? ('WHERE '.implode(' AND ', $conds)) : '';

      if ($productId){
        $sql = "SELECT DATE(o.created_at) as d, SUM(oi.price*oi.qty) as sum
                FROM orders o JOIN order_items oi ON oi.order_id = o.id
                $where AND (oi.product_id = :pid OR oi.name = :pname)
                GROUP BY d ORDER BY d ASC";
        $args[':pid'] = (int)$productId;
        $args[':pname'] = (string)$productId;
      } else {
        $sql = "SELECT DATE(o.created_at) as d, SUM(o.total) as sum FROM orders o $where GROUP BY d ORDER BY d ASC";
      }
      $stmt = $pdo->prepare($sql);
      $stmt->execute($args);
      $rows = $stmt->fetchAll();
      $total = 0.0;
      foreach ($rows as $r){ $total += (float)$r['sum']; }
      out(200, ['rows'=>array_map(function($r){ return ['date'=>$r['d'], 'sum'=>round((float)$r['sum'], 2)]; }, $rows), 'total'=>round($total,2)]);
    }

    if (count($segments) === 2 && $segments[1] === 'customers' && $method === 'GET'){
      $from = $_GET['from'] ?? null;
      $to   = $_GET['to'] ?? null;
      $min  = (int)($_GET['min_orders'] ?? 2);
      $conds = [];
      $args = [];
      if ($from){ $conds[] = "created_at >= :from"; $args[':from'] = $from.' 00:00:00'; }
      if ($to){   $conds[] = "created_at <= :to";   $args[':to']   = $to.' 23:59:59'; }
      $where = $conds ? ('WHERE '.implode(' AND ', $conds)) : '';
      $rows = db()->query("SELECT id, customer_name, phone, email, total, created_at FROM orders $where")->fetchAll();
      $map = [];
      foreach ($rows as $o){
        $key = trim(($o['phone'] ?? '').$o['email'].$o['customer_name']);
        if (!$key) continue;
        if (!isset($map[$key])) $map[$key] = ['key'=>$key, 'orders'=>0, 'total'=>0.0];
        $map[$key]['orders'] += 1;
        $map[$key]['total']  += (float)$o['total'];
      }
      $res = array_values(array_filter($map, fn($x)=>$x['orders'] >= $min));
      usort($res, fn($a,$b)=> $b['orders'] <=> $a['orders']);
      out(200, $res);
    }

    out(404, ['error'=>'nf']);
  }

  out(404, ['error'=>'nf']);
}
catch(Throwable $e){
  out(500, ['error'=>'server', 'detail'=>$e->getMessage()]);
}
