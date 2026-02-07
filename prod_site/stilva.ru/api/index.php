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

function cf_find_order_income(PDO $pdo, int $orderId){
  $stmt = $pdo->prepare("SELECT * FROM cashflow_entries WHERE order_id = :oid AND source = 'order' AND type = 'income' LIMIT 1");
  $stmt->execute([':oid'=>$orderId]);
  return $stmt->fetch();
}

function cf_bootstrap(PDO $pdo){
  $pdo->exec("CREATE TABLE IF NOT EXISTS cashflow_categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    type ENUM('income','expense') NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    UNIQUE KEY uniq_name_type (name, type)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  $pdo->exec("CREATE TABLE IF NOT EXISTS cashflow_entries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    type ENUM('income','expense') NOT NULL,
    amount DECIMAL(10,2) NOT NULL DEFAULT 0,
    category VARCHAR(120) NOT NULL,
    payment_method ENUM('cash','card','transfer','bank') NOT NULL DEFAULT 'bank',
    comment TEXT,
    order_id INT NULL,
    source ENUM('manual','order') NOT NULL DEFAULT 'manual',
    status ENUM('active','void') NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_date (date),
    KEY idx_type (type),
    KEY idx_status (status),
    KEY idx_order (order_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  $pdo->exec("INSERT IGNORE INTO cashflow_categories (name, type, active) VALUES
    ('Продажа','income',1),
    ('Доп. услуги','income',1),
    ('Прочее','income',1),
    ('Закупка','expense',1),
    ('Доставка','expense',1),
    ('Аренда','expense',1),
    ('Зарплата','expense',1),
    ('Реклама','expense',1),
    ('Налоги','expense',1),
    ('Хозяйственные','expense',1),
    ('Прочее','expense',1)");
}

function cf_upsert_order_income(PDO $pdo, int $orderId, float $amount){
  cf_bootstrap($pdo);
  $row = cf_find_order_income($pdo, $orderId);
  $today = date('Y-m-d');
  if ($row){
    $stmt = $pdo->prepare("UPDATE cashflow_entries SET amount = :amt, status = 'active', date = :d, updated_at = NOW() WHERE id = :id");
    $stmt->execute([':amt'=>$amount, ':d'=>$today, ':id'=>$row['id']]);
  } else {
    $stmt = $pdo->prepare("INSERT INTO cashflow_entries (date, type, amount, category, payment_method, comment, order_id, source, status)
      VALUES (:d, 'income', :amt, :cat, :pm, :cmt, :oid, 'order', 'active')");
    $stmt->execute([
      ':d'   => $today,
      ':amt' => $amount,
      ':cat' => 'Продажа',
      ':pm'  => 'bank',
      ':cmt' => 'Заказ #'.$orderId,
      ':oid' => $orderId,
    ]);
  }
}

function cf_void_order_income(PDO $pdo, int $orderId){
  cf_bootstrap($pdo);
  $row = cf_find_order_income($pdo, $orderId);
  if ($row){
    $stmt = $pdo->prepare("UPDATE cashflow_entries SET status = 'void', updated_at = NOW() WHERE id = :id");
    $stmt->execute([':id'=>$row['id']]);
  }
}

function cf_sync_order(PDO $pdo, int $orderId, string $prevStatus, string $newStatus, float $total){
  try {
    if ($newStatus === 'Выполнен'){
      cf_upsert_order_income($pdo, $orderId, $total);
    } elseif ($prevStatus === 'Выполнен' && $newStatus !== 'Выполнен') {
      cf_void_order_income($pdo, $orderId);
    }
  } catch (Throwable $e) {
    // intentionally ignore to avoid breaking order flow
  }
}

function stock_column_missing(PDO $pdo, string $table, string $column): bool {
  $stmt = $pdo->prepare("SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c");
  $stmt->execute([':t'=>$table, ':c'=>$column]);
  $row = $stmt->fetch();
  return empty($row) || (int)$row['c'] === 0;
}

function stock_bootstrap(PDO $pdo){
  $pdo->exec("CREATE TABLE IF NOT EXISTS stock_movements (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    qty INT NOT NULL DEFAULT 0,
    type ENUM('in','out','reserve','release','adjust') NOT NULL,
    reason ENUM('purchase','order','writeoff','manual','production') NOT NULL DEFAULT 'manual',
    order_id INT NULL,
    doc_id INT NULL,
    comment TEXT,
    created_by VARCHAR(64) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_product (product_id),
    KEY idx_type (type),
    KEY idx_order (order_id),
    KEY idx_doc (doc_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  $pdo->exec("CREATE TABLE IF NOT EXISTS stock_docs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_type ENUM('invoice','act','receipt','other') NOT NULL DEFAULT 'other',
    doc_number VARCHAR(64) NULL,
    doc_date DATE NULL,
    supplier VARCHAR(255) NULL,
    file_url VARCHAR(512) NULL,
    comment TEXT,
    created_by VARCHAR(64) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  $pdo->exec("CREATE TABLE IF NOT EXISTS stock_doc_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_id INT NOT NULL,
    product_id INT NOT NULL,
    qty INT NOT NULL DEFAULT 0,
    price DECIMAL(10,2) NULL,
    KEY idx_doc (doc_id),
    KEY idx_product (product_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  $pdo->exec("CREATE TABLE IF NOT EXISTS production_orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    order_id INT NULL,
    qty INT NOT NULL DEFAULT 0,
    qty_done INT NOT NULL DEFAULT 0,
    status ENUM('open','closed','cancelled') NOT NULL DEFAULT 'open',
    comment TEXT,
    created_by VARCHAR(64) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_product (product_id),
    KEY idx_order (order_id),
    KEY idx_status (status)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  try {
    if (stock_column_missing($pdo, 'products', 'supply_mode')){
      $pdo->exec("ALTER TABLE products ADD COLUMN supply_mode ENUM('stock','mto','mixed') NOT NULL DEFAULT 'stock'");
    }
  } catch (Throwable $e) {
    // ignore if column already exists or no permission
  }
}

function stock_reserved_map(PDO $pdo, array $productIds = []): array {
  if ($productIds){
    $in = implode(',', array_map('intval', $productIds));
    $sql = "SELECT product_id, SUM(CASE WHEN type='reserve' THEN qty WHEN type='release' THEN -qty ELSE 0 END) AS reserved
            FROM stock_movements WHERE product_id IN ($in) GROUP BY product_id";
  } else {
    $sql = "SELECT product_id, SUM(CASE WHEN type='reserve' THEN qty WHEN type='release' THEN -qty ELSE 0 END) AS reserved
            FROM stock_movements GROUP BY product_id";
  }
  $rows = $pdo->query($sql)->fetchAll();
  $map = [];
  foreach ($rows as $r){ $map[(int)$r['product_id']] = (int)$r['reserved']; }
  return $map;
}

function stock_on_order_map(PDO $pdo, array $productIds = []): array {
  if ($productIds){
    $in = implode(',', array_map('intval', $productIds));
    $sql = "SELECT product_id, SUM(qty - qty_done) AS on_order
            FROM production_orders WHERE status='open' AND product_id IN ($in) GROUP BY product_id";
  } else {
    $sql = "SELECT product_id, SUM(qty - qty_done) AS on_order FROM production_orders WHERE status='open' GROUP BY product_id";
  }
  $rows = $pdo->query($sql)->fetchAll();
  $map = [];
  foreach ($rows as $r){ $map[(int)$r['product_id']] = (int)$r['on_order']; }
  return $map;
}

function stock_reserved_by_order(PDO $pdo, int $orderId): array {
  $stmt = $pdo->prepare("SELECT product_id, SUM(CASE WHEN type='reserve' THEN qty WHEN type='release' THEN -qty ELSE 0 END) AS reserved
                         FROM stock_movements WHERE order_id = :oid GROUP BY product_id");
  $stmt->execute([':oid'=>$orderId]);
  $rows = $stmt->fetchAll();
  $map = [];
  foreach ($rows as $r){ $map[(int)$r['product_id']] = (int)$r['reserved']; }
  return $map;
}

function stock_production_by_order(PDO $pdo, int $orderId): array {
  $stmt = $pdo->prepare("SELECT * FROM production_orders WHERE order_id = :oid AND status='open'");
  $stmt->execute([':oid'=>$orderId]);
  $rows = $stmt->fetchAll();
  $map = [];
  foreach ($rows as $r){ $map[(int)$r['product_id']] = $r; }
  return $map;
}

function stock_insert_movement(PDO $pdo, array $row){
  $stmt = $pdo->prepare("INSERT INTO stock_movements (product_id, qty, type, reason, order_id, doc_id, comment, created_by)
    VALUES (:pid,:qty,:type,:reason,:oid,:did,:cmt,:cb)");
  $stmt->execute([
    ':pid'=>(int)($row['product_id'] ?? 0),
    ':qty'=>(int)($row['qty'] ?? 0),
    ':type'=>(string)($row['type'] ?? 'adjust'),
    ':reason'=>(string)($row['reason'] ?? 'manual'),
    ':oid'=>($row['order_id'] ?? null),
    ':did'=>($row['doc_id'] ?? null),
    ':cmt'=>(string)($row['comment'] ?? ''),
    ':cb'=>(string)($row['created_by'] ?? 'admin'),
  ]);
}

function stock_apply_production_on_incoming(PDO $pdo, int $productId, int $qty){
  if ($qty <= 0) return;
  $stmt = $pdo->prepare("SELECT id, qty, qty_done FROM production_orders WHERE product_id = :pid AND status='open' ORDER BY id ASC");
  $stmt->execute([':pid'=>$productId]);
  $rows = $stmt->fetchAll();
  foreach ($rows as $r){
    $need = (int)$r['qty'] - (int)$r['qty_done'];
    if ($need <= 0) continue;
    $take = min($need, $qty);
    $newDone = (int)$r['qty_done'] + $take;
    $newStatus = ($newDone >= (int)$r['qty']) ? 'closed' : 'open';
    $upd = $pdo->prepare("UPDATE production_orders SET qty_done = :qd, status = :st WHERE id = :id");
    $upd->execute([':qd'=>$newDone, ':st'=>$newStatus, ':id'=>$r['id']]);
    $qty -= $take;
    if ($qty <= 0) break;
  }
}

function stock_apply_reservation(PDO $pdo, int $orderId){
  stock_bootstrap($pdo);
  $orderStmt = $pdo->prepare("SELECT id FROM orders WHERE id = :id");
  $orderStmt->execute([':id'=>$orderId]);
  if (!$orderStmt->fetch()) return;

  $itemsStmt = $pdo->prepare("SELECT product_id, qty FROM order_items WHERE order_id = :id");
  $itemsStmt->execute([':id'=>$orderId]);
  $items = $itemsStmt->fetchAll();
  if (!$items) return;

  $productIds = array_values(array_unique(array_map(fn($x)=>(int)$x['product_id'], $items)));
  $in = implode(',', array_map('intval', $productIds));
  $products = $pdo->query("SELECT id, stock_qty, supply_mode FROM products WHERE id IN ($in)")->fetchAll();
  $productMap = [];
  foreach ($products as $p){ $productMap[(int)$p['id']] = $p; }

  $reservedTotal = stock_reserved_map($pdo, $productIds);
  $reservedByOrder = stock_reserved_by_order($pdo, $orderId);
  $prodByOrder = stock_production_by_order($pdo, $orderId);

  foreach ($items as $it){
    $pid = (int)$it['product_id'];
    $orderQty = (int)$it['qty'];
    $p = $productMap[$pid] ?? null;
    if (!$p) continue;
    $mode = $p['supply_mode'] ?: 'stock';

    $available = max(0, (int)$p['stock_qty'] - (int)($reservedTotal[$pid] ?? 0));
    $desiredReserve = ($mode === 'mto') ? 0 : min($available, $orderQty);
    $currentReserve = (int)($reservedByOrder[$pid] ?? 0);
    $delta = $desiredReserve - $currentReserve;
    if ($delta > 0){
      stock_insert_movement($pdo, [
        'product_id'=>$pid,
        'qty'=>$delta,
        'type'=>'reserve',
        'reason'=>'order',
        'order_id'=>$orderId,
        'comment'=>'Резерв под заказ #'.$orderId,
      ]);
      $reservedTotal[$pid] = ($reservedTotal[$pid] ?? 0) + $delta;
    } elseif ($delta < 0){
      stock_insert_movement($pdo, [
        'product_id'=>$pid,
        'qty'=>abs($delta),
        'type'=>'release',
        'reason'=>'order',
        'order_id'=>$orderId,
        'comment'=>'Снятие резерва #'.$orderId,
      ]);
      $reservedTotal[$pid] = ($reservedTotal[$pid] ?? 0) - abs($delta);
    }

    $missing = ($mode === 'mto') ? $orderQty : max(0, $orderQty - $desiredReserve);
    $prod = $prodByOrder[$pid] ?? null;
    if ($missing > 0){
      if ($prod){
        $newQty = (int)$prod['qty_done'] + $missing;
        $upd = $pdo->prepare("UPDATE production_orders SET qty = :q, status='open' WHERE id = :id");
        $upd->execute([':q'=>$newQty, ':id'=>$prod['id']]);
      } else {
        $ins = $pdo->prepare("INSERT INTO production_orders (product_id, order_id, qty, qty_done, status, comment, created_by)
          VALUES (:pid,:oid,:q,0,'open',:cmt,:cb)");
        $ins->execute([
          ':pid'=>$pid,
          ':oid'=>$orderId,
          ':q'=>$missing,
          ':cmt'=>'Под заказ #'.$orderId,
          ':cb'=>'admin',
        ]);
      }
    } else {
      if ($prod){
        $upd = $pdo->prepare("UPDATE production_orders SET status='cancelled' WHERE id = :id");
        $upd->execute([':id'=>$prod['id']]);
      }
    }
  }
}

function stock_cancel_order(PDO $pdo, int $orderId){
  stock_bootstrap($pdo);
  $reservedByOrder = stock_reserved_by_order($pdo, $orderId);
  foreach ($reservedByOrder as $pid=>$qty){
    if ($qty <= 0) continue;
    stock_insert_movement($pdo, [
      'product_id'=>$pid,
      'qty'=>$qty,
      'type'=>'release',
      'reason'=>'order',
      'order_id'=>$orderId,
      'comment'=>'Снятие резерва (отмена) #'.$orderId,
    ]);
  }
  $pdo->prepare("UPDATE production_orders SET status='cancelled' WHERE order_id = :oid AND status='open'")
      ->execute([':oid'=>$orderId]);
}

function stock_fulfill_order(PDO $pdo, int $orderId){
  stock_bootstrap($pdo);
  $reservedByOrder = stock_reserved_by_order($pdo, $orderId);
  foreach ($reservedByOrder as $pid=>$qty){
    if ($qty <= 0) continue;
    stock_insert_movement($pdo, [
      'product_id'=>$pid,
      'qty'=>$qty,
      'type'=>'out',
      'reason'=>'order',
      'order_id'=>$orderId,
      'comment'=>'Списание по заказу #'.$orderId,
    ]);
    $pdo->prepare("UPDATE products SET stock_qty = stock_qty - :q WHERE id = :id")
        ->execute([':q'=>$qty, ':id'=>$pid]);
    stock_insert_movement($pdo, [
      'product_id'=>$pid,
      'qty'=>$qty,
      'type'=>'release',
      'reason'=>'order',
      'order_id'=>$orderId,
      'comment'=>'Снятие резерва (выполнен) #'.$orderId,
    ]);
  }
  $pdo->prepare("UPDATE production_orders SET status='closed', qty_done = qty WHERE order_id = :oid AND status='open'")
      ->execute([':oid'=>$orderId]);
}

function stock_sync_order(PDO $pdo, int $orderId, string $prevStatus, string $newStatus){
  try {
    $reserveStatuses = ['В работе','Критическое ожидание'];
    if (in_array($newStatus, $reserveStatuses, true)){
      stock_apply_reservation($pdo, $orderId);
    } elseif ($newStatus === 'Выполнен') {
      stock_fulfill_order($pdo, $orderId);
    } else {
      stock_cancel_order($pdo, $orderId);
    }
  } catch (Throwable $e) {
    // intentionally ignore to avoid breaking order flow
  }
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
        $stmt = $pdo->prepare("SELECT id, total, status FROM orders WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        $prev = $stmt->fetch();
        if (!$prev) out(404, ['error'=>'nf']);
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
        $newStatus = array_key_exists('status', $b) ? (string)$b['status'] : (string)$prev['status'];
        $newTotal  = array_key_exists('total', $b) ? (float)$b['total'] : (float)$prev['total'];
        cf_sync_order($pdo, $id, (string)$prev['status'], $newStatus, $newTotal);
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
      $stmt = $pdo->prepare("SELECT id, total, status FROM orders WHERE id = :id");
      $stmt->execute([':id'=>$id]);
      $prev = $stmt->fetch();
      if (!$prev) out(404, ['error'=>'nf']);
      $stmt = $pdo->prepare("UPDATE orders SET status = :s WHERE id = :id");
      $stmt->execute([':s'=>$st, ':id'=>$id]);
      cf_sync_order($pdo, $id, (string)$prev['status'], (string)$st, (float)$prev['total']);
      out(200, ['ok'=>true]);
    }

    out(404, ['error'=>'nf']);
  }

  if (($segments[0] ?? '') === 'cashflow'){
    $pdo = db();
    cf_bootstrap($pdo);
    $sub = $segments[1] ?? '';

    if ($sub === 'categories'){
      if ($method === 'GET'){
        $type = $_GET['type'] ?? null;
        if ($type){
          $stmt = $pdo->prepare("SELECT * FROM cashflow_categories WHERE type = :t AND active = 1 ORDER BY name ASC");
          $stmt->execute([':t'=>$type]);
        } else {
          $stmt = $pdo->query("SELECT * FROM cashflow_categories WHERE active = 1 ORDER BY type ASC, name ASC");
        }
        out(200, $stmt->fetchAll());
      }

      if ($method === 'POST'){
        $b = read_json();
        $name = trim((string)($b['name'] ?? ''));
        $type = (string)($b['type'] ?? '');
        if ($name === '' || !in_array($type, ['income','expense'], true)) out(400, ['error'=>'bad']);
        $stmt = $pdo->prepare("INSERT INTO cashflow_categories (name, type, active) VALUES (:n,:t,1)");
        $stmt->execute([':n'=>$name, ':t'=>$type]);
        out(200, ['id'=>(int)$pdo->lastInsertId()]);
      }

      out(404, ['error'=>'nf']);
    }

    if ($method === 'GET' && count($segments) === 1){
      $from = $_GET['from'] ?? null;
      $to   = $_GET['to'] ?? null;
      $type = $_GET['type'] ?? null;
      $status = $_GET['status'] ?? null;
      $category = $_GET['category'] ?? null;
      $payment = $_GET['payment'] ?? null;

      $conds = [];
      $args = [];
      if ($from){ $conds[] = "date >= :from"; $args[':from'] = $from; }
      if ($to){   $conds[] = "date <= :to";   $args[':to']   = $to; }
      if ($type){ $conds[] = "type = :t";     $args[':t']    = $type; }
      if ($status){ $conds[] = "status = :st"; $args[':st'] = $status; }
      if ($category){ $conds[] = "category = :cat"; $args[':cat'] = $category; }
      if ($payment){ $conds[] = "payment_method = :pm"; $args[':pm'] = $payment; }
      $where = $conds ? ('WHERE '.implode(' AND ', $conds)) : '';

      $stmt = $pdo->prepare("SELECT * FROM cashflow_entries $where ORDER BY date DESC, id DESC");
      $stmt->execute($args);
      $rows = $stmt->fetchAll();
      out(200, $rows);
    }

    if ($method === 'POST' && count($segments) === 1){
      $b = read_json();
      $date = (string)($b['date'] ?? '');
      $type = (string)($b['type'] ?? '');
      $amount = isset($b['amount']) ? (float)$b['amount'] : 0.0;
      $category = trim((string)($b['category'] ?? ''));
      $payment = (string)($b['payment_method'] ?? '');
      $comment = (string)($b['comment'] ?? '');
      $status = (string)($b['status'] ?? 'active');
      $source = (string)($b['source'] ?? 'manual');
      $orderId = isset($b['order_id']) && is_numeric($b['order_id']) ? (int)$b['order_id'] : null;

      if ($date === '') $date = date('Y-m-d');
      if (!in_array($type, ['income','expense'], true)) $type = 'income';
      if ($category === '') $category = 'Прочее';
      if (!in_array($payment, ['cash','card','transfer','bank'], true)) $payment = 'bank';
      if (!in_array($status, ['active','void'], true)) $status = 'active';
      if (!in_array($source, ['manual','order'], true)) $source = 'manual';

      $stmt = $pdo->prepare("INSERT INTO cashflow_entries (date, type, amount, category, payment_method, comment, order_id, source, status)
        VALUES (:d, :t, :amt, :cat, :pm, :cmt, :oid, :src, :st)");
      $stmt->execute([
        ':d'=>$date,
        ':t'=>$type,
        ':amt'=>$amount,
        ':cat'=>$category,
        ':pm'=>$payment,
        ':cmt'=>$comment,
        ':oid'=>$orderId,
        ':src'=>$source,
        ':st'=>$status,
      ]);
      out(200, ['id'=>(int)$pdo->lastInsertId()]);
    }

    if (count($segments) === 2){
      $id = (int)$segments[1];

      if ($method === 'PATCH'){
        $b = read_json();
        $set = [];
        $args = [':id'=>$id];

        if (array_key_exists('date', $b)){
          $set[] = "date = :date";
          $args[':date'] = (string)$b['date'] ?: date('Y-m-d');
        }
        if (array_key_exists('type', $b)){
          $t = (string)$b['type'];
          if (!in_array($t, ['income','expense'], true)) $t = 'income';
          $set[] = "type = :type";
          $args[':type'] = $t;
        }
        if (array_key_exists('amount', $b)){
          $set[] = "amount = :amount";
          $args[':amount'] = (float)$b['amount'];
        }
        if (array_key_exists('category', $b)){
          $cat = trim((string)$b['category']);
          $set[] = "category = :category";
          $args[':category'] = ($cat === '' ? 'Прочее' : $cat);
        }
        if (array_key_exists('payment_method', $b)){
          $pm = (string)$b['payment_method'];
          if (!in_array($pm, ['cash','card','transfer','bank'], true)) $pm = 'bank';
          $set[] = "payment_method = :pm";
          $args[':pm'] = $pm;
        }
        if (array_key_exists('comment', $b)){
          $set[] = "comment = :comment";
          $args[':comment'] = (string)$b['comment'];
        }
        if (array_key_exists('status', $b)){
          $st = (string)$b['status'];
          if (!in_array($st, ['active','void'], true)) $st = 'active';
          $set[] = "status = :status";
          $args[':status'] = $st;
        }

        if (!$set) out(400, ['error'=>'no_fields']);
        $sql = "UPDATE cashflow_entries SET ".implode(',', $set)." WHERE id = :id";
        $stmt = $pdo->prepare($sql);
        $stmt->execute($args);
        out(200, ['ok'=>true]);
      }

      if ($method === 'DELETE'){
        $stmt = $pdo->prepare("DELETE FROM cashflow_entries WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        out(200, ['ok'=>true]);
      }
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
