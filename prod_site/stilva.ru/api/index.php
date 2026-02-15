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

function cf_normalize_date(string $date): string {
  $date = trim($date);
  if ($date === '' || $date === '0000-00-00') return date('Y-m-d');
  $dt = DateTime::createFromFormat('Y-m-d', $date);
  if (!$dt || $dt->format('Y-m-d') !== $date) return date('Y-m-d');
  return $date;
}

function cf_upsert_order_income(PDO $pdo, int $orderId, float $amount, string $paymentMethod, string $date){
  cf_bootstrap($pdo);
  $row = cf_find_order_income($pdo, $orderId);
  $today = cf_normalize_date($date);
  if (!in_array($paymentMethod, ['cash','card','transfer','bank'], true)) $paymentMethod = 'bank';
  if ($row){
    $stmt = $pdo->prepare("UPDATE cashflow_entries SET amount = :amt, status = 'active', date = :d, payment_method = :pm, updated_at = NOW() WHERE id = :id");
    $stmt->execute([':amt'=>$amount, ':d'=>$today, ':pm'=>$paymentMethod, ':id'=>$row['id']]);
  } else {
    $stmt = $pdo->prepare("INSERT INTO cashflow_entries (date, type, amount, category, payment_method, comment, order_id, source, status)
      VALUES (:d, 'income', :amt, :cat, :pm, :cmt, :oid, 'order', 'active')");
    $stmt->execute([
      ':d'   => $today,
      ':amt' => $amount,
      ':cat' => 'Продажа',
      ':pm'  => $paymentMethod,
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

function cf_sync_order(PDO $pdo, int $orderId, string $prevStatus, string $newStatus){
  try {
    if ($newStatus === 'Выполнен'){
      $stmt = $pdo->prepare("SELECT total, payment_amount, payment_method, payment_date FROM orders WHERE id = :id");
      $stmt->execute([':id'=>$orderId]);
      $row = $stmt->fetch();
      $amount = (float)($row['payment_amount'] ?? 0);
      if ($amount <= 0) $amount = (float)($row['total'] ?? 0);
      $pm = (string)($row['payment_method'] ?? 'bank');
      $date = (string)($row['payment_date'] ?? '');
      cf_upsert_order_income($pdo, $orderId, $amount, $pm, $date ?: date('Y-m-d'));
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

function product_bootstrap(PDO $pdo): void {
  static $done = false;
  if ($done) return;
  $done = true;

  stock_bootstrap($pdo);

  $alter = [
    'seo_slug' => "ALTER TABLE products ADD COLUMN seo_slug VARCHAR(190) NOT NULL DEFAULT '' AFTER description",
    'seo_title' => "ALTER TABLE products ADD COLUMN seo_title VARCHAR(255) NOT NULL DEFAULT '' AFTER description",
    'seo_description' => "ALTER TABLE products ADD COLUMN seo_description TEXT NULL AFTER seo_title",
    'seo_keywords' => "ALTER TABLE products ADD COLUMN seo_keywords TEXT NULL AFTER seo_description",
    'seo_h1' => "ALTER TABLE products ADD COLUMN seo_h1 VARCHAR(255) NOT NULL DEFAULT '' AFTER seo_keywords",
    'seo_robots' => "ALTER TABLE products ADD COLUMN seo_robots VARCHAR(120) NOT NULL DEFAULT '' AFTER seo_h1",
    'seo_canonical' => "ALTER TABLE products ADD COLUMN seo_canonical VARCHAR(512) NOT NULL DEFAULT '' AFTER seo_robots",
    'seo_og_title' => "ALTER TABLE products ADD COLUMN seo_og_title VARCHAR(255) NOT NULL DEFAULT '' AFTER seo_canonical",
    'seo_og_description' => "ALTER TABLE products ADD COLUMN seo_og_description TEXT NULL AFTER seo_og_title",
  ];

  foreach ($alter as $column => $sql){
    try {
      if (stock_column_missing($pdo, 'products', $column)){
        $pdo->exec($sql);
      }
    } catch (Throwable $e) {
      // ignore, bootstrap should never break API flow
    }
  }
}

function seo_slugify(string $value): string {
  $value = trim(mb_strtolower($value, 'UTF-8'));
  if ($value === '') return '';
  $map = [
    'а'=>'a','б'=>'b','в'=>'v','г'=>'g','д'=>'d','е'=>'e','ё'=>'e','ж'=>'zh','з'=>'z','и'=>'i','й'=>'y',
    'к'=>'k','л'=>'l','м'=>'m','н'=>'n','о'=>'o','п'=>'p','р'=>'r','с'=>'s','т'=>'t','у'=>'u','ф'=>'f',
    'х'=>'h','ц'=>'c','ч'=>'ch','ш'=>'sh','щ'=>'sch','ъ'=>'','ы'=>'y','ь'=>'','э'=>'e','ю'=>'yu','я'=>'ya'
  ];
  $value = strtr($value, $map);
  $value = preg_replace('/[^a-z0-9]+/u', '-', $value) ?? '';
  $value = trim($value, '-');
  return $value;
}

function seo_slug_unique(PDO $pdo, string $slug, int $excludeId = 0): string {
  $base = trim($slug);
  if ($base === '') $base = 'product';
  $candidate = $base;
  $i = 2;
  while (true){
    $sql = "SELECT id FROM products WHERE seo_slug = :slug";
    $args = [':slug'=>$candidate];
    if ($excludeId > 0){
      $sql .= " AND id <> :id";
      $args[':id'] = $excludeId;
    }
    $sql .= " LIMIT 1";
    $stmt = $pdo->prepare($sql);
    $stmt->execute($args);
    $row = $stmt->fetch();
    if (!$row) return $candidate;
    $candidate = $base . '-' . $i;
    $i++;
  }
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
        $qtyDone = (int)$prod['qty_done'];
        $target = max($missing, $qtyDone);
        $newStatus = ($qtyDone >= $target) ? 'closed' : 'open';
        $upd = $pdo->prepare("UPDATE production_orders SET qty = :q, status = :st WHERE id = :id");
        $upd->execute([':q'=>$target, ':st'=>$newStatus, ':id'=>$prod['id']]);
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

function stock_plan_order(PDO $pdo, int $orderId){
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
  $prodByOrder = stock_production_by_order($pdo, $orderId);

  foreach ($items as $it){
    $pid = (int)$it['product_id'];
    $orderQty = (int)$it['qty'];
    $p = $productMap[$pid] ?? null;
    if (!$p) continue;
    $mode = $p['supply_mode'] ?: 'stock';

    $available = max(0, (int)$p['stock_qty'] - (int)($reservedTotal[$pid] ?? 0));
    $missing = ($mode === 'mto') ? $orderQty : max(0, $orderQty - $available);
    $prod = $prodByOrder[$pid] ?? null;
    if ($missing > 0){
      if ($prod){
        $qtyDone = (int)$prod['qty_done'];
        $target = max($missing, $qtyDone);
        $newStatus = ($qtyDone >= $target) ? 'closed' : 'open';
        $upd = $pdo->prepare("UPDATE production_orders SET qty = :q, status = :st WHERE id = :id");
        $upd->execute([':q'=>$target, ':st'=>$newStatus, ':id'=>$prod['id']]);
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

function stock_release_reserve(PDO $pdo, int $orderId){
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
      'comment'=>'Снятие резерва #'.$orderId,
    ]);
  }
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

function stock_unfulfill_order(PDO $pdo, int $orderId){
  stock_bootstrap($pdo);
  // сколько реально списано (out) минус уже откаты (in)
  $stmt = $pdo->prepare("SELECT product_id,
    SUM(CASE WHEN type='out' THEN qty WHEN type='in' THEN -qty ELSE 0 END) AS net_qty
    FROM stock_movements
    WHERE order_id = :oid AND reason = 'order' AND type IN ('out','in')
    GROUP BY product_id");
  $stmt->execute([':oid'=>$orderId]);
  $rows = $stmt->fetchAll();
  foreach ($rows as $r){
    $pid = (int)($r['product_id'] ?? 0);
    $net = (int)($r['net_qty'] ?? 0);
    if ($pid <= 0 || $net <= 0) continue;
    $pdo->prepare("UPDATE products SET stock_qty = stock_qty + :q WHERE id = :id")
        ->execute([':q'=>$net, ':id'=>$pid]);
    stock_insert_movement($pdo, [
      'product_id'=>$pid,
      'qty'=>$net,
      'type'=>'in',
      'reason'=>'order',
      'order_id'=>$orderId,
      'comment'=>'Откат выполнения заказа #'.$orderId,
    ]);
  }
}

function stock_sync_order(PDO $pdo, int $orderId, string $prevStatus, string $newStatus){
  try {
    if ($prevStatus === $newStatus) return;
    if ($prevStatus === 'Выполнен' && $newStatus !== 'Выполнен'){
      stock_unfulfill_order($pdo, $orderId);
    }
    $reserveStatuses = ['В работе','Критическое ожидание','Передан в доставку'];
    if (in_array($newStatus, $reserveStatuses, true)){
      stock_apply_reservation($pdo, $orderId);
    } elseif ($newStatus === 'Выполнен') {
      stock_apply_reservation($pdo, $orderId);
      stock_fulfill_order($pdo, $orderId);
    } elseif ($newStatus === 'Новый') {
      stock_release_reserve($pdo, $orderId);
      stock_plan_order($pdo, $orderId);
    } else {
      stock_cancel_order($pdo, $orderId);
    }
  } catch (Throwable $e) {
    // intentionally ignore to avoid breaking order flow
  }
}

function order_bootstrap(PDO $pdo){
  $cols = [
    'delivery_type' => "ENUM('pickup','delivery') NOT NULL DEFAULT 'pickup'",
    'delivery_address' => "VARCHAR(255) NULL",
    'payment_status' => "ENUM('unpaid','partial','paid') NOT NULL DEFAULT 'unpaid'",
    'payment_method' => "ENUM('cash','card','transfer','bank') NOT NULL DEFAULT 'bank'",
    'payment_amount' => "DECIMAL(10,2) NOT NULL DEFAULT 0",
    'payment_date' => "DATE NULL",
    'checklist_contacted' => "TINYINT(1) NOT NULL DEFAULT 0",
    'checklist_confirmed' => "TINYINT(1) NOT NULL DEFAULT 0",
    'checklist_picked' => "TINYINT(1) NOT NULL DEFAULT 0",
    'checklist_shipped' => "TINYINT(1) NOT NULL DEFAULT 0",
    'checklist_docs' => "TINYINT(1) NOT NULL DEFAULT 0",
    // UПД / отгрузочные данные
    'upd_buyer_type' => "ENUM('org','ip','person') NOT NULL DEFAULT 'org'",
    'upd_buyer_name' => "VARCHAR(255) NULL",
    'upd_buyer_inn' => "VARCHAR(12) NULL",
    'upd_buyer_kpp' => "VARCHAR(9) NULL",
    'upd_buyer_address' => "VARCHAR(255) NULL",
    'upd_buyer_other' => "VARCHAR(255) NULL",
    'upd_shipper_same' => "TINYINT(1) NOT NULL DEFAULT 1",
    'upd_shipper_name' => "VARCHAR(255) NULL",
    'upd_shipper_inn' => "VARCHAR(12) NULL",
    'upd_shipper_kpp' => "VARCHAR(9) NULL",
    'upd_shipper_address' => "VARCHAR(255) NULL",
    'upd_consignee_same' => "TINYINT(1) NOT NULL DEFAULT 1",
    'upd_consignee_name' => "VARCHAR(255) NULL",
    'upd_consignee_address' => "VARCHAR(255) NULL",
    'upd_doc_date' => "DATE NULL",
    'upd_vat_rate' => "VARCHAR(12) NULL",
    'upd_vat_included' => "TINYINT(1) NOT NULL DEFAULT 0",
    'upd_signer1_fio' => "VARCHAR(255) NULL",
    'upd_signer1_pos' => "VARCHAR(255) NULL",
    'upd_signer1_type' => "VARCHAR(1) NULL",
    'upd_signer1_authority' => "VARCHAR(1) NULL",
    'upd_signer1_date' => "DATE NULL",
    'upd_signer2_fio' => "VARCHAR(255) NULL",
    'upd_signer2_pos' => "VARCHAR(255) NULL",
    'upd_signer2_type' => "VARCHAR(1) NULL",
    'upd_signer2_authority' => "VARCHAR(1) NULL",
    'upd_signer2_date' => "DATE NULL",
  ];
  foreach ($cols as $name=>$def){
    try{
      if (stock_column_missing($pdo, 'orders', $name)){
        $pdo->exec("ALTER TABLE orders ADD COLUMN $name $def");
      }
    } catch (Throwable $e) {
      // ignore if no permission
    }
  }
}

function settings_bootstrap(PDO $pdo){
  $pdo->exec("CREATE TABLE IF NOT EXISTS app_settings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    k VARCHAR(64) NOT NULL UNIQUE,
    v MEDIUMTEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
}

function settings_get_all(PDO $pdo): array {
  settings_bootstrap($pdo);
  $rows = $pdo->query("SELECT k, v FROM app_settings")->fetchAll();
  $out = [];
  foreach ($rows as $r){
    $val = json_decode((string)$r['v'], true);
    $out[$r['k']] = $val !== null ? $val : $r['v'];
  }
  return $out;
}

function settings_get(PDO $pdo, string $key, $default=null){
  settings_bootstrap($pdo);
  $stmt = $pdo->prepare("SELECT v FROM app_settings WHERE k = :k");
  $stmt->execute([':k'=>$key]);
  $row = $stmt->fetch();
  if (!$row) return $default;
  $val = json_decode((string)$row['v'], true);
  return $val !== null ? $val : $row['v'];
}

function settings_set(PDO $pdo, string $key, $value): void {
  settings_bootstrap($pdo);
  $json = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  $stmt = $pdo->prepare("INSERT INTO app_settings (k, v) VALUES (:k, :v)
    ON DUPLICATE KEY UPDATE v = VALUES(v), updated_at = NOW()");
  $stmt->execute([':k'=>$key, ':v'=>$json]);
}

function uuid_v4(): string {
  $data = random_bytes(16);
  $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
  $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
  return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}

function upd_bootstrap(PDO $pdo){
  $pdo->exec("CREATE TABLE IF NOT EXISTS upd_docs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    doc_year INT NOT NULL,
    doc_seq INT NOT NULL,
    doc_number VARCHAR(32) NOT NULL,
    doc_date DATE NOT NULL,
    file_id VARCHAR(255) NULL,
    file_name VARCHAR(255) NULL,
    data_json MEDIUMTEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_order (order_id),
    KEY idx_year (doc_year)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
}

function upd_delete_by_order(PDO $pdo, int $orderId): void {
  upd_bootstrap($pdo);
  $stmt = $pdo->prepare("DELETE FROM upd_docs WHERE order_id = :oid");
  $stmt->execute([':oid'=>$orderId]);
}

function upd_next_seq(PDO $pdo, int $year): int {
  $stmt = $pdo->prepare("SELECT MAX(doc_seq) AS m FROM upd_docs WHERE doc_year = :y");
  $stmt->execute([':y'=>$year]);
  $row = $stmt->fetch();
  $m = $row && $row['m'] !== null ? (int)$row['m'] : 0;
  return $m + 1;
}

function upd_doc_number(int $year, int $seq): string {
  return $year.'-'.str_pad((string)$seq, 4, '0', STR_PAD_LEFT);
}

function upd_format_date(?string $ymd): string {
  if (!$ymd) return date('d.m.Y');
  $t = strtotime($ymd);
  if (!$t) return date('d.m.Y');
  return date('d.m.Y', $t);
}

function upd_format_time(?string $time=null): string {
  if ($time){
    $t = strtotime($time);
    if ($t) return date('H.i.s', $t);
  }
  return date('H.i.s');
}

function upd_normalize_vat_rate(string $rate): string {
  $allowed = ['0%','5%','7%','9,09%','10%','16,67%','20%','5/105','7/107','10/110','20/120','без НДС','НДС исчисляется налоговым агентом'];
  return in_array($rate, $allowed, true) ? $rate : 'без НДС';
}

function upd_rate_to_float(string $rate): float {
  $rate = str_replace('%','',$rate);
  if ($rate === 'без НДС' || $rate === 'НДС исчисляется налоговым агентом') return 0.0;
  if (strpos($rate, '/') !== false){
    [$a,$b] = array_map('floatval', explode('/', $rate, 2));
    if ($b > 0) return $a / $b;
  }
  $val = str_replace(',','.', $rate);
  return is_numeric($val) ? ((float)$val / 100.0) : 0.0;
}

function upd_split_fio(string $fio): array {
  $parts = array_values(array_filter(preg_split('/\\s+/', trim($fio))));
  $last = $parts[0] ?? '';
  $first = $parts[1] ?? '';
  $middle = $parts[2] ?? '';
  return [$last, $first, $middle];
}

function upd_calc_line(float $price, float $qty, string $rate, bool $included): array {
  $rateNorm = upd_normalize_vat_rate($rate);
  $k = upd_rate_to_float($rateNorm);
  $sumRaw = $price * $qty;
  if ($rateNorm === 'без НДС' || $rateNorm === 'НДС исчисляется налоговым агентом' || $k <= 0){
    $noVat = $sumRaw;
    $vat = 0.0;
    $withVat = $sumRaw;
    $priceNoVat = $qty > 0 ? ($noVat / $qty) : 0.0;
  } elseif ($included){
    $noVat = $sumRaw / (1 + $k);
    $vat = $sumRaw - $noVat;
    $withVat = $sumRaw;
    $priceNoVat = $qty > 0 ? ($noVat / $qty) : 0.0;
  } else {
    $noVat = $sumRaw;
    $vat = $noVat * $k;
    $withVat = $noVat + $vat;
    $priceNoVat = $price;
  }
  return [
    'price_no_vat' => round($priceNoVat, 2),
    'sum_no_vat' => round($noVat, 2),
    'vat_sum' => round($vat, 2),
    'sum_with_vat' => round($withVat, 2),
    'vat_rate' => $rateNorm
  ];
}

function upd_build_xml_participant(SimpleXMLElement $node, array $p): void {
  $type = $p['type'] ?? 'org';
  $id = $node->addChild('ИдСв');
  if ($type === 'ip'){
    $svip = $id->addChild('СвИП');
    $fio = $svip->addChild('ФИО');
    [$last,$first,$middle] = upd_split_fio((string)($p['fio'] ?? $p['name'] ?? ''));
    if ($last !== '') $fio->addAttribute('Фамилия', $last);
    if ($first !== '') $fio->addAttribute('Имя', $first);
    if ($middle !== '') $fio->addAttribute('Отчество', $middle);
    if (!empty($p['inn'])) $svip->addAttribute('ИННФЛ', (string)$p['inn']);
  } elseif ($type === 'person'){
    $svfl = $id->addChild('СвФЛУч');
    $fio = $svfl->addChild('ФИО');
    [$last,$first,$middle] = upd_split_fio((string)($p['fio'] ?? $p['name'] ?? ''));
    if ($last !== '') $fio->addAttribute('Фамилия', $last);
    if ($first !== '') $fio->addAttribute('Имя', $first);
    if ($middle !== '') $fio->addAttribute('Отчество', $middle);
    if (!empty($p['inn'])) $svfl->addAttribute('ИННФЛ', (string)$p['inn']);
    if (!empty($p['other'])) $svfl->addAttribute('ИныеСвед', (string)$p['other']);
  } else {
    $svul = $id->addChild('СвЮЛУч');
    if (!empty($p['name'])) $svul->addAttribute('НаимОрг', (string)$p['name']);
    if (!empty($p['inn'])) $svul->addAttribute('ИННЮЛ', (string)$p['inn']);
    if (!empty($p['kpp'])) $svul->addAttribute('КПП', (string)$p['kpp']);
  }

  $addrText = trim((string)($p['address'] ?? ''));
  if ($addrText !== ''){
    $addr = $node->addChild('Адрес');
    $adr = $addr->addChild('АдрИнф');
    $adr->addAttribute('КодСтр', '643');
    $adr->addAttribute('НаимСтран', 'Российская Федерация');
    $adr->addAttribute('АдрТекст', $addrText);
  }

  if (!empty($p['bank']) && is_array($p['bank'])){
    $bank = $node->addChild('БанкРекв');
    if (!empty($p['bank']['account'])) $bank->addAttribute('НомерСчета', (string)$p['bank']['account']);
    $svb = $bank->addChild('СвБанк');
    if (!empty($p['bank']['name'])) $svb->addAttribute('НаимБанк', (string)$p['bank']['name']);
    if (!empty($p['bank']['bik'])) $svb->addAttribute('БИК', (string)$p['bank']['bik']);
    if (!empty($p['bank']['corr'])) $svb->addAttribute('КорСчет', (string)$p['bank']['corr']);
  }

  if (!empty($p['phone']) || !empty($p['email'])){
    $contact = $node->addChild('Контакт');
    if (!empty($p['phone'])) $contact->addChild('Тлф', (string)$p['phone']);
    if (!empty($p['email'])) $contact->addChild('ЭлПочта', (string)$p['email']);
  }
}

function upd_build_xml(array $doc): string {
  // Build in UTF-8 first to avoid SimpleXML parse errors, then convert to Windows-1251
  $xml = new SimpleXMLElement('<?xml version="1.0" encoding="utf-8"?><Файл></Файл>');
  $xml->addAttribute('ИдФайл', (string)$doc['file_id']);
  $xml->addAttribute('ВерсФорм', '5.03');
  $xml->addAttribute('ВерсПрог', (string)($doc['program'] ?? 'Stilva'));

  $d = $xml->addChild('Документ');
  $d->addAttribute('КНД', '1115131');
  $d->addAttribute('Функция', 'СЧФДОП');
  $d->addAttribute('ДатаИнфПр', (string)$doc['info_date']);
  $d->addAttribute('ВремИнфПр', (string)$doc['info_time']);
  $d->addAttribute('НаимДокОпр', (string)($doc['doc_title'] ?? 'УПД'));

  $sv = $d->addChild('СвСчФакт');
  $sv->addAttribute('НомерДок', (string)$doc['doc_number']);
  $sv->addAttribute('ДатаДок', (string)$doc['doc_date_fmt']);

  $svProd = $sv->addChild('СвПрод');
  upd_build_xml_participant($svProd, $doc['seller'] ?? []);

  if (!empty($doc['shipper_same'])){
    $go = $sv->addChild('ГрузОт');
    $go->addChild('ОнЖе', 'он же');
  } else {
    $go = $sv->addChild('ГрузОт');
    $goP = $go->addChild('ГрузОтпр');
    upd_build_xml_participant($goP, $doc['shipper'] ?? []);
  }

  $gp = $sv->addChild('ГрузПолуч');
  upd_build_xml_participant($gp, $doc['consignee'] ?? []);

  $svBuy = $sv->addChild('СвПокуп');
  upd_build_xml_participant($svBuy, $doc['buyer'] ?? []);

  $den = $sv->addChild('ДенИзм');
  $den->addAttribute('КодОКВ', (string)($doc['currency_code'] ?? '643'));
  $den->addAttribute('НаимОКВ', (string)($doc['currency_name'] ?? 'Российский рубль'));

  $tab = $d->addChild('ТаблСчФакт');
  $items = $doc['items'] ?? [];
  $rowNum = 1;
  foreach ($items as $it){
    $row = $tab->addChild('СведТов');
    $row->addAttribute('НомСтр', (string)$rowNum);
    if (!empty($it['name'])) $row->addAttribute('НаимТов', (string)$it['name']);
    if (!empty($it['unit_code'])) $row->addAttribute('ОКЕИ_Тов', (string)$it['unit_code']);
    if (!empty($it['unit_name'])) $row->addAttribute('НаимЕдИзм', (string)$it['unit_name']);
    if (isset($it['qty'])) $row->addAttribute('КолТов', number_format((float)$it['qty'], 3, '.', ''));
    if (isset($it['price_no_vat'])) $row->addAttribute('ЦенаТов', number_format((float)$it['price_no_vat'], 2, '.', ''));
    if (isset($it['sum_no_vat'])) $row->addAttribute('СтТовБезНДС', number_format((float)$it['sum_no_vat'], 2, '.', ''));
    $row->addAttribute('НалСт', (string)($it['vat_rate'] ?? 'без НДС'));
    if (isset($it['sum_with_vat'])) $row->addAttribute('СтТовУчНал', number_format((float)$it['sum_with_vat'], 2, '.', ''));

    $sumVat = $row->addChild('СумНал');
    $vatRate = (string)($it['vat_rate'] ?? 'без НДС');
    $vatSum = (float)($it['vat_sum'] ?? 0);
    if ($vatRate === 'без НДС' || $vatRate === 'НДС исчисляется налоговым агентом' || $vatSum <= 0){
      $sumVat->addChild('БезНДС', 'без НДС');
    } else {
      $sumVat->addChild('СумНал', number_format($vatSum, 2, '.', ''));
    }
    $rowNum++;
  }

  $tot = $doc['totals'] ?? ['sum_no_vat'=>0,'sum_with_vat'=>0,'vat_sum'=>0,'vat_rate'=>'без НДС'];
  $totEl = $tab->addChild('ВсегоОпл');
  $totEl->addAttribute('СтТовБезНДСВсего', number_format((float)($tot['sum_no_vat'] ?? 0), 2, '.', ''));
  $totEl->addAttribute('СтТовУчНалВсего', number_format((float)($tot['sum_with_vat'] ?? 0), 2, '.', ''));
  $sumVatAll = $totEl->addChild('СумНалВсего');
  $tRate = (string)($tot['vat_rate'] ?? 'без НДС');
  $tVat = (float)($tot['vat_sum'] ?? 0);
  if ($tRate === 'без НДС' || $tRate === 'НДС исчисляется налоговым агентом' || $tVat <= 0){
    $sumVatAll->addChild('БезНДС', 'без НДС');
  } else {
    $sumVatAll->addChild('СумНал', number_format($tVat, 2, '.', ''));
  }

  $spp = $d->addChild('СвПродПер');
  $svper = $spp->addChild('СвПер');
  $svper->addAttribute('СодОпер', (string)($doc['operation_content'] ?? 'Отгрузка товаров'));
  if (!empty($doc['operation_kind'])) $svper->addAttribute('ВидОпер', (string)$doc['operation_kind']);
  if (!empty($doc['transfer_date_fmt'])) $svper->addAttribute('ДатаПер', (string)$doc['transfer_date_fmt']);

  $osn = $svper->addChild('ОснПер');
  $osn->addAttribute('РеквНаимДок', (string)($doc['basis_name'] ?? 'Заказ'));
  $osn->addAttribute('РеквНомерДок', (string)($doc['basis_number'] ?? $doc['order_id'] ?? ''));
  $osn->addAttribute('РеквДатаДок', (string)($doc['basis_date_fmt'] ?? $doc['doc_date_fmt']));

  foreach (($doc['signers'] ?? []) as $s){
    if (empty($s['fio'])) continue;
    $p = $d->addChild('Подписант');
    if (!empty($s['pos'])) $p->addAttribute('Должн', (string)$s['pos']);
    if (!empty($s['sign_type'])) $p->addAttribute('ТипПодпис', (string)$s['sign_type']);
    if (!empty($s['date_fmt'])) $p->addAttribute('ДатаПодДок', (string)$s['date_fmt']);
    $p->addAttribute('СпосПодтПолном', (string)($s['authority'] ?? '1'));
    $fio = $p->addChild('ФИО');
    [$last,$first,$middle] = upd_split_fio((string)$s['fio']);
    if ($last !== '') $fio->addAttribute('Фамилия', $last);
    if ($first !== '') $fio->addAttribute('Имя', $first);
    if ($middle !== '') $fio->addAttribute('Отчество', $middle);
  }

  $out = $xml->asXML();
  if ($out === false) $out = '';
  $out = str_replace('encoding="utf-8"', 'encoding="windows-1251"', $out);
  $converted = iconv('UTF-8', 'Windows-1251//TRANSLIT', $out);
  return $converted !== false ? $converted : $out;
}

function upd_build_print_html(array $doc): string {
  $h = fn($v)=>htmlspecialchars((string)$v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
  $seller = $doc['seller'] ?? [];
  $buyer = $doc['buyer'] ?? [];
  $shipper = $doc['shipper'] ?? $seller;
  $consignee = $doc['consignee'] ?? $buyer;
  $items = $doc['items'] ?? [];
  $tot = $doc['totals'] ?? ['sum_no_vat'=>0,'vat_sum'=>0,'sum_with_vat'=>0,'vat_rate'=>'без НДС'];
  $signers = $doc['signers'] ?? [];

  $rows = '';
  $i = 1;
  foreach ($items as $it){
    $rows .= '<tr>'
      .'<td>'.$i.'</td>'
      .'<td>'.$h($it['name'] ?? '').'</td>'
      .'<td>'.$h($it['unit_name'] ?? '').'</td>'
      .'<td class="num">'.number_format((float)($it['qty'] ?? 0), 3, '.', '').'</td>'
      .'<td class="num">'.number_format((float)($it['price_no_vat'] ?? 0), 2, '.', '').'</td>'
      .'<td class="num">'.number_format((float)($it['sum_no_vat'] ?? 0), 2, '.', '').'</td>'
      .'<td>'.$h($it['vat_rate'] ?? 'без НДС').'</td>'
      .'<td class="num">'.number_format((float)($it['vat_sum'] ?? 0), 2, '.', '').'</td>'
      .'<td class="num">'.number_format((float)($it['sum_with_vat'] ?? 0), 2, '.', '').'</td>'
      .'</tr>';
    $i++;
  }

  $sellerName = $seller['type'] === 'ip' ? ($seller['fio'] ?? '') : ($seller['name'] ?? '');
  $buyerName = $buyer['type'] === 'ip' ? ($buyer['fio'] ?? '') : ($buyer['name'] ?? '');

  $signHtml = '';
  foreach ($signers as $s){
    $signHtml .= '<div class="sign-row"><div class="sign-role">'.$h($s['pos'] ?? 'Подписант').'</div><div class="sign-name">'.$h($s['fio'] ?? '').'</div></div>';
  }
  if ($signHtml === '') $signHtml = '<div class="sign-row"><div class="sign-role">Подписант</div><div class="sign-name">____________________</div></div>';

  $html = '<!doctype html><html lang="ru"><head><meta charset="utf-8"><title>УПД</title><style>
  body{font-family:Arial, sans-serif; color:#111; margin:20px;}
  h1{font-size:20px; margin:0 0 8px;}
  .muted{color:#666; font-size:12px;}
  .grid{display:grid; grid-template-columns:1fr 1fr; gap:12px; margin:10px 0;}
  .box{border:1px solid #000; padding:8px; font-size:12px;}
  table{width:100%; border-collapse:collapse; font-size:12px; margin-top:10px;}
  th,td{border:1px solid #000; padding:4px; vertical-align:top;}
  th{background:#f3f3f3;}
  td.num{text-align:right; white-space:nowrap;}
  .signs{margin-top:16px;}
  .sign-row{display:flex; justify-content:space-between; border-bottom:1px dashed #999; padding:6px 0; font-size:12px;}
  .sign-role{font-weight:bold;}
  @media print{body{margin:0} .no-print{display:none}}
  </style></head><body>';

  $html .= '<h1>Универсальный передаточный документ (УПД)</h1>';
  $html .= '<div class="muted">Статус: 1 (счет‑фактура + передаточный документ)</div>';
  $html .= '<div class="grid">';
  $html .= '<div class="box"><div><b>Продавец:</b> '.$h($sellerName).'</div>'
    .'<div><b>ИНН/КПП:</b> '.$h($seller['inn'] ?? '').' / '.$h($seller['kpp'] ?? '').'</div>'
    .'<div><b>Адрес:</b> '.$h($seller['address'] ?? '').'</div>'
    .'</div>';
  $html .= '<div class="box"><div><b>Покупатель:</b> '.$h($buyerName).'</div>'
    .'<div><b>ИНН/КПП:</b> '.$h($buyer['inn'] ?? '').' / '.$h($buyer['kpp'] ?? '').'</div>'
    .'<div><b>Адрес:</b> '.$h($buyer['address'] ?? '').'</div>'
    .'</div>';
  $html .= '</div>';

  $html .= '<div class="grid">';
  $html .= '<div class="box"><div><b>Грузоотправитель:</b> '.$h($shipper['name'] ?? $sellerName).'</div>'
    .'<div><b>Адрес:</b> '.$h($shipper['address'] ?? '').'</div></div>';
  $html .= '<div class="box"><div><b>Грузополучатель:</b> '.$h($consignee['name'] ?? $buyerName).'</div>'
    .'<div><b>Адрес:</b> '.$h($consignee['address'] ?? '').'</div></div>';
  $html .= '</div>';

  $html .= '<div class="box"><b>Документ:</b> № '.$h($doc['doc_number'] ?? '').' от '.$h($doc['doc_date_fmt'] ?? '').'</div>';

  $html .= '<table><thead><tr>'
    .'<th>№</th><th>Наименование</th><th>Ед.</th><th>Кол-во</th><th>Цена без НДС</th>'
    .'<th>Сумма без НДС</th><th>НДС</th><th>Сумма НДС</th><th>Сумма с НДС</th>'
    .'</tr></thead><tbody>'.$rows.'</tbody></table>';

  $html .= '<div class="box" style="margin-top:10px">'
    .'<div><b>Итого без НДС:</b> '.number_format((float)($tot['sum_no_vat'] ?? 0), 2, '.', '').'</div>'
    .'<div><b>НДС:</b> '.number_format((float)($tot['vat_sum'] ?? 0), 2, '.', '').'</div>'
    .'<div><b>Итого с НДС:</b> '.number_format((float)($tot['sum_with_vat'] ?? 0), 2, '.', '').'</div>'
    .'</div>';

  $html .= '<div class="signs">'.$signHtml.'</div>';
  $html .= '</body></html>';
  return $html;
}

function upd_build_doc(PDO $pdo, int $orderId, array $opts = []): array {
  order_bootstrap($pdo);
  upd_bootstrap($pdo);
  settings_bootstrap($pdo);

  $stmt = $pdo->prepare("SELECT * FROM orders WHERE id = :id");
  $stmt->execute([':id'=>$orderId]);
  $order = $stmt->fetch();
  if (!$order) return ['error'=>'order_nf'];

  $it = $pdo->prepare("SELECT product_id, name, price, qty FROM order_items WHERE order_id = :id ORDER BY id ASC");
  $it->execute([':id'=>$orderId]);
  $items = $it->fetchAll();
  if (!$items) return ['error'=>'no_items'];

  $settings = settings_get($pdo, 'seller', []);
  $sellerType = $settings['type'] ?? 'org';
  $seller = [
    'type' => $sellerType,
    'name' => trim((string)($settings['name'] ?? '')),
    'inn' => trim((string)($settings['inn'] ?? '')),
    'kpp' => trim((string)($settings['kpp'] ?? '')),
    'fio' => trim((string)($settings['fio'] ?? '')),
    'address' => trim((string)($settings['address'] ?? '')),
    'phone' => trim((string)($settings['phone'] ?? '')),
    'email' => trim((string)($settings['email'] ?? '')),
    'bank' => [
      'name' => trim((string)($settings['bank_name'] ?? '')),
      'bik' => trim((string)($settings['bank_bik'] ?? '')),
      'corr' => trim((string)($settings['bank_corr'] ?? '')),
      'account' => trim((string)($settings['bank_account'] ?? '')),
    ],
  ];

  $errors = [];
  if ($sellerType === 'org'){
    if ($seller['name'] === '') $errors[] = 'seller_name';
    if ($seller['inn'] === '') $errors[] = 'seller_inn';
  } elseif ($sellerType === 'ip'){
    if ($seller['fio'] === '') $errors[] = 'seller_fio';
    if ($seller['inn'] === '') $errors[] = 'seller_inn';
  }
  if ($seller['address'] === '') $errors[] = 'seller_address';

  $buyerType = $order['upd_buyer_type'] ?? 'org';
  $buyerName = trim((string)($order['upd_buyer_name'] ?? $order['customer_name'] ?? ''));
  $buyerInn = trim((string)($order['upd_buyer_inn'] ?? ''));
  $buyerKpp = trim((string)($order['upd_buyer_kpp'] ?? ''));
  $buyerAddr = trim((string)($order['upd_buyer_address'] ?? $order['delivery_address'] ?? ''));
  $buyerOther = trim((string)($order['upd_buyer_other'] ?? ''));
  $buyer = [
    'type' => $buyerType,
    'name' => $buyerName,
    'inn' => $buyerInn,
    'kpp' => $buyerKpp,
    'fio' => $buyerName,
    'address' => $buyerAddr,
    'phone' => trim((string)($order['phone'] ?? '')),
    'email' => trim((string)($order['email'] ?? '')),
    'other' => $buyerOther,
  ];
  if ($buyerName === '') $errors[] = 'buyer_name';
  if ($buyerAddr === '') $errors[] = 'buyer_address';
  if (($buyerType === 'org' || $buyerType === 'ip') && $buyerInn === '') $errors[] = 'buyer_inn';
  if ($buyerType === 'person' && $buyerInn === '' && $buyerOther === '') $errors[] = 'buyer_other';

  $shipperSame = !isset($order['upd_shipper_same']) ? 1 : (int)$order['upd_shipper_same'];
  $shipper = [
    'type' => $sellerType,
    'name' => trim((string)($order['upd_shipper_name'] ?? $seller['name'] ?? '')),
    'inn' => trim((string)($order['upd_shipper_inn'] ?? $seller['inn'] ?? '')),
    'kpp' => trim((string)($order['upd_shipper_kpp'] ?? $seller['kpp'] ?? '')),
    'fio' => trim((string)($order['upd_shipper_name'] ?? $seller['fio'] ?? '')),
    'address' => trim((string)($order['upd_shipper_address'] ?? $seller['address'] ?? '')),
    'phone' => $seller['phone'],
    'email' => $seller['email'],
  ];

  $consSame = !isset($order['upd_consignee_same']) ? 1 : (int)$order['upd_consignee_same'];
  $consignee = [
    'type' => $buyerType,
    'name' => trim((string)($order['upd_consignee_name'] ?? $buyerName)),
    'inn' => $buyerInn,
    'kpp' => $buyerKpp,
    'fio' => $buyerName,
    'address' => trim((string)($order['upd_consignee_address'] ?? $buyerAddr)),
    'phone' => $buyer['phone'],
    'email' => $buyer['email'],
    'other' => $buyerOther,
  ];
  if ($consignee['address'] === '') $errors[] = 'consignee_address';

  if ($errors) return ['error'=>'validation', 'fields'=>$errors];

  $vatRate = upd_normalize_vat_rate((string)($order['upd_vat_rate'] ?? $settings['vat_rate'] ?? 'без НДС'));
  if (array_key_exists('upd_vat_included', $order)){
    $vatIncluded = !empty($order['upd_vat_included']);
  } else {
    $vatIncluded = !empty($settings['vat_included']);
  }

  $unitCode = trim((string)($settings['unit_code'] ?? '796'));
  $unitName = trim((string)($settings['unit_name'] ?? 'шт'));

  $docDate = (string)($order['upd_doc_date'] ?? '');
  if ($docDate === '') $docDate = date('Y-m-d');
  $docYear = (int)date('Y', strtotime($docDate));

  $existing = null;
  $stmt = $pdo->prepare("SELECT * FROM upd_docs WHERE order_id = :oid");
  $stmt->execute([':oid'=>$orderId]);
  $existing = $stmt->fetch();

  if ($existing){
    $seq = (int)$existing['doc_seq'];
    $docNum = (string)$existing['doc_number'];
  } else {
    $seq = upd_next_seq($pdo, $docYear);
    $docNum = upd_doc_number($docYear, $seq);
  }

  $opCode = strtoupper(trim((string)($settings['edi_operator'] ?? '000')));
  if ($opCode === '') $opCode = '000';
  $senderUid = trim((string)($settings['edi_sender_uid'] ?? ''));
  if ($senderUid === '') { $senderUid = uuid_v4(); $settings['edi_sender_uid'] = $senderUid; settings_set($pdo, 'seller', $settings); }
  $receiverUid = '';
  if ($existing){
    $existingDoc = json_decode((string)$existing['data_json'], true);
    if (is_array($existingDoc) && !empty($existingDoc['receiver_uid'])) $receiverUid = (string)$existingDoc['receiver_uid'];
  }
  if ($receiverUid === '') $receiverUid = uuid_v4();

  $dateYmd = date('Ymd', strtotime($docDate));
  $n1 = uuid_v4();
  $fileId = "ON_NSCHFDOPPR_{$opCode}{$receiverUid}_{$opCode}{$senderUid}_{$dateYmd}_{$n1}_0_0_0_0_0_00";
  $fileName = $fileId.'.xml';

  $itemsOut = [];
  $totNoVat = 0.0; $totVat = 0.0; $totWith = 0.0;
  foreach ($items as $row){
    $name = (string)($row['name'] ?? 'Товар');
    $qty = (float)($row['qty'] ?? 0);
    $price = (float)($row['price'] ?? 0);
    $calc = upd_calc_line($price, $qty, $vatRate, (bool)$vatIncluded);
    $itemsOut[] = [
      'name' => $name,
      'qty' => $qty,
      'unit_code' => $unitCode,
      'unit_name' => $unitName,
      'price_no_vat' => $calc['price_no_vat'],
      'sum_no_vat' => $calc['sum_no_vat'],
      'vat_sum' => $calc['vat_sum'],
      'sum_with_vat' => $calc['sum_with_vat'],
      'vat_rate' => $calc['vat_rate'],
    ];
    $totNoVat += $calc['sum_no_vat'];
    $totVat += $calc['vat_sum'];
    $totWith += $calc['sum_with_vat'];
  }

  $signers = [];
  $s1 = trim((string)($order['upd_signer1_fio'] ?? ''));
  $s2 = trim((string)($order['upd_signer2_fio'] ?? ''));
  if ($s1 !== ''){
    $signers[] = [
      'fio' => $s1,
      'pos' => trim((string)($order['upd_signer1_pos'] ?? '')),
      'sign_type' => trim((string)($order['upd_signer1_type'] ?? '1')),
      'authority' => trim((string)($order['upd_signer1_authority'] ?? '1')),
      'date_fmt' => upd_format_date((string)($order['upd_signer1_date'] ?? $docDate)),
    ];
  }
  if ($s2 !== ''){
    $signers[] = [
      'fio' => $s2,
      'pos' => trim((string)($order['upd_signer2_pos'] ?? '')),
      'sign_type' => trim((string)($order['upd_signer2_type'] ?? '1')),
      'authority' => trim((string)($order['upd_signer2_authority'] ?? '1')),
      'date_fmt' => upd_format_date((string)($order['upd_signer2_date'] ?? $docDate)),
    ];
  }
  if (!$signers) return ['error'=>'no_signers'];

  $basisName = trim((string)($settings['basis_name'] ?? 'Заказ'));
  $basisDate = $order['created_at'] ? date('Y-m-d', strtotime((string)$order['created_at'])) : $docDate;

  $doc = [
    'order_id' => $orderId,
    'doc_number' => $docNum,
    'doc_date' => $docDate,
    'doc_date_fmt' => upd_format_date($docDate),
    'info_date' => upd_format_date($docDate),
    'info_time' => upd_format_time(null),
    'doc_title' => 'УПД',
    'file_id' => $fileId,
    'file_name' => $fileName,
    'receiver_uid' => $receiverUid,
    'seller' => $seller,
    'buyer' => $buyer,
    'shipper' => $shipper,
    'shipper_same' => $shipperSame ? 1 : 0,
    'consignee' => $consignee,
    'consignee_same' => $consSame ? 1 : 0,
    'currency_code' => '643',
    'currency_name' => 'Российский рубль',
    'vat_rate' => $vatRate,
    'vat_included' => $vatIncluded ? 1 : 0,
    'items' => $itemsOut,
    'totals' => [
      'sum_no_vat' => round($totNoVat, 2),
      'vat_sum' => round($totVat, 2),
      'sum_with_vat' => round($totWith, 2),
      'vat_rate' => $vatRate,
    ],
    'signers' => $signers,
    'operation_content' => trim((string)($settings['operation_content'] ?? 'Отгрузка товаров')),
    'basis_name' => $basisName === '' ? 'Заказ' : $basisName,
    'basis_number' => (string)$orderId,
    'basis_date' => $basisDate,
    'basis_date_fmt' => upd_format_date($basisDate),
    'transfer_date_fmt' => upd_format_date($docDate),
  ];

  $json = json_encode($doc, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  if ($existing){
    $stmt = $pdo->prepare("UPDATE upd_docs SET doc_date = :d, file_id = :fid, file_name = :fn, data_json = :js WHERE order_id = :oid");
    $stmt->execute([':d'=>$docDate, ':fid'=>$fileId, ':fn'=>$fileName, ':js'=>$json, ':oid'=>$orderId]);
    $docId = (int)$existing['id'];
  } else {
    $stmt = $pdo->prepare("INSERT INTO upd_docs (order_id, doc_year, doc_seq, doc_number, doc_date, file_id, file_name, data_json)
      VALUES (:oid,:y,:s,:n,:d,:fid,:fn,:js)");
    $stmt->execute([
      ':oid'=>$orderId, ':y'=>$docYear, ':s'=>$seq, ':n'=>$docNum, ':d'=>$docDate, ':fid'=>$fileId, ':fn'=>$fileName, ':js'=>$json
    ]);
    $docId = (int)$pdo->lastInsertId();
  }

  try {
    $pdo->prepare("UPDATE orders SET checklist_docs = 1 WHERE id = :id")->execute([':id'=>$orderId]);
  } catch (Throwable $e) {}

  $doc['id'] = $docId;
  return $doc;
}


function order_items_with_availability(PDO $pdo, int $orderId): array {
  stock_bootstrap($pdo);
  $it = $pdo->prepare("SELECT id, product_id, name, price, qty FROM order_items WHERE order_id = :id ORDER BY id ASC");
  $it->execute([':id'=>$orderId]);
  $items = $it->fetchAll();
  if (!$items) return [];

  $productIds = array_values(array_unique(array_filter(array_map(fn($x)=>(int)$x['product_id'], $items))));
  $productIds = array_filter($productIds, fn($x)=>$x>0);
  $stockMap = [];
  if ($productIds){
    $in = implode(',', array_map('intval', $productIds));
    $rows = $pdo->query("SELECT id, stock_qty FROM products WHERE id IN ($in)")->fetchAll();
    foreach ($rows as $r){ $stockMap[(int)$r['id']] = (int)$r['stock_qty']; }
  }
  $reservedTotal = $productIds ? stock_reserved_map($pdo, $productIds) : [];
  $reservedByOrder = stock_reserved_by_order($pdo, $orderId);

  foreach ($items as &$row){
    $pid = (int)($row['product_id'] ?? 0);
    if ($pid > 0){
      $stock = (int)($stockMap[$pid] ?? 0);
      $resTotal = (int)($reservedTotal[$pid] ?? 0);
      $resThis = (int)($reservedByOrder[$pid] ?? 0);
      $resOther = $resTotal - $resThis;
      if ($resOther < 0) $resOther = 0;
      $availableForOrder = $stock - $resOther;
      if ($availableForOrder < 0) $availableForOrder = 0;
      $need = (int)($row['qty'] ?? 0);
      $shortage = $need > $availableForOrder ? ($need - $availableForOrder) : 0;
      $row['available_qty'] = $availableForOrder;
      $row['reserved_by_order'] = $resThis;
      $row['reserved_other'] = $resOther;
      $row['shortage_qty'] = $shortage;
    } else {
      $row['available_qty'] = null;
      $row['reserved_by_order'] = null;
      $row['reserved_other'] = null;
      $row['shortage_qty'] = null;
    }
  }
  unset($row);
  return $items;
}

function stock_attach_product_stats(PDO $pdo, array $rows): array {
  if (!$rows) return $rows;
  stock_bootstrap($pdo);
  $ids = array_values(array_unique(array_map(fn($x)=>(int)($x['id'] ?? 0), $rows)));
  $ids = array_filter($ids, fn($x)=>$x>0);
  if (!$ids) return $rows;
  $reserved = stock_reserved_map($pdo, $ids);
  $onOrder  = stock_on_order_map($pdo, $ids);
  foreach ($rows as &$r){
    $id = (int)($r['id'] ?? 0);
    $res = (int)($reserved[$id] ?? 0);
    $on  = (int)($onOrder[$id] ?? 0);
    $stock = (int)($r['stock_qty'] ?? 0);
    $avail = $stock - $res;
    if ($avail < 0) $avail = 0;
    $r['reserved_qty'] = $res;
    $r['available_qty'] = $avail;
    $r['on_order_qty'] = $on;
    if (!isset($r['supply_mode']) || $r['supply_mode'] === null || $r['supply_mode'] === '') {
      $r['supply_mode'] = 'stock';
    }
  }
  unset($r);
  return $rows;
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
    product_bootstrap($pdo);

    if ($method === 'GET' && count($segments) === 1){
      $published = isset($_GET['published']) ? $_GET['published'] : null;
      if ($published !== null && ($published === 'true' || $published === '1')){
        $stmt = $pdo->query("SELECT * FROM products WHERE published = 1 ORDER BY id ASC");
      } else {
        $stmt = $pdo->query("SELECT * FROM products ORDER BY id ASC");
      }
      $rows = $stmt->fetchAll();
      $rows = stock_attach_product_stats($pdo, $rows);
      out(200, $rows);
    }

    if ($method === 'POST' && count($segments) === 1){
      $b = read_json();
      if (!isset($b['name']) || trim((string)$b['name']) === '') out(400, ['error'=>'name']);
      $name = trim((string)$b['name']);
      $mode = (string)($b['supply_mode'] ?? 'stock');
      if (!in_array($mode, ['stock','mto','mixed'], true)) $mode = 'stock';
      $seoSlug = trim((string)($b['seo_slug'] ?? ''));
      if ($seoSlug === '') $seoSlug = seo_slugify($name);
      if ($seoSlug === '') $seoSlug = (string)(time());
      $seoSlug = seo_slug_unique($pdo, $seoSlug);
      $seoTitle = trim((string)($b['seo_title'] ?? ''));
      if ($seoTitle === '') $seoTitle = $name;
      $seoDesc = trim((string)($b['seo_description'] ?? ''));
      if ($seoDesc === '') $seoDesc = trim((string)($b['description'] ?? ''));
      $seoH1 = trim((string)($b['seo_h1'] ?? ''));
      if ($seoH1 === '') $seoH1 = $name;
      $seoOgTitle = trim((string)($b['seo_og_title'] ?? ''));
      if ($seoOgTitle === '') $seoOgTitle = $seoTitle;
      $seoOgDesc = trim((string)($b['seo_og_description'] ?? ''));
      if ($seoOgDesc === '') $seoOgDesc = $seoDesc;
      $stmt = $pdo->prepare("INSERT INTO products
        (name, price, published, image_url, shelves, material, construction, perforation, shelf_thickness, description, stock_qty, lead_time_days, supply_mode,
         seo_slug, seo_title, seo_description, seo_keywords, seo_h1, seo_robots, seo_canonical, seo_og_title, seo_og_description)
        VALUES (:name, :price, :published, :image_url, :shelves, :material, :construction, :perforation, :shelf_thickness, :description, :stock_qty, :lead_time_days, :supply_mode,
         :seo_slug, :seo_title, :seo_description, :seo_keywords, :seo_h1, :seo_robots, :seo_canonical, :seo_og_title, :seo_og_description)");
      $stmt->execute([
        ':name' => $name,
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
        ':supply_mode' => $mode,
        ':seo_slug' => $seoSlug,
        ':seo_title' => $seoTitle,
        ':seo_description' => $seoDesc,
        ':seo_keywords' => (string)($b['seo_keywords'] ?? ''),
        ':seo_h1' => $seoH1,
        ':seo_robots' => (string)($b['seo_robots'] ?? ''),
        ':seo_canonical' => (string)($b['seo_canonical'] ?? ''),
        ':seo_og_title' => $seoOgTitle,
        ':seo_og_description' => $seoOgDesc,
      ]);
      $id = (int)$pdo->lastInsertId();
      out(200, ['id'=>$id]);
    }

    if (count($segments) === 2){
      $id = (int)$segments[1];

      if ($method === 'GET'){
        $stmt = $pdo->prepare("SELECT * FROM products WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        $row = $stmt->fetch();
        if (!$row) out(404, ['error'=>'nf']);
        $rows = stock_attach_product_stats($pdo, [$row]);
        out(200, $rows[0] ?? $row);
      }

      if ($method === 'PUT' || $method === 'PATCH'){
        $b = read_json();
        $fields = ['name','price','published','image_url','shelves','material','construction','perforation','shelf_thickness','description','stock_qty','lead_time_days','supply_mode','seo_slug','seo_title','seo_description','seo_keywords','seo_h1','seo_robots','seo_canonical','seo_og_title','seo_og_description'];
        $set = [];
        $args = [':id'=>$id];
        foreach($fields as $f){
          if (array_key_exists($f, $b)){
            $set[] = "$f = :$f";
            if ($f === 'supply_mode'){
              $mode = (string)$b[$f];
              if (!in_array($mode, ['stock','mto','mixed'], true)) $mode = 'stock';
              $args[":$f"] = $mode;
            } elseif ($f === 'seo_slug') {
              $slug = seo_slugify((string)$b[$f]);
              if ($slug === ''){
                $nameForSlug = trim((string)($b['name'] ?? ''));
                $slug = seo_slugify($nameForSlug);
              }
              if ($slug === '') $slug = (string)$id;
              $slug = seo_slug_unique($pdo, $slug, $id);
              $args[":$f"] = $slug;
            } else {
              $args[":$f"] = in_array($f, ['price']) ? (float)$b[$f]
              : (in_array($f, ['shelves','stock_qty','lead_time_days']) ? (int)$b[$f]
              : ($f==='published' ? (!empty($b[$f]) ? 1 : 0) : (string)$b[$f]));
            }
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
    order_bootstrap($pdo);

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
        try { stock_plan_order($pdo, $oid); } catch (Throwable $e) {}
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
        $o['items'] = order_items_with_availability($pdo, $id);
        out(200, $o);
      }

      if ($method === 'PATCH'){
        $b = read_json();
        $stmt = $pdo->prepare("SELECT * FROM orders WHERE id = :id");
        $stmt->execute([':id'=>$id]);
        $prev = $stmt->fetch();
        if (!$prev) out(404, ['error'=>'nf']);
        $fields = [
          'note','cancel_reason','customer_name','phone','email','total','status','delivery_type','delivery_address',
          'payment_status','payment_method','payment_amount','payment_date',
          'checklist_contacted','checklist_confirmed','checklist_picked','checklist_shipped','checklist_docs',
          // UПД
          'upd_buyer_type','upd_buyer_name','upd_buyer_inn','upd_buyer_kpp','upd_buyer_address','upd_buyer_other',
          'upd_shipper_same','upd_shipper_name','upd_shipper_inn','upd_shipper_kpp','upd_shipper_address',
          'upd_consignee_same','upd_consignee_name','upd_consignee_address',
          'upd_doc_date','upd_vat_rate','upd_vat_included',
          'upd_signer1_fio','upd_signer1_pos','upd_signer1_type','upd_signer1_authority','upd_signer1_date',
          'upd_signer2_fio','upd_signer2_pos','upd_signer2_type','upd_signer2_authority','upd_signer2_date'
        ];
        $updInvalidateFields = [
          'customer_name','phone','email','delivery_address',
          'upd_buyer_type','upd_buyer_name','upd_buyer_inn','upd_buyer_kpp','upd_buyer_address','upd_buyer_other',
          'upd_shipper_same','upd_shipper_name','upd_shipper_inn','upd_shipper_kpp','upd_shipper_address',
          'upd_consignee_same','upd_consignee_name','upd_consignee_address',
          'upd_doc_date','upd_vat_rate','upd_vat_included',
          'upd_signer1_fio','upd_signer1_pos','upd_signer1_type','upd_signer1_authority','upd_signer1_date',
          'upd_signer2_fio','upd_signer2_pos','upd_signer2_type','upd_signer2_authority','upd_signer2_date'
        ];
        $updDirty = false;
        $set = [];
        $args = [':id'=>$id];
        $norm = function($v){
          if ($v === null) return '';
          if (is_bool($v)) return $v ? '1' : '0';
          return trim((string)$v);
        };
        foreach($fields as $f){
          if (array_key_exists($f, $b)){
            $set[] = "$f = :$f";
            if (in_array($f, ['total','payment_amount'], true)){
              $args[":$f"] = (float)$b[$f];
            } elseif (in_array($f, ['checklist_contacted','checklist_confirmed','checklist_picked','checklist_shipped','checklist_docs','upd_shipper_same','upd_consignee_same','upd_vat_included'], true)){
              $args[":$f"] = !empty($b[$f]) ? 1 : 0;
            } elseif ($f === 'payment_method'){
              $pm = (string)$b[$f];
              if (!in_array($pm, ['cash','card','transfer','bank'], true)) $pm = 'bank';
              $args[":$f"] = $pm;
            } elseif ($f === 'payment_status'){
              $ps = (string)$b[$f];
              if (!in_array($ps, ['unpaid','partial','paid'], true)) $ps = 'unpaid';
              $args[":$f"] = $ps;
            } elseif ($f === 'delivery_type'){
              $dt = (string)$b[$f];
              if (!in_array($dt, ['pickup','delivery'], true)) $dt = 'pickup';
              $args[":$f"] = $dt;
            } elseif ($f === 'upd_buyer_type'){
              $bt = (string)$b[$f];
              if (!in_array($bt, ['org','ip','person'], true)) $bt = 'org';
              $args[":$f"] = $bt;
            } elseif ($f === 'upd_vat_rate'){
              $args[":$f"] = upd_normalize_vat_rate((string)$b[$f]);
            } else {
              $args[":$f"] = (string)$b[$f];
            }
            if (!$updDirty && in_array($f, $updInvalidateFields, true)){
              $oldVal = $prev[$f] ?? null;
              $newVal = $args[":$f"];
              if ($norm($newVal) !== $norm($oldVal)) $updDirty = true;
            }
          }
        }
        if (!$set) out(400, ['error'=>'no_fields']);
        $newStatus = array_key_exists('status', $b) ? (string)$b['status'] : (string)$prev['status'];
        if ($newStatus === 'Выполнен'){
          $newPayAmount = array_key_exists('payment_amount', $b) ? (float)$b['payment_amount'] : (float)($prev['payment_amount'] ?? 0);
          $newPayMethod = array_key_exists('payment_method', $b) ? (string)$b['payment_method'] : (string)($prev['payment_method'] ?? '');
          if (!in_array($newPayMethod, ['cash','card','transfer','bank'], true)) $newPayMethod = '';
          if ($newPayAmount <= 0 || $newPayMethod === '') out(400, ['error'=>'payment_required']);
        }
        $sql = "UPDATE orders SET ".implode(',', $set)." WHERE id = :id";
        $stmt = $pdo->prepare($sql);
        $stmt->execute($args);
        if ($updDirty){
          try { upd_delete_by_order($pdo, $id); } catch (Throwable $e) {}
        }
        cf_sync_order($pdo, $id, (string)$prev['status'], $newStatus);
        stock_sync_order($pdo, $id, (string)$prev['status'], $newStatus);
        out(200, ['ok'=>true]);
      }

      if ($method === 'DELETE'){
        try { stock_cancel_order($pdo, $id); } catch (Throwable $e) {}
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
      $stmt = $pdo->prepare("SELECT id, total, status, payment_amount, payment_method FROM orders WHERE id = :id");
      $stmt->execute([':id'=>$id]);
      $prev = $stmt->fetch();
      if (!$prev) out(404, ['error'=>'nf']);
      if ($st === 'Выполнен'){
        $payAmount = (float)($prev['payment_amount'] ?? 0);
        $payMethod = (string)($prev['payment_method'] ?? '');
        if (!in_array($payMethod, ['cash','card','transfer','bank'], true)) $payMethod = '';
        if ($payAmount <= 0 || $payMethod === '') out(400, ['error'=>'payment_required']);
      }
      $stmt = $pdo->prepare("UPDATE orders SET status = :s WHERE id = :id");
      $stmt->execute([':s'=>$st, ':id'=>$id]);
      cf_sync_order($pdo, $id, (string)$prev['status'], (string)$st);
      stock_sync_order($pdo, $id, (string)$prev['status'], (string)$st);
      out(200, ['ok'=>true]);
    }

    out(404, ['error'=>'nf']);
  }

  if (($segments[0] ?? '') === 'settings'){
    $pdo = db();
    settings_bootstrap($pdo);
    if ($method === 'GET'){
      out(200, settings_get_all($pdo));
    }
    if ($method === 'POST' || $method === 'PATCH'){
      $b = read_json();
      if (!is_array($b)) out(400, ['error'=>'bad']);
      foreach ($b as $k=>$v){
        if (!is_string($k) || $k === '') continue;
        settings_set($pdo, $k, $v);
      }
      if (array_key_exists('seller', $b)){
        try { $pdo->exec("DELETE FROM upd_docs"); } catch (Throwable $e) {}
      }
      out(200, ['ok'=>true]);
    }
    out(404, ['error'=>'nf']);
  }

  if (($segments[0] ?? '') === 'upd'){
    $pdo = db();
    upd_bootstrap($pdo);

    if ($method === 'GET' && count($segments) === 1 && isset($_GET['order_id'])){
      $oid = (int)$_GET['order_id'];
      if ($oid <= 0) out(400, ['error'=>'order_required']);
      $stmt = $pdo->prepare("SELECT * FROM upd_docs WHERE order_id = :oid");
      $stmt->execute([':oid'=>$oid]);
      $row = $stmt->fetch();
      if (!$row) out(404, ['error'=>'nf']);
      $doc = json_decode((string)$row['data_json'], true);
      if (!is_array($doc)) $doc = [];
      $doc['id'] = (int)$row['id'];
      $doc['doc_number'] = $row['doc_number'];
      $doc['doc_date'] = $row['doc_date'];
      out(200, $doc);
    }

    if ($method === 'POST' && count($segments) === 1){
      $b = read_json();
      $orderId = isset($b['order_id']) ? (int)$b['order_id'] : 0;
      if ($orderId <= 0) out(400, ['error'=>'order_required']);
      $doc = upd_build_doc($pdo, $orderId, $b);
      if (!empty($doc['error'])) out(400, $doc);
      out(200, $doc);
    }

    if ($method === 'GET' && count($segments) === 2){
      $id = (int)$segments[1];
      $stmt = $pdo->prepare("SELECT * FROM upd_docs WHERE id = :id");
      $stmt->execute([':id'=>$id]);
      $row = $stmt->fetch();
      if (!$row) out(404, ['error'=>'nf']);
      $doc = json_decode((string)$row['data_json'], true);
      if (!is_array($doc)) $doc = [];
      $doc['id'] = (int)$row['id'];
      out(200, $doc);
    }

    if ($method === 'GET' && count($segments) === 3 && $segments[2] === 'xml'){
      $id = (int)$segments[1];
      $stmt = $pdo->prepare("SELECT * FROM upd_docs WHERE id = :id");
      $stmt->execute([':id'=>$id]);
      $row = $stmt->fetch();
      if (!$row) out(404, ['error'=>'nf']);
      $doc = json_decode((string)$row['data_json'], true);
      if (!is_array($doc)) out(500, ['error'=>'bad_doc']);
      $xml = upd_build_xml($doc);
      header('Content-Type: application/xml; charset=windows-1251');
      $fname = $row['file_name'] ?: 'upd.xml';
      header('Content-Disposition: attachment; filename="'.$fname.'"');
      echo $xml;
      exit;
    }

    if ($method === 'GET' && count($segments) === 3 && $segments[2] === 'print'){
      $id = (int)$segments[1];
      $stmt = $pdo->prepare("SELECT * FROM upd_docs WHERE id = :id");
      $stmt->execute([':id'=>$id]);
      $row = $stmt->fetch();
      if (!$row) out(404, ['error'=>'nf']);
      $doc = json_decode((string)$row['data_json'], true);
      if (!is_array($doc)) out(500, ['error'=>'bad_doc']);
      header('Content-Type: text/html; charset=utf-8');
      echo upd_build_print_html($doc);
      exit;
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

  if (($segments[0] ?? '') === 'stock'){
    $pdo = db();
    stock_bootstrap($pdo);
    $sub = $segments[1] ?? '';

    if ($method === 'GET' && count($segments) === 1){
      $stmt = $pdo->query("SELECT id, name, stock_qty, supply_mode FROM products ORDER BY id ASC");
      $rows = $stmt->fetchAll();
      $rows = stock_attach_product_stats($pdo, $rows);
      out(200, $rows);
    }

    if ($sub === 'movements' && $method === 'GET'){
      $pid = isset($_GET['product_id']) ? (int)$_GET['product_id'] : 0;
      $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 300;
      if ($limit < 1) $limit = 1;
      if ($limit > 1000) $limit = 1000;
      if ($pid > 0){
        $stmt = $pdo->prepare("SELECT m.*, p.name AS product_name
          FROM stock_movements m LEFT JOIN products p ON p.id = m.product_id
          WHERE m.product_id = :pid ORDER BY m.id DESC LIMIT $limit");
        $stmt->execute([':pid'=>$pid]);
      } else {
        $stmt = $pdo->query("SELECT m.*, p.name AS product_name
          FROM stock_movements m LEFT JOIN products p ON p.id = m.product_id
          ORDER BY m.id DESC LIMIT $limit");
      }
      out(200, $stmt->fetchAll());
    }

    if ($sub === 'upload' && $method === 'POST'){
      if (empty($_FILES['file']) || !is_array($_FILES['file'])) out(400, ['error'=>'no_file']);
      $file = $_FILES['file'];
      if (($file['error'] ?? UPLOAD_ERR_OK) !== UPLOAD_ERR_OK) out(400, ['error'=>'upload']);
      $orig = (string)($file['name'] ?? 'file');
      $ext = pathinfo($orig, PATHINFO_EXTENSION);
      $safeExt = $ext ? ('.' . preg_replace('/[^a-zA-Z0-9]/', '', $ext)) : '';
      $dir = __DIR__ . '/../uploads/stock_docs';
      if (!is_dir($dir)) { @mkdir($dir, 0775, true); }
      $name = date('Ymd_His') . '_' . bin2hex(random_bytes(4)) . $safeExt;
      $dest = $dir . '/' . $name;
      if (!move_uploaded_file($file['tmp_name'], $dest)) out(500, ['error'=>'save']);
      $url = '/uploads/stock_docs/' . $name;
      out(200, ['url'=>$url, 'name'=>$orig]);
    }

    if ($sub === 'incoming' && $method === 'POST'){
      $b = read_json();
      $doc = is_array($b['doc'] ?? null) ? $b['doc'] : [];
      $items = is_array($b['items'] ?? null) ? $b['items'] : [];
      if (!$items) out(400, ['error'=>'no_items']);

      $docType = (string)($doc['doc_type'] ?? 'other');
      if (!in_array($docType, ['invoice','act','receipt','other'], true)) $docType = 'other';
      $docNumber = trim((string)($doc['doc_number'] ?? ''));
      $docDate = trim((string)($doc['doc_date'] ?? ''));
      $supplier = trim((string)($doc['supplier'] ?? ''));
      $fileUrl = trim((string)($doc['file_url'] ?? ''));
      $docComment = trim((string)($doc['comment'] ?? ''));
      $createdBy = trim((string)($b['created_by'] ?? 'admin')) ?: 'admin';

      $pdo->beginTransaction();
      try{
        $stmt = $pdo->prepare("INSERT INTO stock_docs (doc_type, doc_number, doc_date, supplier, file_url, comment, created_by)
          VALUES (:t,:n,:d,:s,:f,:c,:cb)");
        $stmt->execute([
          ':t'=>$docType,
          ':n'=>$docNumber ?: null,
          ':d'=>$docDate ?: null,
          ':s'=>$supplier ?: null,
          ':f'=>$fileUrl ?: null,
          ':c'=>$docComment ?: null,
          ':cb'=>$createdBy,
        ]);
        $docId = (int)$pdo->lastInsertId();

        $insItem = $pdo->prepare("INSERT INTO stock_doc_items (doc_id, product_id, qty, price) VALUES (:doc,:pid,:qty,:price)");
        foreach ($items as $it){
          $pid = isset($it['product_id']) ? (int)$it['product_id'] : 0;
          $qty = isset($it['qty']) ? (int)$it['qty'] : 0;
          $price = isset($it['price']) ? (float)$it['price'] : null;
          if ($pid <= 0 || $qty <= 0) continue;

          $insItem->execute([':doc'=>$docId, ':pid'=>$pid, ':qty'=>$qty, ':price'=>$price]);
          $pdo->prepare("UPDATE products SET stock_qty = stock_qty + :q WHERE id = :id")
              ->execute([':q'=>$qty, ':id'=>$pid]);
          stock_insert_movement($pdo, [
            'product_id'=>$pid,
            'qty'=>$qty,
            'type'=>'in',
            'reason'=>'purchase',
            'doc_id'=>$docId,
            'comment'=>$docComment ?: 'Приход',
            'created_by'=>$createdBy,
          ]);
          stock_apply_production_on_incoming($pdo, $pid, $qty);
        }
        $pdo->commit();
        out(200, ['doc_id'=>$docId]);
      }catch(Throwable $e){
        $pdo->rollBack();
        out(500, ['error'=>'fail']);
      }
    }

    if ($sub === 'adjust' && $method === 'POST'){
      $b = read_json();
      $pid = isset($b['product_id']) ? (int)$b['product_id'] : 0;
      $delta = isset($b['qty']) ? (int)$b['qty'] : 0;
      $comment = trim((string)($b['comment'] ?? ''));
      $createdBy = trim((string)($b['created_by'] ?? 'admin')) ?: 'admin';
      if ($pid <= 0 || $delta === 0) out(400, ['error'=>'bad']);

      $pdo->beginTransaction();
      try{
        $stmt = $pdo->prepare("SELECT stock_qty FROM products WHERE id = :id");
        $stmt->execute([':id'=>$pid]);
        $row = $stmt->fetch();
        if (!$row){ $pdo->rollBack(); out(404, ['error'=>'nf']); }
        $cur = (int)$row['stock_qty'];
        $new = $cur + $delta;
        if ($new < 0) $new = 0;
        $applied = $new - $cur;
        $pdo->prepare("UPDATE products SET stock_qty = :q WHERE id = :id")
            ->execute([':q'=>$new, ':id'=>$pid]);
        if ($applied !== 0){
          stock_insert_movement($pdo, [
            'product_id'=>$pid,
            'qty'=>$applied,
            'type'=>'adjust',
            'reason'=>'manual',
            'comment'=>$comment ?: 'Корректировка',
            'created_by'=>$createdBy,
          ]);
        }
        $pdo->commit();
        out(200, ['ok'=>true, 'qty'=>$new]);
      }catch(Throwable $e){
        $pdo->rollBack();
        out(500, ['error'=>'fail']);
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
      $stmt = $pdo->prepare("SELECT id, customer_name, phone, email, total, created_at FROM orders $where");
      $stmt->execute($args);
      $rows = $stmt->fetchAll();
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
