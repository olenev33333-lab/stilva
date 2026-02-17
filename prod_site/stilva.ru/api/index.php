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
  static $bootstrapped = false;
  if ($bootstrapped) return;
  // DDL in MySQL causes implicit commit; never run bootstrap mid-transaction.
  if ($pdo->inTransaction()) return;

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

  $bootstrapped = true;
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
  $activeOrderStatuses = "'Новый','В работе','Критическое ожидание','Передан в доставку'";
  if ($productIds){
    $in = implode(',', array_map('intval', $productIds));
    $sql = "SELECT po.product_id, SUM(po.qty - po.qty_done) AS on_order
            FROM production_orders po
            LEFT JOIN orders o ON o.id = po.order_id
            WHERE po.status='open'
              AND po.product_id IN ($in)
              AND (po.order_id IS NULL OR (o.id IS NOT NULL AND o.status IN ($activeOrderStatuses)))
            GROUP BY po.product_id";
  } else {
    $sql = "SELECT po.product_id, SUM(po.qty - po.qty_done) AS on_order
            FROM production_orders po
            LEFT JOIN orders o ON o.id = po.order_id
            WHERE po.status='open'
              AND (po.order_id IS NULL OR (o.id IS NOT NULL AND o.status IN ($activeOrderStatuses)))
            GROUP BY po.product_id";
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

function stock_production_rows_by_order(PDO $pdo, int $orderId): array {
  $stmt = $pdo->prepare("SELECT * FROM production_orders
    WHERE order_id = :oid AND status = 'open'
    ORDER BY (prod_no IS NOT NULL AND prod_no <> '') DESC, id DESC");
  $stmt->execute([':oid'=>$orderId]);
  $rows = $stmt->fetchAll();
  $map = [];
  foreach ($rows as $r){
    $pid = (int)($r['product_id'] ?? 0);
    if ($pid <= 0) continue;
    if (!isset($map[$pid])) $map[$pid] = [];
    $map[$pid][] = $r;
  }
  return $map;
}

function stock_production_by_order(PDO $pdo, int $orderId): array {
  $rowsByProduct = stock_production_rows_by_order($pdo, $orderId);
  $map = [];
  foreach ($rowsByProduct as $pid => $rows){
    if (!empty($rows)) $map[(int)$pid] = $rows[0];
  }
  return $map;
}

function stock_cancel_open_production_rows(PDO $pdo, int $orderId, int $productId, ?int $exceptId = null): void {
  if ($orderId <= 0 || $productId <= 0) return;
  if ($exceptId && $exceptId > 0){
    $stmt = $pdo->prepare("UPDATE production_orders
      SET status = 'cancelled', prod_state = 'cancelled'
      WHERE order_id = :oid AND product_id = :pid AND status = 'open' AND id <> :id");
    $stmt->execute([':oid'=>$orderId, ':pid'=>$productId, ':id'=>$exceptId]);
    return;
  }
  $stmt = $pdo->prepare("UPDATE production_orders
    SET status = 'cancelled', prod_state = 'cancelled'
    WHERE order_id = :oid AND product_id = :pid AND status = 'open'");
  $stmt->execute([':oid'=>$orderId, ':pid'=>$productId]);
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

  $orderQtyByProduct = [];
  foreach ($items as $it){
    $pid = (int)($it['product_id'] ?? 0);
    $qty = (int)($it['qty'] ?? 0);
    if ($pid <= 0 || $qty <= 0) continue;
    $orderQtyByProduct[$pid] = ($orderQtyByProduct[$pid] ?? 0) + $qty;
  }
  if (!$orderQtyByProduct) return;

  $productIds = array_map('intval', array_keys($orderQtyByProduct));
  $in = implode(',', array_map('intval', $productIds));
  $products = $pdo->query("SELECT id, stock_qty, supply_mode FROM products WHERE id IN ($in)")->fetchAll();
  $productMap = [];
  foreach ($products as $p){ $productMap[(int)$p['id']] = $p; }

  $reservedTotal = stock_reserved_map($pdo, $productIds);
  $reservedByOrder = stock_reserved_by_order($pdo, $orderId);
  $prodRowsByOrder = stock_production_rows_by_order($pdo, $orderId);
  $prodHead = production_order_head($pdo, $orderId);

  foreach ($orderQtyByProduct as $pid => $orderQty){
    $p = $productMap[$pid] ?? null;
    if (!$p) continue;
    $mode = $p['supply_mode'] ?: 'stock';

    $currentReserve = (int)($reservedByOrder[$pid] ?? 0);
    $reservedOther = (int)($reservedTotal[$pid] ?? 0) - $currentReserve;
    if ($reservedOther < 0) $reservedOther = 0;
    $availableForOrder = max(0, (int)$p['stock_qty'] - $reservedOther);
    $desiredReserve = ($mode === 'mto') ? 0 : min($availableForOrder, $orderQty);
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
      $reservedByOrder[$pid] = $currentReserve + $delta;
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
      $reservedByOrder[$pid] = max(0, $currentReserve - abs($delta));
    }

    $missing = ($mode === 'mto') ? $orderQty : max(0, $orderQty - $desiredReserve);
    $prodRows = $prodRowsByOrder[$pid] ?? [];
    $prod = !empty($prodRows) ? $prodRows[0] : null;
    if ($missing > 0){
      if ($prod){
        $qtyDone = (int)$prod['qty_done'];
        $target = max($missing, $qtyDone);
        $newStatus = ($qtyDone >= $target) ? 'closed' : 'open';
        $set = ["qty = :q", "status = :st"];
        $args = [':q'=>$target, ':st'=>$newStatus, ':id'=>$prod['id']];
        if (!empty($prodHead['prod_no'])){
          $set[] = "prod_no = :no";
          $set[] = "prod_address = :addr";
          $set[] = "deadline_date = :dl";
          $set[] = "source = 'auto'";
          $set[] = "prod_state = IF(prod_state='cancelled','draft',prod_state)";
          $args[':no'] = $prodHead['prod_no'];
          $args[':addr'] = $prodHead['prod_address'] ?: null;
          $args[':dl'] = $prodHead['deadline_date'] ?: null;
        }
        $upd = $pdo->prepare("UPDATE production_orders SET ".implode(',', $set)." WHERE id = :id");
        $upd->execute($args);
        stock_cancel_open_production_rows($pdo, $orderId, $pid, (int)$prod['id']);
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
        if (!empty($prodHead['prod_no'])){
          $pdo->prepare("UPDATE production_orders
            SET prod_no = :no,
                prod_address = :addr,
                deadline_date = :dl,
                source = 'auto',
                prod_state = IF(prod_state='cancelled','draft',prod_state)
            WHERE id = :id")->execute([
              ':no'=>$prodHead['prod_no'],
              ':addr'=>$prodHead['prod_address'] ?: null,
              ':dl'=>$prodHead['deadline_date'] ?: null,
            ':id'=>(int)$pdo->lastInsertId()
            ]);
        }
      }
    } else {
      stock_cancel_open_production_rows($pdo, $orderId, $pid);
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

  $orderQtyByProduct = [];
  foreach ($items as $it){
    $pid = (int)($it['product_id'] ?? 0);
    $qty = (int)($it['qty'] ?? 0);
    if ($pid <= 0 || $qty <= 0) continue;
    $orderQtyByProduct[$pid] = ($orderQtyByProduct[$pid] ?? 0) + $qty;
  }
  if (!$orderQtyByProduct) return;

  $productIds = array_map('intval', array_keys($orderQtyByProduct));
  $in = implode(',', array_map('intval', $productIds));
  $products = $pdo->query("SELECT id, stock_qty, supply_mode FROM products WHERE id IN ($in)")->fetchAll();
  $productMap = [];
  foreach ($products as $p){ $productMap[(int)$p['id']] = $p; }

  $reservedTotal = stock_reserved_map($pdo, $productIds);
  $reservedByOrder = stock_reserved_by_order($pdo, $orderId);
  $prodRowsByOrder = stock_production_rows_by_order($pdo, $orderId);
  $prodHead = production_order_head($pdo, $orderId);

  foreach ($orderQtyByProduct as $pid => $orderQty){
    $p = $productMap[$pid] ?? null;
    if (!$p) continue;
    $mode = $p['supply_mode'] ?: 'stock';

    $currentReserve = (int)($reservedByOrder[$pid] ?? 0);
    $reservedOther = (int)($reservedTotal[$pid] ?? 0) - $currentReserve;
    if ($reservedOther < 0) $reservedOther = 0;
    $availableForOrder = max(0, (int)$p['stock_qty'] - $reservedOther);
    $missing = ($mode === 'mto') ? $orderQty : max(0, $orderQty - $availableForOrder);
    $prodRows = $prodRowsByOrder[$pid] ?? [];
    $prod = !empty($prodRows) ? $prodRows[0] : null;
    if ($missing > 0){
      if ($prod){
        $qtyDone = (int)$prod['qty_done'];
        $target = max($missing, $qtyDone);
        $newStatus = ($qtyDone >= $target) ? 'closed' : 'open';
        $set = ["qty = :q", "status = :st"];
        $args = [':q'=>$target, ':st'=>$newStatus, ':id'=>$prod['id']];
        if (!empty($prodHead['prod_no'])){
          $set[] = "prod_no = :no";
          $set[] = "prod_address = :addr";
          $set[] = "deadline_date = :dl";
          $set[] = "source = 'auto'";
          $set[] = "prod_state = IF(prod_state='cancelled','draft',prod_state)";
          $args[':no'] = $prodHead['prod_no'];
          $args[':addr'] = $prodHead['prod_address'] ?: null;
          $args[':dl'] = $prodHead['deadline_date'] ?: null;
        }
        $upd = $pdo->prepare("UPDATE production_orders SET ".implode(',', $set)." WHERE id = :id");
        $upd->execute($args);
        stock_cancel_open_production_rows($pdo, $orderId, $pid, (int)$prod['id']);
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
        if (!empty($prodHead['prod_no'])){
          $pdo->prepare("UPDATE production_orders
            SET prod_no = :no,
                prod_address = :addr,
                deadline_date = :dl,
                source = 'auto',
                prod_state = IF(prod_state='cancelled','draft',prod_state)
            WHERE id = :id")->execute([
              ':no'=>$prodHead['prod_no'],
              ':addr'=>$prodHead['prod_address'] ?: null,
              ':dl'=>$prodHead['deadline_date'] ?: null,
            ':id'=>(int)$pdo->lastInsertId()
            ]);
        }
      }
    } else {
      stock_cancel_open_production_rows($pdo, $orderId, $pid);
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

function stock_apply_by_order_status(PDO $pdo, int $orderId, string $status): void {
  $reserveStatuses = ['В работе','Критическое ожидание','Передан в доставку'];
  if (in_array($status, $reserveStatuses, true)){
    stock_apply_reservation($pdo, $orderId);
    return;
  }
  if ($status === 'Выполнен'){
    stock_release_reserve($pdo, $orderId);
    return;
  }
  if ($status === 'Новый'){
    stock_release_reserve($pdo, $orderId);
    stock_plan_order($pdo, $orderId);
    return;
  }
  stock_cancel_order($pdo, $orderId);
}

function stock_rebalance_order_current_status(PDO $pdo, int $orderId): void {
  if ($orderId <= 0) return;
  $stmt = $pdo->prepare("SELECT status FROM orders WHERE id = :id");
  $stmt->execute([':id'=>$orderId]);
  $row = $stmt->fetch();
  if (!$row) return;
  stock_apply_by_order_status($pdo, (int)$orderId, (string)($row['status'] ?? ''));
}

function stock_sync_order(PDO $pdo, int $orderId, string $prevStatus, string $newStatus){
  try {
    if ($prevStatus === $newStatus) return;
    if ($prevStatus === 'Выполнен' && $newStatus !== 'Выполнен'){
      stock_unfulfill_order($pdo, $orderId);
    }
    if ($newStatus === 'Выполнен') {
      stock_apply_reservation($pdo, $orderId);
      stock_fulfill_order($pdo, $orderId);
      return;
    }
    stock_apply_by_order_status($pdo, $orderId, $newStatus);
  } catch (Throwable $e) {
    // intentionally ignore to avoid breaking order flow
  }
}

function production_stage_keys(): array {
  return ['stage_cut','stage_bend','stage_fitting','stage_assembly','stage_qc','stage_stock'];
}

function production_state_keys(): array {
  return ['draft','confirmed','in_work','ready','closed','cancelled'];
}

function production_stage_state(string $value): string {
  return in_array($value, ['todo','progress','done'], true) ? $value : 'todo';
}

function production_order_state(string $value): string {
  return in_array($value, production_state_keys(), true) ? $value : 'draft';
}

function production_bootstrap(PDO $pdo): void {
  static $bootstrapped = false;
  if ($bootstrapped) return;
  if ($pdo->inTransaction()) return;

  stock_bootstrap($pdo);
  $alter = [
    'prod_no' => "ALTER TABLE production_orders ADD COLUMN prod_no VARCHAR(40) NULL AFTER order_id",
    'prod_state' => "ALTER TABLE production_orders ADD COLUMN prod_state ENUM('draft','confirmed','in_work','ready','closed','cancelled') NOT NULL DEFAULT 'draft' AFTER status",
    'prod_address' => "ALTER TABLE production_orders ADD COLUMN prod_address VARCHAR(255) NULL AFTER prod_state",
    'deadline_date' => "ALTER TABLE production_orders ADD COLUMN deadline_date DATE NULL AFTER prod_address",
    'source' => "ALTER TABLE production_orders ADD COLUMN source ENUM('auto','manual') NOT NULL DEFAULT 'auto' AFTER deadline_date",
    'stage_cut' => "ALTER TABLE production_orders ADD COLUMN stage_cut ENUM('todo','progress','done') NOT NULL DEFAULT 'todo' AFTER source",
    'stage_bend' => "ALTER TABLE production_orders ADD COLUMN stage_bend ENUM('todo','progress','done') NOT NULL DEFAULT 'todo' AFTER stage_cut",
    'stage_fitting' => "ALTER TABLE production_orders ADD COLUMN stage_fitting ENUM('todo','progress','done') NOT NULL DEFAULT 'todo' AFTER stage_bend",
    'stage_assembly' => "ALTER TABLE production_orders ADD COLUMN stage_assembly ENUM('todo','progress','done') NOT NULL DEFAULT 'todo' AFTER stage_fitting",
    'stage_qc' => "ALTER TABLE production_orders ADD COLUMN stage_qc ENUM('todo','progress','done') NOT NULL DEFAULT 'todo' AFTER stage_assembly",
    'stage_stock' => "ALTER TABLE production_orders ADD COLUMN stage_stock ENUM('todo','progress','done') NOT NULL DEFAULT 'todo' AFTER stage_qc",
    'updated_at' => "ALTER TABLE production_orders ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at"
  ];
  foreach ($alter as $column => $sql){
    try {
      if (stock_column_missing($pdo, 'production_orders', $column)){
        $pdo->exec($sql);
      }
    } catch (Throwable $e) {
      // bootstrap should not break API flow
    }
  }

  try { $pdo->exec("CREATE INDEX idx_prod_no ON production_orders (prod_no)"); } catch (Throwable $e) {}
  try { $pdo->exec("CREATE INDEX idx_deadline_date ON production_orders (deadline_date)"); } catch (Throwable $e) {}
  try { $pdo->exec("CREATE INDEX idx_prod_state ON production_orders (prod_state)"); } catch (Throwable $e) {}

  $pdo->exec("CREATE TABLE IF NOT EXISTS production_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    prod_no VARCHAR(40) NOT NULL,
    order_id INT NULL,
    file_url VARCHAR(512) NOT NULL,
    file_name VARCHAR(255) NULL,
    created_by VARCHAR(64) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_prod_no (prod_no),
    KEY idx_order_id (order_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

  $bootstrapped = true;
}

function production_next_no(PDO $pdo): string {
  production_bootstrap($pdo);
  $year = date('Y');
  $prefix = 'PZ-' . $year . '-';
  $stmt = $pdo->prepare("SELECT MAX(CAST(SUBSTRING_INDEX(prod_no, '-', -1) AS UNSIGNED)) AS m
    FROM production_orders
    WHERE prod_no LIKE :pref");
  $stmt->execute([':pref' => $prefix . '%']);
  $row = $stmt->fetch();
  $seq = ($row && $row['m'] !== null) ? ((int)$row['m'] + 1) : 1;
  return $prefix . str_pad((string)$seq, 4, '0', STR_PAD_LEFT);
}

function production_order_head(PDO $pdo, int $orderId): array {
  if ($orderId <= 0) return ['prod_no'=>'', 'prod_address'=>'', 'deadline_date'=>''];
  $stmt = $pdo->prepare("SELECT prod_no, prod_address, deadline_date
    FROM production_orders
    WHERE order_id = :oid AND prod_no IS NOT NULL AND prod_no <> ''
    ORDER BY id DESC
    LIMIT 1");
  $stmt->execute([':oid'=>$orderId]);
  $row = $stmt->fetch();
  if (!$row) return ['prod_no'=>'', 'prod_address'=>'', 'deadline_date'=>''];
  return [
    'prod_no'=>(string)($row['prod_no'] ?? ''),
    'prod_address'=>(string)($row['prod_address'] ?? ''),
    'deadline_date'=>(string)($row['deadline_date'] ?? ''),
  ];
}

function production_fetch_rows(PDO $pdo, string $prodNo): array {
  $stmt = $pdo->prepare("SELECT po.*, p.name AS product_name
    FROM production_orders po
    LEFT JOIN products p ON p.id = po.product_id
    WHERE po.prod_no = :no
    ORDER BY po.id ASC");
  $stmt->execute([':no'=>$prodNo]);
  return $stmt->fetchAll();
}

function production_fetch_files(PDO $pdo, string $prodNo): array {
  $stmt = $pdo->prepare("SELECT * FROM production_files WHERE prod_no = :no ORDER BY id DESC");
  $stmt->execute([':no'=>$prodNo]);
  return $stmt->fetchAll();
}

function production_stage_progress(array $row): float {
  $w = ['todo'=>0.0,'progress'=>0.5,'done'=>1.0];
  $sum = 0.0;
  foreach (production_stage_keys() as $k){
    $state = production_stage_state((string)($row[$k] ?? 'todo'));
    $sum += $w[$state] ?? 0.0;
  }
  return round(($sum / 6.0) * 100.0, 1);
}

function production_group_state(array $rows): string {
  if (!$rows) return 'draft';
  $allCancelled = true;
  $allStockDone = true;
  $allQcDone = true;
  $anyStarted = false;
  $anyConfirmed = false;
  foreach ($rows as $r){
    $state = production_order_state((string)($r['prod_state'] ?? 'draft'));
    $status = (string)($r['status'] ?? 'open');
    if (!($state === 'cancelled' || $status === 'cancelled')) $allCancelled = false;
    $stageStock = production_stage_state((string)($r['stage_stock'] ?? 'todo'));
    $stageQc = production_stage_state((string)($r['stage_qc'] ?? 'todo'));
    if ($stageStock !== 'done') $allStockDone = false;
    if ($stageQc !== 'done') $allQcDone = false;
    foreach (production_stage_keys() as $k){
      $sv = production_stage_state((string)($r[$k] ?? 'todo'));
      if ($sv !== 'todo') $anyStarted = true;
    }
    if (in_array($state, ['confirmed','in_work','ready','closed'], true)) $anyConfirmed = true;
  }
  if ($allCancelled) return 'cancelled';
  if ($allStockDone) return 'closed';
  if ($allQcDone) return 'ready';
  if ($anyStarted) return 'in_work';
  if ($anyConfirmed) return 'confirmed';
  return 'draft';
}

function production_state_to_row_status(string $state): string {
  if ($state === 'closed') return 'closed';
  if ($state === 'cancelled') return 'cancelled';
  return 'open';
}

function production_touch_rows_state(PDO $pdo, string $prodNo, string $state): void {
  $state = production_order_state($state);
  if ($state === 'cancelled'){
    $stmt = $pdo->prepare("UPDATE production_orders
      SET prod_state = 'cancelled', status = 'cancelled'
      WHERE prod_no = :no");
    $stmt->execute([':no'=>$prodNo]);
    return;
  }
  if ($state === 'closed'){
    $stmt = $pdo->prepare("UPDATE production_orders
      SET prod_state = 'closed', status = 'closed'
      WHERE prod_no = :no");
    $stmt->execute([':no'=>$prodNo]);
    return;
  }
  $stmt = $pdo->prepare("UPDATE production_orders
    SET prod_state = CASE WHEN stage_stock = 'done' THEN 'closed' ELSE :st END,
        status = CASE WHEN stage_stock = 'done' THEN 'closed' ELSE 'open' END
    WHERE prod_no = :no");
  $stmt->execute([':st'=>$state, ':no'=>$prodNo]);
}

function production_make_item_stock_done(PDO $pdo, array $row): void {
  $id = (int)($row['id'] ?? 0);
  $pid = (int)($row['product_id'] ?? 0);
  $qty = (int)($row['qty'] ?? 0);
  $qtyDone = (int)($row['qty_done'] ?? 0);
  if ($id <= 0 || $pid <= 0) return;
  $delta = $qty - $qtyDone;
  if ($delta < 0) $delta = 0;
  if ($delta > 0){
    $pdo->prepare("UPDATE products SET stock_qty = stock_qty + :q WHERE id = :id")
      ->execute([':q'=>$delta, ':id'=>$pid]);
    stock_insert_movement($pdo, [
      'product_id'=>$pid,
      'qty'=>$delta,
      'type'=>'in',
      'reason'=>'production',
      'order_id'=>isset($row['order_id']) ? (int)$row['order_id'] : null,
      'comment'=>'Приход из производства '.(string)($row['prod_no'] ?? ''),
      'created_by'=>'admin',
    ]);
  }
  $pdo->prepare("UPDATE production_orders
    SET qty_done = qty, stage_stock = 'done', prod_state = 'closed', status = 'closed'
    WHERE id = :id")->execute([':id'=>$id]);
  $orderId = isset($row['order_id']) ? (int)$row['order_id'] : 0;
  if ($orderId > 0){
    stock_rebalance_order_current_status($pdo, $orderId);
  }
}

function production_rows_to_job(array $rows, array $files = []): array {
  if (!$rows) return [];
  $first = $rows[0];
  $items = [];
  $sumQty = 0;
  $sumDone = 0;
  $sumProgress = 0.0;
  foreach ($rows as $r){
    $progress = production_stage_progress($r);
    $sumProgress += $progress;
    $sumQty += (int)($r['qty'] ?? 0);
    $sumDone += (int)($r['qty_done'] ?? 0);
    $item = [
      'id' => (int)$r['id'],
      'product_id' => (int)$r['product_id'],
      'product_name' => (string)($r['product_name'] ?? ('ID '.(int)$r['product_id'])),
      'qty' => (int)($r['qty'] ?? 0),
      'qty_done' => (int)($r['qty_done'] ?? 0),
      'status' => (string)($r['status'] ?? 'open'),
      'prod_state' => production_order_state((string)($r['prod_state'] ?? 'draft')),
      'stage_cut' => production_stage_state((string)($r['stage_cut'] ?? 'todo')),
      'stage_bend' => production_stage_state((string)($r['stage_bend'] ?? 'todo')),
      'stage_fitting' => production_stage_state((string)($r['stage_fitting'] ?? 'todo')),
      'stage_assembly' => production_stage_state((string)($r['stage_assembly'] ?? 'todo')),
      'stage_qc' => production_stage_state((string)($r['stage_qc'] ?? 'todo')),
      'stage_stock' => production_stage_state((string)($r['stage_stock'] ?? 'todo')),
      'progress_pct' => $progress,
      'comment' => (string)($r['comment'] ?? ''),
      'created_at' => (string)($r['created_at'] ?? ''),
      'updated_at' => (string)($r['updated_at'] ?? ''),
    ];
    $items[] = $item;
  }

  $state = production_group_state($rows);
  $progressGroup = count($rows) > 0 ? round($sumProgress / count($rows), 1) : 0.0;
  return [
    'prod_no' => (string)($first['prod_no'] ?? ''),
    'order_id' => isset($first['order_id']) ? (int)$first['order_id'] : null,
    'state' => $state,
    'address' => (string)($first['prod_address'] ?? ''),
    'deadline_date' => (string)($first['deadline_date'] ?? ''),
    'comment' => (string)($first['comment'] ?? ''),
    'created_at' => (string)($first['created_at'] ?? ''),
    'updated_at' => (string)($first['updated_at'] ?? ''),
    'items_count' => count($items),
    'total_qty' => $sumQty,
    'total_done' => $sumDone,
    'progress_pct' => $progressGroup,
    'files_count' => count($files),
    'files' => $files,
    'items' => $items,
  ];
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

function notifications_settings(PDO $pdo): array {
  $raw = settings_get($pdo, 'notifications', []);
  if (!is_array($raw)) $raw = [];
  $token = trim((string)($raw['telegram_bot_token'] ?? ''));
  $chatId = trim((string)($raw['telegram_chat_id'] ?? ''));
  $emailTo = trim((string)($raw['email_to'] ?? ''));
  $emailSubject = trim((string)($raw['email_subject'] ?? 'Новая заявка STILVA'));
  $enabled = !empty($raw['telegram_enabled']) && $token !== '' && $chatId !== '';
  $emailEnabled = !empty($raw['email_enabled']) && $emailTo !== '';
  return [
    'telegram_enabled' => $enabled,
    'telegram_bot_token' => $token,
    'telegram_chat_id' => $chatId,
    'email_enabled' => $emailEnabled,
    'email_to' => $emailTo,
    'email_subject' => $emailSubject,
  ];
}

function notifications_send_telegram(string $token, string $chatId, string $text): bool {
  if ($token === '' || $chatId === '') return false;
  $url = 'https://api.telegram.org/bot'.rawurlencode($token).'/sendMessage';
  $payload = http_build_query([
    'chat_id' => $chatId,
    'text' => $text,
    'disable_web_page_preview' => '1',
  ]);

  if (function_exists('curl_init')){
    $ch = curl_init($url);
    if ($ch !== false){
      curl_setopt($ch, CURLOPT_POST, true);
      curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
      curl_setopt($ch, CURLOPT_TIMEOUT, 4);
      curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 3);
      $resp = curl_exec($ch);
      $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
      curl_close($ch);
      if ($resp !== false && $code >= 200 && $code < 300){
        $json = json_decode((string)$resp, true);
        return is_array($json) && !empty($json['ok']);
      }
    }
  }

  $ctx = stream_context_create([
    'http' => [
      'method' => 'POST',
      'timeout' => 4,
      'header' => "Content-Type: application/x-www-form-urlencoded\r\n",
      'content' => $payload,
      'ignore_errors' => true,
    ]
  ]);
  $resp = @file_get_contents($url, false, $ctx);
  if ($resp === false) return false;
  $json = json_decode((string)$resp, true);
  return is_array($json) && !empty($json['ok']);
}

function notifications_build_order_text(int $orderId, array $orderData, array $items): string {
  $name = trim((string)($orderData['customer_name'] ?? ''));
  $phone = trim((string)($orderData['phone'] ?? ''));
  $email = trim((string)($orderData['email'] ?? ''));
  $note = trim((string)($orderData['note'] ?? ''));
  $total = (float)($orderData['total'] ?? 0);

  $lines = [];
  $lines[] = 'Новая заявка #'.$orderId;
  if ($name !== '') $lines[] = 'Клиент: '.$name;
  if ($phone !== '') $lines[] = 'Телефон: '.$phone;
  if ($email !== '') $lines[] = 'Email: '.$email;
  $lines[] = 'Сумма: '.number_format($total, 2, '.', ' ').' ₽';
  $lines[] = 'Товары:';
  foreach ($items as $it){
    $itemName = trim((string)($it['name'] ?? 'Товар'));
    $qty = (int)($it['qty'] ?? 1);
    $lines[] = '- '.$itemName.' × '.$qty;
  }
  if ($note !== '') $lines[] = 'Комментарий: '.$note;
  $lines[] = 'Время: '.date('d.m.Y H:i');

  $text = implode("\n", $lines);
  if (mb_strlen($text, 'UTF-8') > 3900){
    $text = mb_substr($text, 0, 3890, 'UTF-8')."\n...";
  }
  return $text;
}

function notifications_on_new_order(PDO $pdo, int $orderId, array $orderData, array $items): void {
  $cfg = notifications_settings($pdo);
  $text = notifications_build_order_text($orderId, $orderData, $items);
  if (!empty($cfg['telegram_enabled'])){
    notifications_send_telegram((string)$cfg['telegram_bot_token'], (string)$cfg['telegram_chat_id'], $text);
  }
  // Email channel is intentionally left as a placeholder until SMTP settings are added.
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
        try { notifications_on_new_order($pdo, $oid, $b, $items); } catch (Throwable $e) {}
        out(200, ['id'=>$oid]);
      }catch(Throwable $e){
        if ($pdo->inTransaction()) $pdo->rollBack();
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
        try {
          $pdo->prepare("UPDATE production_orders
            SET status='cancelled', prod_state='cancelled'
            WHERE order_id = :id AND status = 'open'")
            ->execute([':id'=>$id]);
        } catch (Throwable $e) {}
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

  if (($segments[0] ?? '') === 'notifications'){
    $pdo = db();
    settings_bootstrap($pdo);

    if ($method === 'POST' && count($segments) === 2 && $segments[1] === 'test'){
      $cfg = notifications_settings($pdo);
      if (empty($cfg['telegram_enabled'])){
        out(400, ['error'=>'notifications_not_configured']);
      }
      $text = "Тест уведомлений STILVA\nВремя: ".date('d.m.Y H:i:s');
      $ok = notifications_send_telegram((string)$cfg['telegram_bot_token'], (string)$cfg['telegram_chat_id'], $text);
      if (!$ok) out(502, ['error'=>'send_failed']);
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
        if ($pdo->inTransaction()) $pdo->rollBack();
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
        if (!$row){ if ($pdo->inTransaction()) $pdo->rollBack(); out(404, ['error'=>'nf']); }
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
        if ($pdo->inTransaction()) $pdo->rollBack();
        out(500, ['error'=>'fail']);
      }
    }

    out(404, ['error'=>'nf']);
  }

  if (($segments[0] ?? '') === 'production'){
    $pdo = db();
    production_bootstrap($pdo);
    $sub = $segments[1] ?? '';

    if ($sub === 'upload' && $method === 'POST'){
      if (empty($_FILES['file']) || !is_array($_FILES['file'])) out(400, ['error'=>'no_file']);
      $file = $_FILES['file'];
      if (($file['error'] ?? UPLOAD_ERR_OK) !== UPLOAD_ERR_OK) out(400, ['error'=>'upload']);
      $orig = (string)($file['name'] ?? 'file');
      $ext = strtolower((string)pathinfo($orig, PATHINFO_EXTENSION));
      $allow = ['pdf','png','jpg','jpeg','dwg','dxf'];
      if (!in_array($ext, $allow, true)) out(400, ['error'=>'ext']);
      $dir = __DIR__ . '/../uploads/production_docs';
      if (!is_dir($dir)) { @mkdir($dir, 0775, true); }
      $name = date('Ymd_His') . '_' . bin2hex(random_bytes(4)) . '.' . $ext;
      $dest = $dir . '/' . $name;
      if (!move_uploaded_file($file['tmp_name'], $dest)) out(500, ['error'=>'save']);
      out(200, ['url'=>'/uploads/production_docs/'.$name, 'name'=>$orig]);
    }

    if ($method === 'GET' && count($segments) === 1){
      $no = trim((string)($_GET['no'] ?? ''));
      $orderId = isset($_GET['order_id']) && is_numeric($_GET['order_id']) ? (int)$_GET['order_id'] : 0;
      $address = trim((string)($_GET['address'] ?? ''));
      $stateRaw = trim((string)($_GET['state'] ?? ''));
      $state = $stateRaw !== '' ? production_order_state($stateRaw) : '';
      $deadlineFrom = trim((string)($_GET['deadline_from'] ?? ''));
      $deadlineTo = trim((string)($_GET['deadline_to'] ?? ''));

      $conds = ["po.prod_no IS NOT NULL", "po.prod_no <> ''"];
      $args = [];
      if ($no !== ''){
        $conds[] = "po.prod_no LIKE :no";
        $args[':no'] = '%'.$no.'%';
      }
      if ($orderId > 0){
        $conds[] = "po.order_id = :oid";
        $args[':oid'] = $orderId;
      }
      if ($address !== ''){
        $conds[] = "po.prod_address LIKE :addr";
        $args[':addr'] = '%'.$address.'%';
      }
      if ($deadlineFrom !== ''){
        $conds[] = "po.deadline_date >= :df";
        $args[':df'] = $deadlineFrom;
      }
      if ($deadlineTo !== ''){
        $conds[] = "po.deadline_date <= :dt";
        $args[':dt'] = $deadlineTo;
      }
      if ($state !== ''){
        $conds[] = "po.prod_state = :st";
        $args[':st'] = $state;
      }
      $where = 'WHERE '.implode(' AND ', $conds);
      $stmt = $pdo->prepare("SELECT po.*, p.name AS product_name
        FROM production_orders po
        LEFT JOIN products p ON p.id = po.product_id
        $where
        ORDER BY po.updated_at DESC, po.id DESC");
      $stmt->execute($args);
      $rows = $stmt->fetchAll();
      $groups = [];
      foreach ($rows as $r){
        $key = (string)($r['prod_no'] ?? '');
        if ($key === '') continue;
        if (!isset($groups[$key])) $groups[$key] = [];
        $groups[$key][] = $r;
      }

      $fileCounts = [];
      if ($groups){
        $nos = array_keys($groups);
        $placeholders = implode(',', array_fill(0, count($nos), '?'));
        $q = $pdo->prepare("SELECT prod_no, COUNT(*) AS c FROM production_files WHERE prod_no IN ($placeholders) GROUP BY prod_no");
        $q->execute($nos);
        foreach ($q->fetchAll() as $fr){
          $fileCounts[(string)$fr['prod_no']] = (int)$fr['c'];
        }
      }

      $jobs = [];
      foreach ($groups as $prodNo=>$groupRows){
        $job = production_rows_to_job($groupRows, []);
        $job['files_count'] = (int)($fileCounts[$prodNo] ?? 0);
        unset($job['files']);
        if (!isset($_GET['with_items']) || $_GET['with_items'] !== '1'){
          unset($job['items']);
        }
        $jobs[] = $job;
      }
      out(200, $jobs);
    }

    if ($method === 'POST' && count($segments) === 2 && $segments[1] === 'from-order'){
      $b = read_json();
      $orderId = isset($b['order_id']) ? (int)$b['order_id'] : 0;
      if ($orderId <= 0) out(400, ['error'=>'order_required']);

      $ordStmt = $pdo->prepare("SELECT id, delivery_address FROM orders WHERE id = :id");
      $ordStmt->execute([':id'=>$orderId]);
      $ord = $ordStmt->fetch();
      if (!$ord) out(404, ['error'=>'order_nf']);

      $items = order_items_with_availability($pdo, $orderId);
      $short = [];
      foreach ($items as $it){
        $pid = isset($it['product_id']) ? (int)$it['product_id'] : 0;
        $qty = isset($it['shortage_qty']) ? (int)$it['shortage_qty'] : 0;
        if ($pid > 0 && $qty > 0) $short[] = ['product_id'=>$pid, 'qty'=>$qty];
      }
      if (!$short) out(400, ['error'=>'no_shortage']);

      $existingStmt = $pdo->prepare("SELECT prod_no FROM production_orders
        WHERE order_id = :oid AND prod_no IS NOT NULL AND prod_no <> ''
        ORDER BY id DESC LIMIT 1");
      $existingStmt->execute([':oid'=>$orderId]);
      $ex = $existingStmt->fetch();
      $prodNo = $ex ? (string)$ex['prod_no'] : production_next_no($pdo);

      $address = trim((string)($b['address'] ?? ''));
      if ($address === '') $address = trim((string)($ord['delivery_address'] ?? ''));
      $deadlineDate = trim((string)($b['deadline_date'] ?? ''));
      $comment = trim((string)($b['comment'] ?? ''));

      $pdo->beginTransaction();
      try {
        $allOpenStmt = $pdo->prepare("SELECT * FROM production_orders
          WHERE order_id = :oid AND status = 'open'
          ORDER BY (prod_no = :no) DESC, (prod_no IS NOT NULL AND prod_no <> '') DESC, id DESC");
        $allOpenStmt->execute([':oid'=>$orderId, ':no'=>$prodNo]);
        $allOpenRows = $allOpenStmt->fetchAll();

        $byProduct = [];
        foreach ($allOpenRows as $r){
          $pid = (int)($r['product_id'] ?? 0);
          if ($pid <= 0) continue;
          if (!isset($byProduct[$pid])) $byProduct[$pid] = [];
          $byProduct[$pid][] = $r;
        }

        $keepIds = [];
        foreach ($short as $it){
          $pid = (int)$it['product_id'];
          $qty = (int)$it['qty'];
          $rowsForPid = $byProduct[$pid] ?? [];
          $row = !empty($rowsForPid) ? $rowsForPid[0] : null;
          if ($row){
            $pdo->prepare("UPDATE production_orders
              SET qty = :q,
                  order_id = :oid,
                  prod_no = :no,
                  prod_address = :addr,
                  deadline_date = :dl,
                  source = 'auto',
                  comment = :cmt,
                  status = 'open',
                  prod_state = IF(prod_state='cancelled','draft',prod_state)
              WHERE id = :id")
              ->execute([
                ':q'=>$qty,
                ':oid'=>$orderId,
                ':no'=>$prodNo,
                ':addr'=>$address ?: null,
                ':dl'=>$deadlineDate ?: null,
                ':cmt'=>$comment ?: null,
                ':id'=>(int)$row['id'],
              ]);
            $keepIds[(int)$row['id']] = true;
          } else {
            $pdo->prepare("INSERT INTO production_orders
              (product_id, order_id, prod_no, qty, qty_done, status, prod_state, prod_address, deadline_date, source, comment, created_by)
              VALUES (:pid,:oid,:no,:q,0,'open','draft',:addr,:dl,'auto',:cmt,:cb)")
              ->execute([
                ':pid'=>$pid,
                ':oid'=>$orderId,
                ':no'=>$prodNo,
                ':q'=>$qty,
                ':addr'=>$address ?: null,
                ':dl'=>$deadlineDate ?: null,
                ':cmt'=>$comment ?: ('Под заказ #'.$orderId),
                ':cb'=>'admin',
              ]);
            $keepIds[(int)$pdo->lastInsertId()] = true;
          }
        }

        foreach ($allOpenRows as $r){
          $id = (int)($r['id'] ?? 0);
          if ($id <= 0 || isset($keepIds[$id])) continue;
          $pdo->prepare("UPDATE production_orders SET status='cancelled', prod_state='cancelled' WHERE id = :id")
            ->execute([':id'=>$id]);
        }
        $pdo->commit();
      } catch (Throwable $e){
        if ($pdo->inTransaction()) $pdo->rollBack();
        out(500, ['error'=>'fail']);
      }

      $rows = production_fetch_rows($pdo, $prodNo);
      $files = production_fetch_files($pdo, $prodNo);
      out(200, production_rows_to_job($rows, $files));
    }

    if ($method === 'GET' && count($segments) === 3 && $segments[1] === 'order'){
      $orderId = (int)$segments[2];
      if ($orderId <= 0) out(400, ['error'=>'order_required']);
      $stmt = $pdo->prepare("SELECT DISTINCT prod_no FROM production_orders
        WHERE order_id = :oid AND prod_no IS NOT NULL AND prod_no <> ''
        ORDER BY id DESC");
      $stmt->execute([':oid'=>$orderId]);
      $nos = array_map(fn($r)=>(string)$r['prod_no'], $stmt->fetchAll());
      $jobs = [];
      foreach ($nos as $prodNo){
        $rows = production_fetch_rows($pdo, $prodNo);
        if (!$rows) continue;
        $jobs[] = production_rows_to_job($rows, []);
      }
      out(200, $jobs);
    }

    if ($method === 'POST' && count($segments) === 4 && $segments[1] === 'order' && $segments[3] === 'cancel-action'){
      $orderId = (int)$segments[2];
      $b = read_json();
      $action = (string)($b['action'] ?? '');
      if ($orderId <= 0) out(400, ['error'=>'order_required']);
      if (!in_array($action, ['cancel','detach'], true)) out(400, ['error'=>'action']);

      if ($action === 'cancel'){
        $stmt = $pdo->prepare("UPDATE production_orders
          SET status='cancelled', prod_state='cancelled'
          WHERE order_id = :oid AND prod_state <> 'closed'");
        $stmt->execute([':oid'=>$orderId]);
        out(200, ['ok'=>true, 'action'=>'cancel', 'affected'=>$stmt->rowCount()]);
      }

      $stmt = $pdo->prepare("UPDATE production_orders
        SET order_id = NULL, source = 'manual'
        WHERE order_id = :oid AND prod_state NOT IN ('closed','cancelled')");
      $stmt->execute([':oid'=>$orderId]);
      out(200, ['ok'=>true, 'action'=>'detach', 'affected'=>$stmt->rowCount()]);
    }

    if ($method === 'POST' && count($segments) === 1){
      $b = read_json();
      $orderId = isset($b['order_id']) && is_numeric($b['order_id']) ? (int)$b['order_id'] : null;
      $prodNo = trim((string)($b['prod_no'] ?? ''));
      if ($prodNo === '') $prodNo = production_next_no($pdo);
      $address = trim((string)($b['address'] ?? ''));
      $deadlineDate = trim((string)($b['deadline_date'] ?? ''));
      $comment = trim((string)($b['comment'] ?? ''));
      $stateCreate = production_order_state((string)($b['state'] ?? 'draft'));
      $statusCreate = production_state_to_row_status($stateCreate);
      $items = is_array($b['items'] ?? null) ? $b['items'] : [];
      if (!$items) out(400, ['error'=>'no_items']);

      if ($orderId && $orderId > 0){
        $stmt = $pdo->prepare("SELECT prod_no FROM production_orders
          WHERE order_id = :oid AND prod_state <> 'cancelled' AND status <> 'cancelled'
          LIMIT 1");
        $stmt->execute([':oid'=>$orderId]);
        $ex = $stmt->fetch();
        if ($ex && (string)($ex['prod_no'] ?? '') !== ''){
          out(409, ['error'=>'exists', 'prod_no'=>(string)$ex['prod_no']]);
        }
      } else {
        $orderId = null;
      }

      $pdo->beginTransaction();
      try{
        $ins = $pdo->prepare("INSERT INTO production_orders
          (product_id, order_id, prod_no, qty, qty_done, status, prod_state, prod_address, deadline_date, source, comment, created_by)
          VALUES (:pid,:oid,:no,:q,0,:status,:state,:addr,:dl,'manual',:cmt,:cb)");
        $count = 0;
        foreach ($items as $it){
          $pid = isset($it['product_id']) ? (int)$it['product_id'] : 0;
          $qty = isset($it['qty']) ? (int)$it['qty'] : 0;
          if ($pid <= 0 || $qty <= 0) continue;
          $ins->execute([
            ':pid'=>$pid,
            ':oid'=>$orderId,
            ':no'=>$prodNo,
            ':q'=>$qty,
            ':status'=>$statusCreate,
            ':state'=>$stateCreate,
            ':addr'=>$address ?: null,
            ':dl'=>$deadlineDate ?: null,
            ':cmt'=>$comment ?: null,
            ':cb'=>'admin',
          ]);
          $count++;
        }
        if ($count <= 0){
          if ($pdo->inTransaction()) $pdo->rollBack();
          out(400, ['error'=>'no_valid_items']);
        }
        $pdo->commit();
      }catch(Throwable $e){
        if ($pdo->inTransaction()) $pdo->rollBack();
        out(500, ['error'=>'fail']);
      }

      if ($stateCreate === 'closed'){
        $rowsClose = production_fetch_rows($pdo, $prodNo);
        foreach ($rowsClose as $r){ production_make_item_stock_done($pdo, $r); }
        production_touch_rows_state($pdo, $prodNo, 'closed');
      }

      $rows = production_fetch_rows($pdo, $prodNo);
      $files = production_fetch_files($pdo, $prodNo);
      out(200, production_rows_to_job($rows, $files));
    }

    if (count($segments) === 2){
      $prodNo = urldecode((string)$segments[1]);
      if ($method === 'GET'){
        $rows = production_fetch_rows($pdo, $prodNo);
        if (!$rows) out(404, ['error'=>'nf']);
        $files = production_fetch_files($pdo, $prodNo);
        out(200, production_rows_to_job($rows, $files));
      }

      if ($method === 'PATCH'){
        $rows = production_fetch_rows($pdo, $prodNo);
        if (!$rows) out(404, ['error'=>'nf']);
        $b = read_json();
        $state = array_key_exists('state', $b) ? production_order_state((string)$b['state']) : null;
        $addr = array_key_exists('address', $b) ? trim((string)$b['address']) : null;
        $dl = array_key_exists('deadline_date', $b) ? trim((string)$b['deadline_date']) : null;
        $cmt = array_key_exists('comment', $b) ? trim((string)$b['comment']) : null;
        $orderIdSet = null;
        if (array_key_exists('order_id', $b)){
          $orderIdSet = is_numeric($b['order_id']) && (int)$b['order_id'] > 0 ? (int)$b['order_id'] : null;
        }

        $pdo->beginTransaction();
        try{
          if ($addr !== null || $dl !== null || $cmt !== null || array_key_exists('order_id', $b)){
            $set = [];
            $args = [':no'=>$prodNo];
            if ($addr !== null){ $set[] = "prod_address = :addr"; $args[':addr'] = $addr ?: null; }
            if ($dl !== null){ $set[] = "deadline_date = :dl"; $args[':dl'] = $dl ?: null; }
            if ($cmt !== null){ $set[] = "comment = :cmt"; $args[':cmt'] = $cmt ?: null; }
            if (array_key_exists('order_id', $b)){ $set[] = "order_id = :oid"; $args[':oid'] = $orderIdSet; }
            if ($set){
              $pdo->prepare("UPDATE production_orders SET ".implode(',', $set)." WHERE prod_no = :no")->execute($args);
            }
          }

          $rowsById = [];
          foreach ($rows as $r){ $rowsById[(int)$r['id']] = $r; }
          $itemsPatch = is_array($b['items'] ?? null) ? $b['items'] : [];
          $removeIds = array_values(array_unique(array_map('intval', is_array($b['remove_ids'] ?? null) ? $b['remove_ids'] : [])));
          foreach ($removeIds as $rid){
            if ($rid <= 0 || !isset($rowsById[$rid])) continue;
            $pdo->prepare("UPDATE production_orders SET status='cancelled', prod_state='cancelled' WHERE id = :id")
              ->execute([':id'=>$rid]);
          }
          $rowHead = $rows[0];
          $headOrderId = array_key_exists('order_id', $b) ? $orderIdSet : (isset($rowHead['order_id']) ? (int)$rowHead['order_id'] : null);
          $headAddr = $addr !== null ? ($addr ?: null) : ((string)($rowHead['prod_address'] ?? '') ?: null);
          $headDl = $dl !== null ? ($dl ?: null) : ((string)($rowHead['deadline_date'] ?? '') ?: null);
          $headComment = $cmt !== null ? ($cmt ?: null) : ((string)($rowHead['comment'] ?? '') ?: null);
          foreach ($itemsPatch as $it){
            $id = isset($it['id']) ? (int)$it['id'] : 0;
            if ($id <= 0 || !isset($rowsById[$id])){
              $pid = isset($it['product_id']) ? (int)$it['product_id'] : 0;
              $qty = isset($it['qty']) ? (int)$it['qty'] : 0;
              if ($pid <= 0 || $qty < 0) continue;
              $argsIns = [
                ':pid'=>$pid,
                ':oid'=>$headOrderId,
                ':no'=>$prodNo,
                ':q'=>$qty,
                ':addr'=>$headAddr,
                ':dl'=>$headDl,
                ':src'=>'manual',
                ':cmt'=>$headComment,
                ':cb'=>'admin',
                ':stage_cut'=>production_stage_state((string)($it['stage_cut'] ?? 'todo')),
                ':stage_bend'=>production_stage_state((string)($it['stage_bend'] ?? 'todo')),
                ':stage_fitting'=>production_stage_state((string)($it['stage_fitting'] ?? 'todo')),
                ':stage_assembly'=>production_stage_state((string)($it['stage_assembly'] ?? 'todo')),
                ':stage_qc'=>production_stage_state((string)($it['stage_qc'] ?? 'todo')),
                ':stage_stock'=>production_stage_state((string)($it['stage_stock'] ?? 'todo')),
              ];
              $pdo->prepare("INSERT INTO production_orders
                (product_id, order_id, prod_no, qty, qty_done, status, prod_state, prod_address, deadline_date, source, comment, created_by,
                 stage_cut, stage_bend, stage_fitting, stage_assembly, stage_qc, stage_stock)
                VALUES
                (:pid,:oid,:no,:q,0,'open','draft',:addr,:dl,:src,:cmt,:cb,:stage_cut,:stage_bend,:stage_fitting,:stage_assembly,:stage_qc,:stage_stock)")
                ->execute($argsIns);
              if ($argsIns[':stage_stock'] === 'done'){
                $newId = (int)$pdo->lastInsertId();
                $rowAfter = $pdo->prepare("SELECT * FROM production_orders WHERE id = :id");
                $rowAfter->execute([':id'=>$newId]);
                $newRow = $rowAfter->fetch();
                if ($newRow) production_make_item_stock_done($pdo, $newRow);
              }
              continue;
            }
            $cur = $rowsById[$id];
            $set = [];
            $args = [':id'=>$id];
            if (array_key_exists('qty', $it)){
              $q = (int)$it['qty'];
              if ($q < 0) $q = 0;
              $set[] = "qty = :q";
              $args[':q'] = $q;
              if ((int)$cur['qty_done'] > $q){
                $set[] = "qty_done = :qd";
                $args[':qd'] = $q;
              }
            }
            foreach (production_stage_keys() as $sk){
              if (array_key_exists($sk, $it)){
                $set[] = "$sk = :$sk";
                $args[":$sk"] = production_stage_state((string)$it[$sk]);
              }
            }
            if (!$set) continue;
            $pdo->prepare("UPDATE production_orders SET ".implode(',', $set)." WHERE id = :id")->execute($args);
            $rowAfter = $pdo->prepare("SELECT * FROM production_orders WHERE id = :id");
            $rowAfter->execute([':id'=>$id]);
            $newRow = $rowAfter->fetch();
            if (!$newRow) continue;
            $prevStock = production_stage_state((string)($cur['stage_stock'] ?? 'todo'));
            $nextStock = production_stage_state((string)($newRow['stage_stock'] ?? 'todo'));
            if ($prevStock !== 'done' && $nextStock === 'done'){
              production_make_item_stock_done($pdo, $newRow);
            }
          }

          if ($state !== null){
            if ($state === 'closed'){
              $rowsClose = production_fetch_rows($pdo, $prodNo);
              foreach ($rowsClose as $r){ production_make_item_stock_done($pdo, $r); }
              production_touch_rows_state($pdo, $prodNo, 'closed');
            } else {
              production_touch_rows_state($pdo, $prodNo, $state);
            }
          }

          $rowsAfter = production_fetch_rows($pdo, $prodNo);
          if (!$rowsAfter){
            if ($pdo->inTransaction()) $pdo->rollBack();
            out(404, ['error'=>'nf']);
          }
          if ($state === null){
            $derived = production_group_state($rowsAfter);
            production_touch_rows_state($pdo, $prodNo, $derived);
          }
          $pdo->commit();
        }catch(Throwable $e){
          if ($pdo->inTransaction()) $pdo->rollBack();
          out(500, ['error'=>'fail']);
        }
        $rowsFinal = production_fetch_rows($pdo, $prodNo);
        $files = production_fetch_files($pdo, $prodNo);
        out(200, production_rows_to_job($rowsFinal, $files));
      }
    }

    if (count($segments) === 3){
      $prodNo = urldecode((string)$segments[1]);
      if ($segments[2] === 'files' && $method === 'GET'){
        out(200, production_fetch_files($pdo, $prodNo));
      }
      if ($segments[2] === 'files' && $method === 'POST'){
        $b = read_json();
        $url = trim((string)($b['url'] ?? ''));
        $name = trim((string)($b['name'] ?? ''));
        if ($url === '') out(400, ['error'=>'url']);
        $stmt = $pdo->prepare("SELECT order_id FROM production_orders WHERE prod_no = :no ORDER BY id ASC LIMIT 1");
        $stmt->execute([':no'=>$prodNo]);
        $row = $stmt->fetch();
        if (!$row) out(404, ['error'=>'nf']);
        $orderId = isset($row['order_id']) ? (int)$row['order_id'] : null;
        $ins = $pdo->prepare("INSERT INTO production_files (prod_no, order_id, file_url, file_name, created_by)
          VALUES (:no, :oid, :url, :name, :cb)");
        $ins->execute([
          ':no'=>$prodNo,
          ':oid'=>$orderId ?: null,
          ':url'=>$url,
          ':name'=>$name ?: null,
          ':cb'=>'admin',
        ]);
        out(200, ['id'=>(int)$pdo->lastInsertId()]);
      }
    }

    if (count($segments) === 4 && $segments[2] === 'files' && $method === 'DELETE'){
      $prodNo = urldecode((string)$segments[1]);
      $fileId = (int)$segments[3];
      if ($fileId <= 0) out(400, ['error'=>'file_id']);
      $stmt = $pdo->prepare("DELETE FROM production_files WHERE id = :id AND prod_no = :no");
      $stmt->execute([':id'=>$fileId, ':no'=>$prodNo]);
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
