<?php
declare(strict_types=1);
require __DIR__ . '/api/config.php';

header('Content-Type: application/xml; charset=utf-8');

function db(): PDO {
  static $pdo = null;
  if ($pdo) return $pdo;
  $cfg = $GLOBALS['__DB_CFG'];
  $dsn = "mysql:host={$cfg['host']};dbname={$cfg['name']};charset=utf8mb4";
  $pdo = new PDO($dsn, $cfg['user'], $cfg['pass'], [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
  ]);
  return $pdo;
}

function h(string $value): string {
  return htmlspecialchars($value, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
}

$host = $_SERVER['HTTP_HOST'] ?? 'stilva.ru';
$scheme = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off') ? 'https' : 'http';
$base = $scheme . '://' . $host . '/';
$today = gmdate('Y-m-d');

$urls = [];
$urls[] = [
  'loc' => $base,
  'lastmod' => $today,
  'changefreq' => 'weekly',
  'priority' => '1.0'
];

try {
  $pdo = db();
  $stmt = $pdo->query("SELECT id FROM products WHERE published = 1 ORDER BY id ASC");
  $rows = $stmt->fetchAll();
  foreach ($rows as $r){
    $id = (int)($r['id'] ?? 0);
    if ($id <= 0) continue;
    $urls[] = [
      'loc' => $base . '?product=' . $id,
      'lastmod' => $today,
      'changefreq' => 'weekly',
      'priority' => '0.7'
    ];
  }
} catch (Throwable $e) {
  // ignore, return base only
}

echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<?php foreach ($urls as $u): ?>
  <url>
    <loc><?= h($u['loc']) ?></loc>
    <lastmod><?= h($u['lastmod']) ?></lastmod>
    <changefreq><?= h($u['changefreq']) ?></changefreq>
    <priority><?= h($u['priority']) ?></priority>
  </url>
<?php endforeach; ?>
</urlset>
