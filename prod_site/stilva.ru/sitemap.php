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

function abs_url(string $url, string $base, string $scheme): string {
  $url = trim($url);
  if ($url === '') return '';
  if (preg_match('~^https?://~i', $url)) return $url;
  if (strpos($url, '//') === 0) return $scheme . ':' . $url;
  if ($url[0] === '/') return rtrim($base, '/') . $url;
  return rtrim($base, '/') . '/' . $url;
}

function slugify_ru(string $value): string {
  $value = trim(mb_strtolower($value, 'UTF-8'));
  $map = [
    'а'=>'a','б'=>'b','в'=>'v','г'=>'g','д'=>'d','е'=>'e','ё'=>'e','ж'=>'zh','з'=>'z','и'=>'i','й'=>'y',
    'к'=>'k','л'=>'l','м'=>'m','н'=>'n','о'=>'o','п'=>'p','р'=>'r','с'=>'s','т'=>'t','у'=>'u','ф'=>'f',
    'х'=>'h','ц'=>'c','ч'=>'ch','ш'=>'sh','щ'=>'sch','ъ'=>'','ы'=>'y','ь'=>'','э'=>'e','ю'=>'yu','я'=>'ya'
  ];
  $value = strtr($value, $map);
  $value = preg_replace('/[^a-z0-9]+/i', '-', $value) ?? '';
  $value = trim($value, '-');
  return $value;
}

function parse_landing_overrides(string $raw): array {
  $out = [];
  $lines = preg_split('/\\r\\n|\\r|\\n/', $raw);
  foreach ($lines as $line){
    $line = trim($line);
    if ($line === '') continue;
    $parts = array_map('trim', explode('|', $line));
    if (count($parts) < 2) continue;
    $slug = $parts[0] ?? '';
    if ($slug === '') continue;
    $out[] = $slug;
  }
  return $out;
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

$defaultLandings = ['obshchepit','kukhnya','sklad','pod-zakaz','perforirovannye-polki','sploshnye-polki'];
try {
  $pdo = db();
  $stmt = $pdo->prepare("SELECT v FROM app_settings WHERE k = 'home' LIMIT 1");
  $stmt->execute();
  $row = $stmt->fetch();
  if ($row && !empty($row['v'])) {
    $data = json_decode((string)$row['v'], true);
    if (is_array($data) && !empty($data['landing_items'])) {
      $extra = parse_landing_overrides((string)$data['landing_items']);
      $defaultLandings = array_values(array_unique(array_merge($defaultLandings, $extra)));
    }
  }
} catch (Throwable $e) {
  // ignore
}

foreach ($defaultLandings as $slug){
  $slug = trim((string)$slug);
  if ($slug === '') continue;
  $urls[] = [
    'loc' => $base . 'lp/' . $slug . '/',
    'lastmod' => $today,
    'changefreq' => 'weekly',
    'priority' => '0.6'
  ];
}

try {
  $pdo = db();
  $stmt = $pdo->query("SELECT id, name, image_url FROM products WHERE published = 1 ORDER BY id ASC");
  $rows = $stmt->fetchAll();
  foreach ($rows as $r){
    $id = (int)($r['id'] ?? 0);
    if ($id <= 0) continue;
    $name = (string)($r['name'] ?? '');
    $slug = slugify_ru($name);
    if ($slug === '') $slug = 'product';
    $img = trim((string)($r['image_url'] ?? ''));
    $img = $img !== '' ? abs_url($img, $base, $scheme) : '';
    $urls[] = [
      'loc' => $base . 'product/' . $slug . '-' . $id . '/',
      'lastmod' => $today,
      'changefreq' => 'weekly',
      'priority' => '0.7',
      'image' => $img
    ];
  }
} catch (Throwable $e) {
  // ignore, return base only
}

echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
<?php foreach ($urls as $u): ?>
  <url>
    <loc><?= h($u['loc']) ?></loc>
    <lastmod><?= h($u['lastmod']) ?></lastmod>
    <changefreq><?= h($u['changefreq']) ?></changefreq>
    <priority><?= h($u['priority']) ?></priority>
    <?php if (!empty($u['image'])): ?>
    <image:image>
      <image:loc><?= h($u['image']) ?></image:loc>
    </image:image>
    <?php endif; ?>
  </url>
<?php endforeach; ?>
</urlset>
