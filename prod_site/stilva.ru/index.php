<?php
declare(strict_types=1);
require __DIR__ . '/api/config.php';

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

function settings_bootstrap(PDO $pdo){
  $pdo->exec("CREATE TABLE IF NOT EXISTS app_settings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    k VARCHAR(64) NOT NULL UNIQUE,
    v MEDIUMTEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
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

function product_query_param(array $product): string {
  $id = (int)($product['id'] ?? 0);
  return $id > 0 ? (string)$id : '';
}

function product_public_url(array $product, string $siteUrl): string {
  $id = product_query_param($product);
  if ($id === '') return $siteUrl;
  return $siteUrl . '?product=' . rawurlencode($id);
}

function phone_digits(string $value): string {
  return preg_replace('/\\D+/', '', $value) ?? '';
}

function render_multiline(string $value): string {
  return nl2br(h($value));
}

function render_paragraphs(string $value): string {
  $value = trim($value);
  if ($value === '') return '';
  $parts = preg_split('/\\n\\s*\\n/', $value);
  $html = '';
  foreach ($parts as $p){
    $p = trim($p);
    if ($p === '') continue;
    $html .= '<p>' . h($p) . '</p>';
  }
  return $html;
}

function parse_pairs(string $raw): array {
  $out = [];
  $lines = preg_split('/\\r\\n|\\r|\\n/', $raw);
  foreach ($lines as $line){
    $line = trim($line);
    if ($line === '') continue;
    $pos = strpos($line, '|');
    if ($pos === false) continue;
    $title = trim(substr($line, 0, $pos));
    $text = trim(substr($line, $pos + 1));
    if ($title === '' || $text === '') continue;
    $out[] = ['title'=>$title, 'text'=>$text];
  }
  return $out;
}

function table_exists(PDO $pdo, string $name): bool {
  $stmt = $pdo->prepare("SHOW TABLES LIKE :t");
  $stmt->execute([':t'=>$name]);
  return (bool)$stmt->fetch();
}

$seo = [];
$seller = [];
$home = [];
$products = [];
$reservedMap = [];
$onOrderMap = [];
try {
  $pdo = db();
  $seo = settings_get($pdo, 'seo', []);
  $seller = settings_get($pdo, 'seller', []);
  $home = settings_get($pdo, 'home', []);
  $stmt = $pdo->query("SELECT * FROM products WHERE published = 1 ORDER BY id ASC");
  $products = $stmt->fetchAll();
  foreach ($products as &$p){
    if (!isset($p['supply_mode']) || $p['supply_mode'] === null || $p['supply_mode'] === '') $p['supply_mode'] = 'stock';
    if (!isset($p['seo_title'])) $p['seo_title'] = '';
    if (!isset($p['seo_description'])) $p['seo_description'] = '';
    if (!isset($p['seo_keywords'])) $p['seo_keywords'] = '';
    if (!isset($p['seo_h1'])) $p['seo_h1'] = '';
    if (!isset($p['seo_robots'])) $p['seo_robots'] = '';
    if (!isset($p['seo_canonical'])) $p['seo_canonical'] = '';
    if (!isset($p['seo_og_title'])) $p['seo_og_title'] = '';
    if (!isset($p['seo_og_description'])) $p['seo_og_description'] = '';
  }
  unset($p);
  if ($products) {
    $ids = array_values(array_unique(array_map(fn($p)=>(int)($p['id'] ?? 0), $products)));
    $ids = array_filter($ids, fn($x)=>$x>0);
    if ($ids && table_exists($pdo, 'stock_movements')) {
      $in = implode(',', array_map('intval', $ids));
      $rows = $pdo->query("SELECT product_id, SUM(CASE WHEN type='reserve' THEN qty WHEN type='release' THEN -qty ELSE 0 END) AS reserved
                           FROM stock_movements WHERE product_id IN ($in) GROUP BY product_id")->fetchAll();
      foreach ($rows as $r){ $reservedMap[(int)$r['product_id']] = (int)$r['reserved']; }
    }
    if ($ids && table_exists($pdo, 'production_orders')) {
      $in = implode(',', array_map('intval', $ids));
      $rows = $pdo->query("SELECT product_id, SUM(qty - qty_done) AS on_order
                           FROM production_orders WHERE status='open' AND product_id IN ($in) GROUP BY product_id")->fetchAll();
      foreach ($rows as $r){ $onOrderMap[(int)$r['product_id']] = (int)$r['on_order']; }
    }
  }
} catch (Throwable $e) {
  $seo = [];
  $seller = [];
  $home = [];
  $products = [];
  $reservedMap = [];
  $onOrderMap = [];
}

$host = $_SERVER['HTTP_HOST'] ?? 'stilva.ru';
$scheme = (!empty($_SERVER['HTTPS']) && $_SERVER['HTTPS'] !== 'off') ? 'https' : 'http';
$base = $scheme . '://' . $host . '/';

$siteName = trim((string)($seo['site_name'] ?? 'STILVA'));
if ($siteName === '') $siteName = 'STILVA';

$homeHeroEyebrow = trim((string)($home['hero_eyebrow'] ?? 'стеллажи из нержавеющей стали'));
$homeHeroTitle = trim((string)($home['hero_title'] ?? "Стеллажи для кухни,\nобщепита и склада"));
$homeHeroLead = trim((string)($home['hero_lead'] ?? 'Типовые размеры и изготовление под задачу. Подбор стали AISI 304/430, перфорированные или сплошные полки, аккуратная сборка.'));
$homeHeroCta1Text = trim((string)($home['hero_cta1_text'] ?? 'Каталог'));
$homeHeroCta1Link = trim((string)($home['hero_cta1_link'] ?? '#catalog'));
$homeHeroCta2Text = trim((string)($home['hero_cta2_text'] ?? 'Получить расчёт'));
$homeHeroCta2Link = trim((string)($home['hero_cta2_link'] ?? '#contacts'));

$homeBenefitsTitle = trim((string)($home['benefits_title'] ?? 'Почему STILVA'));
$homeBenefitsLead = trim((string)($home['benefits_lead'] ?? 'Коммерческий класс для рабочих зон: кухня, общепит, склад.'));
$homeBenefitsRaw = trim((string)($home['benefits_items'] ?? "Сталь под задачу|Подберём AISI 304/430 под условия эксплуатации.\nКонфигурации|Перфорированные или сплошные полки, нужное количество ярусов.\nТиповые и под заказ|Быстрое решение из каталога или изготовление под нишу."));
$homeBenefits = parse_pairs($homeBenefitsRaw);

$homeSpecsTitle = trim((string)($home['specs_title'] ?? 'Характеристики и размеры'));
$homeSpecsLead = trim((string)($home['specs_lead'] ?? 'Типовой ряд и индивидуальные решения.'));
$homeSpecsRaw = trim((string)($home['specs_items'] ?? "Полки|Перфорированные или сплошные — под задачу хранения.\nРазмеры|Типовой ряд: ширина 800–1200, глубина 300–600, высота 1800 мм.\nСборка|Разборные или сварные исполнения.\nРегулировка|Возможна настройка уровня полок по высоте.\nОпоры|Регулируемые опоры для ровной установки.\nСроки|Согласуем до старта производства."));
$homeSpecs = parse_pairs($homeSpecsRaw);

$homeCasesTitle = trim((string)($home['cases_title'] ?? 'Сферы применения'));
$homeCasesLead = trim((string)($home['cases_lead'] ?? 'Решения под рабочие зоны и санитарные требования.'));
$homeCasesRaw = trim((string)($home['cases_items'] ?? "Общепит и рестораны|Для хранения инвентаря, посуды и продуктов, легко моется.\nКухни и производства|Нержавейка выдерживает влажность и санитарную обработку.\nСклады и логистика|Удобная компоновка рядов и доступ к товару.\nТорговые залы|Аккуратный внешний вид и устойчивость.\nЛаборатории|Чистая обработка и минимум стыков."));
$homeCases = parse_pairs($homeCasesRaw);

$homeTypesTitle = trim((string)($home['types_title'] ?? 'Типы стеллажей'));
$homeTypesLead = trim((string)($home['types_lead'] ?? 'Подбираем конструкцию под задачу и условия эксплуатации.'));
$homeTypesRaw = trim((string)($home['types_items'] ?? "Кухонные стеллажи|Для рабочих зон и хранения на кухне.\nСкладские стеллажи|Для влажных и пищевых складов.\nПроизводственные|Для цехов и линий.\nПерфорированные полки|Вентиляция и стек.\nСплошные полки|Для мелких предметов.\nПод заказ|Нестандартные размеры и конфигурации."));
$homeTypes = parse_pairs($homeTypesRaw);

$homeFaqTitle = trim((string)($home['faq_title'] ?? 'FAQ'));
$homeFaqLead = trim((string)($home['faq_lead'] ?? 'Коротко на частые вопросы.'));
$homeFaqRaw = trim((string)($home['faq_items'] ?? "Нестандартные размеры?|Да, подгоняем под нишу и планировку цеха.\nКакая сталь используется?|AISI 304/430 — подберём под условия эксплуатации.\nКакие полки доступны?|Перфорированные или сплошные, количество ярусов согласуем.\nДоставка?|Доставка по России, условия и сроки фиксируем."));
$homeFaq = parse_pairs($homeFaqRaw);

$homeContactsTitle = trim((string)($home['contacts_title'] ?? 'Свяжитесь с нами'));
$homeContactsLead = trim((string)($home['contacts_lead'] ?? 'Подробно осудим все детали и возможные нестандартные решения.'));
$homeContactsNote = trim((string)($home['contacts_note'] ?? ''));
$homeTgHandle = trim((string)($home['tg_handle'] ?? 'stilva_support'));
$homeTgHandle = ltrim($homeTgHandle, '@');
$homeWaPhone = trim((string)($home['wa_phone'] ?? '79000000000'));
$homeWaDigits = phone_digits($homeWaPhone);
if ($homeWaDigits === '') $homeWaDigits = '79000000000';

$homeSeoTitle = trim((string)($home['seo_title'] ?? ''));
$homeSeoText = trim((string)($home['seo_text'] ?? ''));

$defaultTitle = 'Стеллажи из нержавеющей стали для общепита и склада — STILVA';
$title = trim((string)($seo['title'] ?? $defaultTitle));
if ($title === '') $title = $defaultTitle;

$defaultDesc = 'Производим и поставляем стеллажи из нержавейки: типовые размеры и изготовление под задачу, перфорированные и сплошные полки, подбор стали AISI 304/430, расчёт и доставка по России.';
$desc = trim((string)($seo['description'] ?? $defaultDesc));
if ($desc === '') $desc = $defaultDesc;

$keywords = trim((string)($seo['keywords'] ?? 'стеллажи из нержавеющей стали, стеллажи из нержавейки, стеллажи для общепита, стеллажи для кухни, стеллажи для склада, стеллажи AISI 304, стеллажи AISI 430, перфорированные полки, производственные стеллажи, стеллажи на заказ, производство стеллажей'));

$canonical = trim((string)($seo['canonical'] ?? $base));
if ($canonical === '') $canonical = $base;
$canonical = abs_url($canonical, $base, $scheme);
$siteUrl = $base;

$robots = trim((string)($seo['robots'] ?? 'index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1'));
if ($robots === '') $robots = 'index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1';

$themeColor = trim((string)($seo['theme_color'] ?? '#f7f8fb'));
if ($themeColor === '') $themeColor = '#f7f8fb';

$ogTitle = trim((string)($seo['og_title'] ?? $title));
if ($ogTitle === '') $ogTitle = $title;

$ogDesc = trim((string)($seo['og_description'] ?? $desc));
if ($ogDesc === '') $ogDesc = $desc;

$ogType = trim((string)($seo['og_type'] ?? 'website'));
if ($ogType === '') $ogType = 'website';

$ogLocale = trim((string)($seo['og_locale'] ?? 'ru_RU'));
if ($ogLocale === '') $ogLocale = 'ru_RU';

$ogImageRaw = trim((string)($seo['og_image'] ?? ''));
if ($ogImageRaw === '') $ogImageRaw = '/pictures/1000x400x1800.png';
$ogImage = abs_url($ogImageRaw, $base, $scheme);
$twitterCard = trim((string)($seo['twitter_card'] ?? 'summary_large_image'));
if ($twitterCard === '') $twitterCard = 'summary_large_image';
$googleVerify = trim((string)($seo['google_verification'] ?? ''));
$yandexVerify = trim((string)($seo['yandex_verification'] ?? ''));
$bingVerify = trim((string)($seo['bing_verification'] ?? ''));
$yandexMetrikaId = preg_replace('/\D+/', '', (string)($seo['yandex_metrika_id'] ?? '')) ?? '';
$gaMeasurementId = strtoupper(trim((string)($seo['google_analytics_id'] ?? '')));
$gaMeasurementId = preg_replace('/[^A-Z0-9\-]/', '', $gaMeasurementId) ?? '';

$contactEmail = trim((string)($seller['email'] ?? 'sales@stilva.example'));
if ($contactEmail === '') $contactEmail = 'sales@stilva.example';
$contactPhone = trim((string)($seller['phone'] ?? '+7 000 000 00 00'));
if ($contactPhone === '') $contactPhone = '+7 000 000 00 00';
$contactPhoneTel = phone_digits($contactPhone);
if ($contactPhoneTel === '') $contactPhoneTel = '70000000000';
$contactAddress = trim((string)($seller['address'] ?? ''));
$contactNote = $homeContactsNote !== '' ? $homeContactsNote : ($contactAddress !== ''
  ? 'Пн–пт 10:00–19:00 • '.$contactAddress.' • Работаем по всей России'
  : 'Пн–пт 10:00–19:00 • Москва • Работаем по всей России');

$productView = null;
$productQueryRaw = trim((string)($_GET['product'] ?? ''));
if ($productQueryRaw !== '' && $products) {
  foreach ($products as $p){
    if ((string)product_query_param($p) === $productQueryRaw){
      $productView = $p;
      break;
    }
  }
}

$ogImageAlt = $productView ? trim((string)($productView['name'] ?? '')) : ($siteName.' — стеллажи из нержавеющей стали');
if ($ogImageAlt === '') $ogImageAlt = $siteName;

if ($productView){
  $pName = trim((string)($productView['name'] ?? ''));
  $pDesc = trim((string)($productView['description'] ?? ''));
  $pSeoTitle = trim((string)($productView['seo_title'] ?? ''));
  $pSeoDesc = trim((string)($productView['seo_description'] ?? ''));
  $pSeoKeywords = trim((string)($productView['seo_keywords'] ?? ''));
  $pSeoH1 = trim((string)($productView['seo_h1'] ?? ''));
  $pSeoRobots = trim((string)($productView['seo_robots'] ?? ''));
  $pSeoCanonical = trim((string)($productView['seo_canonical'] ?? ''));
  $pSeoOgTitle = trim((string)($productView['seo_og_title'] ?? ''));
  $pSeoOgDescription = trim((string)($productView['seo_og_description'] ?? ''));
  $pMaterial = trim((string)($productView['material'] ?? ''));
  $pConstruction = trim((string)($productView['construction'] ?? ''));
  $pPerforation = trim((string)($productView['perforation'] ?? ''));
  $pShelf = trim((string)($productView['shelf_thickness'] ?? ''));
  $pShelves = (int)($productView['shelves'] ?? 0);
  if ($pDesc === '') {
    $parts = [];
    if ($pMaterial !== '') $parts[] = 'Материал: '.$pMaterial;
    if ($pConstruction !== '') $parts[] = 'Конструкция: '.$pConstruction;
    if ($pPerforation !== '') $parts[] = 'Перфорация: '.$pPerforation;
    if ($pShelf !== '') $parts[] = 'Толщина: '.$pShelf.' мм';
    if ($pShelves > 0) $parts[] = 'Полок: '.$pShelves;
    $pDesc = $parts ? implode('. ', $parts).'.' : $defaultDesc;
  }
  if ($pName !== '') {
    $title = $pSeoTitle !== '' ? $pSeoTitle : ($pName.' — '.$siteName);
    $desc = $pSeoDesc !== '' ? $pSeoDesc : $pDesc;
    $keywords = $pSeoKeywords !== '' ? $pSeoKeywords : $keywords;
    $robots = $pSeoRobots !== '' ? $pSeoRobots : $robots;
    $canonical = $pSeoCanonical !== ''
      ? abs_url($pSeoCanonical, $base, $scheme)
      : abs_url(product_public_url($productView, $siteUrl), $base, $scheme);
    $ogTitle = $pSeoOgTitle !== '' ? $pSeoOgTitle : $title;
    $ogDesc = $pSeoOgDescription !== '' ? $pSeoOgDescription : $desc;
    $ogType = 'product';
    $img = trim((string)($productView['image_url'] ?? ''));
    if ($img !== '') $ogImage = abs_url($img, $base, $scheme);
  }
}

$heroTitle = $homeHeroTitle;
if ($productView) {
  $pHeroName = trim((string)($productView['name'] ?? ''));
  if ($pHeroName !== '') $heroTitle = $pHeroName;
  $pSeoH1 = trim((string)($productView['seo_h1'] ?? ''));
  if ($pSeoH1 !== '') $heroTitle = $pSeoH1;
}

$catalogHtml = '';
if ($products){
  foreach ($products as $p){
    $id = (int)($p['id'] ?? 0);
    $productUrl = product_public_url($p, $siteUrl);
    $titleText = trim((string)($p['name'] ?? 'Товар'));
    if ($titleText === '') $titleText = 'Товар';
    $price = (float)($p['price'] ?? 0);
    $stock = (int)($p['stock_qty'] ?? 0);
    $reserved = (int)($reservedMap[$id] ?? 0);
    $onOrder = (int)($onOrderMap[$id] ?? 0);
    $available = $stock - $reserved;
    if ($available < 0) $available = 0;
    $mode = (string)($p['supply_mode'] ?? 'stock');

    $statusHtml = '';
    if ($mode === 'mixed') {
      if ($available > 0) $statusHtml .= '<span class="pill pill--stock">В наличии: '.(int)$available.' шт.</span>';
      if ($onOrder > 0) $statusHtml .= '<span class="pill pill--order">Под заказ: '.(int)$onOrder.' шт.</span>';
      if ($statusHtml === '') $statusHtml = '<span class="pill pill--order">Под заказ</span>';
    } else {
      if ($available > 0) $statusHtml = '<span class="pill pill--stock">В наличии: '.(int)$available.' шт.</span>';
      else $statusHtml = '<span class="pill pill--order">Под заказ</span>';
    }

    $tags = [];
    if (!empty($p['material'])) $tags[] = 'Материал: '.(string)$p['material'];
    if (!empty($p['construction'])) $tags[] = 'Конструкция: '.(string)$p['construction'];
    if (!empty($p['perforation'])) $tags[] = 'Перфорация: '.(string)$p['perforation'];
    if (!empty($p['shelf_thickness'])) $tags[] = 'Толщ.: '.(string)$p['shelf_thickness'].' мм';
    if (!empty($p['shelves'])) $tags[] = 'Полок: '.(string)$p['shelves'];
    $tagsHtml = '';
    foreach ($tags as $t){ $tagsHtml .= '<span class="tag">'.h($t).'</span>'; }

    $img = trim((string)($p['image_url'] ?? ''));
    $imgHtml = $img !== ''
      ? '<img src="'.h($img).'" data-full="'.h($img).'" alt="'.h($titleText).'" loading="lazy" decoding="async" style="width:100%;height:100%;object-fit:cover;border-radius:12px;cursor:pointer">'
      : 'Фото изделия';

    $priceText = number_format($price, 0, '.', ' ');
    $catalogHtml .= '
            <article class="product" data-id="'.(int)$id.'">
              <div class="product__body">
                <div class="product__title"><a class="product__link" href="'.h($productUrl.'#catalog').'">'.h($titleText).'</a></div>
                <div class="product__status">'.$statusHtml.'</div>
                <div class="product__price">'.$priceText.'&nbsp;₽</div>
                <div class="product__tags">'.$tagsHtml.'</div>
                <div class="product__actions">
                  <button class="btn btn--lite"
                          data-order
                          data-id="'.(int)$id.'"
                          data-name="'.h($titleText).'"
                          data-price="'.h((string)$price).'">
                    Заказать
                  </button>
                </div>
              </div>
              <div class="product__img">'.$imgHtml.'</div>
            </article>';
  }
}

$org = [
  '@context' => 'https://schema.org',
  '@type' => 'Organization',
  'name' => $siteName,
  'url' => $siteUrl
];
if (!empty($seller['phone'])) {
  $org['contactPoint'] = [[
    '@type' => 'ContactPoint',
    'telephone' => trim((string)$seller['phone']),
    'contactType' => 'customer service'
  ]];
}
if (!empty($seller['email'])) {
  $org['email'] = trim((string)$seller['email']);
}
if (!empty($seller['address'])) {
  $org['address'] = [
    '@type' => 'PostalAddress',
    'streetAddress' => trim((string)$seller['address'])
  ];
}
if ($ogImage !== '') $org['logo'] = $ogImage;

$website = [
  '@context' => 'https://schema.org',
  '@type' => 'WebSite',
  'name' => $siteName,
  'url' => $siteUrl
];

$webPage = [
  '@context' => 'https://schema.org',
  '@type' => 'WebPage',
  '@id' => $canonical,
  'url' => $canonical,
  'name' => $title,
  'description' => $desc
];
if ($ogImage !== '') {
  $webPage['primaryImageOfPage'] = [
    '@type' => 'ImageObject',
    'url' => $ogImage
  ];
}

$localBusiness = null;
if ($contactPhone !== '' || $contactAddress !== '') {
  $localBusiness = [
    '@context' => 'https://schema.org',
    '@type' => 'LocalBusiness',
    'name' => $siteName,
    'url' => $siteUrl
  ];
  if ($contactPhone !== '') $localBusiness['telephone'] = $contactPhone;
  if ($contactEmail !== '') $localBusiness['email'] = $contactEmail;
  if ($contactAddress !== '') {
    $localBusiness['address'] = [
      '@type' => 'PostalAddress',
      'streetAddress' => $contactAddress,
      'addressCountry' => 'RU'
    ];
  }
  $localBusiness['areaServed'] = 'Россия';
  $localBusiness['openingHours'] = 'Mo-Fr 10:00-19:00';
}

$ldObjects = [$org, $website, $webPage];
if ($localBusiness) $ldObjects[] = $localBusiness;
// FAQ structured data
if ($homeFaq){
  $faq = [
    '@context' => 'https://schema.org',
    '@type' => 'FAQPage',
    'mainEntity' => []
  ];
  foreach ($homeFaq as $f){
    $q = trim((string)($f['title'] ?? ''));
    $a = trim((string)($f['text'] ?? ''));
    if ($q === '' || $a === '') continue;
    $faq['mainEntity'][] = [
      '@type' => 'Question',
      'name' => $q,
      'acceptedAnswer' => [
        '@type' => 'Answer',
        'text' => $a
      ]
    ];
  }
  if (!empty($faq['mainEntity'])) $ldObjects[] = $faq;
}
if ($productView) {
  $id = (int)($productView['id'] ?? 0);
  $price = (float)($productView['price'] ?? 0);
  $stock = (int)($productView['stock_qty'] ?? 0);
  $reserved = (int)($reservedMap[$id] ?? 0);
  $onOrder = (int)($onOrderMap[$id] ?? 0);
  $available = $stock - $reserved;
  if ($available < 0) $available = 0;
  $mode = (string)($productView['supply_mode'] ?? 'stock');
  $availability = 'https://schema.org/OutOfStock';
  if ($available > 0) $availability = 'https://schema.org/InStock';
  else if ($mode === 'mto' || $mode === 'mixed' || $onOrder > 0) $availability = 'https://schema.org/PreOrder';
  $img = trim((string)($productView['image_url'] ?? ''));
  $props = [];
  if (!empty($productView['material'])) $props[] = ['@type'=>'PropertyValue','name'=>'Материал','value'=>(string)$productView['material']];
  if (!empty($productView['construction'])) $props[] = ['@type'=>'PropertyValue','name'=>'Конструкция','value'=>(string)$productView['construction']];
  if (!empty($productView['perforation'])) $props[] = ['@type'=>'PropertyValue','name'=>'Перфорация','value'=>(string)$productView['perforation']];
  if (!empty($productView['shelf_thickness'])) $props[] = ['@type'=>'PropertyValue','name'=>'Толщина полки','value'=>(string)$productView['shelf_thickness'].' мм'];
  if (!empty($productView['shelves'])) $props[] = ['@type'=>'PropertyValue','name'=>'Кол-во полок','value'=>(string)$productView['shelves']];
  $productLd = [
    '@context' => 'https://schema.org',
    '@type' => 'Product',
    'name' => trim((string)($productView['name'] ?? '')),
    'sku' => (string)$id,
    'image' => $img !== '' ? abs_url($img, $base, $scheme) : $ogImage,
    'description' => $desc,
    'brand' => [ '@type' => 'Brand', 'name' => $siteName ],
    'offers' => [
      '@type' => 'Offer',
      'url' => $canonical,
      'priceCurrency' => 'RUB',
      'price' => $price > 0 ? number_format($price, 2, '.', '') : '0',
      'availability' => $availability,
      'itemCondition' => 'https://schema.org/NewCondition',
      'seller' => [ '@type' => 'Organization', 'name' => $siteName ]
    ]
  ];
  if ($props) $productLd['additionalProperty'] = $props;
  if (!empty($productView['material'])) $productLd['material'] = (string)$productView['material'];
  $ldObjects[] = $productLd;
  $ldObjects[] = [
    '@context' => 'https://schema.org',
    '@type' => 'BreadcrumbList',
    'itemListElement' => [
      ['@type'=>'ListItem','position'=>1,'name'=>$siteName,'item'=>$siteUrl],
      ['@type'=>'ListItem','position'=>2,'name'=>'Каталог','item'=>$siteUrl.'#catalog'],
      ['@type'=>'ListItem','position'=>3,'name'=>trim((string)($productView['name'] ?? '')),'item'=>$canonical],
    ]
  ];
} elseif ($products) {
  $list = [];
  $productList = [];
  $pos = 1;
  foreach ($products as $p){
    $id = (int)($p['id'] ?? 0);
    $name = trim((string)($p['name'] ?? ''));
    if ($name === '') $name = 'Товар';
    $url = abs_url(product_public_url($p, $siteUrl), $base, $scheme);
    $list[] = [
      '@type' => 'ListItem',
      'position' => $pos++,
      'name' => $name,
      'url' => $url
    ];
    $price = (float)($p['price'] ?? 0);
    $stock = (int)($p['stock_qty'] ?? 0);
    $reserved = (int)($reservedMap[$id] ?? 0);
    $onOrder = (int)($onOrderMap[$id] ?? 0);
    $available = $stock - $reserved;
    if ($available < 0) $available = 0;
    $mode = (string)($p['supply_mode'] ?? 'stock');
    $availability = 'https://schema.org/OutOfStock';
    if ($available > 0) $availability = 'https://schema.org/InStock';
    else if ($mode === 'mto' || $mode === 'mixed' || $onOrder > 0) $availability = 'https://schema.org/PreOrder';
    $img = trim((string)($p['image_url'] ?? ''));
    $props = [];
    if (!empty($p['material'])) $props[] = ['@type'=>'PropertyValue','name'=>'Материал','value'=>(string)$p['material']];
    if (!empty($p['construction'])) $props[] = ['@type'=>'PropertyValue','name'=>'Конструкция','value'=>(string)$p['construction']];
    if (!empty($p['perforation'])) $props[] = ['@type'=>'PropertyValue','name'=>'Перфорация','value'=>(string)$p['perforation']];
    if (!empty($p['shelf_thickness'])) $props[] = ['@type'=>'PropertyValue','name'=>'Толщина полки','value'=>(string)$p['shelf_thickness'].' мм'];
    if (!empty($p['shelves'])) $props[] = ['@type'=>'PropertyValue','name'=>'Кол-во полок','value'=>(string)$p['shelves']];
    $productList[] = [
      '@context' => 'https://schema.org',
      '@type' => 'Product',
      'name' => $name,
      'sku' => (string)$id,
      'image' => $img !== '' ? abs_url($img, $base, $scheme) : $ogImage,
      'description' => trim((string)($p['description'] ?? '')),
      'brand' => [ '@type' => 'Brand', 'name' => $siteName ],
      'offers' => [
        '@type' => 'Offer',
        'url' => $url,
        'priceCurrency' => 'RUB',
        'price' => $price > 0 ? number_format($price, 2, '.', '') : '0',
        'availability' => $availability,
        'itemCondition' => 'https://schema.org/NewCondition',
        'seller' => [ '@type' => 'Organization', 'name' => $siteName ]
      ]
    ];
    if ($props) $productList[count($productList)-1]['additionalProperty'] = $props;
    if (!empty($p['material'])) $productList[count($productList)-1]['material'] = (string)$p['material'];
  }
  $ldObjects[] = [
    '@context' => 'https://schema.org',
    '@type' => 'ItemList',
    'itemListElement' => $list
  ];
  foreach ($productList as $pl){ $ldObjects[] = $pl; }
}

$ld = json_encode($ldObjects, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
?>
<!DOCTYPE html>

<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta content="width=device-width, initial-scale=1" name="viewport"/>
<meta content="light" name="color-scheme"/>
<meta content="<?= h($themeColor) ?>" name="theme-color"/>
<title><?= h($title) ?></title>
<meta content="<?= h($desc) ?>" name="description"/>
<?php if ($keywords !== ''): ?>
<meta content="<?= h($keywords) ?>" name="keywords"/>
<?php endif; ?>
<meta content="<?= h($robots) ?>" name="robots"/>
<link href="<?= h($canonical) ?>" rel="canonical"/>
<link href="/sitemap.php" rel="sitemap" type="application/xml" title="Sitemap"/>
<link href="<?= h($canonical) ?>" rel="alternate" hreflang="ru-RU"/>
<link href="<?= h($canonical) ?>" rel="alternate" hreflang="x-default"/>
<?php if ($googleVerify !== ''): ?>
<meta content="<?= h($googleVerify) ?>" name="google-site-verification"/>
<?php endif; ?>
<?php if ($yandexVerify !== ''): ?>
<meta content="<?= h($yandexVerify) ?>" name="yandex-verification"/>
<?php endif; ?>
<?php if ($bingVerify !== ''): ?>
<meta content="<?= h($bingVerify) ?>" name="msvalidate.01"/>
<?php endif; ?>
<link href="/favicon.svg" rel="icon" type="image/svg+xml"/>
<link href="/favicon.ico" rel="icon" sizes="any"/>
<meta content="<?= h($siteName) ?>" property="og:site_name"/>
<meta content="<?= h($ogTitle) ?>" property="og:title"/>
<meta content="<?= h($ogDesc) ?>" property="og:description"/>
<meta content="<?= h($canonical) ?>" property="og:url"/>
<meta content="<?= h($ogImage) ?>" property="og:image"/>
<meta content="<?= h($ogImageAlt) ?>" property="og:image:alt"/>
<meta content="<?= h($ogType) ?>" property="og:type"/>
<meta content="<?= h($ogLocale) ?>" property="og:locale"/>
<?php if ($productView): ?>
<meta content="<?= h(number_format((float)($productView['price'] ?? 0), 2, '.', '')) ?>" property="product:price:amount"/>
<meta content="RUB" property="product:price:currency"/>
<?php endif; ?>
<meta content="<?= h($twitterCard) ?>" name="twitter:card"/>
<meta content="<?= h($ogTitle) ?>" name="twitter:title"/>
<meta content="<?= h($ogDesc) ?>" name="twitter:description"/>
<meta content="<?= h($ogImage) ?>" name="twitter:image"/>
<meta content="<?= h($ogImageAlt) ?>" name="twitter:image:alt"/>
<?php if ($gaMeasurementId !== ''): ?>
<script async src="https://www.googletagmanager.com/gtag/js?id=<?= h($gaMeasurementId) ?>"></script>
<script>
window.dataLayer = window.dataLayer || [];
function gtag(){ dataLayer.push(arguments); }
window.gtag = window.gtag || gtag;
gtag('js', new Date());
gtag('config', '<?= h($gaMeasurementId) ?>', { anonymize_ip: true });
</script>
<?php endif; ?>
<?php if ($yandexMetrikaId !== ''): ?>
<script>
(function(m,e,t,r,i,k,a){m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};
m[i].l=1*new Date();for (var j=0;j<document.scripts.length;j++) {if (document.scripts[j].src===r) { return; }}
k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)})
(window, document, "script", "https://mc.yandex.ru/metrika/tag.js", "ym");
ym(<?= (int)$yandexMetrikaId ?>, "init", {
  clickmap:true,
  trackLinks:true,
  accurateTrackBounce:true,
  webvisor:true
});
</script>
<?php endif; ?>
<script>
window.STILVA_ANALYTICS = {
  ymId: <?= json_encode($yandexMetrikaId, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) ?>,
  gaId: <?= json_encode($gaMeasurementId, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) ?>
};
window.stilvaTrack = function(eventName, params){
  try{
    var ev = String(eventName || '').trim();
    if (!ev) return;
    var payload = (params && typeof params === 'object') ? params : {};
    var cfg = window.STILVA_ANALYTICS || {};
    if (cfg.gaId && typeof window.gtag === 'function') window.gtag('event', ev, payload);
    if (cfg.ymId && typeof window.ym === 'function') window.ym(Number(cfg.ymId), 'reachGoal', ev, payload);
  }catch(_){}
};
</script>
<?php if ($ld): ?>
<script type="application/ld+json"><?= $ld ?></script>
<?php endif; ?>
<link href="assets/css/main.css" rel="stylesheet"/>
</head>
<body>
<?php if ($yandexMetrikaId !== ''): ?>
<noscript><div><img src="https://mc.yandex.ru/watch/<?= (int)$yandexMetrikaId ?>" style="position:absolute;left:-9999px" alt=""/></div></noscript>
<?php endif; ?>
<!-- Header -->
<div class="wrap header">
<div class="header__row">
<div class="brand">STILVA</div>
<nav aria-label="Навигация" class="nav">
<a href="#benefits">Плюсы</a>
<a href="#catalog">Каталог</a>
<a href="#specs">Характеристики</a>
<a href="#faq">FAQ</a>
<a href="#contacts">Контакты</a>
</nav>
<div class="hdr-cta"><a class="btn" href="#catalog">Каталог</a></div>
</div>
</div>
<!-- Hero -->
<section class="wrap hero section">
<div class="inner">
<div class="hero__grid">
<div>
<span class="eyebrow"><?= h($homeHeroEyebrow) ?></span>
<h1><?= render_multiline($heroTitle) ?></h1>
<p><?= h($homeHeroLead) ?></p>
<div class="cta-row">
<a class="btn" href="<?= h($homeHeroCta1Link) ?>"><?= h($homeHeroCta1Text) ?></a>
<a class="btn btn--ghost" href="<?= h($homeHeroCta2Link) ?>"><?= h($homeHeroCta2Text) ?></a>
</div>
</div>
<div class="hero__media"><div aria-live="polite" class="hero-mini" id="hero-mini"></div></div>
</div>
</div>
</section>
<!-- WHAT YOU GET -->
<section class="wrap section" id="benefits">
<div class="section__head">
<h2 class="h2"><?= h($homeBenefitsTitle) ?></h2>
<p class="lead"><?= h($homeBenefitsLead) ?></p>
</div>
<div class="catalog__grid" style="grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;padding-inline:var(--side)">
<?php foreach ($homeBenefits as $it): ?>
<div class="product"><div class="product__body"><div class="product__title"><?= h($it['title']) ?></div><div class="product__meta"><?= h($it['text']) ?></div></div></div>
<?php endforeach; ?>
</div>
</section>
<!-- Catalog -->
<section aria-busy="true" id="catalog">
<div class="section__head">
<h2 class="h2">Каталог готовых решений</h2>
<p class="lead">Доверьтесь нашему опыту - ниже представлены самые лучше образцы нашей продукции.</p>
</div>
<div class="catalog__grid"><?php if ($catalogHtml !== '') echo $catalogHtml; ?></div>
</section>
<!-- Specs -->
<section class="wrap section" id="specs">
<div class="section__head">
<h2 class="h2"><?= h($homeSpecsTitle) ?></h2>
<p class="lead"><?= h($homeSpecsLead) ?></p>
</div>
<div class="catalog__grid" style="grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;padding-inline:var(--side)">
<?php foreach ($homeSpecs as $it): ?>
<div class="product"><div class="product__body"><div class="product__title"><?= h($it['title']) ?></div><div class="product__meta"><?= h($it['text']) ?></div></div></div>
<?php endforeach; ?>
</div>
</section>
<?php if ($homeCases): ?>
<section class="wrap section" id="cases">
<div class="section__head">
<h2 class="h2"><?= h($homeCasesTitle) ?></h2>
<p class="lead"><?= h($homeCasesLead) ?></p>
</div>
<div class="catalog__grid" style="grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;padding-inline:var(--side)">
<?php foreach ($homeCases as $it): ?>
<div class="product"><div class="product__body"><div class="product__title"><?= h($it['title']) ?></div><div class="product__meta"><?= h($it['text']) ?></div></div></div>
<?php endforeach; ?>
</div>
</section>
<?php endif; ?>
<?php if ($homeTypes): ?>
<section class="wrap section" id="types">
<div class="section__head">
<h2 class="h2"><?= h($homeTypesTitle) ?></h2>
<p class="lead"><?= h($homeTypesLead) ?></p>
</div>
<div class="catalog__grid" style="grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;padding-inline:var(--side)">
<?php foreach ($homeTypes as $it): ?>
<div class="product"><div class="product__body"><div class="product__title"><?= h($it['title']) ?></div><div class="product__meta"><?= h($it['text']) ?></div></div></div>
<?php endforeach; ?>
</div>
</section>
<?php endif; ?>
<!-- FAQ -->
<section class="wrap section" id="faq">
<div class="section__head">
<h2 class="h2"><?= h($homeFaqTitle) ?></h2>
<p class="lead"><?= h($homeFaqLead) ?></p>
</div>
<div class="catalog__grid" style="grid-template-columns:1fr 1fr;gap:16px;padding-inline:var(--side)">
<?php foreach ($homeFaq as $it): ?>
<div class="product"><div class="product__body"><div class="product__title"><?= h($it['title']) ?></div><div class="product__meta"><?= h($it['text']) ?></div></div></div>
<?php endforeach; ?>
</div>
</section>
<?php if ($homeSeoText !== ''): ?>
<section class="wrap section" id="seo-text">
<div class="section__head">
<h2 class="h2"><?= h($homeSeoTitle) ?></h2>
</div>
<div class="seo-text">
<?= render_paragraphs($homeSeoText) ?>
</div>
</section>
<?php endif; ?>
<section aria-label="Контакты" class="contacts" id="contacts">
<div class="section__head">
<h2 class="h2"><?= h($homeContactsTitle) ?></h2>
<p class="lead"><?= h($homeContactsLead) ?></p>
</div>
<div class="contacts__grid">
  <!-- Левая карточка (визитка) -->
  <div class="contacts__card contacts__card--center">
    <h2 class="contacts__title">Контакты</h2>
    <p class="contacts__row"><strong>Email:</strong> <a href="mailto:<?= h($contactEmail) ?>"><?= h($contactEmail) ?></a></p>
    <p class="contacts__row"><strong>Телефон:</strong> <a href="tel:+<?= h($contactPhoneTel) ?>"><?= h($contactPhone) ?></a></p>
    <p class="contacts__note"><?= h($contactNote) ?></p>
  </div>

  <!-- Правая карточка (мессенджеры c QR) -->
  <div class="contacts__card contacts__card--center">
    <h2 class="contacts__title">Связаться в мессенджерах</h2>

    <div class="qr-lines">
      <a class="qr-item" href="https://t.me/<?= h($homeTgHandle) ?>" target="_blank" rel="noopener">
        <img alt="QR Telegram" width="120" height="120"
             src="https://api.qrserver.com/v1/create-qr-code/?size=180x180&margin=10&data=<?= urlencode('https://t.me/'.$homeTgHandle) ?>"/>
        <div class="qr-meta">
          <div class="qr-title">Telegram</div>
          <div class="qr-sub">@<?= h($homeTgHandle) ?></div>
          <div class="qr-cta">Открыть чат</div>
        </div>
      </a>

      <a class="qr-item" href="https://wa.me/<?= h($homeWaDigits) ?>?text=<?= urlencode('Здравствуйте, хочу оформить заказ') ?>" target="_blank" rel="noopener">
        <img alt="QR WhatsApp" width="120" height="120"
             src="https://api.qrserver.com/v1/create-qr-code/?size=180x180&margin=10&data=<?= urlencode('https://wa.me/'.$homeWaDigits.'?text=Здравствуйте, хочу оформить заказ') ?>"/>
        <div class="qr-meta">
          <div class="qr-title">WhatsApp</div>
          <div class="qr-sub">+<?= h($homeWaDigits) ?></div>
          <div class="qr-cta">Открыть чат</div>
        </div>
      </a>
    </div>

    <div class="qr-hint">Сканируйте камерой телефона или нажмите, если вы за компьютером.</div>
  </div>
</div>
</section>
<!-- Footer -->
<footer class="wrap" id="contacts" style="border-top:1px solid var(--border);padding:24px var(--side) 40px;color:var(--muted);font-size:14px">
<div><strong>STILVA</strong> • Производство и поставка стеллажей из нержавеющей стали</div>
<div style="margin-top:6px">
<a href="mailto:<?= h($contactEmail) ?>"><?= h($contactEmail) ?></a> •
      <a href="tel:+<?= h($contactPhoneTel) ?>"><?= h($contactPhone) ?></a>
</div>
<div style="margin-top:6px">ИНН / ОГРН — по запросу • © 2025 STILVA</div>
</footer>
<!-- Бейдж корзины -->
<button id="cart-badge" style="position:fixed;right:20px;bottom:20px;z-index:9999;background:#111;color:#fff;padding:10px 14px;border-radius:12px;border:none;cursor:pointer">
    Корзина: <span id="cart-count">0</span>
</button>
<!-- Панель заказа -->
<aside aria-label="Ваш заказ" id="order-panel" style="display:none;position:fixed;right:14px;top:0;height:100%;width:380px;max-width:95vw;
           background:#fff;border-radius:12px 0 0 12px;
           box-shadow:-20px 0 40px rgba(0,0,0,.25);z-index:9998;padding:16px;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
<strong>Ваш заказ</strong>
<button id="order-close" style="border:none;background:#eee;border-radius:8px;padding:6px 10px;cursor:pointer">✕</button>
</div>
<div id="cart-lines" style="display:grid;gap:8px;margin-bottom:12px;"></div>
<div style="display:flex;justify-content:space-between;align-items:center;font-weight:700;margin:12px 0;">
<span>Итого:</span><span><span id="cart-total">0.00</span> ₽</span>
</div>
<form id="checkout-form" style="display:grid;gap:8px">
<input name="name" placeholder="Ваше имя" style="padding:10px;border:1px solid #ddd;border-radius:8px"/>
<input name="phone" placeholder="Телефон" style="padding:10px;border:1px solid #ddd;border-radius:8px"/>
<input name="email" placeholder="Email" style="padding:10px;border:1px solid #ddd;border-radius:8px"/>
<textarea name="note" placeholder="Пожелания" rows="3" style="padding:10px;border:1px solid #ddd;border-radius:8px"></textarea>
<button id="order-submit" style="width:100%;background:#15803d;color:#fff;border:none;border-radius:10px;
                     padding:14px 16px;font-weight:700;cursor:pointer">
        Отправить заказ
      </button>
<!-- строка прогресса -->
<div id="order-progress" style="display:none;align-items:center;gap:8px;margin-top:8px;color:#15803d">
<span class="spinner"></span>
<span>Отправляем заказ…</span>
</div>
<!-- успех и ошибка -->
<div id="order-success" style="display:none;margin-top:8px;color:#166534"></div>
<div id="order-error" style="display:none;margin-top:8px;color:#b91c1c"></div>
</form>
</aside>
<!-- Корзина/каталог -->
<script src="/js/app.js"></script>
<!-- Рендер каталога и связь с корзиной (кнопки «Заказать») -->
<!--Инлайновый скрипт #2 перенесён в assets/js/inline.js -->
<!--Инлайновый скрипт #3 перенесён в assets/js/inline.js -->
<!-- mini-cards gentle motion -->
<!--Инлайновый скрипт #4 перенесён в assets/js/inline.js -->
<!-- Инлайновый скрипт #5 перенесён в assets/js/inline.js -->
<!-- BEGIN: background cloud animator -->
<!--Инлайновый скрипт #6 перенесён в assets/js/inline.js -->
<!-- END: background cloud animator -->
<script defer="True" src="assets/js/inline.js"></script></body>
</html>
