
# STILVA · PHP + MySQL (root-layout)

Развёрнут для обычного хостинга: `index.html` и `admin.html` лежат в корне. API — папка `/api` с `.htaccess`, так что маршруты вида `/api/products` работают без `index.php` в ссылке.

## Структура
```
/index.html
/admin.html
/js/app.js
/assets/css/main.css
/assets/js/inline.js
/api/index.php
/api/config.sample.php
/api/.htaccess
/.htaccess
/sql/schema.sql
/sql/seed.sql
```

## Нюансы деплоя
- Кладёшь **весь** архив в корень сайта на хостинге: тогда страницы открываются как `https://stilva.ru/` и `https://stilva.ru/admin.html`.
- Файлы CSS/JS уже подключены относительными путями (`assets/...`, `js/...`) и будут резолвиться от корня.
- API доступно по `https://stilva.ru/api/...`. В `/api/.htaccess` настроен роутинг на `index.php`.
- Скопируй `api/config.sample.php` в `api/config.php` и пропиши доступы к БД.

## Установка БД (MySQL)
```sql
SOURCE sql/schema.sql;
SOURCE sql/seed.sql;
```
