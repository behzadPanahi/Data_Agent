# همیار داده · نسخه دوم

عامل تحلیل داده‌ی محلی با Trino به‌عنوان تنها لایه‌ی اتصال و اجرای کوئری. برنامه connectorهای دیتابیس را جداگانه پیاده‌سازی نمی‌کند: Trino به کمک catalogها و connectorهای پیکربندی‌شده منابع را در اختیار برنامه می‌گذارد.

## اجرا

Python 3.11 یا جدیدتر لازم است:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app:app --reload
```

سپس `http://127.0.0.1:8000` را باز کنید. اگر محیط مجازی نسخه قبلی فعال است، `pip install -r requirements.txt` را دوباره اجرا کنید تا Trino client نصب شود. در لینوکس/macOS: `source .venv/bin/activate`.

## گردش کار

1. در «مدل و حافظه»، OpenAI-compatible API، مدل، کلید، تعداد نوبت‌های حافظه، تعداد ردیف نمونه و سقف اجرای کوئری را تنظیم کنید.
2. Trino مدیریت‌شده را با Docker Compose اجرا کنید: `docker compose up -d` (روی پورت `8081`). سپس در «Trino و catalogها» نشانی `http://localhost:8081` را وارد کنید، اتصال را بیازمایید و ذخیره کنید. کانتینر قبلی روی پورت `8080` می‌تواند جداگانه باقی بماند.
3. در «اتصال منبع» نوع منبع را انتخاب و فیلدهای فرم را پر کنید. برای connectorهای دیگر «سایر» را انتخاب کنید و تنظیمات را به شکل `key=value` بدهید. برنامه فایل را در `data/trino/catalog` می‌نویسد، Trino مدیریت‌شده را restart می‌کند و دسترسی catalog به schemaها را بررسی می‌کند. نصب image به‌تنهایی لزوماً همه pluginهای جانبی را شامل نمی‌شود.
4. در «Trino و catalogها» catalogهای نمایان را انتخاب و metadata را بخوانید. برنامه metadata را مستقیماً از `<catalog>.information_schema.columns` می‌گیرد؛ cache کوتاه ۶۰ ثانیه‌ای فقط در حافظه سرور است.
5. از گفت‌وگو بپرسید. عامل metadata مرتبط را به مدل می‌دهد، SQL یک یا چندمنبعی با نام کامل `catalog.schema.table` می‌سازد و از Trino اجرا می‌کند. رویدادهای مرحله‌ای حین اجرا دیده می‌شوند و پس از پاسخ در جزئیات پیام جمع می‌شوند.

دسترسی منابع و عملیات واقعی تابع connector و مجوزهای Trino است. برنامه تنها SQLهای `SELECT` و `WITH` را می‌پذیرد؛ برای اجرا از کاربر کم‌دسترسی و read-only استفاده کنید. فایل‌های catalog ممکن است رمز اتصال داشته باشند و به‌صورت متن در `data/` محلی ذخیره می‌شوند؛ آن پوشه از Git صرف‌نظر شده است.

## تنظیمات و داده محلی

تنظیمات، کلیدها و تاریخچه گفتگو در `data/state.json` ذخیره می‌شوند و این پوشه از Git صرف‌نظر شده است. کلیدها به صورت متن محلی هستند؛ فقط روی localhost اجرا کنید و این فایل را به اشتراک نگذارید. حافظه هر چت از تعداد نوبت‌های تنظیم‌شده پیروی می‌کند. سقف تلاش کوئری از ۱ تا ۱۰ و مقدار پیش‌فرض ۵ است.

## محدوده فعلی

- اتصال یک coordinator Trino با بدون احراز هویت یا Basic روی HTTPS.
- دریافت catalogها از `SHOW CATALOGS` و metadata از `information_schema` در خود Trino (fallback با SHOW SCHEMAS/TABLES/COLUMNS).
- انتخاب catalogهای قابل دسترس عامل و خواندن مجدد metadata.
- تعریف عمومی catalogهای Trino از UI برای هر connector نصب‌شده و restart کانتینر مدیریت‌شده.
- فرم‌های آماده برای PostgreSQL، MySQL، MariaDB، SQL Server، Oracle، ClickHouse، MongoDB، Elasticsearch، OpenSearch، Redshift، Snowflake، BigQuery، Hive، Iceberg، Delta Lake، Hudi و Redis؛ به‌علاوه حالت «سایر» برای تنظیمات عمومی.
- کنسول SQL خواندنی و نمونه محدود نتایج.
- گفتگوی فارسی با رویدادهای زنده، حافظه گفتگو و تلاش محدود برای اصلاح SQL.
- API مدل سازگار با OpenAI Chat Completions و tool calling.
