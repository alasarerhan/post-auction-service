# Railway Deployment Steps

Bu dosya Post-Auction & Fulfillment Service'i Railway'a deploy etmek için atılan / atılacak tüm adımları kronolojik olarak listeler.

## Ön koşullar

- Node.js >= 22 ve npm mevcut (`node --version` → v26.3.0).
- Docker mevcut.
- GitHub CLI (`gh`) mevcut ve hesaba login.
- Git repo'su mevcut: `https://github.com/alasarerhan/bidding-service.git`.
- Docker daemon çalışır durumda (lokal doğrulama için).

## Adımlar

### 1. Railway CLI kurulumu

Durum: Tamamlandı.

Komut:

```bash
npm install -g @railway/cli
```

Çıktı:

```text
/opt/homebrew/bin/railway
railway 5.20.0
```

Not: Railway ile interaktif işlem yapabilmek için CLI gerekli.

### 2. Railway hesabı açma

Durum: Kullanıcı tarafından yapılacak.

Komutlar / adımlar:

1. https://railway.app adresine git.
2. **Login with GitHub** seç.
3. GitHub hesabı `alasarerhan` ile yetkilendir.
4. İlk girişte Railway ücretsiz **$5 deneme kredisi** verir (Hobby plan).

### 3. Kart doğrulama

Durum: Kullanıcı tarafından yapılacak.

Adımlar:

1. Railway dashboard → sağ üst profil/avatar → **Account Settings**.
2. **Billing** sekmesi → **Add Payment Method**.
3. Kredi/banka kartı ekle.
4. Kart doğrulanınca Hobby plan aktif olur.

Not: Trial kredisini kullanmak için kart gerekli olmayabilir, fakat Hobby plan için kart zorunlu.

### 4. Yeni proje oluşturma

Durum: Kullanıcı tarafından yapılacak.

Komutlar / adımlar:

1. Railway dashboard → **New Project**.
2. **Deploy from GitHub Repo** seç.
3. `alasarerhan/post-auction-fulfillment-service` reposunu listeden seç.
4. Branch: `main`.
5. Root directory: repo kökü.

Not: Bu repo daha oluşturulmadı; aşağıdaki adımda oluşturulacak.

### 5. Post-Auction için ayrı repo oluşturma

Durum: Yapılacak.

```bash
gh repo create post-auction-fulfillment-service --public \
  --description "Post-Auction & Fulfillment Service for Online Fish Auction"
git clone https://github.com/alasarerhan/post-auction-fulfillment-service.git
cd post-auction-fulfillment-service
```

Mevcut repo'dan kopyalanacak dosyalar:

```text
package.json
src/domain/fulfillment.service.js
src/controllers/fulfillment.controller.js
src/routes/fulfillment.routes.js
src/views/fulfillment.ejs
src/views/index.ejs
src/public/main.js
src/db/schema.sql
src/db/pool.js
src/db/init.js
src/kafka/{config,consumer,producer,topics,schema-registry}.js
src/sockets/socket.js
schema/fulfillment.*.schema.json
schema/user.member.registered.schema.json
test/fulfillment.*.test.js
```

Ek dosyalar:

- `src/app.js` (sadece fulfillment route + health endpoint).
- `Dockerfile`.
- `.dockerignore`.
- `.env.example`.
- `.gitignore` (Post-Auction versiyonu).
- `README.md` (Post-Auction versiyonu).

### 6. Dockerfile yazma

Durum: Yapılacak.

```dockerfile
FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .
EXPOSE 3000
CMD ["sh", "-c", "node src/db/init.js && node src/app.js"]
```

`.dockerignore`:

```text
node_modules
.git
.tmp
*.md
api-key-*.txt
.env
```

### 7. Lokal doğrulama

Durum: Yapılacak.

```bash
docker run --name post-auction-postgres \
  -e POSTGRES_DB=post_auction_service \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  -d postgres:16

docker start post-auction-postgres
npm install
npm run db:init
npm run dev

curl -I http://localhost:3000/health
curl -I http://localhost:3000/fulfillment
npm test
```

### 8. GitHub'a push

Durum: Yapılacak.

```bash
git add .
git commit -m "feat: post-auction & fulfillment service"
git push origin main
```

### 9. Railway env değişkenleri

Durum: Yapılacak.

Railway servis panelinde **Variables** sekmesinde eklenecek:

```env
PORT=3000
PGHOST=<postgres-host>
PGPORT=5432
PGDATABASE=post_auction_service
PGUSER=postgres
PGPASSWORD=<postgres-password>
KAFKA_BROKERS=<bootstrap-server>
KAFKA_SASL_USERNAME=<api-key>
KAFKA_SASL_PASSWORD=<api-secret>
KAFKA_GROUP_ID=post-auction-service-prod
KAFKA_CLIENT_ID=post-auction-service
```

### 10. Railway servisi ayarları

Durum: Yapılacak.

Railway servisinde **Settings** sekmesi:

- Build Command: boş (Dockerfile kullanılır).
- Start Command: `sh -c "node src/db/init.js && node src/app.js"`.
- Healthcheck Path: `/health`.

### 11. Deploy tetikleme

Durum: Yapılacak.

GitHub push sonrası Railway otomatik deploy tetikler. Manuel tetiklemek için **Deploy** düğmesi.

### 12. Domain oluşturma

Durum: Yapılacak.

1. Railway servisinde **Settings → Domains**.
2. **Generate Domain** tıkla.
3. URL otomatik atanır:

```text
https://post-auction-service-production.up.railway.app
```

### 13. Doğrulama

Durum: Yapılacak.

```bash
curl https://post-auction-service-production.up.railway.app/health
# {"status":"ok","service":"post-auction","version":"1.0.0"}

curl -I https://post-auction-service-production.up.railway.app/fulfillment
# HTTP/1.1 200 OK
```

Railway panelinden **Logs** sekmesinden kontrol:

```text
Post-Auction service listening on port 3000
Kafka consumer started
memberAssignment: { "bid.basket.sold": [...], ... }
```

## Deployment Stage

**Durum:** Henüz deployment stage'e ulaşılmadı.

Tamamlanan otomatik adımlar:

- Railway CLI kuruldu (`v5.20.0`).

Engellenen adımlar (kullanıcı gerekiyor):

- Railway hesabı açma (interaktif GitHub login).
- Kart doğrulama.
- Proje oluşturma (UI veya CLI ile token gerekiyor).
- Domain oluşturma.

## Notlar / Engeller

- Railway CLI kurulu olmasına rağmen token/login olmadığı için `railway whoami` çalıştırıldığında:

```text
Unauthorized. Please login with `railway login`
```

- Bu yüzden Railway üzerinde proje oluşturma, deploy ve domain atama adımları kullanıcı tarafından yapılmalı.
- Kullanıcı terminalinde şu adımları çalıştırırsa deployment stage'e ulaşılır:

```bash
railway login
railway init
railway variables set PORT=3000
railway variables set KAFKA_GROUP_ID=post-auction-service-prod
# (diğer env değişkenleri UI veya CLI ile)
railway up
```

- Alternatif: Railway UI üzerinden New Project → GitHub repo seç → Variables ekle → Deploy.

## Sonraki adımlar (kullanıcı için)

1. Railway hesabı aç ve kart ekle.
2. Yeni repo `post-auction-fulfillment-service` oluştur ve Post-Auction kodunu push et.
3. Railway'de projeyi oluştur ve repo'yu bağla.
4. Env değişkenlerini ekle.
5. Deploy tetikle.
6. Domain oluştur ve URL'i kaydet.
7. URL'i Kafka ekibiyle paylaş.
