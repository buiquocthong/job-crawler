# Vancouver Job Crawler — Hướng dẫn cài đặt

## Cấu trúc dự án
```
your-repo/
├── .github/
│   └── workflows/
│       └── job_crawler.yml      ← GitHub Actions workflow
├── vancouver_job_crawler.py     ← Script crawler chính
├── requirements.txt
└── README_setup.md
```

---

## Bước 1 — Tạo GitHub Repository
1. Tạo repo mới trên GitHub (public hoặc private đều được)
2. Push 4 files trên lên nhánh `main`

---

## Bước 2 — Cấu hình GitHub Secrets

Vào repo → **Settings → Secrets and variables → Actions → New repository secret**

| Secret Name         | Giá trị                               | Bắt buộc |
|---------------------|---------------------------------------|----------|
| `TEAMS_WEBHOOK_URL` | URL webhook từ MS Teams (xem bên dưới)| ✅       |
| `JOB_PROXY`         | `user:pass@proxy.host:8080`           | ⚠️ nếu Indeed chặn |

### Lấy TEAMS_WEBHOOK_URL

1. Vào kênh MS Teams muốn nhận thông báo
2. Click `...` (More options) → **Connectors**
3. Tìm **"Incoming Webhook"** → Configure
4. Đặt tên (vd: "Job Crawler") → Create
5. Copy URL → dán vào GitHub Secret

> **Lưu ý:** Microsoft đang dần ẩn Incoming Webhook trong Teams mới.  
> Nếu không thấy, vào **Teams Admin Center** và bật lại Connector.

---

## Bước 3 — Tạo nhánh `data` (lưu CSV)

```bash
git checkout --orphan data
git rm -rf .
echo "# Data branch" > README.md
git add README.md
git commit -m "init data branch"
git push origin data
git checkout main
```

---

## Bước 4 — Kiểm tra hoạt động

1. Vào **Actions** tab trên GitHub
2. Click **"Vancouver Job Crawler"** → **"Run workflow"**
3. Tick **"demo mode"** để test không cần proxy
4. Xem log và kiểm tra kênh Teams

---

## Lịch chạy tự động

File đã cài đặt: **7:00 AM PST (15:00 UTC)** mỗi ngày

Để đổi giờ, sửa dòng cron trong `.github/workflows/job_crawler.yml`:
```yaml
- cron: '0 15 * * *'   # 7am PST
- cron: '0 14 * * *'   # 7am PDT (hè)
- cron: '0 16 * * *'   # 8am PST
```

**Converter:** https://crontab.guru

---

## Xử lý lỗi thường gặp

### Indeed trả về 0 jobs hoặc 403
- Indeed chặn crawl từ IP server → cần proxy từ CA hoặc VPN
- Cấu hình `JOB_PROXY` secret: `username:password@proxy.host:port`
- Dịch vụ proxy gợi ý: Brightdata, Oxylabs, Smartproxy (có IP Canada)

### Salary N/A nhiều
- Hoàn toàn bình thường — nhiều employer không đăng lương
- Script đã tự parse từ description nếu có
- Jobs N/A lương vẫn được giữ lại (cột `salary_source` trống)

### CSV không vào nhánh data
- Đảm bảo nhánh `data` đã được tạo (Bước 3)
- Đảm bảo `GITHUB_TOKEN` có quyền write (mặc định là có)
- Vào Settings → Actions → General → Workflow permissions → "Read and write"

### Teams không nhận được tin
- Kiểm tra `TEAMS_WEBHOOK_URL` đã đúng chưa
- Thử paste URL vào Postman gửi POST request test
- Kiểm tra Admin có tắt Incoming Webhook không

---

## Download CSV

Sau khi chạy, CSV có thể download tại:
- **GitHub Artifact:** Actions → chọn run → Artifacts → `vancouver-jobs-*`
- **Nhánh data:** `https://raw.githubusercontent.com/YOUR_USER/YOUR_REPO/data/vancouver_jobs_YYYY-MM-DD.csv`
- **Link trong Teams card:** click "📥 Download CSV" trong tin nhắn

---

## Chạy local

```bash
# Cài đặt
pip install -r requirements.txt

# Demo (không cần proxy)
python vancouver_job_crawler.py --demo

# Chạy thật (cần proxy/VPN Canada)
export JOB_PROXY="user:pass@proxy.host:8080"
export TEAMS_WEBHOOK_URL="https://outlook.office.com/webhook/..."
python vancouver_job_crawler.py
```
