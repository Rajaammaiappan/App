# Vehicle Loan Manager — Web App

Flask web version of FinalCode_V6_0.py. Render-ready.

## Features
- Login with 3 roles: Admin / Manager / Viewer
- Dashboard with KPI cards and charts (Chart.js — no matplotlib needed)
- New loan entry, approval/rejection workflow
- EMI schedule with partial/full payment recording
- Overdue & upcoming alerts
- Closed and rejected loan archives
- Loan calculator (EMI / Affordability / Tenure / Rate) with amortisation schedule
- PDF report download (requires reportlab)
- User management (admin only)

## Default logins
| Username | Password   | Role    |
|----------|------------|---------|
| admin    | admin123   | Admin   |
| manager  | manager123 | Manager |
| viewer   | viewer123  | Viewer  |

## Deploy to Render (same steps as your trading app)

1. Push this folder to a private GitHub repo
2. Go to render.com → New → Web Service → connect repo
3. Settings:
   - Runtime: Python 3
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn app:app`
   - Region: Singapore
4. Click Deploy

## Environment Variables (optional)
| Variable      | Description                        |
|---------------|------------------------------------|
| SECRET_KEY    | Flask session secret (auto-gen ok) |
| DB_FILE       | SQLite path (default: vehicle_loans.db) |
| SMTP_HOST     | Gmail SMTP host                    |
| SMTP_SENDER   | Sender email address               |
| SMTP_PASSWORD | Gmail app password                 |
| SMTP_ENABLED  | Set to `true` to enable emails     |

## Database
SQLite is used by default. For permanent data on Render free tier,
connect a PostgreSQL database later (just swap get_db() connection).
