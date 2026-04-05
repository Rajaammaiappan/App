# =============================================================================
# app.py  —  Vehicle Loan Manager  (Pure Flask Web App, v8.0)
# =============================================================================
# Features:
#   ✅ Full Flask web app (no Tkinter)
#   ✅ PostgreSQL (cloud) + SQLite (local) — auto-detected via DATABASE_URL
#   ✅ User roles: Admin / Manager / Viewer
#   ✅ Loan entry, approval, rejection
#   ✅ EMI schedule generation and payments (full + partial)
#   ✅ Overdue and upcoming EMI alerts
#   ✅ PDF report generation (reportlab)
#   ✅ Email notifications (SMTP)
#   ✅ Search and filter on every page
#   ✅ Loan calculator with amortization
#   ✅ Dashboard with KPI cards
#   ✅ CSV export
#   ✅ Render / Railway / Heroku ready
#
# Local dev:
#   pip install -r requirements.txt
#   python app.py
#
# Cloud deploy (Render):
#   Set env vars: DATABASE_URL, SECRET_KEY
#   Start command: gunicorn "app:create_app()"
# =============================================================================

import os, csv, io, math, hashlib, secrets, threading, smtplib, calendar
import sqlite3
from datetime import date, datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
from flask import (Flask, render_template_string, request, redirect,
                   session, flash, send_file, make_response, jsonify)

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle, PageBreak)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False

# =============================================================================
# CONFIG
# =============================================================================
DB_FILE       = "vehicle_loans.db"
UPCOMING_DAYS = 10

_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if _DATABASE_URL.startswith("postgres://"):
    _DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1)
USE_POSTGRES = _DATABASE_URL.startswith("postgresql")

EMAIL_CONFIG = {
    "smtp_host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port": int(os.environ.get("SMTP_PORT", 587)),
    "sender":    os.environ.get("SMTP_EMAIL", ""),
    "password":  os.environ.get("SMTP_PASSWORD", ""),
    "enabled":   os.environ.get("SMTP_ENABLED", "false").lower() == "true",
}

ROLES = {
    "admin":   {"can_approve":True,  "can_reject":True,  "can_add":True,  "can_pay":True,  "can_report":True,  "can_users":True},
    "manager": {"can_approve":True,  "can_reject":True,  "can_add":True,  "can_pay":True,  "can_report":True,  "can_users":False},
    "viewer":  {"can_approve":False, "can_reject":False, "can_add":False, "can_pay":False, "can_report":True,  "can_users":False},
}

DEFAULT_USERS = {
    "admin":   {"role":"admin",   "pw_hash":hashlib.sha256(b"admin123").hexdigest()},
    "manager": {"role":"manager", "pw_hash":hashlib.sha256(b"manager123").hexdigest()},
    "viewer":  {"role":"viewer",  "pw_hash":hashlib.sha256(b"viewer123").hexdigest()},
}

# =============================================================================
# DATABASE LAYER
# =============================================================================
_local = threading.local()

def get_conn():
    if not hasattr(_local, "conn") or _local.conn is None:
        if USE_POSTGRES:
            import psycopg2, psycopg2.extras
            _local.conn = psycopg2.connect(_DATABASE_URL)
            _local.conn.autocommit = False
        else:
            _local.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            _local.conn.row_factory = sqlite3.Row
    return _local.conn

def get_cur():
    conn = get_conn()
    if USE_POSTGRES:
        import psycopg2.extras
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn.cursor()

def db_commit(): get_conn().commit()

def q(sql): return sql.replace("?", "%s") if USE_POSTGRES else sql

def scalar(cur, sql, params=()):
    cur.execute(q(sql), params)
    row = cur.fetchone()
    if row is None: return 0
    v = list(row.values())[0] if isinstance(row, dict) else row[0]
    return v or 0

# =============================================================================
# UTILITIES
# =============================================================================
def parse_date(s): return datetime.strptime(s, "%Y-%m-%d").date()

def add_months(d, months):
    month = d.month - 1 + months
    year  = d.year + month // 12
    month = month % 12 + 1
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))

def normalize_interest(rate):
    try:
        r = float(rate); return r / 100.0 if r > 1 else r
    except Exception: return None

def compute_emi(loan_amount, annual_rate, tenure_months):
    total = loan_amount + loan_amount * annual_rate * (tenure_months / 12.0)
    return round(total / tenure_months, 2)

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def can_do(action): return ROLES.get(session.get("role"), {}).get(action, False)

# =============================================================================
# DB INIT
# =============================================================================
def init_db():
    c = get_cur()
    if USE_POSTGRES:
        stmts = [
            """CREATE TABLE IF NOT EXISTS LoanEntry (
                id SERIAL PRIMARY KEY, loan_number TEXT UNIQUE, customer_name TEXT,
                vehicle_type TEXT, loan_amount REAL, interest_rate REAL, tenure INTEGER,
                start_date TEXT, status TEXT, created_at TEXT, customer_email TEXT)""",
            """CREATE TABLE IF NOT EXISTS Customers (
                customer_id SERIAL PRIMARY KEY, loan_id INTEGER UNIQUE, name TEXT,
                vehicle_type TEXT, loan_amount REAL, emi_amount REAL, status TEXT, created_at TEXT)""",
            """CREATE TABLE IF NOT EXISTS EMI (
                emi_id SERIAL PRIMARY KEY, loan_id INTEGER, installment_no INTEGER,
                due_date TEXT, emi_amount REAL, status TEXT, paid_at TEXT,
                amount_paid REAL, remaining_amount REAL, extra_interest REAL)""",
            """CREATE TABLE IF NOT EXISTS RejectedLoans (
                reject_id SERIAL PRIMARY KEY, loan_id INTEGER UNIQUE, reason TEXT, created_at TEXT)""",
            """CREATE TABLE IF NOT EXISTS ClosedLoans (
                close_id SERIAL PRIMARY KEY, loan_id INTEGER UNIQUE, closure_date TEXT, created_at TEXT)""",
            """CREATE TABLE IF NOT EXISTS Users (
                user_id SERIAL PRIMARY KEY, username TEXT UNIQUE, pw_hash TEXT, role TEXT, created_at TEXT)""",
        ]
        for s in stmts: c.execute(s)
        db_commit()
        for uname, info in DEFAULT_USERS.items():
            c.execute("INSERT INTO Users (username,pw_hash,role,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT(username) DO NOTHING",
                      (uname, info["pw_hash"], info["role"], datetime.now(timezone.utc).isoformat()))
    else:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS LoanEntry (
            id INTEGER PRIMARY KEY AUTOINCREMENT, loan_number TEXT UNIQUE, customer_name TEXT,
            vehicle_type TEXT, loan_amount REAL, interest_rate REAL, tenure INTEGER,
            start_date TEXT, status TEXT, created_at TEXT, customer_email TEXT);
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT, loan_id INTEGER UNIQUE, name TEXT,
            vehicle_type TEXT, loan_amount REAL, emi_amount REAL, status TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS EMI (
            emi_id INTEGER PRIMARY KEY AUTOINCREMENT, loan_id INTEGER, installment_no INTEGER,
            due_date TEXT, emi_amount REAL, status TEXT, paid_at TEXT,
            amount_paid REAL, remaining_amount REAL, extra_interest REAL);
        CREATE TABLE IF NOT EXISTS RejectedLoans (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT, loan_id INTEGER UNIQUE, reason TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS ClosedLoans (
            close_id INTEGER PRIMARY KEY AUTOINCREMENT, loan_id INTEGER UNIQUE, closure_date TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS Users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, pw_hash TEXT, role TEXT, created_at TEXT);
        """)
        db_commit()
        for uname, info in DEFAULT_USERS.items():
            c.execute("INSERT OR IGNORE INTO Users (username,pw_hash,role,created_at) VALUES (?,?,?,?)",
                      (uname, info["pw_hash"], info["role"], datetime.now(timezone.utc).isoformat()))
    db_commit()

# =============================================================================
# DB OPERATIONS
# =============================================================================
def authenticate_user(username, password):
    c = get_cur()
    c.execute(q("SELECT * FROM Users WHERE username=?"), (username,))
    row = c.fetchone()
    if row and row["pw_hash"] == hash_pw(password): return dict(row)
    return None

def can_pay_seq(loan_id, installment_no):
    if installment_no == 1: return True
    c = get_cur()
    c.execute(q("SELECT COUNT(*) as n FROM EMI WHERE loan_id=? AND installment_no<? AND status!='Paid'"), (loan_id, installment_no))
    row = c.fetchone()
    return (row["n"] if isinstance(row, dict) else row[0]) == 0

def create_loan(loan_number, customer_name, vehicle_type, loan_amount, interest_rate_raw, tenure, start_date_iso, customer_email=""):
    r = normalize_interest(interest_rate_raw)
    if r is None: raise ValueError("Invalid interest rate")
    c = get_cur()
    c.execute(q("INSERT INTO LoanEntry (loan_number,customer_name,vehicle_type,loan_amount,interest_rate,tenure,start_date,status,created_at,customer_email) VALUES (?,?,?,?,?,?,?,?,?,?)"),
              (loan_number, customer_name, vehicle_type, float(loan_amount), float(r), int(tenure), start_date_iso, "PendingApproval", datetime.now(timezone.utc).isoformat(), customer_email))
    db_commit()

def approve_loan(loan_id):
    c = get_cur()
    c.execute(q("SELECT * FROM LoanEntry WHERE id=?"), (loan_id,))
    loan = c.fetchone()
    if not loan: raise ValueError("Loan not found")
    if loan["status"] != "PendingApproval": raise ValueError("Not pending approval")
    emi_amt = compute_emi(float(loan["loan_amount"]), float(loan["interest_rate"]), int(loan["tenure"]))
    now = datetime.now(timezone.utc).isoformat()
    if USE_POSTGRES:
        c.execute("INSERT INTO Customers (loan_id,name,vehicle_type,loan_amount,emi_amount,status,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(loan_id) DO UPDATE SET emi_amount=EXCLUDED.emi_amount,status=EXCLUDED.status",
                  (loan_id, loan["customer_name"], loan["vehicle_type"], loan["loan_amount"], emi_amt, "Active", now))
    else:
        c.execute("INSERT OR REPLACE INTO Customers (loan_id,name,vehicle_type,loan_amount,emi_amount,status,created_at) VALUES (?,?,?,?,?,?,?)",
                  (loan_id, loan["customer_name"], loan["vehicle_type"], loan["loan_amount"], emi_amt, "Active", now))
    try: sd = parse_date(loan["start_date"])
    except Exception: sd = date.today()
    for i in range(1, int(loan["tenure"]) + 1):
        c.execute(q("INSERT INTO EMI (loan_id,installment_no,due_date,emi_amount,status,paid_at,amount_paid,remaining_amount,extra_interest) VALUES (?,?,?,?,?,?,?,?,?)"),
                  (loan_id, i, add_months(sd, i-1).isoformat(), emi_amt, "Pending", None, 0.0, emi_amt, 0.0))
    c.execute(q("UPDATE LoanEntry SET status='Approved' WHERE id=?"), (loan_id,))
    db_commit()
    _notify_approval(loan)

def reject_loan(loan_id, reason):
    c = get_cur()
    c.execute(q("UPDATE LoanEntry SET status='Rejected' WHERE id=?"), (loan_id,))
    if USE_POSTGRES:
        c.execute("INSERT INTO RejectedLoans (loan_id,reason,created_at) VALUES (%s,%s,%s) ON CONFLICT(loan_id) DO UPDATE SET reason=EXCLUDED.reason",
                  (loan_id, reason, datetime.now(timezone.utc).isoformat()))
    else:
        c.execute("INSERT OR REPLACE INTO RejectedLoans (loan_id,reason,created_at) VALUES (?,?,?)",
                  (loan_id, reason, datetime.now(timezone.utc).isoformat()))
    db_commit()

def pay_emi_db(emi_id, pay_amount, extra_interest=0.0):
    c = get_cur()
    now = datetime.now(timezone.utc).isoformat()
    c.execute(q("SELECT * FROM EMI WHERE emi_id=?"), (emi_id,))
    emi = c.fetchone()
    if not emi: raise ValueError("EMI not found")
    if emi["status"] == "Paid": raise ValueError("Already paid")
    if not can_pay_seq(emi["loan_id"], emi["installment_no"]):
        raise ValueError(f"Complete installment {emi['installment_no']-1} first")
    paid_so_far = emi["amount_paid"] or 0.0
    remaining   = emi["remaining_amount"] if emi["remaining_amount"] is not None else emi["emi_amount"]
    total_due   = remaining + (extra_interest or 0.0)
    if pay_amount < total_due:
        c.execute(q("UPDATE EMI SET amount_paid=?,remaining_amount=?,extra_interest=?,status='Partial' WHERE emi_id=?"),
                  (paid_so_far + pay_amount, total_due - pay_amount, extra_interest, emi_id))
        db_commit()
        return f"Partial payment saved. Remaining: Rs {total_due - pay_amount:.2f}"
    c.execute(q("UPDATE EMI SET status='Paid',paid_at=?,amount_paid=?,remaining_amount=0,extra_interest=0 WHERE emi_id=?"),
              (now, paid_so_far + pay_amount, emi_id))
    db_commit()
    loan_id = emi["loan_id"]
    c.execute(q("SELECT COUNT(*) as t, SUM(CASE WHEN status='Paid' THEN 1 ELSE 0 END) as p FROM EMI WHERE loan_id=?"), (loan_id,))
    row = c.fetchone()
    total = row["t"] if isinstance(row, dict) else row[0]
    paid  = row["p"] if isinstance(row, dict) else row[1]
    if total and paid and total == paid:
        c.execute(q("UPDATE LoanEntry SET status='Closed' WHERE id=?"), (loan_id,))
        c.execute(q("UPDATE Customers SET status='Closed' WHERE loan_id=?"), (loan_id,))
        if USE_POSTGRES:
            c.execute("INSERT INTO ClosedLoans (loan_id,closure_date,created_at) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                      (loan_id, date.today().isoformat(), now))
        else:
            c.execute("INSERT OR REPLACE INTO ClosedLoans (loan_id,closure_date,created_at) VALUES (?,?,?)",
                      (loan_id, date.today().isoformat(), now))
        db_commit()
        _notify_closure(loan_id)
    return "EMI paid successfully!"

def flag_overdues():
    today = date.today().isoformat()
    c = get_cur()
    c.execute(q("UPDATE EMI SET status='Overdue' WHERE status IN ('Pending','Partial') AND due_date<? AND (remaining_amount>0 OR remaining_amount IS NULL)"), (today,))
    db_commit()

# =============================================================================
# QUERIES
# =============================================================================
def get_loans(search="", status=None):
    c = get_cur()
    qp = f"%{search}%"
    sql = "SELECT * FROM LoanEntry WHERE (loan_number LIKE ? OR customer_name LIKE ? OR vehicle_type LIKE ?)"
    params = [qp, qp, qp]
    if status: sql += " AND status=?"; params.append(status)
    sql += " ORDER BY created_at DESC"
    c.execute(q(sql), params); return c.fetchall()

def get_customers(search=""):
    c = get_cur(); qp = f"%{search}%"
    c.execute(q("SELECT * FROM Customers WHERE name LIKE ? OR vehicle_type LIKE ? OR status LIKE ? ORDER BY created_at DESC"), (qp,qp,qp))
    return c.fetchall()

def get_emis_for_loan(loan_id):
    c = get_cur()
    c.execute(q("SELECT * FROM EMI WHERE loan_id=? ORDER BY installment_no"), (loan_id,))
    return c.fetchall()

def get_overdue_emis():
    today = date.today().isoformat(); c = get_cur()
    c.execute(q("SELECT e.*, le.loan_number, le.customer_name FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id WHERE e.status='Overdue' OR (e.status IN ('Pending','Partial') AND e.due_date<?) ORDER BY e.due_date"), (today,))
    return c.fetchall()

def get_upcoming_emis():
    today = date.today().isoformat(); limit = (date.today() + timedelta(days=UPCOMING_DAYS)).isoformat(); c = get_cur()
    c.execute(q("SELECT e.*, le.loan_number, le.customer_name FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id WHERE e.status IN ('Pending','Partial') AND e.due_date>=? AND e.due_date<=? ORDER BY e.due_date"), (today, limit))
    return c.fetchall()

def get_kpis():
    c = get_cur()
    return dict(
        total    = scalar(c, "SELECT COUNT(*) FROM LoanEntry"),
        pending  = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='PendingApproval'"),
        approved = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='Approved'"),
        closed   = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='Closed'"),
        rejected = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='Rejected'"),
        disbursed= scalar(c, "SELECT SUM(loan_amount) FROM LoanEntry") or 0,
        received = scalar(c, "SELECT SUM(emi_amount) FROM EMI WHERE status='Paid'") or 0,
        pending_amt = scalar(c, "SELECT SUM(remaining_amount) FROM EMI WHERE status IN ('Pending','Partial','Overdue')") or 0,
        overdue  = len(get_overdue_emis()),
        upcoming = len(get_upcoming_emis()),
    )

def get_users():
    c = get_cur()
    c.execute("SELECT user_id,username,role,created_at FROM Users ORDER BY user_id")
    return c.fetchall()

# =============================================================================
# EMAIL
# =============================================================================
def _send_email(to, subject, body):
    if not EMAIL_CONFIG.get("enabled") or not to: return
    try:
        msg = MIMEMultipart(); msg["From"]=EMAIL_CONFIG["sender"]; msg["To"]=to; msg["Subject"]=subject
        msg.attach(MIMEText(body,"html"))
        with smtplib.SMTP(EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]) as s:
            s.starttls(); s.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
            s.sendmail(EMAIL_CONFIG["sender"], to, msg.as_string())
    except Exception as e: print(f"[Email] {e}")

def _notify_approval(loan):
    _send_email(loan.get("customer_email",""), "Your Vehicle Loan Has Been Approved",
        f"<h2>Loan Approved</h2><p>Dear {loan['customer_name']}, your loan <b>{loan['loan_number']}</b> of Rs {loan['loan_amount']:,.2f} has been approved.</p>")

def _notify_closure(loan_id):
    c = get_cur(); c.execute(q("SELECT * FROM LoanEntry WHERE id=?"), (loan_id,)); loan = c.fetchone()
    if loan and loan.get("customer_email"):
        _send_email(loan["customer_email"], "Loan Fully Repaid — Congratulations!",
            f"<h2>Loan Closed</h2><p>Dear {loan['customer_name']}, all EMIs for loan <b>{loan['loan_number']}</b> are paid. Congratulations!</p>")

# =============================================================================
# PDF REPORT
# =============================================================================
def generate_pdf():
    if not REPORTLAB_AVAILABLE: raise RuntimeError("reportlab not installed")
    buf = io.BytesIO(); styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], fontSize=20, spaceAfter=12)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=14, spaceAfter=8)
    story = []
    story.append(Paragraph("Vehicle Loan Management Report", h1))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%d %b %Y %H:%M')}", styles["Normal"]))
    story.append(Spacer(1, 0.5*cm))
    kpis = get_kpis()
    kpi_data = [["Metric","Value"],["Total Loans",str(kpis["total"])],
                ["Total Disbursed",f"Rs {kpis['disbursed']:,.2f}"],["Total Received",f"Rs {kpis['received']:,.2f}"],
                ["Total Pending",f"Rs {kpis['pending_amt']:,.2f}"],["Pending Approval",str(kpis["pending"])],
                ["Active",str(kpis["approved"])],["Closed",str(kpis["closed"])],
                ["Rejected",str(kpis["rejected"])],["Overdue EMIs",str(kpis["overdue"])],["Upcoming (10d)",str(kpis["upcoming"])]]
    t = Table(kpi_data, colWidths=[8*cm,7*cm])
    t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#0057b8")),("TEXTCOLOR",(0,0),(-1,0),colors.white),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
        ("GRID",(0,0),(-1,-1),0.5,colors.grey),("ALIGN",(1,1),(-1,-1),"RIGHT")]))
    story.append(t); story.append(PageBreak())
    story.append(Paragraph("All Loans", h2))
    c = get_cur(); c.execute("SELECT * FROM LoanEntry ORDER BY created_at DESC"); loans = c.fetchall()
    ld = [["Loan #","Customer","Type","Amount (Rs)","Rate","Tenure","Status"]]
    for l in loans: ld.append([l["loan_number"],l["customer_name"],l["vehicle_type"],f"{l['loan_amount']:,.0f}",f"{l['interest_rate']*100:.1f}%",f"{l['tenure']}m",l["status"]])
    if len(ld) > 1:
        lt = Table(ld, repeatRows=1, colWidths=[2.8*cm,4.5*cm,2.2*cm,3*cm,1.8*cm,1.8*cm,2.8*cm])
        lt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#0057b8")),("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),("GRID",(0,0),(-1,-1),0.4,colors.grey)]))
        story.append(lt)
    story.append(PageBreak()); story.append(Paragraph("Overdue EMIs", h2))
    overdue = get_overdue_emis()
    if overdue:
        od = [["Loan #","Customer","Inst #","Due Date","Amount Due (Rs)","Days Overdue"]]
        for e in overdue:
            try: days = (date.today() - parse_date(e["due_date"])).days
            except Exception: days = 0
            od.append([e["loan_number"],e["customer_name"],str(e["installment_no"]),e["due_date"],f"{(e['remaining_amount'] or e['emi_amount']):,.2f}",str(days)])
        ot = Table(od, repeatRows=1, colWidths=[2.8*cm,4.5*cm,1.5*cm,2.5*cm,3.5*cm,3*cm])
        ot.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#d7263d")),("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#fdecea")]),("GRID",(0,0),(-1,-1),0.4,colors.grey)]))
        story.append(ot)
    else: story.append(Paragraph("No overdue EMIs.", styles["Normal"]))
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=2*cm, bottomMargin=2*cm)
    doc.build(story); buf.seek(0); return buf

# =============================================================================
# BASE HTML TEMPLATE
# =============================================================================
BASE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vehicle Loan Manager</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
<style>
:root{--sb:#0a2540;--sbh:#163a60;--accent:#0057b8}
body{background:#f0f4f8;font-family:'Segoe UI',sans-serif}
.sidebar{background:var(--sb);min-height:100vh;width:225px;position:fixed;top:0;left:0;z-index:100;padding-top:.8rem;overflow-y:auto}
.sidebar .brand{color:#fff;font-size:1.05rem;font-weight:700;padding:.8rem 1.1rem 1.2rem;display:flex;align-items:center;gap:.5rem}
.sidebar a{color:#a8c4e0;text-decoration:none;display:flex;align-items:center;gap:.55rem;padding:.5rem 1.1rem;font-size:.875rem;transition:.15s}
.sidebar a:hover,.sidebar a.active{background:var(--sbh);color:#fff;border-radius:0}
.nav-sec{color:#5a8ab0;font-size:.68rem;font-weight:600;text-transform:uppercase;padding:.7rem 1.1rem .2rem;letter-spacing:.08em}
.main{margin-left:225px;padding:1.8rem}
.kpi-card{border-radius:14px;padding:1.3rem;color:#fff}
.kpi-card .val{font-size:1.7rem;font-weight:700;line-height:1.1}
.kpi-card .lbl{font-size:.78rem;opacity:.85;margin-top:.25rem}
.card{border:none;border-radius:12px;box-shadow:0 1px 5px rgba(0,0,0,.09)}
.table th{background:#f8f9fa;font-size:.78rem;text-transform:uppercase;letter-spacing:.04em;color:#666}
.badge-admin{background:#0057b8!important}.badge-manager{background:#00844c!important}.badge-viewer{background:#888!important}
@media(max-width:768px){.sidebar{position:relative;width:100%;min-height:auto}.main{margin-left:0}}
</style></head>
<body><div class="d-flex">
<div class="sidebar">
  <div class="brand"><i class="bi bi-car-front-fill" style="font-size:1.2rem;color:#60a5fa"></i> LoanManager</div>
  {% if session.username %}
  <div class="px-3 mb-1" style="color:#6fa3c8;font-size:.8rem">
    <i class="bi bi-person-circle"></i> {{session.username}}
    <span class="badge ms-1 badge-{{session.role}} text-white" style="font-size:.65rem">{{session.role}}</span>
  </div>
  <div class="nav-sec">Main</div>
  <a href="/dashboard" class="{{'active' if active=='dashboard'}}"><i class="bi bi-speedometer2"></i>Dashboard</a>
  <a href="/loans" class="{{'active' if active=='loans'}}"><i class="bi bi-file-earmark-text"></i>All Loans</a>
  <a href="/customers" class="{{'active' if active=='customers'}}"><i class="bi bi-people"></i>Customers</a>
  {% if can_do('can_add') %}
  <div class="nav-sec">Manage</div>
  <a href="/loan/add" class="{{'active' if active=='add'}}"><i class="bi bi-plus-circle"></i>New Loan</a>
  {% endif %}
  {% if can_do('can_approve') %}
  <a href="/approval" class="{{'active' if active=='approval'}}"><i class="bi bi-check2-circle"></i>Approvals</a>
  {% endif %}
  <a href="/emis" class="{{'active' if active=='emis'}}"><i class="bi bi-calendar3"></i>EMI Schedule</a>
  <div class="nav-sec">Reports</div>
  <a href="/alerts" class="{{'active' if active=='alerts'}}"><i class="bi bi-bell"></i>Alerts</a>
  <a href="/closed" class="{{'active' if active=='closed'}}"><i class="bi bi-archive"></i>Closed Loans</a>
  <a href="/rejected" class="{{'active' if active=='rejected'}}"><i class="bi bi-x-circle"></i>Rejected</a>
  <a href="/calculator" class="{{'active' if active=='calculator'}}"><i class="bi bi-calculator"></i>Calculator</a>
  {% if can_do('can_report') %}
  <a href="/report"><i class="bi bi-file-pdf"></i>PDF Report</a>
  <a href="/export/loans"><i class="bi bi-download"></i>Export CSV</a>
  {% endif %}
  {% if can_do('can_users') %}
  <div class="nav-sec">Admin</div>
  <a href="/users" class="{{'active' if active=='users'}}"><i class="bi bi-shield-lock"></i>User Mgmt</a>
  {% endif %}
  <div style="padding:.8rem 0">
  <a href="/logout" style="color:#f87171"><i class="bi bi-box-arrow-left"></i>Logout</a>
  </div>
  {% endif %}
</div>
<div class="main flex-grow-1">
  {% with msgs=get_flashed_messages(with_categories=true) %}{% for cat,msg in msgs %}
  <div class="alert alert-{{'success' if cat=='success' else 'danger'}} alert-dismissible fade show">
    {{msg}}<button type="button" class="btn-close" data-bs-dismiss="alert"></button></div>
  {% endfor %}{% endwith %}
  {% block content %}{% endblock %}
</div></div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body></html>"""

def render(tpl, **kw):
    kw.setdefault("active",""); kw["can_do"]=can_do
    return render_template_string(BASE.replace("{% block content %}{% endblock %}", tpl), **kw)

# =============================================================================
# AUTH DECORATORS
# =============================================================================
def login_required(f):
    @wraps(f)
    def dec(*a,**k):
        if "username" not in session: return redirect("/login")
        return f(*a,**k)
    return dec

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def dec(*a,**k):
            if session.get("role") not in roles:
                flash("Access denied.", "danger"); return redirect("/dashboard")
            return f(*a,**k)
        return dec
    return decorator

# =============================================================================
# FLASK APP FACTORY
# =============================================================================
def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

    with app.app_context():
        init_db()

    @app.route("/login", methods=["GET","POST"])
    def login():
        if request.method == "POST":
            user = authenticate_user(request.form["username"], request.form["password"])
            if user:
                session["username"] = user["username"]; session["role"] = user["role"]
                flash(f"Welcome, {user['username']}!", "success")
                return redirect("/dashboard")
            flash("Invalid credentials.", "danger")
        return render_template_string("""<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
<style>body{background:#0a2540;min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:#fff;border-radius:16px;padding:2.5rem;width:100%;max-width:400px;box-shadow:0 8px 40px rgba(0,0,0,.35)}
.brand{color:#0057b8;font-size:1.4rem;font-weight:700;text-align:center;margin-bottom:1.5rem}</style></head>
<body><div class="box">
<div class="brand"><i class="bi bi-car-front-fill"></i> LoanManager</div>
{% with msgs=get_flashed_messages(with_categories=true) %}{% for cat,msg in msgs %}
<div class="alert alert-{{'success' if cat=='success' else 'danger'}}">{{msg}}</div>
{% endfor %}{% endwith %}
<form method="post">
<div class="mb-3"><label class="form-label fw-semibold">Username</label>
<input name="username" class="form-control form-control-lg" autofocus required></div>
<div class="mb-4"><label class="form-label fw-semibold">Password</label>
<input name="password" type="password" class="form-control form-control-lg" required></div>
<button class="btn btn-primary w-100 btn-lg">Sign In</button>
</form>
<hr><p class="text-muted text-center" style="font-size:.8rem">admin/admin123 | manager/manager123 | viewer/viewer123</p>
</div></body></html>""")

    @app.route("/logout")
    def logout(): session.clear(); return redirect("/login")

    @app.route("/")
    def index(): return redirect("/dashboard")

    @app.route("/dashboard")
    @login_required
    def dashboard():
        flag_overdues(); kpis=get_kpis(); od=get_overdue_emis()[:5]; up=get_upcoming_emis()[:5]; today=date.today()
        return render("""
<div class="d-flex justify-content-between align-items-center mb-4">
<h4 class="mb-0 fw-bold">Dashboard</h4>
<span class="text-muted small"><i class="bi bi-calendar3"></i> {{today}}</span></div>
<div class="row g-3 mb-4">
<div class="col-6 col-md-3"><div class="kpi-card" style="background:#0057b8"><div class="val">{{kpis.total}}</div><div class="lbl">Total Loans</div></div></div>
<div class="col-6 col-md-3"><div class="kpi-card" style="background:#00844c"><div class="val">Rs {{'%,.0f'|format(kpis.disbursed)}}</div><div class="lbl">Disbursed</div></div></div>
<div class="col-6 col-md-3"><div class="kpi-card" style="background:#0d6efd"><div class="val">Rs {{'%,.0f'|format(kpis.received)}}</div><div class="lbl">Collected</div></div></div>
<div class="col-6 col-md-3"><div class="kpi-card" style="background:#d7263d"><div class="val">Rs {{'%,.0f'|format(kpis.pending_amt)}}</div><div class="lbl">Pending</div></div></div>
</div>
<div class="row g-3 mb-4">
<div class="col-4 col-md-2"><div class="card text-center p-3"><div class="fw-bold fs-4 text-warning">{{kpis.pending}}</div><div class="text-muted small">Awaiting</div></div></div>
<div class="col-4 col-md-2"><div class="card text-center p-3"><div class="fw-bold fs-4 text-success">{{kpis.approved}}</div><div class="text-muted small">Active</div></div></div>
<div class="col-4 col-md-2"><div class="card text-center p-3"><div class="fw-bold fs-4 text-secondary">{{kpis.closed}}</div><div class="text-muted small">Closed</div></div></div>
<div class="col-4 col-md-2"><div class="card text-center p-3"><div class="fw-bold fs-4 text-danger">{{kpis.rejected}}</div><div class="text-muted small">Rejected</div></div></div>
<div class="col-4 col-md-2"><div class="card text-center p-3"><div class="fw-bold fs-4 text-danger">{{kpis.overdue}}</div><div class="text-muted small">Overdue</div></div></div>
<div class="col-4 col-md-2"><div class="card text-center p-3"><div class="fw-bold fs-4 text-warning">{{kpis.upcoming}}</div><div class="text-muted small">Due 10d</div></div></div>
</div>
<div class="row g-3">
<div class="col-md-6"><div class="card"><div class="card-header bg-danger text-white"><i class="bi bi-exclamation-triangle"></i> Overdue EMIs</div>
<div class="card-body p-0"><table class="table table-sm mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Due</th><th>Amount</th></tr></thead><tbody>
{% for e in od %}<tr class="table-danger"><td>{{e.loan_number}}</td><td>{{e.customer_name}}</td><td>{{e.due_date}}</td><td>Rs {{'%,.0f'|format(e.remaining_amount or e.emi_amount)}}</td></tr>
{% else %}<tr><td colspan=4 class="text-center text-success py-2">No overdue EMIs</td></tr>{% endfor %}
</tbody></table></div></div></div>
<div class="col-md-6"><div class="card"><div class="card-header bg-warning text-dark"><i class="bi bi-clock"></i> Upcoming (10 days)</div>
<div class="card-body p-0"><table class="table table-sm mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Due</th><th>Amount</th></tr></thead><tbody>
{% for e in up %}<tr class="table-warning"><td>{{e.loan_number}}</td><td>{{e.customer_name}}</td><td>{{e.due_date}}</td><td>Rs {{'%,.0f'|format(e.remaining_amount or e.emi_amount)}}</td></tr>
{% else %}<tr><td colspan=4 class="text-center text-muted py-2">No upcoming EMIs</td></tr>{% endfor %}
</tbody></table></div></div></div></div>""",
        active="dashboard", kpis=kpis, od=od, up=up, today=today.isoformat())

    @app.route("/loans")
    @login_required
    def loans():
        q_=request.args.get("q",""); status=request.args.get("status",""); rows=get_loans(q_, status or None)
        return render("""
<div class="d-flex justify-content-between align-items-center mb-3">
<h4 class="mb-0 fw-bold">All Loans</h4>
{% if can_do('can_add') %}<a href="/loan/add" class="btn btn-primary btn-sm"><i class="bi bi-plus"></i> New Loan</a>{% endif %}</div>
<div class="card mb-3"><div class="card-body py-2">
<form class="d-flex gap-2 flex-wrap" method="get">
<input name="q" value="{{q_}}" class="form-control form-control-sm" style="max-width:260px" placeholder="Search...">
<select name="status" class="form-select form-select-sm" style="width:170px">
<option value="">All statuses</option><option {{'selected' if status=='PendingApproval'}}>PendingApproval</option>
<option {{'selected' if status=='Approved'}}>Approved</option><option {{'selected' if status=='Closed'}}>Closed</option>
<option {{'selected' if status=='Rejected'}}>Rejected</option></select>
<button class="btn btn-outline-primary btn-sm">Search</button>
<a href="/loans" class="btn btn-outline-secondary btn-sm">Clear</a>
</form></div></div>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Type</th><th>Amount</th><th>Rate</th><th>Tenure</th><th>Start</th><th>Status</th><th></th></tr></thead>
<tbody>{% for l in rows %}<tr>
<td class="fw-semibold">{{l.loan_number}}</td><td>{{l.customer_name}}</td><td>{{l.vehicle_type}}</td>
<td>Rs {{'%,.0f'|format(l.loan_amount)}}</td><td>{{'%.1f'|format(l.interest_rate*100)}}%</td>
<td>{{l.tenure}}m</td><td>{{l.start_date}}</td>
<td><span class="badge {{'bg-success' if l.status=='Approved' else 'bg-danger' if l.status=='Rejected' else 'bg-secondary' if l.status=='Closed' else 'bg-warning text-dark'}}">{{l.status}}</span></td>
<td><a href="/emis/{{l.id}}" class="btn btn-sm btn-outline-primary">EMIs</a></td>
</tr>{% else %}<tr><td colspan=9 class="text-center text-muted py-3">No loans found.</td></tr>{% endfor %}
</tbody></table></div></div>""", active="loans", rows=rows, q_=q_, status=status)

    @app.route("/loan/add", methods=["GET","POST"])
    @login_required
    @role_required("admin","manager")
    def add_loan():
        if request.method == "POST":
            f = request.form
            try:
                create_loan(f["loan_number"],f["customer_name"],f["vehicle_type"],f["loan_amount"],f["interest_rate"],f["tenure"],f["start_date"],f.get("customer_email",""))
                flash("Loan submitted for approval.", "success"); return redirect("/loans")
            except Exception as e: flash(str(e), "danger")
        return render("""
<h4 class="fw-bold mb-4">New Loan Entry</h4>
<div class="card col-lg-8"><div class="card-body"><form method="post"><div class="row g-3">
<div class="col-md-6"><label class="form-label">Loan Number *</label><input name="loan_number" class="form-control" required></div>
<div class="col-md-6"><label class="form-label">Customer Name *</label><input name="customer_name" class="form-control" required></div>
<div class="col-md-6"><label class="form-label">Customer Email</label><input name="customer_email" type="email" class="form-control"></div>
<div class="col-md-6"><label class="form-label">Vehicle Type *</label>
<select name="vehicle_type" class="form-select"><option>Car</option><option>Bike</option><option>Truck</option><option>Other</option></select></div>
<div class="col-md-4"><label class="form-label">Loan Amount (Rs) *</label><input name="loan_amount" type="number" step="0.01" class="form-control" required></div>
<div class="col-md-4"><label class="form-label">Interest Rate (% p.a.) *</label><input name="interest_rate" type="number" step="0.01" class="form-control" required placeholder="e.g. 12"></div>
<div class="col-md-4"><label class="form-label">Tenure (months) *</label><input name="tenure" type="number" class="form-control" required></div>
<div class="col-md-6"><label class="form-label">Start Date *</label><input name="start_date" type="date" class="form-control" value="{{today}}" required></div>
</div><div class="mt-4 d-flex gap-2"><button class="btn btn-primary">Submit Loan</button>
<a href="/loans" class="btn btn-outline-secondary">Cancel</a></div>
</form></div></div>""", active="add", today=date.today().isoformat())

    @app.route("/approval", methods=["GET","POST"])
    @login_required
    @role_required("admin","manager")
    def approval():
        if request.method == "POST":
            lid=int(request.form["loan_id"]); action=request.form["action"]
            try:
                if action=="approve": approve_loan(lid); flash("Loan approved and EMI schedule created!","success")
                else: reject_loan(lid, request.form.get("reason","No reason")); flash("Loan rejected.","success")
            except Exception as e: flash(str(e),"danger")
        q_=request.args.get("q",""); rows=get_loans(q_,"PendingApproval")
        return render("""
<h4 class="fw-bold mb-4">Loan Approvals</h4>
<div class="card mb-3"><div class="card-body py-2">
<form class="d-flex gap-2" method="get"><input name="q" value="{{q_}}" class="form-control form-control-sm" style="max-width:260px" placeholder="Search...">
<button class="btn btn-outline-primary btn-sm">Search</button></form></div></div>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Type</th><th>Amount</th><th>Rate</th><th>Tenure</th><th>Start</th><th>Actions</th></tr></thead>
<tbody>{% for l in rows %}<tr>
<td class="fw-semibold">{{l.loan_number}}</td><td>{{l.customer_name}}</td><td>{{l.vehicle_type}}</td>
<td>Rs {{'%,.0f'|format(l.loan_amount)}}</td><td>{{'%.1f'|format(l.interest_rate*100)}}%</td><td>{{l.tenure}}m</td><td>{{l.start_date}}</td>
<td>
<form method="post" class="d-inline"><input type="hidden" name="loan_id" value="{{l.id}}"><input type="hidden" name="action" value="approve">
<button class="btn btn-success btn-sm">Approve</button></form>
<form method="post" class="d-inline ms-1" onsubmit="return prompt_reason(this)">
<input type="hidden" name="loan_id" value="{{l.id}}"><input type="hidden" name="action" value="reject"><input type="hidden" name="reason">
<button type="submit" class="btn btn-danger btn-sm">Reject</button></form>
</td></tr>
{% else %}<tr><td colspan=8 class="text-center text-muted py-3">No pending loans.</td></tr>{% endfor %}
</tbody></table></div></div>
<script>function prompt_reason(f){var r=prompt('Rejection reason:');if(!r)return false;f.querySelector('[name=reason]').value=r;return true;}</script>""",
        active="approval", rows=rows, q_=q_)

    @app.route("/customers")
    @login_required
    def customers():
        q_=request.args.get("q",""); rows=get_customers(q_)
        return render("""
<h4 class="fw-bold mb-4">Customers</h4>
<div class="card mb-3"><div class="card-body py-2">
<form class="d-flex gap-2" method="get"><input name="q" value="{{q_}}" class="form-control form-control-sm" style="max-width:280px" placeholder="Search name, type, status...">
<button class="btn btn-outline-primary btn-sm">Search</button><a href="/customers" class="btn btn-outline-secondary btn-sm">Clear</a>
</form></div></div>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>ID</th><th>Name</th><th>Type</th><th>Loan Amt</th><th>EMI/Month</th><th>Status</th><th></th></tr></thead>
<tbody>{% for c in rows %}<tr>
<td>{{c.customer_id}}</td><td class="fw-semibold">{{c.name}}</td><td>{{c.vehicle_type}}</td>
<td>Rs {{'%,.0f'|format(c.loan_amount)}}</td><td>Rs {{'%,.2f'|format(c.emi_amount)}}</td>
<td><span class="badge {{'bg-success' if c.status=='Active' else 'bg-secondary'}}">{{c.status}}</span></td>
<td><a href="/emis/{{c.loan_id}}" class="btn btn-sm btn-outline-primary">View EMIs</a></td>
</tr>{% else %}<tr><td colspan=7 class="text-center text-muted py-3">No customers.</td></tr>{% endfor %}
</tbody></table></div></div>""", active="customers", rows=rows, q_=q_)

    @app.route("/emis")
    @login_required
    def emis_home(): return redirect("/customers")

    @app.route("/emis/<int:loan_id>", methods=["GET","POST"])
    @login_required
    def emis(loan_id):
        if request.method=="POST" and can_do("can_pay"):
            try:
                msg=pay_emi_db(int(request.form["emi_id"]),float(request.form.get("pay_amount",0)),float(request.form.get("extra_interest",0)))
                flash(msg,"success")
            except Exception as e: flash(str(e),"danger")
            return redirect(f"/emis/{loan_id}")
        rows=get_emis_for_loan(loan_id); c=get_cur()
        c.execute(q("SELECT * FROM LoanEntry WHERE id=?"), (loan_id,)); loan=c.fetchone()
        return render("""
<div class="d-flex justify-content-between align-items-center mb-3">
<div><h4 class="fw-bold mb-0">EMI Schedule</h4>
{% if loan %}<span class="text-muted small">{{loan.loan_number}} — {{loan.customer_name}}</span>{% endif %}</div>
<div class="d-flex gap-2">
<a href="/export/emis/{{loan_id}}" class="btn btn-outline-secondary btn-sm"><i class="bi bi-download"></i> Export</a>
<a href="/customers" class="btn btn-outline-secondary btn-sm"><i class="bi bi-arrow-left"></i> Back</a>
</div></div>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>#</th><th>Due Date</th><th>EMI</th><th>Paid</th><th>Remaining</th><th>Status</th><th>Paid At</th>
{% if can_do('can_pay') %}<th>Action</th>{% endif %}</tr></thead>
<tbody>{% for e in rows %}
<tr class="{{'table-success' if e.status=='Paid' else 'table-danger' if e.status=='Overdue' else 'table-warning' if e.status=='Partial' else ''}}">
<td>{{e.installment_no}}</td><td>{{e.due_date}}</td>
<td>Rs {{'%,.2f'|format(e.emi_amount)}}</td><td>Rs {{'%,.2f'|format(e.amount_paid or 0)}}</td>
<td>Rs {{'%,.2f'|format(e.remaining_amount if e.remaining_amount is not none else e.emi_amount)}}</td>
<td><span class="badge {{'bg-success' if e.status=='Paid' else 'bg-danger' if e.status=='Overdue' else 'bg-warning text-dark' if e.status=='Partial' else 'bg-secondary'}}">{{e.status}}</span></td>
<td style="font-size:.8rem">{{e.paid_at[:16] if e.paid_at else '—'}}</td>
{% if can_do('can_pay') %}<td>{% if e.status!='Paid' %}
<button class="btn btn-sm btn-success" data-bs-toggle="modal" data-bs-target="#payModal"
  data-emiid="{{e.emi_id}}" data-rem="{{e.remaining_amount if e.remaining_amount is not none else e.emi_amount}}">Pay</button>
{% endif %}</td>{% endif %}
</tr>{% endfor %}
</tbody></table></div></div>
{% if can_do('can_pay') %}
<div class="modal fade" id="payModal" tabindex="-1"><div class="modal-dialog"><div class="modal-content">
<div class="modal-header"><h5 class="modal-title">Record EMI Payment</h5><button class="btn-close" data-bs-dismiss="modal"></button></div>
<form method="post"><div class="modal-body">
<input type="hidden" name="emi_id" id="m_emi">
<div class="mb-3"><label class="form-label">Amount to Pay (Rs)</label><input name="pay_amount" id="m_amt" type="number" step="0.01" class="form-control" required></div>
<div class="mb-3"><label class="form-label">Extra Interest (Rs)</label><input name="extra_interest" type="number" step="0.01" class="form-control" value="0"></div>
</div><div class="modal-footer"><button class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button><button class="btn btn-success">Confirm</button></div>
</form></div></div></div>
<script>document.getElementById('payModal').addEventListener('show.bs.modal',function(e){var b=e.relatedTarget;document.getElementById('m_emi').value=b.dataset.emiid;document.getElementById('m_amt').value=b.dataset.rem;});</script>
{% endif %}""", active="emis", rows=rows, loan=loan, loan_id=loan_id)

    @app.route("/alerts")
    @login_required
    def alerts():
        flag_overdues(); od=get_overdue_emis(); up=get_upcoming_emis(); today=date.today()
        return render("""
<h4 class="fw-bold mb-4">Payment Alerts</h4>
<h6 class="text-danger mb-2"><i class="bi bi-exclamation-triangle"></i> Overdue EMIs ({{od|length}})</h6>
<div class="card mb-4"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Inst #</th><th>Due Date</th><th>Amount Due</th><th>Days Overdue</th><th></th></tr></thead>
<tbody>{% for e in od %}<tr class="table-danger">
<td>{{e.loan_number}}</td><td>{{e.customer_name}}</td><td>{{e.installment_no}}</td><td>{{e.due_date}}</td>
<td>Rs {{'%,.2f'|format(e.remaining_amount or e.emi_amount)}}</td>
<td><span class="badge bg-danger">{{(today - (e.due_date[:10]|string))|string}} days</span></td>
<td><a href="/emis/{{e.loan_id}}" class="btn btn-sm btn-outline-danger">View</a></td>
</tr>{% else %}<tr><td colspan=7 class="text-center text-success py-3"><i class="bi bi-check-circle"></i> No overdue EMIs</td></tr>{% endfor %}
</tbody></table></div></div>
<h6 class="text-warning mb-2"><i class="bi bi-clock"></i> Upcoming EMIs — Next 10 Days ({{up|length}})</h6>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Inst #</th><th>Due Date</th><th>Amount</th><th></th></tr></thead>
<tbody>{% for e in up %}<tr class="table-warning">
<td>{{e.loan_number}}</td><td>{{e.customer_name}}</td><td>{{e.installment_no}}</td><td>{{e.due_date}}</td>
<td>Rs {{'%,.2f'|format(e.remaining_amount or e.emi_amount)}}</td>
<td><a href="/emis/{{e.loan_id}}" class="btn btn-sm btn-outline-warning">View</a></td>
</tr>{% else %}<tr><td colspan=6 class="text-center text-muted py-3">No upcoming EMIs</td></tr>{% endfor %}
</tbody></table></div></div>""", active="alerts", od=od, up=up, today=today)

    @app.route("/closed")
    @login_required
    def closed():
        q_=request.args.get("q",""); qp=f"%{q_}%"; c=get_cur()
        c.execute(q("SELECT le.*,cl.closure_date FROM LoanEntry le JOIN ClosedLoans cl ON cl.loan_id=le.id WHERE le.loan_number LIKE ? OR le.customer_name LIKE ? ORDER BY cl.created_at DESC"),(qp,qp))
        rows=c.fetchall()
        return render("""
<h4 class="fw-bold mb-4">Closed Loans</h4>
<div class="card mb-3"><div class="card-body py-2"><form class="d-flex gap-2" method="get">
<input name="q" value="{{q_}}" class="form-control form-control-sm" style="max-width:280px" placeholder="Search...">
<button class="btn btn-outline-primary btn-sm">Search</button><a href="/closed" class="btn btn-outline-secondary btn-sm">Clear</a>
</form></div></div>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Type</th><th>Amount</th><th>Closed On</th></tr></thead>
<tbody>{% for l in rows %}<tr><td class="fw-semibold">{{l.loan_number}}</td><td>{{l.customer_name}}</td>
<td>{{l.vehicle_type}}</td><td>Rs {{'%,.0f'|format(l.loan_amount)}}</td>
<td><span class="badge bg-secondary">{{l.closure_date}}</span></td></tr>
{% else %}<tr><td colspan=5 class="text-center text-muted py-3">No closed loans.</td></tr>{% endfor %}
</tbody></table></div></div>""", active="closed", rows=rows, q_=q_)

    @app.route("/rejected")
    @login_required
    def rejected():
        q_=request.args.get("q",""); qp=f"%{q_}%"; c=get_cur()
        c.execute(q("SELECT le.loan_number,le.customer_name,rl.reason,rl.created_at FROM LoanEntry le JOIN RejectedLoans rl ON rl.loan_id=le.id WHERE le.loan_number LIKE ? OR le.customer_name LIKE ? ORDER BY rl.created_at DESC"),(qp,qp))
        rows=c.fetchall()
        return render("""
<h4 class="fw-bold mb-4">Rejected Loans</h4>
<div class="card mb-3"><div class="card-body py-2"><form class="d-flex gap-2" method="get">
<input name="q" value="{{q_}}" class="form-control form-control-sm" style="max-width:280px" placeholder="Search...">
<button class="btn btn-outline-primary btn-sm">Search</button></form></div></div>
<div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>Loan #</th><th>Customer</th><th>Reason</th><th>Rejected At</th></tr></thead>
<tbody>{% for l in rows %}<tr><td class="fw-semibold">{{l.loan_number}}</td><td>{{l.customer_name}}</td>
<td>{{l.reason}}</td><td style="font-size:.8rem">{{l.created_at[:10]}}</td></tr>
{% else %}<tr><td colspan=4 class="text-center text-muted py-3">No rejected loans.</td></tr>{% endfor %}
</tbody></table></div></div>""", active="rejected", rows=rows, q_=q_)

    @app.route("/calculator", methods=["GET","POST"])
    @login_required
    def calculator():
        result=None; schedule=[]
        if request.method=="POST":
            try:
                amt=float(request.form.get("loan_amount",0)); rate=float(request.form.get("interest_rate",0))/100/12
                tenure=int(request.form.get("tenure",0)); mode=request.form.get("mode","EMI"); emi_in=float(request.form.get("emi_amount") or 0)
                if mode=="EMI" and amt>0 and rate>0 and tenure>0:
                    ev=amt*rate*(1+rate)**tenure/((1+rate)**tenure-1)
                    result={"label":"Monthly EMI","value":f"Rs {ev:,.2f}","total":f"Rs {ev*tenure:,.2f}","interest":f"Rs {ev*tenure-amt:,.2f}"}
                    bal=amt
                    for i in range(1,tenure+1):
                        intr=bal*rate; prin=ev-intr; bal-=prin
                        schedule.append({"month":i,"principal":round(max(prin,0),2),"interest":round(max(intr,0),2),"emi":round(ev,2),"balance":round(max(bal,0),2)})
                elif mode=="Affordability" and emi_in>0 and rate>0 and tenure>0:
                    lv=emi_in*((1+rate)**tenure-1)/(rate*(1+rate)**tenure)
                    result={"label":"Max Loan Amount","value":f"Rs {lv:,.2f}","total":"—","interest":"—"}
                elif mode=="Tenure" and amt>0 and rate>0 and emi_in>0:
                    n=math.log(emi_in/(emi_in-amt*rate))/math.log(1+rate)
                    result={"label":"Loan Tenure","value":f"{int(round(n))} months","total":"—","interest":"—"}
            except Exception as e: flash(str(e),"danger")
        return render("""
<h4 class="fw-bold mb-4">Loan Calculator</h4><div class="row g-4">
<div class="col-lg-4"><div class="card"><div class="card-body"><form method="post">
<div class="mb-3"><label class="form-label">Calculate</label>
<select name="mode" class="form-select"><option>EMI</option><option>Affordability</option><option>Tenure</option></select></div>
<div class="mb-3"><label class="form-label">Loan Amount (Rs)</label><input name="loan_amount" type="number" step="0.01" class="form-control" value="{{request.form.get('loan_amount','')}}"></div>
<div class="mb-3"><label class="form-label">Annual Interest Rate (%)</label><input name="interest_rate" type="number" step="0.01" class="form-control" value="{{request.form.get('interest_rate','')}}"></div>
<div class="mb-3"><label class="form-label">Tenure (months)</label><input name="tenure" type="number" class="form-control" value="{{request.form.get('tenure','')}}"></div>
<div class="mb-3"><label class="form-label">EMI Amount (for Affordability/Tenure)</label><input name="emi_amount" type="number" step="0.01" class="form-control" value="{{request.form.get('emi_amount','')}}"></div>
<button class="btn btn-primary w-100">Calculate</button></form></div></div></div>
<div class="col-lg-8">
{% if result %}<div class="card mb-3"><div class="card-body">
<h5 class="text-primary fw-bold">{{result.label}}: {{result.value}}</h5>
<div class="row mt-2">
<div class="col"><span class="text-muted small">Total Payment</span><div class="fw-semibold">{{result.total}}</div></div>
<div class="col"><span class="text-muted small">Total Interest</span><div class="fw-semibold text-danger">{{result.interest}}</div></div>
</div></div></div>{% endif %}
{% if schedule %}<div class="card"><div class="card-header fw-semibold">Amortization Schedule</div>
<div class="card-body p-0" style="max-height:420px;overflow-y:auto"><table class="table table-sm mb-0">
<thead><tr><th>#</th><th>Principal</th><th>Interest</th><th>EMI</th><th>Balance</th></tr></thead>
<tbody>{% for r in schedule %}<tr><td>{{r.month}}</td><td>Rs {{'%,.2f'|format(r.principal)}}</td>
<td>Rs {{'%,.2f'|format(r.interest)}}</td><td>Rs {{'%,.2f'|format(r.emi)}}</td>
<td>Rs {{'%,.2f'|format(r.balance)}}</td></tr>{% endfor %}
</tbody></table></div></div>{% endif %}
</div></div>""", active="calculator", result=result, schedule=schedule)

    @app.route("/report")
    @login_required
    @role_required("admin","manager","viewer")
    def report():
        if not REPORTLAB_AVAILABLE:
            flash("reportlab not installed. Add it to requirements.txt","danger"); return redirect("/dashboard")
        try:
            buf=generate_pdf()
            return send_file(buf, as_attachment=True, download_name=f"loan_report_{date.today()}.pdf", mimetype="application/pdf")
        except Exception as e: flash(str(e),"danger"); return redirect("/dashboard")

    @app.route("/export/loans")
    @login_required
    @role_required("admin","manager","viewer")
    def export_loans():
        c=get_cur(); c.execute("SELECT * FROM LoanEntry ORDER BY created_at DESC"); rows=c.fetchall()
        si=io.StringIO(); w=csv.writer(si)
        if rows: w.writerow(rows[0].keys()); [w.writerow(list(r.values())) for r in rows]
        out=make_response(si.getvalue()); out.headers["Content-Disposition"]=f"attachment; filename=loans_{date.today()}.csv"; out.headers["Content-type"]="text/csv"
        return out

    @app.route("/export/emis/<int:loan_id>")
    @login_required
    def export_emis(loan_id):
        rows=get_emis_for_loan(loan_id); si=io.StringIO(); w=csv.writer(si)
        if rows: w.writerow(rows[0].keys()); [w.writerow(list(r.values())) for r in rows]
        out=make_response(si.getvalue()); out.headers["Content-Disposition"]=f"attachment; filename=emis_loan{loan_id}.csv"; out.headers["Content-type"]="text/csv"
        return out

    @app.route("/users", methods=["GET","POST"])
    @login_required
    @role_required("admin")
    def users():
        if request.method=="POST":
            uname=request.form["username"].strip(); pw=request.form["password"]; role=request.form["role"]; c=get_cur()
            if USE_POSTGRES:
                c.execute("INSERT INTO Users (username,pw_hash,role,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT(username) DO UPDATE SET pw_hash=%s,role=%s",
                          (uname,hash_pw(pw),role,datetime.now(timezone.utc).isoformat(),hash_pw(pw),role))
            else:
                c.execute("INSERT INTO Users (username,pw_hash,role,created_at) VALUES (?,?,?,?) ON CONFLICT(username) DO UPDATE SET pw_hash=?,role=?",
                          (uname,hash_pw(pw),role,datetime.now(timezone.utc).isoformat(),hash_pw(pw),role))
            db_commit(); flash(f"User '{uname}' saved.","success")
        rows=get_users()
        return render("""
<h4 class="fw-bold mb-4">User Management</h4><div class="row g-4">
<div class="col-md-5"><div class="card"><div class="card-header fw-semibold">Add / Update User</div><div class="card-body">
<form method="post">
<div class="mb-3"><label class="form-label">Username</label><input name="username" class="form-control" required></div>
<div class="mb-3"><label class="form-label">Password</label><input name="password" type="password" class="form-control" required></div>
<div class="mb-3"><label class="form-label">Role</label>
<select name="role" class="form-select"><option>admin</option><option>manager</option><option>viewer</option></select></div>
<button class="btn btn-primary">Save User</button></form></div></div></div>
<div class="col-md-7"><div class="card"><div class="card-body p-0"><table class="table table-hover mb-0">
<thead><tr><th>ID</th><th>Username</th><th>Role</th><th>Created</th></tr></thead>
<tbody>{% for u in rows %}<tr><td>{{u.user_id}}</td><td class="fw-semibold">{{u.username}}</td>
<td><span class="badge badge-{{u.role}} text-white">{{u.role}}</span></td>
<td style="font-size:.8rem">{{u.created_at[:10]}}</td></tr>{% endfor %}
</tbody></table></div></div></div></div>""", active="users", rows=rows)

    @app.route("/health")
    def health(): return jsonify({"status":"ok","db":"postgres" if USE_POSTGRES else "sqlite"})

    return app

# Module-level app instance for gunicorn (gunicorn app:app)
app = create_app()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
