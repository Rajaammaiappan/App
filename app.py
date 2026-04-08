"""
Vehicle Loan Manager — Web Application
Render-ready Flask version of FinalCode_V6_0.py
No pandas, no tkinter, no matplotlib required.
"""

import os
import sys
import math
import sqlite3
import hashlib
import secrets
import calendar
import smtplib
import threading
from datetime import date, datetime, timezone, timedelta
from functools import wraps
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import (Flask, render_template, request, redirect,
                   url_for, session, flash, send_file, jsonify, g)

# ── reportlab (optional for PDF) ─────────────────────────────────────────────
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

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
DB_FILE       = os.environ.get("DB_FILE", "vehicle_loans.db")
UPCOMING_DAYS = 10

EMAIL_CONFIG = {
    "smtp_host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port": int(os.environ.get("SMTP_PORT", 587)),
    "sender":    os.environ.get("SMTP_SENDER", ""),
    "password":  os.environ.get("SMTP_PASSWORD", ""),
    "enabled":   os.environ.get("SMTP_ENABLED", "false").lower() == "true",
}

ROLES = {
    "admin":   {"label":"Admin",   "can_approve":True,  "can_reject":True,  "can_add":True,  "can_pay":True,  "can_report":True},
    "manager": {"label":"Manager", "can_approve":True,  "can_reject":True,  "can_add":True,  "can_pay":True,  "can_report":True},
    "viewer":  {"label":"Viewer",  "can_approve":False, "can_reject":False, "can_add":False, "can_pay":False, "can_report":True},
}

DEFAULT_USERS = {
    "admin":   {"role":"admin",   "pw_hash": hashlib.sha256(b"admin123").hexdigest()},
    "manager": {"role":"manager", "pw_hash": hashlib.sha256(b"manager123").hexdigest()},
    "viewer":  {"role":"viewer",  "pw_hash": hashlib.sha256(b"viewer123").hexdigest()},
}

# ══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

def parse_date(s):
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try: return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except: pass
    return date.today()

def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year  = sourcedate.year + month // 12
    month = month % 12 + 1
    day   = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)

def normalize_interest(rate):
    try:
        r = float(rate)
        return r / 100.0 if r > 1 else r
    except: return None

def compute_emi_amount(loan_amount, annual_interest_decimal, tenure_months):
    total = loan_amount + (loan_amount * annual_interest_decimal * (tenure_months / 12.0))
    return round(total / tenure_months, 2)

def linear_fit(xs, ys):
    n = len(xs)
    if n == 0: return 0.0, 0.0
    mx, my = sum(xs)/n, sum(ys)/n
    num = sum((x-mx)*(y-my) for x,y in zip(xs,ys))
    den = sum((x-mx)**2 for x in xs)
    slope = num/den if den else 0.0
    return slope, my - slope*mx

def fmt_inr(v):
    try: return f"₹{float(v):,.2f}"
    except: return "₹0.00"

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_FILE, check_same_thread=False)
        g.db.row_factory = sqlite3.Row
    return g.db

def get_cur(): return get_db().cursor()

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS LoanEntry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_number TEXT UNIQUE,
        customer_name TEXT,
        vehicle_type TEXT,
        loan_amount REAL,
        interest_rate REAL,
        tenure INTEGER,
        start_date TEXT,
        status TEXT,
        created_at TEXT,
        attachment TEXT,
        customer_email TEXT
    );
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER UNIQUE,
        name TEXT,
        vehicle_type TEXT,
        loan_amount REAL,
        emi_amount REAL,
        status TEXT,
        created_at TEXT,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS EMI (
        emi_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER,
        installment_no INTEGER,
        due_date TEXT,
        emi_amount REAL,
        status TEXT,
        paid_at TEXT,
        amount_paid REAL,
        remaining_amount REAL,
        extra_interest REAL,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS RejectedLoans (
        reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER UNIQUE,
        reason TEXT,
        created_at TEXT,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS ClosedLoans (
        close_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER UNIQUE,
        closure_date TEXT,
        created_at TEXT,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        pw_hash TEXT,
        role TEXT,
        created_at TEXT
    );
    """)
    for uname, info in DEFAULT_USERS.items():
        cur.execute("INSERT OR IGNORE INTO Users (username,pw_hash,role,created_at) VALUES (?,?,?,?)",
                    (uname, info["pw_hash"], info["role"], datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

# ══════════════════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC
# ══════════════════════════════════════════════════════════════════════════════
def authenticate_user(username, password):
    c = get_cur()
    c.execute("SELECT * FROM Users WHERE username=?", (username,))
    row = c.fetchone()
    if row and row["pw_hash"] == hash_pw(password):
        return dict(row)
    return None

def can_pay_emi(loan_id, installment_no):
    if installment_no == 1: return True
    c = get_cur()
    c.execute("SELECT COUNT(*) as n FROM EMI WHERE loan_id=? AND installment_no<? AND status!='Paid'",
              (loan_id, installment_no))
    return c.fetchone()["n"] == 0

def create_loan(loan_number, customer_name, vehicle_type, loan_amount,
                interest_rate_raw, tenure, start_date_str, customer_email=""):
    r = normalize_interest(interest_rate_raw)
    if r is None: raise ValueError("Invalid interest rate")
    c = get_cur()
    c.execute("""INSERT INTO LoanEntry
                 (loan_number,customer_name,vehicle_type,loan_amount,interest_rate,
                  tenure,start_date,status,created_at,attachment,customer_email)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
              (loan_number, customer_name, vehicle_type, float(loan_amount), float(r),
               int(tenure), start_date_str, "PendingApproval",
               datetime.now(timezone.utc).isoformat(), None, customer_email))
    get_db().commit()
    return c.lastrowid

def approve_loan(loan_id):
    c = get_cur()
    c.execute("SELECT * FROM LoanEntry WHERE id=?", (loan_id,))
    loan = c.fetchone()
    if not loan: raise ValueError("Loan not found")
    if loan["status"] != "PendingApproval": raise ValueError("Loan is not pending approval")
    emi_amt = compute_emi_amount(float(loan["loan_amount"]),
                                 float(loan["interest_rate"]), int(loan["tenure"]))
    now = datetime.now(timezone.utc).isoformat()
    c.execute("""INSERT OR REPLACE INTO Customers
                 (loan_id,name,vehicle_type,loan_amount,emi_amount,status,created_at)
                 VALUES (?,?,?,?,?,?,?)""",
              (loan_id, loan["customer_name"], loan["vehicle_type"],
               loan["loan_amount"], emi_amt, "Active", now))
    try: sd = parse_date(loan["start_date"])
    except: sd = date.today()
    for i in range(1, int(loan["tenure"]) + 1):
        c.execute("""INSERT INTO EMI
                     (loan_id,installment_no,due_date,emi_amount,status,paid_at,
                      amount_paid,remaining_amount,extra_interest)
                     VALUES (?,?,?,?,?,?,?,?,?)""",
                  (loan_id, i, add_months(sd, i-1).isoformat(),
                   emi_amt, "Pending", None, 0.0, emi_amt, 0.0))
    c.execute("UPDATE LoanEntry SET status='Approved' WHERE id=?", (loan_id,))
    get_db().commit()
    _notify_approval(dict(loan))

def reject_loan(loan_id, reason):
    c = get_cur()
    c.execute("UPDATE LoanEntry SET status='Rejected' WHERE id=?", (loan_id,))
    c.execute("INSERT OR REPLACE INTO RejectedLoans (loan_id,reason,created_at) VALUES (?,?,?)",
              (loan_id, reason, datetime.now(timezone.utc).isoformat()))
    get_db().commit()

def pay_emi(emi_id, pay_amount=None, extra_interest=0.0):
    c = get_cur()
    now = datetime.now(timezone.utc).isoformat()
    c.execute("SELECT * FROM EMI WHERE emi_id=?", (emi_id,))
    emi = c.fetchone()
    if not emi: raise ValueError("EMI not found")
    if emi["status"] == "Paid": raise ValueError("EMI already paid")
    if not can_pay_emi(emi["loan_id"], emi["installment_no"]):
        raise ValueError(f"Cannot pay installment {emi['installment_no']}. Complete previous installments first.")
    amount_paid = emi["amount_paid"] or 0.0
    remaining   = emi["remaining_amount"] if emi["remaining_amount"] is not None else emi["emi_amount"]
    if pay_amount is None: pay_amount = remaining
    total_due = remaining + (extra_interest or 0.0)
    if pay_amount < total_due:
        new_paid      = amount_paid + pay_amount
        new_remaining = total_due - pay_amount
        c.execute("UPDATE EMI SET amount_paid=?,remaining_amount=?,extra_interest=?,status=? WHERE emi_id=?",
                  (new_paid, new_remaining, extra_interest, "Partial", emi_id))
        get_db().commit()
        return f"Partial payment recorded. Remaining due: {fmt_inr(new_remaining)}"
    else:
        c.execute("""UPDATE EMI SET status='Paid',paid_at=?,amount_paid=?,
                     remaining_amount=0,extra_interest=0 WHERE emi_id=?""",
                  (now, amount_paid + pay_amount, emi_id))
        get_db().commit()
        loan_id = emi["loan_id"]
        c.execute("""SELECT COUNT(*) as total,
                     SUM(CASE WHEN status='Paid' THEN 1 ELSE 0 END) as paidcount
                     FROM EMI WHERE loan_id=?""", (loan_id,))
        counts = c.fetchone()
        if counts["total"] > 0 and counts["paidcount"] == counts["total"]:
            c.execute("UPDATE LoanEntry SET status='Closed' WHERE id=?", (loan_id,))
            c.execute("UPDATE Customers SET status='Closed' WHERE loan_id=?", (loan_id,))
            c.execute("INSERT OR REPLACE INTO ClosedLoans (loan_id,closure_date,created_at) VALUES (?,?,?)",
                      (loan_id, date.today().isoformat(), now))
            get_db().commit()
            _notify_closure(loan_id)
        return "EMI paid successfully!"

# ── Query helpers ──────────────────────────────────────────────────────────────
def list_pending_loans(search=""):
    q = f"%{search}%"
    c = get_cur()
    c.execute("""SELECT * FROM LoanEntry WHERE status='PendingApproval'
                 AND (loan_number LIKE ? OR customer_name LIKE ? OR vehicle_type LIKE ?)
                 ORDER BY created_at DESC""", (q,q,q))
    return [dict(r) for r in c.fetchall()]

def list_all_loans(search=""):
    q = f"%{search}%"
    c = get_cur()
    c.execute("""SELECT * FROM LoanEntry WHERE
                 loan_number LIKE ? OR customer_name LIKE ? OR vehicle_type LIKE ? OR status LIKE ?
                 ORDER BY created_at DESC""", (q,q,q,q))
    return [dict(r) for r in c.fetchall()]

def list_customers(search=""):
    q = f"%{search}%"
    c = get_cur()
    c.execute("""SELECT * FROM Customers WHERE name LIKE ? OR vehicle_type LIKE ? OR status LIKE ?
                 ORDER BY created_at DESC""", (q,q,q))
    return [dict(r) for r in c.fetchall()]

def get_emis_for_loan(loan_id):
    c = get_cur()
    c.execute("SELECT * FROM EMI WHERE loan_id=? ORDER BY installment_no ASC", (loan_id,))
    return [dict(r) for r in c.fetchall()]

def list_closed_loans(search=""):
    q = f"%{search}%"
    c = get_cur()
    c.execute("""SELECT le.id as loan_id, le.loan_number, le.customer_name,
                        le.vehicle_type, le.loan_amount, cl.closure_date
                 FROM LoanEntry le JOIN ClosedLoans cl ON cl.loan_id=le.id
                 WHERE le.loan_number LIKE ? OR le.customer_name LIKE ?
                 ORDER BY cl.created_at DESC""", (q,q))
    return [dict(r) for r in c.fetchall()]

def list_rejected_loans(search=""):
    q = f"%{search}%"
    c = get_cur()
    c.execute("""SELECT le.id as loan_id, le.loan_number, le.customer_name,
                        rl.reason, rl.created_at
                 FROM LoanEntry le JOIN RejectedLoans rl ON rl.loan_id=le.id
                 WHERE le.loan_number LIKE ? OR le.customer_name LIKE ?
                 ORDER BY rl.created_at DESC""", (q,q))
    return [dict(r) for r in c.fetchall()]

def get_overdue_emis():
    today = date.today().isoformat()
    c = get_cur()
    c.execute("""SELECT e.*, le.loan_number, le.customer_name
                 FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id
                 WHERE e.status='Overdue'
                    OR (e.status IN ('Pending','Partial') AND e.due_date < ?)
                 ORDER BY e.due_date ASC""", (today,))
    return [dict(r) for r in c.fetchall()]

def get_upcoming_emis():
    today = date.today().isoformat()
    limit = (date.today() + timedelta(days=UPCOMING_DAYS)).isoformat()
    c = get_cur()
    c.execute("""SELECT e.*, le.loan_number, le.customer_name
                 FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id
                 WHERE e.status IN ('Pending','Partial')
                   AND e.due_date >= ? AND e.due_date <= ?
                 ORDER BY e.due_date ASC""", (today, limit))
    return [dict(r) for r in c.fetchall()]

def get_loan_summary_counts():
    c = get_cur()
    def cnt(sql, params=[]): c.execute(sql, params); return c.fetchone()[0] or 0
    return dict(
        total    = cnt("SELECT COUNT(*) FROM LoanEntry"),
        pending  = cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='PendingApproval'"),
        approved = cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='Approved'"),
        rejected = cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='Rejected'"),
        closed   = cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='Closed'"),
        overdue  = len(get_overdue_emis()),
        upcoming = len(get_upcoming_emis()),
    )

def get_kpi_totals():
    c = get_cur()
    c.execute("SELECT COUNT(*), SUM(loan_amount) FROM LoanEntry")
    row = c.fetchone(); tl = row[0] or 0; tla = row[1] or 0.0
    c.execute("SELECT SUM(emi_amount) FROM EMI WHERE status='Paid'")
    tr = c.fetchone()[0] or 0.0
    c.execute("SELECT SUM(remaining_amount) FROM EMI WHERE status IN ('Pending','Overdue','Partial')")
    tp = c.fetchone()[0] or 0.0
    return tl, tla, tr, tp

def get_monthly_paid_series():
    c = get_cur()
    c.execute("""SELECT strftime('%Y-%m',paid_at) as ym, SUM(emi_amount) as amt
                 FROM EMI WHERE status='Paid' AND paid_at IS NOT NULL
                 GROUP BY ym ORDER BY ym ASC""")
    rows = c.fetchall()
    return [r["ym"] for r in rows], [float(r["amt"] or 0) for r in rows]

def get_loan_type_breakdown():
    c = get_cur()
    c.execute("SELECT vehicle_type, COUNT(*) as cnt FROM LoanEntry GROUP BY vehicle_type")
    return [dict(r) for r in c.fetchall()]

# ── Email ──────────────────────────────────────────────────────────────────────
def _send_email(to, subject, body):
    if not EMAIL_CONFIG.get("enabled") or not to: return
    try:
        msg = MIMEMultipart(); msg["From"]=EMAIL_CONFIG["sender"]
        msg["To"]=to; msg["Subject"]=subject
        msg.attach(MIMEText(body,"html"))
        with smtplib.SMTP(EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]) as s:
            s.starttls(); s.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
            s.sendmail(EMAIL_CONFIG["sender"], to, msg.as_string())
    except Exception as e:
        print(f"[Email] {e}")

def _notify_approval(loan):
    em = loan.get("customer_email","")
    if not em: return
    _send_email(em, "Your Vehicle Loan Has Been Approved",
        f"<h2>Loan Approved</h2><p>Dear {loan['customer_name']},</p>"
        f"<p>Loan <b>{loan['loan_number']}</b> of {fmt_inr(loan['loan_amount'])} approved.</p>")

def _notify_closure(loan_id):
    c = get_cur()
    c.execute("SELECT * FROM LoanEntry WHERE id=?", (loan_id,))
    loan = c.fetchone()
    if not loan or not loan["customer_email"]: return
    _send_email(loan["customer_email"], "Loan Fully Repaid — Congratulations!",
        f"<h2>Loan Closed</h2><p>Dear {loan['customer_name']},</p>"
        f"<p>All EMIs for loan <b>{loan['loan_number']}</b> have been paid. Loan is now closed!</p>")

# ── PDF ────────────────────────────────────────────────────────────────────────
def generate_pdf(path):
    if not REPORTLAB_AVAILABLE:
        raise RuntimeError("reportlab not installed. Run: pip install reportlab")
    styles  = getSampleStyleSheet()
    title_s = ParagraphStyle("T", parent=styles["Title"], fontSize=18, spaceAfter=12)
    h2      = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, spaceAfter=6)
    story   = []
    story.append(Paragraph("Vehicle Loan Management Report", title_s))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%d %b %Y %H:%M')}", styles["Normal"]))
    story.append(Spacer(1, 0.5*cm))
    tl, tla, tr, tp = get_kpi_totals()
    counts = get_loan_summary_counts()
    kpi_data = [["Metric","Value"],
        ["Total Loans", str(tl)],
        ["Total Loan Amount", f"Rs {tla:,.2f}"],
        ["Total Received", f"Rs {tr:,.2f}"],
        ["Total Pending", f"Rs {tp:,.2f}"],
        ["Pending Approval", str(counts["pending"])],
        ["Active / Approved", str(counts["approved"])],
        ["Closed", str(counts["closed"])],
        ["Rejected", str(counts["rejected"])],
        ["Overdue EMIs", str(counts["overdue"])],
        ["Upcoming EMIs (10d)", str(counts["upcoming"])],
    ]
    kt = Table(kpi_data, colWidths=[8*cm, 7*cm])
    kt.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#0057b8")),
        ("TEXTCOLOR",(0,0),(-1,0),colors.white),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
        ("GRID",(0,0),(-1,-1),0.5,colors.grey),
    ]))
    story.append(kt); story.append(PageBreak())
    story.append(Paragraph("All Loans", h2))
    loans = list_all_loans()
    ld = [["Loan #","Customer","Type","Amount","Rate","Tenure","Status"]]
    for l in loans:
        ld.append([l["loan_number"], l["customer_name"], l["vehicle_type"],
                   f"Rs {l['loan_amount']:,.0f}", f"{l['interest_rate']*100:.1f}%",
                   f"{l['tenure']}m", l["status"]])
    if len(ld) > 1:
        lt = Table(ld, repeatRows=1, colWidths=[2.8*cm,4.5*cm,2.2*cm,3*cm,1.8*cm,1.8*cm,2.8*cm])
        lt.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#0057b8")),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
            ("GRID",(0,0),(-1,-1),0.4,colors.grey),
        ]))
        story.append(lt)
    story.append(PageBreak())
    story.append(Paragraph("Overdue EMIs", h2))
    overdue = get_overdue_emis()
    if overdue:
        od = [["Loan #","Customer","Inst #","Due Date","Amount Due","Days Overdue"]]
        for e in overdue:
            due_d = parse_date(e["due_date"])
            od.append([e["loan_number"], e["customer_name"], str(e["installment_no"]),
                       e["due_date"], f"Rs {(e['remaining_amount'] or e['emi_amount']):,.2f}",
                       str((date.today()-due_d).days)])
        ot = Table(od, repeatRows=1, colWidths=[2.8*cm,4.5*cm,1.5*cm,2.5*cm,3.5*cm,3*cm])
        ot.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#d7263d")),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#fdecea")]),
            ("GRID",(0,0),(-1,-1),0.4,colors.grey),
        ]))
        story.append(ot)
    else:
        story.append(Paragraph("No overdue EMIs.", styles["Normal"]))
    doc = SimpleDocTemplate(path, pagesize=A4,
                            leftMargin=1.5*cm, rightMargin=1.5*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    doc.build(story)

# ══════════════════════════════════════════════════════════════════════════════
#  FLASK APP
# ══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db: db.close()

# ── Auth decorators ───────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*a, **kw):
        if "username" not in session: return redirect(url_for("login"))
        return f(*a, **kw)
    return decorated

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated(*a, **kw):
            if session.get("role") not in roles:
                flash("Access denied for your role.", "danger")
                return redirect(url_for("dashboard"))
            return f(*a, **kw)
        return decorated
    return decorator

# ── Template context ──────────────────────────────────────────────────────────
@app.context_processor
def inject_globals():
    return {"today": date.today().isoformat(), "fmt_inr": fmt_inr,
            "REPORTLAB_AVAILABLE": REPORTLAB_AVAILABLE}

# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/")
def index(): return redirect(url_for("dashboard"))

@app.route("/login", methods=["GET","POST"])
def login():
    if "username" in session: return redirect(url_for("dashboard"))
    if request.method == "POST":
        user = authenticate_user(request.form["username"], request.form["password"])
        if user:
            session["username"] = user["username"]
            session["role"]     = user["role"]
            flash(f"Welcome, {user['username']}!", "success")
            return redirect(url_for("dashboard"))
        flash("Invalid credentials.", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    counts   = get_loan_summary_counts()
    tl,tla,tr,tp = get_kpi_totals()
    overdue  = get_overdue_emis()
    upcoming = get_upcoming_emis()
    months, amounts = get_monthly_paid_series()
    breakdown = get_loan_type_breakdown()
    return render_template("dashboard.html", active="dashboard",
        counts=counts, tl=tl, tla=tla, tr=tr, tp=tp,
        overdue=overdue, upcoming=upcoming,
        months=months, amounts=amounts, breakdown=breakdown)

@app.route("/loans")
@login_required
def loans():
    q = request.args.get("q","")
    return render_template("loans.html", active="loans",
                           loans=list_all_loans(q), q=q)

@app.route("/loan/add", methods=["GET","POST"])
@login_required
@role_required("admin","manager")
def add_loan():
    if request.method == "POST":
        f = request.form
        try:
            create_loan(f["loan_number"], f["customer_name"], f["vehicle_type"],
                        f["loan_amount"], f["interest_rate"], f["tenure"],
                        f["start_date"], f.get("customer_email",""))
            flash("Loan submitted for approval.", "success")
            return redirect(url_for("loans"))
        except Exception as e:
            flash(str(e), "danger")
    return render_template("add_loan.html", active="add")

@app.route("/approval", methods=["GET","POST"])
@login_required
@role_required("admin","manager")
def approval():
    if request.method == "POST":
        lid    = int(request.form["loan_id"])
        action = request.form["action"]
        try:
            if action == "approve":
                approve_loan(lid); flash("Loan approved!", "success")
            else:
                reject_loan(lid, request.form.get("reason","No reason given"))
                flash("Loan rejected.", "success")
        except Exception as e:
            flash(str(e), "danger")
    q = request.args.get("q","")
    return render_template("approval.html", active="approval",
                           loans=list_pending_loans(q), q=q)

@app.route("/customers")
@login_required
def customers():
    q = request.args.get("q","")
    return render_template("customers.html", active="customers",
                           customers=list_customers(q), q=q)

@app.route("/emis")
@login_required
def emis_all(): return redirect(url_for("customers"))

@app.route("/emis/<int:loan_id>")
@login_required
def emis(loan_id):
    c = get_cur()
    c.execute("SELECT * FROM LoanEntry WHERE id=?", (loan_id,))
    loan = c.fetchone()
    return render_template("emis.html", active="emis",
                           emis=get_emis_for_loan(loan_id),
                           loan_id=loan_id, loan=dict(loan) if loan else {})

@app.route("/emi/pay", methods=["POST"])
@login_required
@role_required("admin","manager")
def emi_pay():
    emi_id     = int(request.form["emi_id"])
    loan_id    = int(request.form["loan_id"])
    pay_amount = float(request.form.get("pay_amount", 0) or 0)
    try:
        msg = pay_emi(emi_id, pay_amount)
        flash(msg, "success")
    except Exception as e:
        flash(str(e), "danger")
    return redirect(url_for("emis", loan_id=loan_id))

@app.route("/alerts")
@login_required
def alerts():
    od    = get_overdue_emis()
    up    = get_upcoming_emis()
    today = date.today()
    days_overdue  = {e["emi_id"]: (today - parse_date(e["due_date"])).days for e in od}
    days_upcoming = {e["emi_id"]: (parse_date(e["due_date"]) - today).days for e in up}
    return render_template("alerts.html", active="alerts",
                           overdue=od, upcoming=up,
                           days_overdue=days_overdue, days_upcoming=days_upcoming)

@app.route("/closed")
@login_required
def closed():
    q = request.args.get("q","")
    return render_template("closed.html", active="closed",
                           loans=list_closed_loans(q), q=q)

@app.route("/rejected")
@login_required
def rejected():
    q = request.args.get("q","")
    return render_template("rejected.html", active="rejected",
                           loans=list_rejected_loans(q), q=q)

@app.route("/calculator", methods=["GET","POST"])
@login_required
def calculator():
    result = None; schedule = []
    if request.method == "POST":
        try:
            mode   = request.form.get("mode","EMI")
            amt    = float(request.form.get("amount") or 0)
            rate   = float(request.form.get("rate")   or 0) / 100.0 / 12.0
            tenure = int(float(request.form.get("tenure") or 0))
            emi_v  = float(request.form.get("emi")    or 0)
            p_list, i_list, b_list = [], [], []
            if mode == "EMI" and amt>0 and rate>0 and tenure>0:
                emi_calc = amt*rate*(1+rate)**tenure/((1+rate)**tenure-1)
                result = {"label":"Monthly EMI", "value": f"₹{emi_calc:,.2f}"}
                bal = amt
                for _ in range(tenure):
                    intr=bal*rate; prin=emi_calc-intr; bal-=prin
                    p_list.append(max(prin,0)); i_list.append(max(intr,0)); b_list.append(max(bal,0))
            elif mode == "Affordability" and emi_v>0 and rate>0 and tenure>0:
                av = emi_v*((1+rate)**tenure-1)/(rate*(1+rate)**tenure)
                result = {"label":"Affordable Loan Amount", "value": f"₹{av:,.2f}"}
                bal = av
                for _ in range(tenure):
                    intr=bal*rate; prin=emi_v-intr; bal-=prin
                    p_list.append(max(prin,0)); i_list.append(max(intr,0)); b_list.append(max(bal,0))
            elif mode == "Tenure" and amt>0 and rate>0 and emi_v>0:
                n = math.log(emi_v/(emi_v-amt*rate))/math.log(1+rate)
                tv = int(round(n))
                result = {"label":"Loan Tenure", "value": f"{tv} months"}
                bal = amt
                for _ in range(tv):
                    intr=bal*rate; prin=emi_v-intr; bal-=prin
                    p_list.append(max(prin,0)); i_list.append(max(intr,0)); b_list.append(max(bal,0))
            elif mode == "Rate" and amt>0 and tenure>0 and emi_v>0:
                lo, hi, r_found = 0.00001, 1.0, None
                for _ in range(200):
                    mid = (lo+hi)/2
                    guess = amt*mid*(1+mid)**tenure/((1+mid)**tenure-1)
                    if abs(guess-emi_v)<0.01: r_found=mid; break
                    if guess>emi_v: hi=mid
                    else: lo=mid
                if r_found: result={"label":"Annual Interest Rate","value":f"{r_found*12*100:.2f}% p.a."}
                else: result={"label":"Error","value":"Could not converge"}
            if p_list:
                schedule = [{"n":i+1,"principal":p_list[i],"interest":i_list[i],
                             "emi":p_list[i]+i_list[i],"balance":b_list[i]}
                            for i in range(len(p_list))]
        except Exception as e:
            flash(str(e), "danger")
    return render_template("calculator.html", active="calculator",
                           result=result, schedule=schedule)

@app.route("/report", methods=["GET","POST"])
@login_required
def report():
    if request.method == "POST":
        if not REPORTLAB_AVAILABLE:
            flash("reportlab not installed. Add it to requirements.txt.", "danger")
            return redirect(url_for("report"))
        path = "/tmp/loan_report.pdf"
        try:
            generate_pdf(path)
            return send_file(path, as_attachment=True,
                             download_name="loan_report.pdf",
                             mimetype="application/pdf")
        except Exception as e:
            flash(str(e), "danger")
    return render_template("report.html", active="report")

@app.route("/users", methods=["GET","POST"])
@login_required
@role_required("admin")
def users():
    if request.method == "POST":
        uname = request.form["username"].strip()
        pw    = request.form["password"]
        role  = request.form["role"]
        c = get_cur()
        c.execute("""INSERT INTO Users (username,pw_hash,role,created_at)
                     VALUES (?,?,?,?)
                     ON CONFLICT(username) DO UPDATE SET pw_hash=?,role=?""",
                  (uname, hash_pw(pw), role, datetime.now(timezone.utc).isoformat(),
                   hash_pw(pw), role))
        get_db().commit()
        flash(f"User '{uname}' saved.", "success")
    c = get_cur()
    c.execute("SELECT * FROM Users ORDER BY user_id")
    return render_template("users.html", active="users",
                           users=[dict(r) for r in c.fetchall()])

# ── API endpoints for chart data ───────────────────────────────────────────────
@app.route("/api/chart/monthly")
@login_required
def api_monthly():
    months, amounts = get_monthly_paid_series()
    return jsonify({"labels": months, "data": amounts})

@app.route("/api/chart/breakdown")
@login_required
def api_breakdown():
    bd = get_loan_type_breakdown()
    return jsonify({"labels":[r["vehicle_type"] for r in bd],
                    "data":[r["cnt"] for r in bd]})

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
