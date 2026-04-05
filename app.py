"""
Vehicle Loan Manager — Flask Web App v9.0
Uses proper HTML templates (templates/ folder)
Supports PostgreSQL (cloud) and SQLite (local)
"""
import os, csv, io, math, hashlib, secrets, threading, smtplib, calendar
import sqlite3
from datetime import date, datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
from flask import (Flask, render_template, request, redirect,
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
# DATABASE
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

def scalar(cur_obj, sql, params=()):
    cur_obj.execute(q(sql), params)
    row = cur_obj.fetchone()
    if row is None: return 0
    v = list(row.values())[0] if isinstance(row, dict) else row[0]
    return v or 0

# =============================================================================
# UTILS
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
              (loan_number, customer_name, vehicle_type, float(loan_amount), float(r), int(tenure),
               start_date_iso, "PendingApproval", datetime.now(timezone.utc).isoformat(), customer_email))
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
        raise ValueError(f"Please pay installment {emi['installment_no']-1} first")
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
    if total and paid and int(total) == int(paid or 0):
        c.execute(q("UPDATE LoanEntry SET status='Closed' WHERE id=?"), (loan_id,))
        c.execute(q("UPDATE Customers SET status='Closed' WHERE loan_id=?"), (loan_id,))
        if USE_POSTGRES:
            c.execute("INSERT INTO ClosedLoans (loan_id,closure_date,created_at) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                      (loan_id, date.today().isoformat(), now))
        else:
            c.execute("INSERT OR REPLACE INTO ClosedLoans (loan_id,closure_date,created_at) VALUES (?,?,?)",
                      (loan_id, date.today().isoformat(), now))
        db_commit()
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
    c = get_cur(); qp = f"%{search}%"
    sql = "SELECT * FROM LoanEntry WHERE (loan_number LIKE ? OR customer_name LIKE ? OR vehicle_type LIKE ?)"
    params = [qp, qp, qp]
    if status: sql += " AND status=?"; params.append(status)
    c.execute(q(sql + " ORDER BY created_at DESC"), params)
    return c.fetchall()

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
    today = date.today().isoformat()
    limit = (date.today() + timedelta(days=UPCOMING_DAYS)).isoformat()
    c = get_cur()
    c.execute(q("SELECT e.*, le.loan_number, le.customer_name FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id WHERE e.status IN ('Pending','Partial') AND e.due_date>=? AND e.due_date<=? ORDER BY e.due_date"), (today, limit))
    return c.fetchall()

def get_kpis():
    c = get_cur()
    return dict(
        total       = scalar(c, "SELECT COUNT(*) FROM LoanEntry"),
        pending     = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='PendingApproval'"),
        approved    = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='Approved'"),
        closed      = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='Closed'"),
        rejected    = scalar(c, "SELECT COUNT(*) FROM LoanEntry WHERE status='Rejected'"),
        disbursed   = scalar(c, "SELECT SUM(loan_amount) FROM LoanEntry") or 0,
        received    = scalar(c, "SELECT SUM(emi_amount) FROM EMI WHERE status='Paid'") or 0,
        pending_amt = scalar(c, "SELECT SUM(remaining_amount) FROM EMI WHERE status IN ('Pending','Partial','Overdue')") or 0,
        overdue     = len(get_overdue_emis()),
        upcoming    = len(get_upcoming_emis()),
    )

def get_users():
    c = get_cur()
    c.execute("SELECT user_id,username,role,created_at FROM Users ORDER BY user_id")
    return c.fetchall()

# =============================================================================
# FLASK APP
# =============================================================================
def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

    with app.app_context():
        init_db()

    # Template context
    @app.context_processor
    def inject_globals():
        od = get_overdue_emis() if "username" in session else []
        pend = get_loans("","PendingApproval") if "username" in session else []
        return dict(
            can_do=can_do,
            overdue_count=len(od),
            pending_count=len(pend),
        )

    # Auth decorators
    def login_required(f):
        @wraps(f)
        def dec(*a, **k):
            if "username" not in session: return redirect("/login")
            return f(*a, **k)
        return dec

    def role_required(*roles):
        def decorator(f):
            @wraps(f)
            def dec(*a, **k):
                if session.get("role") not in roles:
                    flash("Access denied for your role.", "danger")
                    return redirect("/dashboard")
                return f(*a, **k)
            return dec
        return decorator

    # ── Routes ─────────────────────────────────────────────────────────────────

    @app.route("/login", methods=["GET","POST"])
    def login():
        if request.method == "POST":
            user = authenticate_user(request.form["username"], request.form["password"])
            if user:
                session["username"] = user["username"]
                session["role"]     = user["role"]
                flash(f"Welcome back, {user['username']}!", "success")
                return redirect("/dashboard")
            flash("Invalid username or password.", "danger")
        return render_template("login.html")

    @app.route("/logout")
    def logout():
        session.clear(); return redirect("/login")

    @app.route("/")
    def index(): return redirect("/dashboard")

    @app.route("/dashboard")
    @login_required
    def dashboard():
        flag_overdues()
        return render_template("dashboard.html",
            active="dashboard",
            kpis=get_kpis(),
            overdue=get_overdue_emis()[:5],
            upcoming=get_upcoming_emis()[:5],
            today=date.today().isoformat())

    @app.route("/loans")
    @login_required
    def loans():
        q_     = request.args.get("q","")
        status = request.args.get("status","")
        return render_template("loans.html", active="loans",
            rows=get_loans(q_, status or None), q_=q_, status=status)

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
                flash("Loan submitted for approval successfully!", "success")
                return redirect("/loans")
            except Exception as e:
                flash(str(e), "danger")
        return render_template("add_loan.html", active="add", today=date.today().isoformat())

    @app.route("/approval", methods=["GET","POST"])
    @login_required
    @role_required("admin","manager")
    def approval():
        if request.method == "POST":
            lid    = int(request.form["loan_id"])
            action = request.form["action"]
            try:
                if action == "approve":
                    approve_loan(lid)
                    flash("Loan approved! EMI schedule has been created.", "success")
                else:
                    reject_loan(lid, request.form.get("reason","No reason given"))
                    flash("Loan rejected and recorded.", "success")
            except Exception as e:
                flash(str(e), "danger")
        q_ = request.args.get("q","")
        return render_template("approval.html", active="approval",
            rows=get_loans(q_, "PendingApproval"), q_=q_)

    @app.route("/customers")
    @login_required
    def customers():
        q_ = request.args.get("q","")
        return render_template("customers.html", active="customers",
            rows=get_customers(q_), q_=q_)

    @app.route("/emis")
    @login_required
    def emis_home(): return redirect("/customers")

    @app.route("/emis/<int:loan_id>", methods=["GET","POST"])
    @login_required
    def emis(loan_id):
        if request.method == "POST" and can_do("can_pay"):
            try:
                msg = pay_emi_db(int(request.form["emi_id"]),
                                 float(request.form.get("pay_amount",0)),
                                 float(request.form.get("extra_interest",0)))
                flash(msg, "success")
            except Exception as e:
                flash(str(e), "danger")
            return redirect(f"/emis/{loan_id}")
        c = get_cur()
        c.execute(q("SELECT * FROM LoanEntry WHERE id=?"), (loan_id,))
        loan = c.fetchone()
        return render_template("emis.html", active="emis",
            rows=get_emis_for_loan(loan_id), loan=loan, loan_id=loan_id)

    @app.route("/alerts")
    @login_required
    def alerts():
        flag_overdues()
        return render_template("alerts.html", active="alerts",
            od=get_overdue_emis(), up=get_upcoming_emis(), today=date.today())

    @app.route("/closed")
    @login_required
    def closed():
        q_ = request.args.get("q",""); qp = f"%{q_}%"
        c = get_cur()
        c.execute(q("SELECT le.*,cl.closure_date FROM LoanEntry le JOIN ClosedLoans cl ON cl.loan_id=le.id WHERE le.loan_number LIKE ? OR le.customer_name LIKE ? ORDER BY cl.created_at DESC"), (qp,qp))
        return render_template("closed.html", active="closed", rows=c.fetchall(), q_=q_)

    @app.route("/rejected")
    @login_required
    def rejected():
        q_ = request.args.get("q",""); qp = f"%{q_}%"
        c = get_cur()
        c.execute(q("SELECT le.loan_number,le.customer_name,rl.reason,rl.created_at FROM LoanEntry le JOIN RejectedLoans rl ON rl.loan_id=le.id WHERE le.loan_number LIKE ? OR le.customer_name LIKE ? ORDER BY rl.created_at DESC"), (qp,qp))
        return render_template("rejected.html", active="rejected", rows=c.fetchall(), q_=q_)

    @app.route("/calculator", methods=["GET","POST"])
    @login_required
    def calculator():
        result = None; schedule = []
        if request.method == "POST":
            try:
                amt    = float(request.form.get("loan_amount",0))
                rate   = float(request.form.get("interest_rate",0)) / 100 / 12
                tenure = int(request.form.get("tenure",0))
                mode   = request.form.get("mode","EMI")
                emi_in = float(request.form.get("emi_amount") or 0)
                if mode == "EMI" and amt > 0 and rate > 0 and tenure > 0:
                    ev = amt * rate * (1+rate)**tenure / ((1+rate)**tenure - 1)
                    result = {"label":"Monthly EMI", "value":f"Rs {ev:,.2f}", "total":f"Rs {ev*tenure:,.2f}", "interest":f"Rs {ev*tenure-amt:,.2f}"}
                    bal = amt
                    for i in range(1, tenure+1):
                        intr = bal*rate; prin = ev-intr; bal -= prin
                        schedule.append({"month":i,"principal":round(max(prin,0),2),"interest":round(max(intr,0),2),"emi":round(ev,2),"balance":round(max(bal,0),2)})
                elif mode == "Affordability" and emi_in > 0 and rate > 0 and tenure > 0:
                    lv = emi_in * ((1+rate)**tenure - 1) / (rate*(1+rate)**tenure)
                    result = {"label":"Max Loan Amount","value":f"Rs {lv:,.2f}","total":"—","interest":"—"}
                elif mode == "Tenure" and amt > 0 and rate > 0 and emi_in > 0:
                    n = math.log(emi_in/(emi_in - amt*rate)) / math.log(1+rate)
                    result = {"label":"Loan Tenure","value":f"{int(round(n))} months","total":"—","interest":"—"}
            except Exception as e:
                flash(str(e), "danger")
        return render_template("calculator.html", active="calculator",
            result=result, schedule=schedule)

    @app.route("/report")
    @login_required
    @role_required("admin","manager","viewer")
    def report():
        if not REPORTLAB_AVAILABLE:
            flash("reportlab not installed. Add it to requirements.txt", "danger")
            return redirect("/dashboard")
        try:
            buf = _generate_pdf()
            return send_file(buf, as_attachment=True,
                             download_name=f"loan_report_{date.today()}.pdf",
                             mimetype="application/pdf")
        except Exception as e:
            flash(str(e), "danger")
            return redirect("/dashboard")

    @app.route("/export/loans")
    @login_required
    @role_required("admin","manager","viewer")
    def export_loans():
        c = get_cur(); c.execute("SELECT * FROM LoanEntry ORDER BY created_at DESC"); rows = c.fetchall()
        si = io.StringIO(); w = csv.writer(si)
        if rows:
            w.writerow(rows[0].keys())
            for r in rows: w.writerow(list(r.values()))
        out = make_response(si.getvalue())
        out.headers["Content-Disposition"] = f"attachment; filename=loans_{date.today()}.csv"
        out.headers["Content-type"] = "text/csv"
        return out

    @app.route("/export/emis/<int:loan_id>")
    @login_required
    def export_emis(loan_id):
        rows = get_emis_for_loan(loan_id)
        si = io.StringIO(); w = csv.writer(si)
        if rows:
            w.writerow(rows[0].keys())
            for r in rows: w.writerow(list(r.values()))
        out = make_response(si.getvalue())
        out.headers["Content-Disposition"] = f"attachment; filename=emis_loan{loan_id}.csv"
        out.headers["Content-type"] = "text/csv"
        return out

    @app.route("/users", methods=["GET","POST"])
    @login_required
    @role_required("admin")
    def users():
        if request.method == "POST":
            uname = request.form["username"].strip()
            pw    = request.form["password"]
            role  = request.form["role"]
            c = get_cur()
            if USE_POSTGRES:
                c.execute("INSERT INTO Users (username,pw_hash,role,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT(username) DO UPDATE SET pw_hash=%s,role=%s",
                          (uname, hash_pw(pw), role, datetime.now(timezone.utc).isoformat(), hash_pw(pw), role))
            else:
                c.execute("INSERT INTO Users (username,pw_hash,role,created_at) VALUES (?,?,?,?) ON CONFLICT(username) DO UPDATE SET pw_hash=?,role=?",
                          (uname, hash_pw(pw), role, datetime.now(timezone.utc).isoformat(), hash_pw(pw), role))
            db_commit()
            flash(f"User '{uname}' saved successfully.", "success")
        return render_template("users.html", active="users", rows=get_users())

    @app.route("/health")
    def health():
        return jsonify({"status":"ok", "db":"postgres" if USE_POSTGRES else "sqlite"})

    return app

# =============================================================================
# PDF GENERATION
# =============================================================================
def _generate_pdf():
    buf = io.BytesIO(); styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], fontSize=20, spaceAfter=12)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=14, spaceAfter=8)
    story = []
    story.append(Paragraph("Vehicle Loan Management Report", h1))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%d %b %Y %H:%M')}", styles["Normal"]))
    story.append(Spacer(1, 0.5*cm))
    kpis = get_kpis()
    kd = [["Metric","Value"],
          ["Total Loans", str(kpis["total"])],
          ["Total Disbursed", f"Rs {kpis['disbursed']:,.2f}"],
          ["Total Received", f"Rs {kpis['received']:,.2f}"],
          ["Pending Amount", f"Rs {kpis['pending_amt']:,.2f}"],
          ["Pending Approval", str(kpis["pending"])],
          ["Active", str(kpis["approved"])],
          ["Closed", str(kpis["closed"])],
          ["Rejected", str(kpis["rejected"])],
          ["Overdue EMIs", str(kpis["overdue"])]]
    t = Table(kd, colWidths=[8*cm,7*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#0057b8")),
        ("TEXTCOLOR",(0,0),(-1,0),colors.white),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
        ("GRID",(0,0),(-1,-1),0.5,colors.grey),
        ("ALIGN",(1,1),(-1,-1),"RIGHT")]))
    story.append(t); story.append(PageBreak())
    story.append(Paragraph("All Loans", h2))
    c = get_cur(); c.execute("SELECT * FROM LoanEntry ORDER BY created_at DESC"); loans = c.fetchall()
    ld = [["Loan #","Customer","Type","Amount","Rate","Tenure","Status"]]
    for l in loans:
        ld.append([l["loan_number"],l["customer_name"],l["vehicle_type"],
                   f"Rs {l['loan_amount']:,.0f}",f"{l['interest_rate']*100:.1f}%",
                   f"{l['tenure']}m",l["status"]])
    if len(ld) > 1:
        lt = Table(ld, repeatRows=1, colWidths=[2.8*cm,4.5*cm,2.2*cm,3*cm,1.8*cm,1.8*cm,2.8*cm])
        lt.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#0057b8")),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
            ("GRID",(0,0),(-1,-1),0.4,colors.grey)]))
        story.append(lt)
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=2*cm, bottomMargin=2*cm)
    doc.build(story); buf.seek(0); return buf

# =============================================================================
# ENTRY POINT
# =============================================================================
app = create_app()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
