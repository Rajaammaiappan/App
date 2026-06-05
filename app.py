"""
Thendralla Fincorp — Vehicle Loan Manager v3
Changes v3:
  - Vertical sidebar navigation (mobile-first, hamburger toggle)
  - Vehicle Details all fields mandatory
  - Customer Address mandatory + GPS location field (press button or manual)
  - Alerts grouped by Loan Number (one row per loan with cumulative overdue amount)
  - EMI Schedule: overdue rows highlighted RED, upcoming (≤10d) rows highlighted YELLOW
  - Pay from Alerts redirects directly to the correct EMI row (anchor #emi_<id>)
  - Dashboard: Chart.js bar chart (monthly collections) + doughnut (loan status breakdown)
  - All previous features preserved
"""

import os, math, sqlite3, hashlib, secrets, calendar, smtplib, base64
import requests as http_req
from datetime import date, datetime, timezone, timedelta
from functools import wraps
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import (Flask, request, redirect, url_for, session,
                   flash, send_file, jsonify, g, get_flashed_messages)

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
TURSO_URL     = os.environ.get("TURSO_URL", "")
TURSO_TOKEN   = os.environ.get("TURSO_TOKEN", "")
UPCOMING_DAYS = 10

EMAIL_CONFIG = {
    "smtp_host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port": int(os.environ.get("SMTP_PORT", 587)),
    "sender":    os.environ.get("SMTP_SENDER", ""),
    "password":  os.environ.get("SMTP_PASSWORD", ""),
    "enabled":   os.environ.get("SMTP_ENABLED", "false").lower() == "true",
}

# ── SMS CONFIG (Fast2SMS — India) ──────────────────────────────────────────────
# Get free API key from https://www.fast2sms.com → Dashboard → Dev API
# Set environment variable FAST2SMS_KEY=your_api_key on Render
SMS_CONFIG = {
    "api_key": os.environ.get("FAST2SMS_KEY", "Uv2IV4r5B6dxySCeGlasKmh13oJ7YRHAZEMjXFztuDLnNf9PcTiNBGM3dyv7IaeWbox4LTmAKSYDwXkr"),   # Set this on Render
    "enabled": bool(os.environ.get("FAST2SMS_KEY", "")),
    "sender_id": "TFCORP",                            # 6-char sender ID (apply on Fast2SMS)
}

ROLES = {
    "admin":    {"label":"Admin",    "can_approve":True,  "can_reject":True,  "can_add":True,  "can_pay":True,  "can_report":True},
    "manager":  {"label":"Manager",  "can_approve":False, "can_reject":False, "can_add":True,  "can_pay":True,  "can_report":True},
    "fieldpia": {"label":"Fieldpia", "can_approve":False, "can_reject":False, "can_add":True,  "can_pay":True,  "can_report":False},
    "viewer":   {"label":"Viewer",   "can_approve":False, "can_reject":False, "can_add":False, "can_pay":False, "can_report":True},
}

DEFAULT_USERS = {
    "admin":    {"role":"admin",    "pw_hash": hashlib.sha256(b"admin123").hexdigest()},
    "manager":  {"role":"manager",  "pw_hash": hashlib.sha256(b"manager123").hexdigest()},
    "fieldpia": {"role":"fieldpia", "pw_hash": hashlib.sha256(b"field123").hexdigest()},
    "viewer":   {"role":"viewer",   "pw_hash": hashlib.sha256(b"viewer123").hexdigest()},
}

# ══════════════════════════════════════════════════════════════════════════════
#  LOGO
# ══════════════════════════════════════════════════════════════════════════════
def _load_logo_b64():
    base = os.path.dirname(os.path.abspath(__file__))
    for p in [os.path.join(base, "logo.png"),
              os.path.join(base, "logo.jpg"),
              "/mnt/user-data/uploads/1780681889493_image.png"]:
        if os.path.exists(p):
            with open(p, "rb") as f:
                return base64.b64encode(f.read()).decode()
    return ""

LOGO_B64 = _load_logo_b64()

# ══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

def parse_date(s):
    try: return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except: return date.today()

def add_months(d, m):
    month = d.month - 1 + m
    year  = d.year + month // 12
    month = month % 12 + 1
    day   = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)

def normalize_interest(rate):
    try:
        r = float(rate)
        return r / 100.0 if r > 1 else r
    except: return None

def compute_emi_amount(amt, rate_dec, tenure):
    total = amt + (amt * rate_dec * (tenure / 12.0))
    return round(total / tenure, 2)

def compute_total_due(amt, rate_dec, tenure):
    return round(amt + (amt * rate_dec * (tenure / 12.0)), 2)

def fmt_inr(v):
    try: return f"₹{float(v):,.2f}"
    except: return "₹0.00"

def next_loan_number():
    year = datetime.now().year
    c = get_cur()
    c.execute("SELECT loan_number FROM LoanEntry WHERE loan_number LIKE ? ORDER BY loan_number DESC LIMIT 1",
              (f"LN-{year}-%",))
    row = c.fetchone()
    seq = 1
    if row:
        try: seq = int(str(row["loan_number"]).split("-")[-1]) + 1
        except: pass
    return f"LN-{year}-{seq:02d}"

def assess_reloan_risk(loan_number):
    c = get_cur()
    c.execute("SELECT id FROM LoanEntry WHERE loan_number=?", (loan_number,))
    row = c.fetchone()
    if not row: return None, "Loan number not found."
    lid = row["id"] if isinstance(row, dict) else row[0]
    c.execute("""SELECT COUNT(*) as total,
                        SUM(CASE WHEN status='Paid' THEN 1 ELSE 0 END) as paid,
                        SUM(CASE WHEN status='Overdue' OR (status IN ('Pending','Partial') AND due_date < ?) THEN 1 ELSE 0 END) as overdue
                 FROM EMI WHERE loan_id=?""", (date.today().isoformat(), lid))
    st = c.fetchone()
    total = (st["total"] or 0) if isinstance(st, dict) else (st[0] or 0)
    paid  = (st["paid"]  or 0) if isinstance(st, dict) else (st[1] or 0)
    delay = (st["overdue"] or 0) if isinstance(st, dict) else (st[2] or 0)
    c.execute("SELECT * FROM LoanEntry WHERE id=?", (lid,))
    old = c.fetchone()
    if delay == 0:   risk, dec = "GOOD",    "✅ Good payment history — safe to sanction."
    elif delay <= 3: risk, dec = "AVERAGE", "⚠️ Average history — proceed with caution."
    else:            risk, dec = "RISK",    f"❌ High risk — {delay} delayed payments. Review carefully."
    return {"risk":risk,"decision":dec,"delay_count":delay,"total":total,"paid":paid,
            "customer": dict(old) if old else {}}, None

# ══════════════════════════════════════════════════════════════════════════════
#  TURSO HTTP CLIENT
# ══════════════════════════════════════════════════════════════════════════════
def _tv(v):
    if v is None: return {"type":"null","value":None}
    if isinstance(v,bool): return {"type":"integer","value":str(int(v))}
    if isinstance(v,int):  return {"type":"integer","value":str(v)}
    if isinstance(v,float):return {"type":"float","value":v}
    return {"type":"text","value":str(v)}

def _fv(cell):
    if cell is None or cell.get("type")=="null": return None
    t,v = cell.get("type","text"), cell.get("value")
    if t=="integer":
        try: return int(v)
        except: return v
    if t=="float":
        try: return float(v)
        except: return v
    return v

class TRow(dict):
    def __getitem__(self,k):
        if isinstance(k,int): return list(self.values())[k]
        return super().__getitem__(k)

class TCur:
    def __init__(self,url,tok):
        self._u,self._t,self._rows,self._pos,self.lastrowid=url,tok,[],0,None
    def _exec(self,sql,p=()):
        stmt={"sql":sql.strip()}
        if p: stmt["args"]=[_tv(x) for x in p]
        r=http_req.post(f"{self._u}/v2/pipeline",
            headers={"Authorization":f"Bearer {self._t}","Content-Type":"application/json"},
            json={"requests":[{"type":"execute","stmt":stmt},{"type":"close"}]},timeout=15)
        r.raise_for_status()
        d=r.json();res=d["results"][0]
        if res.get("type")=="error": raise Exception(res["error"]["message"])
        result=res["response"]["result"]
        cols=[c["name"] for c in result.get("cols",[])]
        self._rows=[TRow(zip(cols,[_fv(cell) for cell in row])) for row in result.get("rows",[])]
        self._pos=0
        rid=result.get("last_insert_rowid")
        if rid is not None:
            try: self.lastrowid=int(rid)
            except: self.lastrowid=rid
    def execute(self,sql,p=()):
        self._exec(sql,p); return self
    def executescript(self,script):
        for s in script.split(";"):
            s=s.strip()
            if s: self._exec(s)
        return self
    def fetchone(self):
        if self._pos<len(self._rows): r=self._rows[self._pos];self._pos+=1;return r
        return None
    def fetchall(self):
        r=self._rows[self._pos:];self._pos=len(self._rows);return r
    def __iter__(self): return iter(self._rows)

class TConn:
    def __init__(self,url,tok):
        self._u=url.replace("libsql://","https://");self._t=tok;self.row_factory=None
    def cursor(self): return TCur(self._u,self._t)
    def commit(self): pass
    def close(self): pass

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def _make_conn():
    if TURSO_URL and TURSO_TOKEN: return TConn(TURSO_URL, TURSO_TOKEN)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_db():
    if "db" not in g: g.db = _make_conn()
    return g.db

def get_cur(): return get_db().cursor()

def init_db():
    conn = _make_conn(); cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS LoanEntry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_number TEXT UNIQUE,
        customer_name TEXT,
        customer_mobile TEXT,
        customer_address TEXT,
        customer_location TEXT,
        vehicle_type TEXT,
        vehicle_number TEXT,
        vehicle_model TEXT,
        engine_number TEXT,
        chassis_number TEXT,
        vehicle_colour TEXT,
        loan_amount REAL,
        interest_rate REAL,
        tenure INTEGER,
        start_date TEXT,
        status TEXT,
        created_at TEXT,
        attachment TEXT,
        customer_email TEXT,
        guarantor_name TEXT,
        guarantor_address TEXT,
        guarantor_mobile TEXT,
        is_reloan INTEGER DEFAULT 0,
        reloan_ref TEXT
    );
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER UNIQUE,
        name TEXT, vehicle_type TEXT,
        loan_amount REAL, emi_amount REAL, status TEXT, created_at TEXT,
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
        bill_number TEXT,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS RejectedLoans (
        reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER UNIQUE, reason TEXT, created_at TEXT,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS ClosedLoans (
        close_id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER UNIQUE, closure_date TEXT, created_at TEXT,
        FOREIGN KEY(loan_id) REFERENCES LoanEntry(id)
    );
    CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE, pw_hash TEXT, role TEXT, created_at TEXT
    )
    """)
    for m in [
        "ALTER TABLE LoanEntry ADD COLUMN customer_mobile TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN customer_address TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN customer_location TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN vehicle_number TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN vehicle_model TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN engine_number TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN chassis_number TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN vehicle_colour TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN guarantor_name TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN guarantor_address TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN guarantor_mobile TEXT",
        "ALTER TABLE LoanEntry ADD COLUMN is_reloan INTEGER DEFAULT 0",
        "ALTER TABLE LoanEntry ADD COLUMN reloan_ref TEXT",
        "ALTER TABLE EMI ADD COLUMN bill_number TEXT",
    ]:
        try: cur.execute(m)
        except: pass
    for u, info in DEFAULT_USERS.items():
        cur.execute("INSERT OR IGNORE INTO Users (username,pw_hash,role,created_at) VALUES (?,?,?,?)",
                    (u, info["pw_hash"], info["role"], datetime.now(timezone.utc).isoformat()))
    conn.commit(); conn.close()

# ══════════════════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC
# ══════════════════════════════════════════════════════════════════════════════
def authenticate_user(username, password):
    c = get_cur(); c.execute("SELECT * FROM Users WHERE username=?", (username,))
    row = c.fetchone()
    if row and row["pw_hash"] == hash_pw(password): return dict(row)
    return None

def can_pay_emi(loan_id, inst_no):
    if inst_no == 1: return True
    c = get_cur()
    c.execute("SELECT COUNT(*) as n FROM EMI WHERE loan_id=? AND installment_no<? AND status!='Paid'",
              (loan_id, inst_no))
    return c.fetchone()["n"] == 0

def create_loan(ln, cname, cmobile, caddr, cloc,
                vtype, vnum, vmodel, eng, chas, vcol,
                amt, rate_raw, tenure, sdate, cemail="",
                gname="", gaddr="", gmob="", is_reloan=0, reloan_ref=""):
    r = normalize_interest(rate_raw)
    if r is None: raise ValueError("Invalid interest rate")
    c = get_cur()
    c.execute("""INSERT INTO LoanEntry
                 (loan_number,customer_name,customer_mobile,customer_address,customer_location,
                  vehicle_type,vehicle_number,vehicle_model,engine_number,chassis_number,vehicle_colour,
                  loan_amount,interest_rate,tenure,start_date,status,created_at,
                  attachment,customer_email,guarantor_name,guarantor_address,guarantor_mobile,
                  is_reloan,reloan_ref)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (ln,cname,cmobile,caddr,cloc,
               vtype,vnum,vmodel,eng,chas,vcol,
               float(amt),float(r),int(tenure),sdate,"PendingApproval",
               datetime.now(timezone.utc).isoformat(),
               None,cemail,gname,gaddr,gmob,int(is_reloan),reloan_ref))
    get_db().commit(); return c.lastrowid

def approve_loan(loan_id):
    c = get_cur()
    c.execute("SELECT * FROM LoanEntry WHERE id=?", (loan_id,))
    loan = c.fetchone()
    if not loan: raise ValueError("Loan not found")
    if loan["status"] != "PendingApproval": raise ValueError("Loan is not pending approval")
    emi_amt = compute_emi_amount(float(loan["loan_amount"]), float(loan["interest_rate"]), int(loan["tenure"]))
    now = datetime.now(timezone.utc).isoformat()
    c.execute("INSERT OR REPLACE INTO Customers (loan_id,name,vehicle_type,loan_amount,emi_amount,status,created_at) VALUES (?,?,?,?,?,?,?)",
              (loan_id, loan["customer_name"], loan["vehicle_type"], loan["loan_amount"], emi_amt, "Active", now))
    try: sd = parse_date(loan["start_date"])
    except: sd = date.today()
    for i in range(1, int(loan["tenure"]) + 1):
        c.execute("INSERT INTO EMI (loan_id,installment_no,due_date,emi_amount,status,paid_at,amount_paid,remaining_amount,extra_interest,bill_number) VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (loan_id, i, add_months(sd, i-1).isoformat(), emi_amt, "Pending", None, 0.0, emi_amt, 0.0, None))
    c.execute("UPDATE LoanEntry SET status='Approved' WHERE id=?", (loan_id,))
    get_db().commit(); _notify_approval(dict(loan))

def reject_loan(loan_id, reason):
    c = get_cur()
    c.execute("UPDATE LoanEntry SET status='Rejected' WHERE id=?", (loan_id,))
    c.execute("INSERT OR REPLACE INTO RejectedLoans (loan_id,reason,created_at) VALUES (?,?,?)",
              (loan_id, reason, datetime.now(timezone.utc).isoformat()))
    get_db().commit()

def pay_emi(emi_id, pay_amount=None, extra_interest=0.0, bill_number=""):
    c = get_cur(); now = datetime.now(timezone.utc).isoformat()
    c.execute("SELECT * FROM EMI WHERE emi_id=?", (emi_id,))
    emi = c.fetchone()
    if not emi: raise ValueError("EMI not found")
    if emi["status"] == "Paid": raise ValueError("EMI already paid")
    if not can_pay_emi(emi["loan_id"], emi["installment_no"]):
        raise ValueError(f"Cannot pay installment {emi['installment_no']}. Complete previous first.")
    if not bill_number or not bill_number.strip():
        raise ValueError("Bill number is mandatory before payment.")
    amount_paid = emi["amount_paid"] or 0.0
    remaining   = emi["remaining_amount"] if emi["remaining_amount"] is not None else emi["emi_amount"]
    if pay_amount is None: pay_amount = remaining
    total_due   = remaining + (extra_interest or 0.0)
    if pay_amount < total_due:
        c.execute("UPDATE EMI SET amount_paid=?,remaining_amount=?,extra_interest=?,status=?,bill_number=? WHERE emi_id=?",
                  (amount_paid+pay_amount, total_due-pay_amount, extra_interest, "Partial", bill_number.strip(), emi_id))
        get_db().commit()
        return f"Partial payment recorded. Remaining: {fmt_inr(total_due-pay_amount)}"
    else:
        c.execute("UPDATE EMI SET status='Paid',paid_at=?,amount_paid=?,remaining_amount=0,extra_interest=0,bill_number=? WHERE emi_id=?",
                  (now, amount_paid+pay_amount, bill_number.strip(), emi_id))
        get_db().commit()
        lid = emi["loan_id"]
        c.execute("SELECT COUNT(*) as total, SUM(CASE WHEN status='Paid' THEN 1 ELSE 0 END) as pc FROM EMI WHERE loan_id=?", (lid,))
        ct = c.fetchone()
        if ct["total"] > 0 and ct["pc"] == ct["total"]:
            c.execute("UPDATE LoanEntry SET status='Closed' WHERE id=?", (lid,))
            c.execute("UPDATE Customers SET status='Closed' WHERE loan_id=?", (lid,))
            c.execute("INSERT OR REPLACE INTO ClosedLoans (loan_id,closure_date,created_at) VALUES (?,?,?)",
                      (lid, date.today().isoformat(), now))
            get_db().commit(); _notify_closure(lid)
        return "EMI paid successfully!"

# ── Query helpers ──────────────────────────────────────────────────────────────
def list_pending_loans(search=""):
    q=f"%{search}%"; c=get_cur()
    c.execute("SELECT * FROM LoanEntry WHERE status='PendingApproval' AND (loan_number LIKE ? OR customer_name LIKE ?) ORDER BY created_at DESC",(q,q))
    return [dict(r) for r in c.fetchall()]

def list_all_loans(search=""):
    q=f"%{search}%"; c=get_cur()
    c.execute("SELECT * FROM LoanEntry WHERE loan_number LIKE ? OR customer_name LIKE ? OR vehicle_type LIKE ? OR status LIKE ? ORDER BY created_at DESC",(q,q,q,q))
    return [dict(r) for r in c.fetchall()]

def list_customers(search=""):
    q=f"%{search}%"; c=get_cur()
    c.execute("SELECT * FROM Customers WHERE name LIKE ? OR vehicle_type LIKE ? OR status LIKE ? ORDER BY created_at DESC",(q,q,q))
    return [dict(r) for r in c.fetchall()]

def get_emis_for_loan(loan_id):
    c=get_cur(); c.execute("SELECT * FROM EMI WHERE loan_id=? ORDER BY installment_no ASC",(loan_id,))
    return [dict(r) for r in c.fetchall()]

def list_closed_loans(search=""):
    q=f"%{search}%"; c=get_cur()
    c.execute("""SELECT le.id as loan_id,le.loan_number,le.customer_name,le.vehicle_type,le.loan_amount,cl.closure_date
                 FROM LoanEntry le JOIN ClosedLoans cl ON cl.loan_id=le.id
                 WHERE le.loan_number LIKE ? OR le.customer_name LIKE ? ORDER BY cl.created_at DESC""",(q,q))
    return [dict(r) for r in c.fetchall()]

def list_rejected_loans(search=""):
    q=f"%{search}%"; c=get_cur()
    c.execute("""SELECT le.id as loan_id,le.loan_number,le.customer_name,rl.reason,rl.created_at
                 FROM LoanEntry le JOIN RejectedLoans rl ON rl.loan_id=le.id
                 WHERE le.loan_number LIKE ? OR le.customer_name LIKE ? ORDER BY rl.created_at DESC""",(q,q))
    return [dict(r) for r in c.fetchall()]

def get_overdue_emis():
    today=date.today().isoformat(); c=get_cur()
    c.execute("""SELECT e.*,le.loan_number,le.customer_name,le.id as lid
                 FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id
                 WHERE e.status='Overdue' OR (e.status IN ('Pending','Partial') AND e.due_date < ?)
                 ORDER BY le.loan_number ASC, e.due_date ASC""",(today,))
    return [dict(r) for r in c.fetchall()]

def get_upcoming_emis():
    today=date.today().isoformat()
    limit=(date.today()+timedelta(days=UPCOMING_DAYS)).isoformat(); c=get_cur()
    c.execute("""SELECT e.*,le.loan_number,le.customer_name,le.id as lid
                 FROM EMI e JOIN LoanEntry le ON e.loan_id=le.id
                 WHERE e.status IN ('Pending','Partial') AND e.due_date>=? AND e.due_date<=?
                 ORDER BY le.loan_number ASC, e.due_date ASC""",(today,limit))
    return [dict(r) for r in c.fetchall()]

def group_alerts_by_loan(emi_list):
    """Group EMI list by loan number → one row per loan with cumulative due amount."""
    grouped = {}
    for e in emi_list:
        ln = e["loan_number"]
        if ln not in grouped:
            grouped[ln] = {
                "loan_number": ln,
                "customer_name": e["customer_name"],
                "loan_id": e["loan_id"],
                "lid": e.get("lid", e["loan_id"]),
                "emi_count": 0,
                "total_due": 0.0,
                "oldest_due": e["due_date"],
                "emis": []
            }
        due = float(e.get("remaining_amount") or e["emi_amount"])
        grouped[ln]["total_due"] += due
        grouped[ln]["emi_count"] += 1
        grouped[ln]["emis"].append(e)
        if e["due_date"] < grouped[ln]["oldest_due"]:
            grouped[ln]["oldest_due"] = e["due_date"]
    return list(grouped.values())

def get_loan_summary_counts():
    c=get_cur()
    def cnt(sql): c.execute(sql); return c.fetchone()[0] or 0
    return dict(
        total   =cnt("SELECT COUNT(*) FROM LoanEntry"),
        pending =cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='PendingApproval'"),
        approved=cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='Approved'"),
        rejected=cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='Rejected'"),
        closed  =cnt("SELECT COUNT(*) FROM LoanEntry WHERE status='Closed'"),
        overdue =len(get_overdue_emis()),
        upcoming=len(get_upcoming_emis()),
    )

def get_kpi_totals():
    c=get_cur()
    c.execute("SELECT COUNT(*),SUM(loan_amount) FROM LoanEntry")
    row=c.fetchone(); tl=row[0] or 0; tla=row[1] or 0.0
    c.execute("SELECT SUM(emi_amount) FROM EMI WHERE status='Paid'")
    tr=c.fetchone()[0] or 0.0
    c.execute("SELECT SUM(remaining_amount) FROM EMI WHERE status IN ('Pending','Overdue','Partial')")
    tp=c.fetchone()[0] or 0.0
    return tl,tla,tr,tp

def get_monthly_paid_series():
    c=get_cur()
    c.execute("SELECT strftime('%Y-%m',paid_at) as ym,SUM(emi_amount) as amt FROM EMI WHERE status='Paid' AND paid_at IS NOT NULL GROUP BY ym ORDER BY ym ASC")
    rows=c.fetchall()
    return [r["ym"] for r in rows],[float(r["amt"] or 0) for r in rows]

def get_loan_status_breakdown():
    c=get_cur()
    c.execute("SELECT status,COUNT(*) as cnt FROM LoanEntry GROUP BY status")
    return [dict(r) for r in c.fetchall()]

def get_loan_type_breakdown():
    c=get_cur()
    c.execute("SELECT vehicle_type,COUNT(*) as cnt FROM LoanEntry GROUP BY vehicle_type")
    return [dict(r) for r in c.fetchall()]

# ── Email ──────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
#  SMS via Fast2SMS (India)
# ══════════════════════════════════════════════════════════════════════════════
def _send_sms(mobile, message):
    """Send SMS via Fast2SMS DLT route. Returns (success:bool, info:str)."""
    if not SMS_CONFIG.get("enabled"):
        return False, "SMS not configured (FAST2SMS_KEY not set)"
    if not mobile or len(str(mobile).strip()) != 10:
        return False, f"Invalid mobile: {mobile}"
    try:
        resp = http_req.post(
            "https://www.fast2sms.com/dev/bulkV2",
            headers={"authorization": SMS_CONFIG["api_key"]},
            data={
                "route":   "q",           # transactional route (DLT registered)
                "message": message[:160], # max 160 chars per SMS
                "language":"english",
                "flash":   0,
                "numbers": str(mobile).strip(),
            },
            timeout=10
        )
        result = resp.json()
        if result.get("return"):
            return True, f"SMS sent to {mobile}"
        else:
            return False, f"Fast2SMS error: {result.get('message','Unknown')}"
    except Exception as e:
        return False, f"SMS exception: {e}"

def _send_sms_bulk(mobiles, message):
    """Send same SMS to multiple 10-digit numbers."""
    if not SMS_CONFIG.get("enabled"):
        return False, "SMS not configured"
    nums = [str(m).strip() for m in mobiles if m and len(str(m).strip())==10]
    if not nums:
        return False, "No valid numbers"
    try:
        resp = http_req.post(
            "https://www.fast2sms.com/dev/bulkV2",
            headers={"authorization": SMS_CONFIG["api_key"]},
            data={
                "route":   "q",
                "message": message[:160],
                "language":"english",
                "flash":   0,
                "numbers": ",".join(nums),
            },
            timeout=10
        )
        result = resp.json()
        if result.get("return"):
            return True, f"SMS sent to {len(nums)} number(s)"
        else:
            return False, f"Fast2SMS error: {result.get('message','Unknown')}"
    except Exception as e:
        return False, f"SMS exception: {e}"

# ══════════════════════════════════════════════════════════════════════════════
#  EMAIL
# ══════════════════════════════════════════════════════════════════════════════
def _send_email(to,subject,body):
    if not EMAIL_CONFIG.get("enabled") or not to: return
    try:
        msg=MIMEMultipart(); msg["From"]=EMAIL_CONFIG["sender"]; msg["To"]=to; msg["Subject"]=subject
        msg.attach(MIMEText(body,"html"))
        with smtplib.SMTP(EMAIL_CONFIG["smtp_host"],EMAIL_CONFIG["smtp_port"]) as s:
            s.starttls(); s.login(EMAIL_CONFIG["sender"],EMAIL_CONFIG["password"])
            s.sendmail(EMAIL_CONFIG["sender"],to,msg.as_string())
    except Exception as e: print(f"[Email] {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  NOTIFICATION TRIGGERS
# ══════════════════════════════════════════════════════════════════════════════
def _notify_approval(loan):
    name   = loan.get("customer_name","Customer")
    ln     = loan.get("loan_number","")
    amt    = fmt_inr(loan.get("loan_amount",0))
    mobile = loan.get("customer_mobile","")
    msg = (f"Dear {name}, Your loan {ln} of {amt} has been APPROVED by "
           f"Thendralla Fincorp. Thank you for choosing us.")
    _send_sms(mobile, msg)
    em = loan.get("customer_email","")
    if em:
        _send_email(em,"Your Vehicle Loan Has Been Approved",
            f"<h2>Loan Approved</h2><p>Dear {name},</p>"
            f"<p>Loan <b>{ln}</b> of {amt} has been approved.</p>")

def _notify_closure(loan_id):
    c=get_cur(); c.execute("SELECT * FROM LoanEntry WHERE id=?",(loan_id,))
    loan=c.fetchone()
    if not loan: return
    name   = loan["customer_name"]
    ln     = loan["loan_number"]
    mobile = loan.get("customer_mobile","")
    msg = (f"Dear {name}, Congratulations! All EMIs for loan {ln} are PAID. "
           f"Loan is now CLOSED. Thank you - Thendralla Fincorp.")
    _send_sms(mobile, msg)
    if loan.get("customer_email"):
        _send_email(loan["customer_email"],"Loan Fully Repaid — Congratulations!",
            f"<h2>Loan Closed</h2><p>Dear {name},</p>"
            f"<p>All EMIs for loan <b>{ln}</b> paid. Loan is now closed!</p>")

def _notify_emi_due(loan_number, customer_name, mobile, due_date, amount, days_left):
    """Upcoming EMI reminder SMS."""
    if days_left <= 0:
        msg = (f"Dear {customer_name}, EMI of {fmt_inr(amount)} for loan {loan_number} "
               f"was DUE on {due_date}. Please pay immediately. -Thendralla Fincorp")
    else:
        msg = (f"Dear {customer_name}, Reminder: EMI of {fmt_inr(amount)} for loan "
               f"{loan_number} is DUE on {due_date} ({days_left} days). -Thendralla Fincorp")
    return _send_sms(mobile, msg)

def send_bulk_overdue_sms():
    """Send SMS to all overdue loan customers. Called from Alerts page."""
    overdue = get_overdue_emis()
    # Group by loan number to avoid duplicate SMS
    seen = set(); results = []; total_sent = 0
    for e in overdue:
        ln = e["loan_number"]
        if ln in seen: continue
        seen.add(ln)
        c = get_cur()
        c.execute("SELECT customer_name,customer_mobile,loan_number FROM LoanEntry WHERE loan_number=?", (ln,))
        row = c.fetchone()
        if not row: continue
        name   = row["customer_name"]
        mobile = row.get("customer_mobile","")
        due_d  = parse_date(e["due_date"])
        days   = (date.today() - due_d).days
        amt    = sum(float(x.get("remaining_amount") or x["emi_amount"])
                     for x in overdue if x["loan_number"]==ln)
        msg = (f"Dear {name}, URGENT: Total overdue EMI of {fmt_inr(amt)} for loan "
               f"{ln} is pending {days} day(s). Pay now to avoid penalty. -Thendralla Fincorp")
        ok, info = _send_sms(mobile, msg)
        results.append({"loan":ln,"name":name,"mobile":mobile,"ok":ok,"info":info})
        if ok: total_sent += 1
    return results, total_sent

def send_bulk_upcoming_sms():
    """Send SMS to customers with EMIs due in 3 days."""
    upcoming = get_upcoming_emis()
    seen = set(); results = []; total_sent = 0
    today = date.today()
    for e in upcoming:
        ln = e["loan_number"]
        if ln in seen: continue
        due_d = parse_date(e["due_date"])
        days_left = (due_d - today).days
        if days_left > 3: continue   # only send if ≤3 days away
        seen.add(ln)
        c = get_cur()
        c.execute("SELECT customer_name,customer_mobile FROM LoanEntry WHERE loan_number=?", (ln,))
        row = c.fetchone()
        if not row: continue
        name   = row["customer_name"]
        mobile = row.get("customer_mobile","")
        amt    = sum(float(x.get("remaining_amount") or x["emi_amount"])
                     for x in upcoming if x["loan_number"]==ln)
        msg = (f"Dear {name}, Reminder: EMI of {fmt_inr(amt)} for loan {ln} is due "
               f"on {e['due_date']} ({days_left} day(s)). -Thendralla Fincorp")
        ok, info = _send_sms(mobile, msg)
        results.append({"loan":ln,"name":name,"mobile":mobile,"ok":ok,"info":info})
        if ok: total_sent += 1
    return results, total_sent

# ══════════════════════════════════════════════════════════════════════════════
#  PDF
# ══════════════════════════════════════════════════════════════════════════════
def generate_pdf(path):
    if not REPORTLAB_AVAILABLE: raise RuntimeError("reportlab not installed.")
    styles=getSampleStyleSheet()
    ts=ParagraphStyle("T",parent=styles["Title"],fontSize=18,spaceAfter=12)
    h2=ParagraphStyle("H2",parent=styles["Heading2"],fontSize=13,spaceAfter=6)
    story=[Paragraph("Thendralla Fincorp — Loan Report",ts),
           Paragraph(f"Generated: {datetime.now().strftime('%d %b %Y %H:%M')}",styles["Normal"]),
           Spacer(1,0.5*cm)]
    tl,tla,tr,tp=get_kpi_totals(); counts=get_loan_summary_counts()
    kd=[["Metric","Value"],["Total Loans",str(tl)],["Disbursed",f"Rs {tla:,.2f}"],
        ["Collected",f"Rs {tr:,.2f}"],["Outstanding",f"Rs {tp:,.2f}"],
        ["Pending",str(counts["pending"])],["Active",str(counts["approved"])],
        ["Closed",str(counts["closed"])],["Rejected",str(counts["rejected"])],
        ["Overdue EMIs",str(counts["overdue"])]]
    kt=Table(kd,colWidths=[8*cm,7*cm])
    kt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#1a4fad")),
        ("TEXTCOLOR",(0,0),(-1,0),colors.white),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
        ("GRID",(0,0),(-1,-1),0.5,colors.grey)]))
    story.append(kt); story.append(PageBreak())
    story.append(Paragraph("All Loans",h2))
    loans=list_all_loans()
    ld=[["Loan #","Customer","Mobile","Amount","Rate","Tenure","Status"]]
    for l in loans:
        ld.append([l["loan_number"],l["customer_name"],l.get("customer_mobile",""),
                   f"Rs {l['loan_amount']:,.0f}",f"{l['interest_rate']*100:.1f}%",f"{l['tenure']}m",l["status"]])
    if len(ld)>1:
        lt=Table(ld,repeatRows=1,colWidths=[2.8*cm,3.5*cm,2.8*cm,2.8*cm,1.8*cm,1.8*cm,2.8*cm])
        lt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#1a4fad")),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#e8f0fb")]),
            ("GRID",(0,0),(-1,-1),0.4,colors.grey)]))
        story.append(lt)
    doc=SimpleDocTemplate(path,pagesize=A4,leftMargin=1.5*cm,rightMargin=1.5*cm,topMargin=2*cm,bottomMargin=2*cm)
    doc.build(story)

# ══════════════════════════════════════════════════════════════════════════════
#  CSS + LAYOUT  (Vertical Sidebar, Mobile-first)
# ══════════════════════════════════════════════════════════════════════════════
BASE_CSS = """
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f0f4fb;--surface:#fff;--surface2:#f1f5fb;
  --border:#c8d4e8;--accent:#1a4fad;--accent2:#1d6fdb;
  --green:#059669;--red:#dc2626;--amber:#d97706;
  --text:#1e293b;--muted:#475569;
  --sidebar-w:220px;
}
html,body{height:100%;font-family:system-ui,'Segoe UI',sans-serif;font-size:14px;
          background:var(--bg);color:var(--text);}

/* ── LAYOUT ── */
.layout{display:flex;min-height:100vh;}

/* ── SIDEBAR ── */
.sidebar{
  width:var(--sidebar-w);min-width:var(--sidebar-w);
  background:var(--accent);color:#fff;
  display:flex;flex-direction:column;
  position:fixed;top:0;left:0;height:100vh;
  z-index:200;transition:transform .25s ease;
  overflow-y:auto;
}
.sidebar .brand{
  display:flex;align-items:center;gap:10px;
  padding:16px 14px 12px;border-bottom:1px solid rgba(255,255,255,.15);
}
.sidebar .brand img{height:36px;border-radius:4px;background:#fff;padding:2px;flex-shrink:0;}
.sidebar .brand-text{font-size:13px;font-weight:700;line-height:1.3;letter-spacing:.3px;}
.sidebar nav{padding:10px 0;flex:1;}
.sidebar nav a{
  display:flex;align-items:center;gap:10px;
  padding:11px 18px;color:rgba(255,255,255,.82);
  text-decoration:none;font-size:13.5px;transition:.15s;
  border-left:3px solid transparent;
}
.sidebar nav a:hover,.sidebar nav a.active{
  background:rgba(255,255,255,.13);color:#fff;
  border-left-color:rgba(255,255,255,.8);
}
.sidebar nav a .icon{font-size:16px;min-width:20px;text-align:center;}
.sidebar .sidebar-footer{
  padding:12px 14px;border-top:1px solid rgba(255,255,255,.15);
  font-size:12px;color:rgba(255,255,255,.7);
}
.sidebar .sidebar-footer a{color:rgba(255,255,255,.8);text-decoration:none;}
.sidebar .sidebar-footer a:hover{color:#fff;}

/* ── TOPBAR (mobile hamburger) ── */
.topbar{
  display:none;background:var(--accent);color:#fff;
  padding:0 16px;height:52px;align-items:center;gap:12px;
  position:fixed;top:0;left:0;right:0;z-index:100;
  box-shadow:0 2px 6px rgba(0,0,0,.2);
}
.topbar .brand-text{font-size:15px;font-weight:700;flex:1;}
.topbar img{height:32px;border-radius:3px;background:#fff;padding:2px;}
#hamburger{background:none;border:none;color:#fff;font-size:22px;cursor:pointer;padding:4px;}
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:150;}

/* ── MAIN CONTENT ── */
.main-wrap{
  margin-left:var(--sidebar-w);
  min-height:100vh;padding:24px 20px;
  flex:1;max-width:calc(100% - var(--sidebar-w));
}
h1{font-size:22px;margin-bottom:16px;color:var(--accent);}
h2{font-size:17px;margin-bottom:12px;}

/* ── CARDS ── */
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;
      padding:20px;margin-bottom:20px;box-shadow:0 1px 4px rgba(0,0,0,.06);}
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;}
.kpi{background:var(--surface);border-radius:10px;padding:14px;
     border-left:4px solid var(--accent);box-shadow:0 1px 4px rgba(0,0,0,.06);}
.kpi .val{font-size:24px;font-weight:700;color:var(--accent);}
.kpi .lbl{font-size:11px;color:var(--muted);margin-top:3px;}

/* ── FORMS ── */
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
.form-grid.three{grid-template-columns:1fr 1fr 1fr;}
.form-group{display:flex;flex-direction:column;gap:5px;}
.form-group.full{grid-column:1/-1;}
label{font-size:12px;font-weight:700;color:var(--muted);text-transform:uppercase;
      letter-spacing:.3px;white-space:normal;line-height:1.3;}
input,select,textarea{
  padding:11px 12px;border:1px solid var(--border);border-radius:8px;
  font-size:15px;background:#fff;color:var(--text);width:100%;
  transition:border-color .15s;box-sizing:border-box;
}
input:focus,select:focus,textarea:focus{outline:none;border-color:var(--accent2);
  box-shadow:0 0 0 3px rgba(29,111,219,.12);}
input[readonly]{background:var(--surface2);color:var(--muted);}
.section-title{font-size:12px;font-weight:700;color:var(--accent);text-transform:uppercase;
               letter-spacing:.5px;padding:10px 0 5px;border-bottom:2px solid var(--accent);
               margin-bottom:12px;grid-column:1/-1;margin-top:10px;}

/* ── BUTTONS ── */
.btn{display:inline-flex;align-items:center;gap:5px;padding:9px 18px;border-radius:7px;
     font-size:13px;font-weight:600;cursor:pointer;border:none;transition:.15s;text-decoration:none;}
.btn-primary{background:var(--accent);color:#fff;}
.btn-primary:hover{background:var(--accent2);}
.btn-success{background:var(--green);color:#fff;}
.btn-danger{background:var(--red);color:#fff;}
.btn-amber{background:var(--amber);color:#fff;}
.btn-sm{padding:5px 11px;font-size:12px;}
.btn:disabled{opacity:.5;cursor:not-allowed;}

/* ── TABLES ── */
.table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;}
table{width:100%;border-collapse:collapse;font-size:13px;}
th{background:var(--accent);color:#fff;padding:10px 8px;text-align:left;white-space:nowrap;}
td{padding:9px 8px;border-bottom:1px solid var(--border);vertical-align:middle;}
tr:nth-child(even) td{background:var(--surface2);}
tr:hover td{background:#e8f0fb;}

/* EMI row highlight */
tr.row-overdue td{background:#fee2e2 !important;border-left:3px solid var(--red);}
tr.row-overdue:hover td{background:#fecaca !important;}
tr.row-upcoming td{background:#fef9c3 !important;border-left:3px solid var(--amber);}
tr.row-upcoming:hover td{background:#fef08a !important;}
tr.row-paid td{opacity:.65;}

/* ── BADGES ── */
.badge{display:inline-block;padding:3px 9px;border-radius:12px;font-size:11px;font-weight:700;}
.badge-pending{background:#fef3c7;color:#92400e;}
.badge-approved,.badge-paid,.badge-good{background:#d1fae5;color:#065f46;}
.badge-rejected,.badge-overdue,.badge-risk{background:#fee2e2;color:#991b1b;}
.badge-closed{background:#e0e7ff;color:#3730a3;}
.badge-partial,.badge-average{background:#ffedd5;color:#9a3412;}
.badge-admin{background:var(--accent);color:#fff;}
.badge-manager{background:#059669;color:#fff;}
.badge-fieldpia{background:#d97706;color:#fff;}
.badge-viewer{background:#6b7280;color:#fff;}

/* ── ALERTS ── */
.alert{padding:10px 14px;border-radius:6px;margin-bottom:12px;font-size:13px;}
.alert-success{background:#d1fae5;color:#065f46;border:1px solid #a7f3d0;}
.alert-danger{background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;}
.alert-info{background:#dbeafe;color:#1e40af;border:1px solid #93c5fd;}
.alert-warning{background:#fef3c7;color:#92400e;border:1px solid #fde68a;}

/* ── DUE PREVIEW ── */
.due-preview{background:linear-gradient(135deg,#1a4fad,#1d6fdb);color:#fff;
             border-radius:10px;padding:16px 20px;margin:14px 0;display:none;}
.due-preview h3{font-size:14px;margin-bottom:10px;opacity:.9;}
.due-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;}
.due-item .lbl{font-size:10px;opacity:.7;text-transform:uppercase;}
.due-item .val{font-size:18px;font-weight:700;}

/* ── RISK BOX ── */
.risk-box{border-radius:8px;padding:12px;margin-top:10px;display:none;}
.risk-good{background:#d1fae5;border:1px solid #6ee7b7;}
.risk-average{background:#fef3c7;border:1px solid #fde68a;}
.risk-risk{background:#fee2e2;border:1px solid #fca5a5;}

/* ── CALCULATOR ── */
.calc-result{background:linear-gradient(135deg,#1a4fad,#0ea5e9);color:#fff;
             border-radius:10px;padding:18px 24px;margin:14px 0;text-align:center;}
.calc-result .big-val{font-size:36px;font-weight:800;}
.calc-result .lbl{font-size:13px;opacity:.8;margin-bottom:6px;}
.calc-summary{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-top:12px;}
.calc-summary .item{background:rgba(255,255,255,.15);border-radius:7px;padding:9px;}
.calc-summary .item .val{font-size:16px;font-weight:700;}
.calc-summary .item .lbl{font-size:11px;opacity:.8;}

/* ── CHARTS ── */
.chart-grid{display:grid;grid-template-columns:2fr 1fr;gap:14px;margin-top:14px;}
.chart-box{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px;max-height:260px;overflow:hidden;
           box-shadow:0 1px 4px rgba(0,0,0,.06);}
.chart-box h3{font-size:14px;color:var(--muted);margin-bottom:12px;text-transform:uppercase;letter-spacing:.4px;}

/* ── GPS ── */
.gps-row{display:flex;gap:6px;align-items:center;}
.gps-row input{flex:1;}

/* ── MOBILE ── */
@media(max-width:768px){
  /* Sidebar */
  .sidebar{transform:translateX(-100%);}
  .sidebar.open{transform:translateX(0);}
  .sidebar-overlay.open{display:block;}

  /* Topbar */
  .topbar{display:flex;}

  /* Main wrap — full width, below topbar */
  .main-wrap{margin-left:0 !important;padding:62px 10px 20px !important;
             max-width:100% !important;width:100% !important;}

  /* Page title */
  h1{font-size:18px;margin-bottom:12px;}
  h2{font-size:15px;}

  /* Cards */
  .card{padding:14px;border-radius:8px;}

  /* Forms — single column on mobile */
  .form-grid,
  .form-grid.three{grid-template-columns:1fr !important;}
  .form-group.full{grid-column:1 !important;}

  /* Inputs bigger touch targets */
  input,select,textarea{
    font-size:16px !important;   /* prevents iOS zoom */
    padding:12px 12px !important;
    min-height:46px;
  }
  label{font-size:11px;margin-bottom:2px;}
  .section-title{font-size:11px;}

  /* Buttons */
  .btn{padding:10px 16px;font-size:13px;}
  .btn-sm{padding:8px 12px;font-size:12px;}

  /* KPIs */
  .kpi-grid{grid-template-columns:repeat(2,1fr);gap:8px;}
  .kpi{padding:10px 12px;}
  .kpi .val{font-size:20px;}

  /* Due preview */
  .due-grid{grid-template-columns:1fr 1fr !important;}
  .due-item .val{font-size:14px;}

  /* Charts */
  .calc-summary,.chart-grid{grid-template-columns:1fr;}
  .chart-box{max-height:180px !important;padding:10px;}
  .chart-box h3{font-size:11px;margin-bottom:6px;}
  canvas{max-height:130px !important;}

  /* Tables — horizontal scroll */
  .table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:6px;}
  table{font-size:12px;min-width:480px;}
  th,td{padding:7px 6px;}

  /* Alert messages */
  .alert{font-size:12px;padding:8px 10px;}
}
</style>
"""

def _nav_links(role, active):
    can_approve = ROLES.get(role,{}).get("can_approve", False)
    can_add     = ROLES.get(role,{}).get("can_add",     False)
    can_report  = ROLES.get(role,{}).get("can_report",  False)

    def lnk(href, icon, label, key):
        cls = "active" if active == key else ""
        return f'<a href="{href}" class="{cls}"><span class="icon">{icon}</span>{label}</a>'

    links = lnk("/dashboard","🏠","Dashboard","dashboard")
    links += lnk("/loans","📋","Loans","loans")
    if can_add:     links += lnk("/loan/add","➕","New Loan","add")
    if can_approve: links += lnk("/approval","✅","Approval","approval")
    links += lnk("/customers","👥","Customers","customers")
    links += lnk("/alerts","🔔","Alerts","alerts")
    links += lnk("/closed","🔒","Closed","closed")
    links += lnk("/rejected","❌","Rejected","rejected")
    links += lnk("/calculator","🧮","Calculator","calculator")
    if can_report:  links += lnk("/report","📊","Report","report")
    if role=="admin": links += lnk("/users","⚙️","Users","users")
    if role=="admin": links += lnk("/sms_settings","📱","SMS Setup","sms")
    return links

def page(title, content, active=""):
    username = session.get("username","")
    role     = session.get("role","")
    logo_img = f'<img src="data:image/jpeg;base64,{LOGO_B64}" alt="TFC">' if LOGO_B64 else "🏦"
    flash_html = ""
    for cat, msg in get_flashed_messages(with_categories=True):
        cat_map = {"success":"success","danger":"danger","info":"info","warning":"warning"}
        flash_html += f'<div class="alert alert-{cat_map.get(cat,"info")}">{msg}</div>'

    sidebar_nav = _nav_links(role, active)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>TFC — {title}</title>
{BASE_CSS}
</head>
<body>
<!-- Mobile topbar -->
<div class="topbar">
  <button id="hamburger" onclick="toggleSidebar()">☰</button>
  {logo_img}
  <span class="brand-text">Thendralla Fincorp</span>
</div>
<!-- Sidebar overlay (mobile) -->
<div class="sidebar-overlay" id="overlay" onclick="toggleSidebar()"></div>
<!-- Sidebar -->
<div class="sidebar" id="sidebar">
  <div class="brand">
    {logo_img}
    <div class="brand-text">Thendralla<br>Fincorp</div>
  </div>
  <nav>{sidebar_nav}</nav>
  <div class="sidebar-footer">
    👤 <b>{username}</b> <span style="opacity:.6">({role})</span><br>
    <a href="/logout" style="color:#ff9999;">🔓 Logout</a>
  </div>
</div>
<!-- Main -->
<div class="layout">
  <div class="main-wrap">
    {flash_html}
    {content}
  </div>
</div>
<script>
function toggleSidebar(){{
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('overlay').classList.toggle('open');
}}
// Close sidebar on nav link click (mobile)
document.querySelectorAll('.sidebar nav a').forEach(a=>{{
  a.addEventListener('click',()=>{{
    if(window.innerWidth<=768){{
      document.getElementById('sidebar').classList.remove('open');
      document.getElementById('overlay').classList.remove('open');
    }}
  }});
}});
</script>
</body>
</html>"""

# ══════════════════════════════════════════════════════════════════════════════
#  FLASK APP
# ══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db: db.close()

def login_required(f):
    @wraps(f)
    def dec(*a,**kw):
        if "username" not in session: return redirect(url_for("login"))
        return f(*a,**kw)
    return dec

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def dec(*a,**kw):
            if session.get("role") not in roles:
                flash("Access denied for your role.","danger")
                return redirect(url_for("dashboard"))
            return f(*a,**kw)
        return dec
    return decorator

# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/")
def index(): return redirect(url_for("dashboard"))

# ── Login ──────────────────────────────────────────────────────────────────────
@app.route("/login", methods=["GET","POST"])
def login():
    if "username" in session: return redirect(url_for("dashboard"))
    err = ""
    if request.method == "POST":
        user = authenticate_user(request.form["username"], request.form["password"])
        if user:
            session["username"] = user["username"]; session["role"] = user["role"]
            flash(f"Welcome, {user['username']}!","success")
            return redirect(url_for("dashboard"))
        err = "Invalid credentials."
    logo_html = f"<img src='data:image/jpeg;base64,{LOGO_B64}' style='height:80px;margin-bottom:10px;'><br>" if LOGO_B64 else ""
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>TFC Login</title>{BASE_CSS}</head>
<body style="background:linear-gradient(135deg,#1a4fad,#0ea5e9);display:flex;
             align-items:center;justify-content:center;min-height:100vh;">
<div style="background:#fff;border-radius:16px;padding:36px 32px;width:340px;max-width:95vw;
            box-shadow:0 8px 32px rgba(0,0,0,.18);">
  <div style="text-align:center;margin-bottom:22px;">
    {logo_html}
    <h2 style="color:#1a4fad;font-size:21px;">Thendralla Fincorp</h2>
    <p style="color:#64748b;font-size:12px;">Vehicle Loan Management</p>
  </div>
  {"<div class='alert alert-danger'>"+err+"</div>" if err else ""}
  <form method="POST">
    <div class="form-group" style="margin-bottom:12px;">
      <label>Username</label><input name="username" required autofocus autocomplete="username">
    </div>
    <div class="form-group" style="margin-bottom:18px;">
      <label>Password</label><input type="password" name="password" required autocomplete="current-password">
    </div>
    <button class="btn btn-primary" style="width:100%;padding:11px;font-size:15px;">Login</button>
  </form>
</div>
</body></html>"""

@app.route("/logout")
def logout(): session.clear(); return redirect(url_for("login"))

# ── Dashboard ──────────────────────────────────────────────────────────────────
@app.route("/dashboard")
@login_required
def dashboard():
    counts = get_loan_summary_counts()
    tl,tla,tr,tp = get_kpi_totals()
    overdue  = get_overdue_emis()
    upcoming = get_upcoming_emis()
    months, amounts = get_monthly_paid_series()
    status_bd = get_loan_status_breakdown()
    type_bd   = get_loan_type_breakdown()

    # Chart.js data
    import json
    bar_labels  = json.dumps(months)
    bar_data    = json.dumps(amounts)
    stat_labels = json.dumps([r["status"] for r in status_bd])
    stat_data   = json.dumps([r["cnt"]    for r in status_bd])
    type_labels = json.dumps([r["vehicle_type"] for r in type_bd])
    type_data   = json.dumps([r["cnt"]          for r in type_bd])

    kpi = f"""
    <div class="kpi-grid">
      <div class="kpi"><div class="val">{counts['total']}</div><div class="lbl">Total Loans</div></div>
      <div class="kpi" style="border-color:#d97706"><div class="val" style="color:#d97706">{counts['pending']}</div><div class="lbl">Pending Approval</div></div>
      <div class="kpi" style="border-color:#059669"><div class="val" style="color:#059669">{counts['approved']}</div><div class="lbl">Active Loans</div></div>
      <div class="kpi" style="border-color:#dc2626"><div class="val" style="color:#dc2626">{counts['overdue']}</div><div class="lbl">Overdue EMIs</div></div>
      <div class="kpi" style="border-color:#0ea5e9"><div class="val" style="color:#0ea5e9">{counts['upcoming']}</div><div class="lbl">Due in 10 Days</div></div>
      <div class="kpi" style="border-color:#6366f1"><div class="val" style="color:#6366f1">{counts['closed']}</div><div class="lbl">Closed Loans</div></div>
    </div>
    <div class="form-grid" style="margin-top:16px;">
      <div class="card"><b>💰 Total Disbursed</b><br><span style="font-size:21px;color:var(--accent);font-weight:700;">₹{tla:,.2f}</span></div>
      <div class="card"><b>✅ Total Collected</b><br><span style="font-size:21px;color:var(--green);font-weight:700;">₹{tr:,.2f}</span></div>
      <div class="card"><b>⏳ Outstanding</b><br><span style="font-size:21px;color:var(--red);font-weight:700;">₹{tp:,.2f}</span></div>
      <div class="card"><b>📋 Total Loans</b><br><span style="font-size:21px;color:var(--muted);font-weight:700;">{tl}</span></div>
    </div>
    """

    charts = f"""
    <div class="chart-grid">
      <div class="chart-box">
        <h3>📈 Monthly Collections</h3>
        <canvas id="barChart" height="110" style="max-height:110px"></canvas>
      </div>
      <div class="chart-box">
        <h3>🍩 Loan Status</h3>
        <canvas id="donutChart" height="110" style="max-height:110px"></canvas>
      </div>
    </div>
    <div class="chart-grid" style="margin-top:0;">
      <div class="chart-box">
        <h3>🚗 Loans by Vehicle Type</h3>
        <canvas id="typeChart" height="110" style="max-height:110px"></canvas>
      </div>
      <div class="chart-box" style="display:flex;flex-direction:column;justify-content:center;">
        <h3>📊 Quick Stats</h3>
        <div style="display:flex;flex-direction:column;gap:8px;font-size:13px;">
          <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);">
            <span>Collection Rate</span>
            <b style="color:var(--green)">{"N/A" if tla==0 else f"{tr/tla*100:.1f}%"}</b>
          </div>
          <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);">
            <span>Overdue Loans</span>
            <b style="color:var(--red)">{len(set(e['loan_number'] for e in overdue))}</b>
          </div>
          <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);">
            <span>Total Overdue Amount</span>
            <b style="color:var(--red)">₹{sum(float(e.get('remaining_amount') or e['emi_amount']) for e in overdue):,.2f}</b>
          </div>
          <div style="display:flex;justify-content:space-between;padding:6px 0;">
            <span>Upcoming Due (10d)</span>
            <b style="color:var(--amber)">₹{sum(float(e.get('remaining_amount') or e['emi_amount']) for e in upcoming):,.2f}</b>
          </div>
        </div>
      </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script>
    const COLORS=['#1a4fad','#059669','#d97706','#dc2626','#6366f1','#0ea5e9','#7c3aed','#db2777'];

    // Monthly bar chart
    const barCtx=document.getElementById('barChart').getContext('2d');
    new Chart(barCtx,{{
      type:'bar',
      data:{{
        labels:{bar_labels},
        datasets:[{{
          label:'Collections (₹)',
          data:{bar_data},
          backgroundColor:'rgba(26,79,173,0.7)',
          borderColor:'#1a4fad',
          borderWidth:1,
          borderRadius:4
        }}]
      }},
      options:{{
        maintainAspectRatio:false,
        responsive:true,plugins:{{legend:{{display:false}},
          tooltip:{{callbacks:{{label:c=>'₹'+c.parsed.y.toLocaleString('en-IN')}}}}
        }},
        scales:{{y:{{ticks:{{callback:v=>'₹'+v.toLocaleString('en-IN')}},grid:{{color:'#eef2f9'}}}}}}
      }}
    }});

    // Status donut
    const dCtx=document.getElementById('donutChart').getContext('2d');
    new Chart(dCtx,{{
      type:'doughnut',
      data:{{
        labels:{stat_labels},
        datasets:[{{data:{stat_data},backgroundColor:COLORS,borderWidth:2,borderColor:'#fff'}}]
      }},
      options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{font:{{size:10}},boxWidth:12}}}}}}}}
    }});

    // Vehicle type bar
    const tCtx=document.getElementById('typeChart').getContext('2d');
    new Chart(tCtx,{{
      type:'bar',
      data:{{
        labels:{type_labels},
        datasets:[{{
          label:'Loans',
          data:{type_data},
          backgroundColor:COLORS,
          borderRadius:4
        }}]
      }},
      options:{{
        maintainAspectRatio:false,
        responsive:true,
        plugins:{{legend:{{display:false}}}},
        scales:{{y:{{beginAtZero:true,ticks:{{stepSize:1}}}}}}
      }}
    }});
    </script>
    """

    content = f"<h1>📊 Dashboard</h1>{kpi}{charts}"
    return page("Dashboard", content, "dashboard")

# ── Loans List ─────────────────────────────────────────────────────────────────
@app.route("/loans")
@login_required
def loans():
    q = request.args.get("q","")
    ll = list_all_loans(q)
    rows = ""
    for l in ll:
        sc = {"PendingApproval":"pending","Approved":"approved","Rejected":"rejected","Closed":"closed"}.get(l["status"],"pending")
        rows += f"""<tr>
          <td><b>{l['loan_number']}</b></td><td>{l['customer_name']}</td>
          <td>{l.get('customer_mobile','')}</td><td>{l['vehicle_type']}</td>
          <td>₹{l['loan_amount']:,.2f}</td><td>{l['interest_rate']*100:.1f}%</td>
          <td>{l['tenure']}m</td>
          <td><span class="badge badge-{sc}">{l['status']}</span></td>
          <td><a class="btn btn-sm btn-primary" href="/emis/{l['id']}">EMI</a></td>
        </tr>"""
    content = f"""
    <h1>📋 All Loans</h1>
    <form method="GET" style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;">
      <input name="q" value="{q}" placeholder="Search loans…" style="max-width:260px;">
      <button class="btn btn-primary btn-sm">Search</button>
      <a href="/loan/add" class="btn btn-success btn-sm">➕ New Loan</a>
    </form>
    <div class="card"><div class="table-wrap">
      <table>
        <tr><th>Loan #</th><th>Customer</th><th>Mobile</th><th>Type</th>
            <th>Amount</th><th>Rate</th><th>Tenure</th><th>Status</th><th>EMIs</th></tr>
        {rows or '<tr><td colspan="9" style="text-align:center;color:var(--muted);">No loans found</td></tr>'}
      </table>
    </div></div>"""
    return page("Loans", content, "loans")

# ── Add Loan ───────────────────────────────────────────────────────────────────
@app.route("/loan/add", methods=["GET","POST"])
@login_required
@role_required("admin","manager","fieldpia")
def add_loan():
    if request.method == "POST":
        f = request.form
        mob = f.get("customer_mobile","").strip()
        if len(mob) != 10:
            flash("Customer mobile number must be exactly 10 digits.","danger")
            return redirect(url_for("add_loan"))
        # Validate mandatory vehicle fields
        for field,label in [("vehicle_number","Vehicle Number"),("vehicle_model","Vehicle Model"),
                             ("engine_number","Engine Number"),("chassis_number","Chassis Number"),("vehicle_colour","Vehicle Colour")]:
            if not f.get(field,"").strip():
                flash(f"{label} is mandatory.","danger")
                return redirect(url_for("add_loan"))
        if not f.get("customer_address","").strip():
            flash("Customer address is mandatory.","danger")
            return redirect(url_for("add_loan"))
        try:
            create_loan(
                f["loan_number"], f["customer_name"], mob,
                f["customer_address"], f.get("customer_location",""),
                f["vehicle_type"], f["vehicle_number"], f["vehicle_model"],
                f["engine_number"], f["chassis_number"], f["vehicle_colour"],
                f["loan_amount"], f["interest_rate"], f["tenure"], f["start_date"],
                f.get("customer_email",""),
                f.get("guarantor_name",""), f.get("guarantor_address",""), f.get("guarantor_mobile",""),
                1 if f.get("is_reloan")=="yes" else 0, f.get("reloan_ref","")
            )
            flash("Loan submitted for approval.","success")
            return redirect(url_for("loans"))
        except Exception as e:
            flash(str(e),"danger")

    try: next_ln = next_loan_number()
    except: next_ln = f"LN-{datetime.now().year}-01"

    content = f"""
    <h1>➕ New Loan Application</h1>
    <div class="card">
    <form method="POST" id="loanForm">
      <div class="form-grid">

        <div class="section-title">📄 Loan Details</div>
        <div class="form-group">
          <label>Loan Number *</label>
          <input name="loan_number" value="{next_ln}" readonly>
        </div>
        <div class="form-group">
          <label>Start Date *</label>
          <input type="date" name="start_date" value="{date.today().isoformat()}" required>
        </div>
        <div class="form-group">
          <label>Loan Amount (₹) *</label>
          <input type="number" name="loan_amount" min="1" step="0.01" required oninput="calcDue()">
        </div>
        <div class="form-group">
          <label>Interest Rate (% p.a.) *</label>
          <input type="number" name="interest_rate" min="0" step="0.01" required oninput="calcDue()">
        </div>
        <div class="form-group">
          <label>Tenure (Months) *</label>
          <input type="number" name="tenure" min="1" max="360" required oninput="calcDue()">
        </div>
        <div class="form-group">
          <label>Is Reloan?</label>
          <select name="is_reloan" onchange="toggleReloan(this.value)">
            <option value="no">No</option><option value="yes">Yes</option>
          </select>
        </div>

        <div class="section-title">👤 Customer Details</div>
        <div class="form-group">
          <label>Customer Name *</label>
          <input name="customer_name" id="cust_name" required>
        </div>
        <div class="form-group">
          <label>Mobile (10 digits) *</label>
          <input name="customer_mobile" id="cust_mobile" maxlength="10"
                 pattern="[0-9]{{10}}" required
                 oninput="this.value=this.value.replace(/[^0-9]/g,'').slice(0,10)">
        </div>
        <div class="form-group full">
          <label>Address * (mandatory)</label>
          <textarea name="customer_address" id="cust_address" rows="2" required></textarea>
        </div>
        <div class="form-group full">
          <label>📍 GPS Location <span style="font-size:10px;color:var(--muted);">(tap button or enter manually)</span></label>
          <div style="display:flex;gap:8px;align-items:stretch;">
            <input name="customer_location" id="cust_location"
                   placeholder="e.g. 10.9876,78.1234 or area name"
                   style="flex:1;">
            <button type="button" onclick="getGPS()"
                    style="background:var(--amber);color:#fff;border:none;border-radius:8px;
                           padding:0 14px;font-size:13px;font-weight:700;cursor:pointer;
                           white-space:nowrap;min-height:46px;flex-shrink:0;">
              📡 Get GPS
            </button>
          </div>
          <small id="gps_status" style="color:var(--muted);font-size:11px;margin-top:2px;"></small>
        </div>
        <div class="form-group">
          <label>Email</label>
          <input type="email" name="customer_email">
        </div>

        <!-- Reloan -->
        <div id="reloan_section" style="display:none;grid-column:1/-1;">
          <div class="form-grid">
            <div class="section-title">🔄 Reloan Details</div>
            <div class="form-group">
              <label>Previous Loan Number</label>
              <input name="reloan_ref" id="reloan_ref" placeholder="LN-2025-XX">
            </div>
            <div class="form-group" style="align-self:flex-end;">
              <button type="button" class="btn btn-amber" onclick="checkReloan()">Check History</button>
            </div>
          </div>
          <div id="risk_box" class="risk-box"></div>
        </div>

        <div class="section-title">🚗 Vehicle Details (all mandatory)</div>
        <div class="form-group">
          <label>Vehicle Type *</label>
          <select name="vehicle_type" required>
            <option value="">-- Select --</option>
            <option>Two Wheeler</option><option>Three Wheeler</option>
            <option>Four Wheeler</option><option>Commercial Vehicle</option><option>Other</option>
          </select>
        </div>
        <div class="form-group">
          <label>Vehicle Number *</label>
          <input name="vehicle_number" placeholder="TN01AB1234" required>
        </div>
        <div class="form-group">
          <label>Vehicle Model *</label>
          <input name="vehicle_model" placeholder="e.g. Honda Activa 6G" required>
        </div>
        <div class="form-group">
          <label>Engine Number *</label>
          <input name="engine_number" required>
        </div>
        <div class="form-group">
          <label>Chassis Number *</label>
          <input name="chassis_number" required>
        </div>
        <div class="form-group">
          <label>Vehicle Colour *</label>
          <input name="vehicle_colour" required>
        </div>

        <div class="section-title">🛡️ Guarantor Details (optional)</div>
        <div class="form-group">
          <label>Guarantor Name</label><input name="guarantor_name">
        </div>
        <div class="form-group">
          <label>Guarantor Mobile</label>
          <input name="guarantor_mobile" maxlength="10"
                 oninput="this.value=this.value.replace(/[^0-9]/g,'').slice(0,10)">
        </div>
        <div class="form-group full">
          <label>Guarantor Address</label>
          <textarea name="guarantor_address" rows="2"></textarea>
        </div>
      </div>

      <!-- Due Preview -->
      <div class="due-preview" id="due_preview">
        <h3>💵 Loan Summary (Preview)</h3>
        <div class="due-grid">
          <div class="due-item"><div class="lbl">Loan Amount</div><div class="val" id="dp_principal">—</div></div>
          <div class="due-item"><div class="lbl">Total Interest</div><div class="val" id="dp_interest">—</div></div>
          <div class="due-item"><div class="lbl">Total Due</div><div class="val" id="dp_total">—</div></div>
          <div class="due-item"><div class="lbl">Monthly EMI</div><div class="val" id="dp_emi">—</div></div>
          <div class="due-item"><div class="lbl">Tenure</div><div class="val" id="dp_tenure">—</div></div>
          <div class="due-item"><div class="lbl">Rate p.a.</div><div class="val" id="dp_rate">—</div></div>
        </div>
      </div>

      <div style="margin-top:18px;display:flex;gap:10px;flex-wrap:wrap;">
        <button type="submit" class="btn btn-primary">Submit Application</button>
        <a href="/loans" class="btn" style="background:var(--surface2);color:var(--text);">Cancel</a>
      </div>
    </form>
    </div>

    <script>
    function getGPS(){{
      const btn=document.querySelector('[onclick="getGPS()"]');
      const st=document.getElementById('gps_status');
      if(!navigator.geolocation){{
        st.textContent='❌ GPS not supported on this browser.';
        st.style.color='var(--red)'; return;
      }}
      btn.textContent='⏳ Getting…'; btn.disabled=true;
      st.textContent='📡 Getting your location…';
      st.style.color='var(--muted)';
      navigator.geolocation.getCurrentPosition(
        pos=>{{
          const coords=pos.coords.latitude.toFixed(6)+','+pos.coords.longitude.toFixed(6);
          document.getElementById('cust_location').value=coords;
          st.textContent='✅ Location captured: '+coords;
          st.style.color='var(--green)';
          btn.textContent='📡 Get GPS'; btn.disabled=false;
        }},
        err=>{{
          st.textContent='❌ '+err.message+' — enter manually.';
          st.style.color='var(--red)';
          btn.textContent='📡 Get GPS'; btn.disabled=false;
        }},
        {{enableHighAccuracy:true,timeout:15000,maximumAge:0}}
      );
    }}
    function toggleReloan(v){{
      document.getElementById('reloan_section').style.display=v==='yes'?'block':'none';
    }}
    function checkReloan(){{
      const ref=document.getElementById('reloan_ref').value.trim();
      if(!ref){{alert('Enter previous loan number first.');return;}}
      fetch('/api/reloan_check?loan_number='+encodeURIComponent(ref))
        .then(r=>r.json()).then(data=>{{
          const box=document.getElementById('risk_box');
          if(data.error){{box.className='risk-box risk-risk';box.style.display='block';box.innerHTML='<b>⚠️ '+data.error+'</b>';return;}}
          box.className='risk-box risk-'+data.risk.toLowerCase();
          box.style.display='block';
          box.innerHTML=`<b>${{data.decision}}</b><br><small>EMIs: ${{data.total}} | Paid: ${{data.paid}} | Delays: ${{data.delay_count}}</small>`;
          if(data.customer&&data.customer.customer_name){{
            document.getElementById('cust_name').value=data.customer.customer_name||'';
            document.getElementById('cust_mobile').value=data.customer.customer_mobile||'';
            document.getElementById('cust_address').value=data.customer.customer_address||'';
            document.getElementById('cust_location').value=data.customer.customer_location||'';
          }}
        }});
    }}
    function fmt(v){{return '₹'+parseFloat(v).toLocaleString('en-IN',{{minimumFractionDigits:2,maximumFractionDigits:2}});}}
    function calcDue(){{
      const amt=parseFloat(document.querySelector('[name=loan_amount]').value)||0;
      const rate=parseFloat(document.querySelector('[name=interest_rate]').value)||0;
      const tenure=parseInt(document.querySelector('[name=tenure]').value)||0;
      const p=document.getElementById('due_preview');
      if(amt>0&&rate>0&&tenure>0){{
        const interest=amt*(rate/100)*(tenure/12);
        const total=amt+interest; const emi=total/tenure;
        document.getElementById('dp_principal').textContent=fmt(amt);
        document.getElementById('dp_interest').textContent=fmt(interest);
        document.getElementById('dp_total').textContent=fmt(total);
        document.getElementById('dp_emi').textContent=fmt(emi);
        document.getElementById('dp_tenure').textContent=tenure+' months';
        document.getElementById('dp_rate').textContent=rate+'%';
        p.style.display='block';
      }}else{{p.style.display='none';}}
    }}
    </script>
    """
    return page("New Loan", content, "add")

# ── Reloan / Next LN APIs ──────────────────────────────────────────────────────
@app.route("/api/reloan_check")
@login_required
def api_reloan_check():
    ln = request.args.get("loan_number","").strip()
    if not ln: return jsonify({"error":"No loan number provided"})
    result, err = assess_reloan_risk(ln)
    if err: return jsonify({"error": err})
    return jsonify(result)

@app.route("/api/next_loan_number")
@login_required
def api_next_loan_number():
    try: return jsonify({"loan_number": next_loan_number()})
    except Exception as e: return jsonify({"error": str(e)})

# ── Approval ───────────────────────────────────────────────────────────────────
@app.route("/approval", methods=["GET","POST"])
@login_required
@role_required("admin")
def approval():
    if request.method == "POST":
        lid = int(request.form["loan_id"]); action = request.form["action"]
        try:
            if action == "approve":
                approve_loan(lid); flash("Loan approved!","success")
            else:
                reject_loan(lid, request.form.get("reason","No reason given"))
                flash("Loan rejected.","success")
        except Exception as e:
            flash(str(e),"danger")
    q = request.args.get("q","")
    ll = list_pending_loans(q)
    rows = ""
    for l in ll:
        amt = float(l["loan_amount"]); rate = float(l["interest_rate"]); t = int(l["tenure"])
        emi_amt = compute_emi_amount(amt, rate, t)
        total_due = compute_total_due(amt, rate, t)
        rows += f"""<tr>
          <td><b>{l['loan_number']}</b></td><td>{l['customer_name']}</td>
          <td>{l.get('customer_mobile','')}</td><td>{l['vehicle_type']}</td>
          <td>₹{amt:,.2f}</td>
          <td><b style="color:var(--accent)">₹{total_due:,.2f}</b></td>
          <td><b>₹{emi_amt:,.2f}</b></td><td>{l['start_date']}</td>
          <td>
            <form method="POST" style="display:inline">
              <input type="hidden" name="loan_id" value="{l['id']}">
              <input type="hidden" name="action" value="approve">
              <button class="btn btn-success btn-sm">✅</button>
            </form>
            <form method="POST" style="display:inline;margin-left:4px;" onsubmit="return getReason(this)">
              <input type="hidden" name="loan_id" value="{l['id']}">
              <input type="hidden" name="action" value="reject">
              <input type="hidden" name="reason" class="reason_inp">
              <button type="submit" class="btn btn-danger btn-sm">❌</button>
            </form>
          </td>
        </tr>"""
    content = f"""
    <h1>✅ Loan Approval</h1>
    <form method="GET" style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;">
      <input name="q" value="{q}" placeholder="Search…" style="max-width:240px;">
      <button class="btn btn-primary btn-sm">Search</button>
    </form>
    <div class="card"><div class="table-wrap"><table>
      <tr><th>Loan #</th><th>Customer</th><th>Mobile</th><th>Type</th>
          <th>Loan Amt</th><th>Total Due</th><th>EMI/mo</th><th>Start</th><th>Action</th></tr>
      {rows or '<tr><td colspan="9" style="text-align:center;color:var(--muted);">No pending loans</td></tr>'}
    </table></div></div>
    <script>
    function getReason(form){{
      const r=prompt('Rejection reason:'); if(!r) return false;
      form.querySelector('.reason_inp').value=r; return true;
    }}
    </script>"""
    return page("Approval", content, "approval")

# ── Customers ──────────────────────────────────────────────────────────────────
@app.route("/customers")
@login_required
def customers():
    q = request.args.get("q","")
    cl = list_customers(q)
    rows = ""
    for c in cl:
        sc = {"Active":"approved","Closed":"closed"}.get(c["status"],"pending")
        rows += f"""<tr>
          <td><b>{c['name']}</b></td><td>{c['vehicle_type']}</td>
          <td>₹{c['loan_amount']:,.2f}</td><td><b>₹{c['emi_amount']:,.2f}</b></td>
          <td><span class="badge badge-{sc}">{c['status']}</span></td>
          <td><a class="btn btn-sm btn-primary" href="/emis/{c['loan_id']}">View EMIs</a></td>
        </tr>"""
    content = f"""
    <h1>👥 Customers</h1>
    <form method="GET" style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;">
      <input name="q" value="{q}" placeholder="Search…" style="max-width:240px;">
      <button class="btn btn-primary btn-sm">Search</button>
    </form>
    <div class="card"><div class="table-wrap"><table>
      <tr><th>Name</th><th>Vehicle</th><th>Loan Amt</th><th>EMI/mo</th><th>Status</th><th>EMIs</th></tr>
      {rows or '<tr><td colspan="6" style="text-align:center;color:var(--muted);">No customers found</td></tr>'}
    </table></div></div>"""
    return page("Customers", content, "customers")

# ── EMIs ────────────────────────────────────────────────────────────────────────
@app.route("/emis/<int:loan_id>")
@login_required
def emis(loan_id):
    c = get_cur(); c.execute("SELECT * FROM LoanEntry WHERE id=?", (loan_id,))
    loan = dict(c.fetchone() or {}); emi_list = get_emis_for_loan(loan_id)
    role = session.get("role",""); can_pay = ROLES.get(role,{}).get("can_pay", False)
    today = date.today(); upcoming_limit = today + timedelta(days=UPCOMING_DAYS)

    total_remaining = sum(float(e.get("remaining_amount") or e["emi_amount"])
                          for e in emi_list if e["status"] != "Paid")
    rows = ""
    for e in emi_list:
        due_d = parse_date(e["due_date"])
        is_paid = e["status"] == "Paid"
        is_overdue = not is_paid and due_d < today
        is_upcoming = not is_paid and not is_overdue and due_d <= upcoming_limit

        row_class = ""
        if is_overdue:   row_class = "row-overdue"
        elif is_upcoming: row_class = "row-upcoming"
        elif is_paid:    row_class = "row-paid"

        sc = {"Paid":"paid","Partial":"partial","Overdue":"overdue"}.get(e["status"],"pending")
        remaining = float(e.get("remaining_amount") or e["emi_amount"])
        bill_no   = e.get("bill_number") or "—"
        paid_at   = (e.get("paid_at") or "")[:10]

        pay_form = ""
        if can_pay and not is_paid:
            pay_form = f"""
            <form method="POST" action="/emi/pay" style="display:flex;gap:4px;align-items:center;flex-wrap:wrap;">
              <input type="hidden" name="emi_id" value="{e['emi_id']}">
              <input type="hidden" name="loan_id" value="{loan_id}">
              <input name="bill_number" placeholder="Bill No.*" required
                     style="width:90px;font-size:12px;padding:5px 6px;">
              <input type="number" name="pay_amount" value="{remaining:.2f}"
                     min="1" step="0.01" style="width:90px;font-size:12px;padding:5px 6px;" required>
              <button class="btn btn-success btn-sm" onclick="return chkBill(this)">Pay</button>
            </form>"""

        rows += f'<tr class="{row_class}" id="emi_{e["emi_id"]}">'
        rows += f"""<td>{e['installment_no']}</td><td>{e['due_date']}</td>
          <td>₹{e['emi_amount']:,.2f}</td><td>₹{float(e['amount_paid'] or 0):,.2f}</td>
          <td><b>₹{remaining:,.2f}</b></td>
          <td><span class="badge badge-{sc}">{e['status']}</span></td>
          <td>{bill_no}</td><td>{paid_at}</td><td>{pay_form}</td>
        </tr>"""

    # Legend
    legend = """
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;font-size:12px;">
      <span style="display:flex;align-items:center;gap:4px;">
        <span style="width:14px;height:14px;background:#fee2e2;border-left:3px solid #dc2626;display:inline-block;"></span> Overdue
      </span>
      <span style="display:flex;align-items:center;gap:4px;">
        <span style="width:14px;height:14px;background:#fef9c3;border-left:3px solid #d97706;display:inline-block;"></span> Due in 10 days
      </span>
      <span style="display:flex;align-items:center;gap:4px;">
        <span style="width:14px;height:14px;background:#d1fae5;display:inline-block;border-radius:2px;"></span> Paid
      </span>
    </div>"""

    content = f"""
    <h1>💳 EMI Schedule — {loan.get('loan_number','')}</h1>
    <div class="card" style="margin-bottom:12px;">
      <div class="form-grid">
        <div><b>Customer:</b> {loan.get('customer_name','')}</div>
        <div><b>Mobile:</b> {loan.get('customer_mobile','')}</div>
        <div><b>Vehicle:</b> {loan.get('vehicle_type','')} — {loan.get('vehicle_number','')}</div>
        <div><b>Model:</b> {loan.get('vehicle_model','')}</div>
        <div><b>Loan Amount:</b> ₹{float(loan.get('loan_amount',0)):,.2f}</div>
        <div><b>Tenure:</b> {loan.get('tenure','')} months | <b>Status:</b> {loan.get('status','')}</div>
        <div style="grid-column:1/-1;background:#fef3c7;border-radius:6px;padding:10px;border-left:4px solid #d97706;">
          <b>⏳ Total Outstanding: </b>
          <span style="font-size:18px;font-weight:700;color:var(--amber);">₹{total_remaining:,.2f}</span>
        </div>
      </div>
    </div>
    <div class="card">
      <p style="font-size:12px;color:var(--muted);margin-bottom:8px;">
        📌 Bill number is <b>mandatory</b> before payment. Partial payments need a bill number each time.
      </p>
      {legend}
      <div class="table-wrap"><table>
        <tr><th>#</th><th>Due Date</th><th>EMI</th><th>Paid</th><th>Remaining</th>
            <th>Status</th><th>Bill No</th><th>Paid On</th><th>Action</th></tr>
        {rows or '<tr><td colspan="9" style="text-align:center;">No EMIs</td></tr>'}
      </table></div>
    </div>
    <script>
    function chkBill(btn){{
      const bn=btn.closest('form').querySelector('[name=bill_number]').value.trim();
      if(!bn){{alert('Bill number is mandatory!');return false;}} return true;
    }}
    // Scroll to highlighted emi if anchor
    const h=window.location.hash;
    if(h){{const el=document.querySelector(h);if(el){{el.scrollIntoView({{behavior:'smooth',block:'center'}});}}}}
    </script>
    <a href="/customers" class="btn" style="background:var(--surface2);color:var(--text);">← Back</a>"""
    return page("EMIs", content, "emis")

@app.route("/emi/pay", methods=["POST"])
@login_required
@role_required("admin","manager","fieldpia")
def emi_pay():
    emi_id  = int(request.form["emi_id"])
    loan_id = int(request.form["loan_id"])
    pay_amt = float(request.form.get("pay_amount",0) or 0)
    bill_no = request.form.get("bill_number","").strip()
    try:
        msg = pay_emi(emi_id, pay_amt, bill_number=bill_no)
        flash(msg,"success")
    except Exception as e:
        flash(str(e),"danger")
    return redirect(url_for("emis", loan_id=loan_id) + f"#emi_{emi_id}")

# ── Alerts (grouped by loan number) ────────────────────────────────────────────
@app.route("/alerts")
@login_required
def alerts():
    od    = get_overdue_emis()
    up    = get_upcoming_emis()
    today = date.today()

    # Group overdue by loan number
    od_grouped = group_alerts_by_loan(od)
    up_grouped = group_alerts_by_loan(up)

    od_rows = ""
    for g in od_grouped:
        oldest_days = (today - parse_date(g["oldest_due"])).days
        od_rows += f"""<tr style="background:#fee2e2;">
          <td><b><a href="/emis/{g['lid']}" style="color:var(--accent);">{g['loan_number']}</a></b></td>
          <td>{g['customer_name']}</td>
          <td style="text-align:center;">{g['emi_count']}</td>
          <td>{g['oldest_due']}</td>
          <td><b style="color:var(--red);">₹{g['total_due']:,.2f}</b></td>
          <td><b style="color:var(--red);">{oldest_days} days</b></td>
          <td><a class="btn btn-sm btn-danger" href="/emis/{g['lid']}">💳 Pay</a></td>
        </tr>"""

    up_rows = ""
    for g in up_grouped:
        days_left = (parse_date(g["oldest_due"]) - today).days
        up_rows += f"""<tr style="background:#fef9c3;">
          <td><b><a href="/emis/{g['lid']}" style="color:var(--accent);">{g['loan_number']}</a></b></td>
          <td>{g['customer_name']}</td>
          <td style="text-align:center;">{g['emi_count']}</td>
          <td>{g['oldest_due']}</td>
          <td><b style="color:var(--amber);">₹{g['total_due']:,.2f}</b></td>
          <td><b style="color:var(--amber);">in {days_left} days</b></td>
          <td><a class="btn btn-sm btn-amber" href="/emis/{g['lid']}">📋 View</a></td>
        </tr>"""

    sms_on = SMS_CONFIG["enabled"]
    sms_badge = ('<span style="background:#059669;color:#fff;font-size:11px;padding:2px 8px;'
                 'border-radius:10px;margin-left:8px;">📱 SMS ON</span>' if sms_on else
                 '<span style="background:#6b7280;color:#fff;font-size:11px;padding:2px 8px;'
                 'border-radius:10px;margin-left:8px;">📱 SMS OFF</span>')

    content = f"""
    <h1>🔔 Alerts {sms_badge}</h1>

    <!-- SMS Action Buttons -->
    <div class="card" style="padding:14px;margin-bottom:14px;">
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
        <b style="font-size:14px;">📱 Send SMS Notifications:</b>
        <button class="btn btn-danger btn-sm" onclick="sendSMS('overdue')" id="btn_overdue">
          🔴 Alert Overdue Customers
        </button>
        <button class="btn btn-amber btn-sm" onclick="sendSMS('upcoming')" id="btn_upcoming">
          🟡 Remind Due in 3 Days
        </button>
        {"<a href='/sms_settings' class='btn btn-sm' style='background:var(--surface2);color:var(--text);'>⚙️ SMS Setup</a>" if session.get("role")=="admin" else ""}
      </div>
      {"<p style='font-size:12px;color:#92400e;margin-top:8px;background:#fef3c7;padding:6px 10px;border-radius:5px;'>⚠️ SMS not configured. <a href='/sms_settings' style='color:var(--accent);'>Click here to set up</a> — it takes 2 minutes.</p>" if not sms_on else ""}
      <div id="sms_result" style="margin-top:10px;display:none;"></div>
    </div>

    <div class="card">
      <h2 style="color:var(--red);">🔴 Overdue — {len(od_grouped)} Loan(s) &nbsp;
        <small style="font-size:13px;color:var(--muted);">Total: ₹{sum(g['total_due'] for g in od_grouped):,.2f}</small></h2>
      <p style="font-size:12px;color:var(--muted);margin-bottom:8px;">
        Each row = one loan. Amount shown is cumulative of all overdue EMIs for that loan.
      </p>
      <div class="table-wrap"><table>
        <tr><th>Loan #</th><th>Customer</th><th>EMIs Due</th><th>Oldest Due</th>
            <th>Total Due Amt</th><th>Days Overdue</th><th>Action</th></tr>
        {od_rows or '<tr><td colspan="7" style="color:var(--green);text-align:center;background:#d1fae5;">✅ No overdue EMIs!</td></tr>'}
      </table></div>
    </div>
    <div class="card">
      <h2 style="color:var(--amber);">🟡 Upcoming (10 days) — {len(up_grouped)} Loan(s) &nbsp;
        <small style="font-size:13px;color:var(--muted);">Total: ₹{sum(g['total_due'] for g in up_grouped):,.2f}</small></h2>
      <p style="font-size:12px;color:var(--muted);margin-bottom:8px;">
        Each row = one loan. Click loan number or Pay to go to EMI schedule.
      </p>
      <div class="table-wrap"><table>
        <tr><th>Loan #</th><th>Customer</th><th>EMIs</th><th>First Due</th>
            <th>Total Amount</th><th>Due In</th><th>Action</th></tr>
        {up_rows or '<tr><td colspan="7" style="color:var(--green);text-align:center;background:#d1fae5;">✅ No upcoming EMIs in 10 days.</td></tr>'}
      </table></div>
    </div>"""
    content += """
    <script>
    function sendSMS(type) {
      const btn = document.getElementById('btn_' + type);
      const res = document.getElementById('sms_result');
      btn.disabled = true;
      btn.textContent = '⏳ Sending…';
      res.style.display = 'none';
      fetch('/api/sms/' + type, {method:'POST',headers:{'Content-Type':'application/json'}})
        .then(r => r.json())
        .then(data => {
          btn.disabled = false;
          btn.textContent = type === 'overdue' ? '🔴 Alert Overdue Customers' : '🟡 Remind Due in 3 Days';
          res.style.display = 'block';
          if (!data.sms_enabled) {
            res.innerHTML = '<div class="alert alert-warning">⚠️ SMS not configured. <a href="/sms_settings">Set up Fast2SMS</a> to enable.</div>';
            return;
          }
          let html = '<div class="alert alert-' + (data.sent > 0 ? 'success' : 'info') + '">';
          html += '📱 SMS sent to <b>' + data.sent + '</b> of ' + data.total + ' customer(s).</div>';
          if (data.results && data.results.length > 0) {
            html += '<div style="font-size:12px;margin-top:6px;">';
            data.results.forEach(r => {
              html += '<span style="margin-right:12px;">' +
                (r.ok ? '✅' : '❌') + ' ' + r.loan + ' (' + r.mobile + ')</span>';
            });
            html += '</div>';
          }
          res.innerHTML = html;
        })
        .catch(e => {
          btn.disabled = false;
          res.style.display = 'block';
          res.innerHTML = '<div class="alert alert-danger">❌ Error: ' + e.message + '</div>';
        });
    }
    </script>
    """
    return page("Alerts", content, "alerts")

# ── Closed / Rejected ──────────────────────────────────────────────────────────
@app.route("/closed")
@login_required
def closed():
    q = request.args.get("q",""); ll = list_closed_loans(q)
    rows = "".join(f"""<tr>
        <td><b>{l['loan_number']}</b></td><td>{l['customer_name']}</td>
        <td>{l['vehicle_type']}</td><td>₹{l['loan_amount']:,.2f}</td>
        <td>{l['closure_date']}</td></tr>""" for l in ll)
    content = f"""
    <h1>🔒 Closed Loans</h1>
    <form method="GET" style="margin-bottom:12px;display:flex;gap:8px;">
      <input name="q" value="{q}" placeholder="Search…" style="max-width:240px;">
      <button class="btn btn-primary btn-sm">Search</button>
    </form>
    <div class="card"><div class="table-wrap"><table>
      <tr><th>Loan #</th><th>Customer</th><th>Type</th><th>Amount</th><th>Closed On</th></tr>
      {rows or '<tr><td colspan="5" style="text-align:center;color:var(--muted);">No closed loans</td></tr>'}
    </table></div></div>"""
    return page("Closed Loans", content, "closed")

@app.route("/rejected")
@login_required
def rejected():
    q = request.args.get("q",""); ll = list_rejected_loans(q)
    rows = "".join(f"""<tr>
        <td><b>{l['loan_number']}</b></td><td>{l['customer_name']}</td>
        <td>{l['reason']}</td><td>{(l.get('created_at') or '')[:10]}</td></tr>""" for l in ll)
    content = f"""
    <h1>❌ Rejected Loans</h1>
    <form method="GET" style="margin-bottom:12px;display:flex;gap:8px;">
      <input name="q" value="{q}" placeholder="Search…" style="max-width:240px;">
      <button class="btn btn-primary btn-sm">Search</button>
    </form>
    <div class="card"><div class="table-wrap"><table>
      <tr><th>Loan #</th><th>Customer</th><th>Reason</th><th>Date</th></tr>
      {rows or '<tr><td colspan="4" style="text-align:center;color:var(--muted);">No rejected loans</td></tr>'}
    </table></div></div>"""
    return page("Rejected Loans", content, "rejected")

# ── Calculator ─────────────────────────────────────────────────────────────────
@app.route("/calculator", methods=["GET","POST"])
@login_required
def calculator():
    result_html = ""
    amt_v = request.form.get("amount","") if request.method=="POST" else ""
    rate_v = request.form.get("rate","") if request.method=="POST" else ""
    tenure_v = request.form.get("tenure","") if request.method=="POST" else ""

    if request.method == "POST":
        try:
            amt = float(amt_v or 0); rate = float(rate_v or 0); tenure = int(float(tenure_v or 0))
            if amt>0 and rate>0 and tenure>0:
                interest = amt * (rate/100) * (tenure/12)
                total = amt + interest; emi = total / tenure
                srows = ""
                for i in range(1, tenure+1):
                    srows += f"<tr><td>{i}</td><td>₹{emi:,.2f}</td><td>₹{amt/tenure:,.2f}</td><td>₹{interest/tenure:,.2f}</td><td>₹{max(amt-(amt/tenure)*i,0):,.2f}</td></tr>"
                result_html = f"""
                <div class="calc-result">
                  <div class="lbl">Monthly EMI (Fixed Rate)</div>
                  <div class="big-val">₹{emi:,.2f}</div>
                  <div class="calc-summary">
                    <div class="item"><div class="val">₹{amt:,.2f}</div><div class="lbl">Principal</div></div>
                    <div class="item"><div class="val">₹{interest:,.2f}</div><div class="lbl">Total Interest</div></div>
                    <div class="item"><div class="val">₹{total:,.2f}</div><div class="lbl">Total Payable</div></div>
                  </div>
                </div>
                <div class="card"><h2>📅 EMI Schedule</h2>
                  <div class="table-wrap"><table>
                    <tr><th>#</th><th>EMI</th><th>Principal</th><th>Interest</th><th>Balance</th></tr>
                    {srows}
                  </table></div>
                </div>"""
            else:
                flash("Please fill all fields with valid values.","warning")
        except Exception as e:
            flash(str(e),"danger")

    content = f"""
    <h1>🧮 Loan Calculator</h1>
    <div class="card">
      <p style="color:var(--muted);font-size:13px;margin-bottom:14px;">
        Fixed flat rate — interest applied on full principal for entire tenure (finance company method).
      </p>
      <form method="POST">
        <div class="form-grid">
          <div class="form-group">
            <label>Loan Amount (₹)</label>
            <input type="number" name="amount" value="{amt_v}" min="1" step="0.01" placeholder="e.g. 50000" required>
          </div>
          <div class="form-group">
            <label>Interest Rate (% p.a.)</label>
            <input type="number" name="rate" value="{rate_v}" min="0.01" step="0.01" placeholder="e.g. 24" required>
          </div>
          <div class="form-group">
            <label>Tenure (Months)</label>
            <input type="number" name="tenure" value="{tenure_v}" min="1" max="360" placeholder="e.g. 12" required>
          </div>
          <div class="form-group" style="align-self:flex-end;">
            <button class="btn btn-primary">Calculate</button>
          </div>
        </div>
      </form>
    </div>
    {result_html}
    <div class="card" style="background:#fef3c7;border-color:#fde68a;">
      <b>📌 Flat Rate Formula:</b><br>
      <code>Total Interest = Principal × Rate% × (Months ÷ 12)</code><br>
      <code>Total Payable = Principal + Interest</code><br>
      <code>Monthly EMI = Total Payable ÷ Months</code>
    </div>"""
    return page("Calculator", content, "calculator")

# ── Report ─────────────────────────────────────────────────────────────────────
@app.route("/report", methods=["GET","POST"])
@login_required
@role_required("admin","manager","viewer")
def report():
    if request.method == "POST":
        if not REPORTLAB_AVAILABLE:
            flash("reportlab not installed. Run: pip install reportlab","danger")
            return redirect(url_for("report"))
        path = "/tmp/tfc_loan_report.pdf"
        try:
            generate_pdf(path)
            return send_file(path, as_attachment=True, download_name="tfc_loan_report.pdf", mimetype="application/pdf")
        except Exception as e:
            flash(str(e),"danger")
    tl,tla,tr,tp = get_kpi_totals(); counts = get_loan_summary_counts()
    content = f"""
    <h1>📊 Report</h1>
    <div class="kpi-grid" style="margin-bottom:16px;">
      <div class="kpi"><div class="val">{counts['total']}</div><div class="lbl">Total Loans</div></div>
      <div class="kpi"><div class="val">₹{tla:,.0f}</div><div class="lbl">Disbursed</div></div>
      <div class="kpi" style="border-color:var(--green)"><div class="val" style="color:var(--green)">₹{tr:,.0f}</div><div class="lbl">Collected</div></div>
      <div class="kpi" style="border-color:var(--red)"><div class="val" style="color:var(--red)">₹{tp:,.0f}</div><div class="lbl">Outstanding</div></div>
    </div>
    <div class="card">
      <form method="POST">
        <button class="btn btn-primary">📥 Download PDF Report</button>
      </form>
      {"<p style='color:var(--red);margin-top:8px;font-size:13px;'>reportlab not installed — PDF unavailable.</p>" if not REPORTLAB_AVAILABLE else ""}
    </div>"""
    return page("Report", content, "report")

# ── Users ──────────────────────────────────────────────────────────────────────
@app.route("/users", methods=["GET","POST"])
@login_required
@role_required("admin")
def users():
    if request.method == "POST":
        uname = request.form["username"].strip(); pw = request.form["password"]; role_u = request.form["role"]
        c = get_cur()
        c.execute("INSERT INTO Users (username,pw_hash,role,created_at) VALUES (?,?,?,?) ON CONFLICT(username) DO UPDATE SET pw_hash=?,role=?",
                  (uname,hash_pw(pw),role_u,datetime.now(timezone.utc).isoformat(),hash_pw(pw),role_u))
        get_db().commit(); flash(f"User '{uname}' saved.","success")
    c = get_cur(); c.execute("SELECT * FROM Users ORDER BY user_id")
    ul = [dict(r) for r in c.fetchall()]
    urows = "".join(f"""<tr><td>{u['username']}</td>
        <td><span class="badge badge-{u['role']}">{u['role'].title()}</span></td>
        <td>{(u.get('created_at') or '')[:10]}</td></tr>""" for u in ul)
    rrws = ""
    for r,p in ROLES.items():
        def ck(k,p=p): return "✅" if p.get(k) else "❌"
        rrws += f"""<tr>
            <td><span class="badge badge-{r}">{p['label']}</span></td>
            <td>{ck('can_add')}</td><td>{ck('can_approve')}</td>
            <td>{ck('can_pay')}</td><td>{ck('can_report')}</td>
        </tr>"""
    content = f"""
    <h1>⚙️ User Management</h1>
    <div class="form-grid">
      <div class="card">
        <h2>Add / Update User</h2>
        <form method="POST">
          <div class="form-group" style="margin-bottom:10px;"><label>Username</label><input name="username" required></div>
          <div class="form-group" style="margin-bottom:10px;"><label>Password</label><input type="password" name="password" required></div>
          <div class="form-group" style="margin-bottom:14px;"><label>Role</label>
            <select name="role">{"".join(f'<option value="{r}">{ROLES[r]["label"]}</option>' for r in ROLES)}</select>
          </div>
          <button class="btn btn-primary">Save User</button>
        </form>
      </div>
      <div class="card">
        <h2>Role Permissions</h2>
        <div class="table-wrap"><table>
          <tr><th>Role</th><th>ADD</th><th>APPROVE</th><th>PAY</th><th>REPORT</th></tr>
          {rrws}
        </table></div>
      </div>
    </div>
    <div class="card">
      <h2>All Users</h2>
      <div class="table-wrap"><table>
        <tr><th>Username</th><th>Role</th><th>Created</th></tr>{urows}
      </table></div>
    </div>"""
    return page("Users", content, "users")

# ── API ────────────────────────────────────────────────────────────────────────
@app.route("/api/chart/monthly")
@login_required
def api_monthly():
    m,a = get_monthly_paid_series(); return jsonify({"labels":m,"data":a})

@app.route("/api/chart/breakdown")
@login_required
def api_breakdown():
    bd = get_loan_type_breakdown()
    return jsonify({"labels":[r["vehicle_type"] for r in bd],"data":[r["cnt"] for r in bd]})

# ── SMS Alert APIs ─────────────────────────────────────────────────────────────
@app.route("/api/sms/overdue", methods=["POST"])
@login_required
@role_required("admin","manager")
def api_sms_overdue():
    results, total = send_bulk_overdue_sms()
    return jsonify({"sent": total, "total": len(results), "results": results,
                    "sms_enabled": SMS_CONFIG["enabled"]})

@app.route("/api/sms/upcoming", methods=["POST"])
@login_required
@role_required("admin","manager")
def api_sms_upcoming():
    results, total = send_bulk_upcoming_sms()
    return jsonify({"sent": total, "total": len(results), "results": results,
                    "sms_enabled": SMS_CONFIG["enabled"]})

@app.route("/api/sms/single", methods=["POST"])
@login_required
@role_required("admin","manager")
def api_sms_single():
    """Send a custom SMS to one customer mobile number."""
    data   = request.get_json()
    mobile = data.get("mobile","").strip()
    msg    = data.get("message","").strip()
    if not mobile or not msg:
        return jsonify({"ok": False, "info": "Mobile and message required"})
    ok, info = _send_sms(mobile, msg)
    return jsonify({"ok": ok, "info": info, "sms_enabled": SMS_CONFIG["enabled"]})

@app.route("/sms_settings")
@login_required
@role_required("admin")
def sms_settings():
    """SMS configuration status page."""
    enabled = SMS_CONFIG["enabled"]
    key_set = bool(SMS_CONFIG["api_key"])
    content = f"""
    <h1>📱 SMS Notification Settings</h1>
    <div class="card" style="border-left:4px solid {'var(--green)' if enabled else 'var(--red)'};">
      <h2>Status: {'✅ Active' if enabled else '❌ Not Configured'}</h2>
      <p style="margin-top:8px;color:var(--muted);font-size:13px;">
        {'SMS notifications are enabled and will be sent automatically.' if enabled else
         'SMS is disabled. Set the FAST2SMS_KEY environment variable on Render to enable.'}
      </p>
    </div>

    <div class="card">
      <h2>🔧 How to Enable SMS</h2>
      <ol style="margin-left:20px;line-height:2;font-size:14px;color:var(--text);">
        <li>Go to <a href="https://www.fast2sms.com" target="_blank" style="color:var(--accent);">
            <b>fast2sms.com</b></a> → Sign Up (Free)</li>
        <li>Go to <b>Dashboard → Dev API</b> → Copy your API Key</li>
        <li>On <b>Render.com</b> → Your App → <b>Environment</b> → Add variable:<br>
            <code style="background:#f1f5fb;padding:4px 8px;border-radius:4px;font-size:13px;">
            FAST2SMS_KEY = your_api_key_here</code></li>
        <li><b>Redeploy</b> the app — SMS will be active immediately</li>
        <li>For DLT compliance (required for transactional SMS in India), register your
            message template on Fast2SMS DLT panel</li>
      </ol>
    </div>

    <div class="card">
      <h2>📋 SMS Events</h2>
      <table><tr><th>Event</th><th>When</th><th>Recipient</th></tr>
        <tr><td>✅ Loan Approved</td><td>When admin approves a loan</td><td>Customer mobile</td></tr>
        <tr><td>🔔 EMI Reminder</td><td>Click "Send Reminders" on Alerts page (≤3 days)</td><td>Customer mobile</td></tr>
        <tr><td>🔴 Overdue Alert</td><td>Click "Send Overdue SMS" on Alerts page</td><td>Customer mobile</td></tr>
        <tr><td>🎉 Loan Closed</td><td>When all EMIs are paid</td><td>Customer mobile</td></tr>
      </table>
    </div>

    <div class="card" style="background:#fef3c7;border-color:#fde68a;">
      <b>💡 Fast2SMS Free Tier:</b> ₹50 free credits on signup (~100-150 SMS).
      Recharge as needed. Very affordable for small finance companies.
    </div>
    """
    return page("SMS Settings", content, "")

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
