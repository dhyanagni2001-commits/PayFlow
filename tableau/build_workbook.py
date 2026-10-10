"""Generate tableau/PayFlow.twbx: three dashboards (Finance, Risk, Pipeline health) over tableau/data/*.csv.

Tableau Public only opens workbooks whose data is extracted, so each CSV is first loaded into a .hyper extract
(Tableau's own file format, via tableauhyperapi), and the workbook plus extracts are zipped into one packaged .twbx.

Run `make tableau-export` first, then `make tableau-workbook`, then open the .twbx in Tableau Public.
"""

import tempfile
import zipfile
from pathlib import Path
from xml.sax.saxutils import quoteattr

from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
    escape_string_literal,
)

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
OUT = HERE / "PayFlow.twbx"

MONEY = 'c"$"#,##0;-"$"#,##0'
PCT = "p0.0%"
REMOTE_TYPE = {"string": 129, "integer": 20, "real": 5, "date": 133, "datetime": 135}
CCY_PARAM = "[Parameters].[Parameter 1]"


def a(v):
    return quoteattr(str(v))


def caption(name):
    return " ".join(w.upper() if w in ("p50", "p95", "p99", "dq") else w.capitalize() for w in name.split("_"))


# Columns per CSV, in file order: (name, datatype, role, format)
SOURCES = {
    "settlement": [
        ("settlement_date", "date", "dimension", None),
        ("merchant_id", "integer", "dimension", None),
        ("merchant_name", "string", "dimension", None),
        ("category", "string", "dimension", None),
        ("currency", "string", "dimension", None),
        ("captured_count", "integer", "measure", None),
        ("gross", "real", "measure", MONEY),
        ("fees", "real", "measure", MONEY),
        ("refunds", "real", "measure", MONEY),
        ("chargebacks", "real", "measure", MONEY),
        ("net", "real", "measure", MONEY),
        ("running_balance", "real", "measure", MONEY),
    ],
    "volume_10min": [
        ("time_10min", "datetime", "dimension", None),
        ("currency", "string", "dimension", None),
        ("payments", "integer", "measure", None),
        ("captured", "real", "measure", MONEY),
        ("refunded", "real", "measure", MONEY),
        ("chargebacks", "real", "measure", MONEY),
        ("failed", "integer", "measure", None),
    ],
    "merchant_risk": [
        ("merchant_id", "integer", "dimension", None),
        ("merchant_name", "string", "dimension", None),
        ("category", "string", "dimension", None),
        ("current_risk_tier", "string", "dimension", None),
        ("captured_30d", "integer", "measure", None),
        ("disputes_30d", "integer", "measure", None),
        ("disputes_lost_30d", "integer", "measure", None),
        ("refunded_30d", "integer", "measure", None),
        ("flagged_30d", "integer", "measure", None),
        ("chargeback_rate", "real", "measure", PCT),
        ("refund_rate", "real", "measure", PCT),
    ],
    "dq_flags": [
        ("rule_name", "string", "dimension", None),
        ("table_name", "string", "dimension", None),
        ("pk", "string", "dimension", None),
        ("observed", "string", "dimension", None),
        ("first_detected_at", "datetime", "dimension", None),
    ],
    "pipeline_runs": [
        ("run_ts", "datetime", "dimension", None),
        ("events_processed", "integer", "measure", None),
        ("bronze_rows", "integer", "measure", None),
        ("max_source_ts", "datetime", "dimension", None),
        ("freshness_min", "real", "measure", None),
        ("latency_p50_min", "real", "measure", None),
        ("latency_p95_min", "real", "measure", None),
        ("latency_p99_min", "real", "measure", None),
        ("is_backfill", "string", "dimension", None),
    ],
    "reconciliation_runs": [
        ("run_ts", "datetime", "dimension", None),
        ("mode", "string", "dimension", None),
        ("checks", "integer", "measure", None),
        ("mismatches", "integer", "measure", None),
    ],
    "dq_catch_rate": [
        ("run_ts", "datetime", "dimension", None),
        ("injected", "integer", "measure", None),
        ("caught", "integer", "measure", None),
        ("false_positives", "integer", "measure", None),
        ("flagged_total", "integer", "measure", None),
        ("catch_rate", "real", "measure", PCT),
    ],
}

# Calculated fields: source -> (name, caption, datatype, role, type, formula, uses_param)
CALCS = {
    # Merchant names repeat (several different "Smith Ltd"), so charts label merchants as "name #id".
    "settlement": [
        ("[Currency Match]", "Currency Match", "boolean", "dimension", "nominal", f"[currency] = {CCY_PARAM}", True),
        (
            "[Merchant]",
            "Merchant",
            "string",
            "dimension",
            "nominal",
            '[merchant_name] + " #" + STR([merchant_id])',
            False,
        ),
    ],
    "volume_10min": [
        ("[Currency Match]", "Currency Match", "boolean", "dimension", "nominal", f"[currency] = {CCY_PARAM}", True)
    ],
    "merchant_risk": [
        ("[Above 1pct]", "Above 1% Chargebacks", "boolean", "dimension", "nominal", "[chargeback_rate] >= 0.01", False),
        (
            "[Merchant]",
            "Merchant",
            "string",
            "dimension",
            "nominal",
            '[merchant_name] + " #" + STR([merchant_id])',
            False,
        ),
    ],
    "dq_flags": [("[Number of Records]", "Number of Records", "integer", "measure", "quantitative", "1", False)],
}

PARAM_COL = (
    "<column caption='Currency' datatype='string' name='[Parameter 1]' param-domain-type='list' role='measure' "
    "type='nominal' value='&quot;USD&quot;'>"
    "<calculation class='tableau' formula='&quot;USD&quot;' />"
    "<members><member value='&quot;USD&quot;' /><member value='&quot;EUR&quot;' /><member value='&quot;GBP&quot;' /></members>"
    "</column>"
)


def ds_name(src):
    return f"federated.payflow_{src}"


def col_def(src, name):
    """Datasource-level <column> element for a raw or calculated field."""
    for n, dt, role, fmt in SOURCES[src]:
        if n == name:
            typ = "quantitative" if role == "measure" else ("ordinal" if dt in ("date", "datetime") else "nominal")
            if fmt is None and dt == "integer" and role == "measure":
                fmt = "n#,##0"  # counts: 16, not 16.00
            f = f" default-format={a(fmt)}" if fmt else ""
            return f"<column caption={a(caption(n))} datatype={a(dt)}{f} name={a('[' + n + ']')} role={a(role)} type={a(typ)} />"
    for n, cap, dt, role, typ, formula, _ in CALCS.get(src, []):
        if n == f"[{name}]":
            return (
                f"<column caption={a(cap)} datatype={a(dt)} name={a(n)} role={a(role)} type={a(typ)}>"
                f"<calculation class='tableau' formula={a(formula)} /></column>"
            )
    raise KeyError(f"{src}.{name}")


def datasource(src):
    cols = SOURCES[src]
    rel = f"<relation connection='hyper.payflow_{src}' name='Extract' table='[Extract].[Extract]' type='table' />"
    meta = "".join(
        f"<metadata-record class='column'><remote-name>{n}</remote-name><remote-type>{REMOTE_TYPE[dt]}</remote-type>"
        f"<local-name>[{n}]</local-name><parent-name>[Extract]</parent-name><remote-alias>{n}</remote-alias>"
        f"<ordinal>{i}</ordinal><local-type>{dt}</local-type>"
        f"<aggregation>{'Sum' if dt in ('integer', 'real') else ('Year' if dt in ('date', 'datetime') else 'Count')}</aggregation>"
        f"<contains-null>true</contains-null></metadata-record>"
        for i, (n, dt, _, _) in enumerate(cols)
    )
    fields = "".join(col_def(src, n) for n, *_ in cols)
    fields += "".join(col_def(src, c[0][1:-1]) for c in CALCS.get(src, []))
    param_dep = ""
    if any(c[6] for c in CALCS.get(src, [])):
        param_dep = f"<datasource-dependencies datasource='Parameters'>{PARAM_COL}</datasource-dependencies>"
    return (
        f"<datasource caption={a(src)} inline='true' name={a(ds_name(src))} version='18.1'>"
        f"<connection class='federated'><named-connections>"
        f"<named-connection caption={a(src)} name='hyper.payflow_{src}'>"
        f"<connection authentication='auth-none' author-locale='en_US' class='hyper' dbname='Data/{src}.hyper' "
        f"default-settings='yes' port='' sslmode='' username='tableau_internal_user' />"
        f"</named-connection></named-connections>{rel}<metadata-records>{meta}</metadata-records></connection>"
        f"<aliases enabled='yes' />{fields}"
        f"<layout dim-ordering='alphabetic' dim-percentage='0.5' measure-percentage='0.4' measure-ordering='alphabetic' show-structure='true' />"
        f"{param_dep}</datasource>"
    )


class Sheet:
    """Collects the fields a worksheet uses so its datasource-dependencies are complete."""

    def __init__(self, name, src, title=None):
        self.name, self.src, self.title = name, src, title or name
        self.ds = ds_name(src)
        self.cols, self.instances = [], {}
        self.filters, self.slices, self.sorts = [], [], []
        self.uses_param = False

    def f(self, col, deriv="None", kind="nk"):
        """Reference a field; returns its fully qualified instance name, e.g. [ds].[sum:net:qk]."""
        if col not in self.cols:
            self.cols.append(col)
            if any(c[0] == f"[{col}]" and c[6] for c in CALCS.get(self.src, [])):
                self.uses_param = True
        prefix = {"None": "none", "Sum": "sum", "Max": "max", "Min": "min", "Avg": "avg"}[deriv]
        inst = f"[{prefix}:{col}:{kind}]"
        typ = {"nk": "nominal", "qk": "quantitative", "ok": "ordinal"}[kind]
        self.instances[inst] = (
            f"<column-instance column={a('[' + col + ']')} derivation={a(deriv)} name={a(inst)} pivot='key' type={a(typ)} />"
        )
        return f"[{self.ds}].{inst}"

    @property
    def mn(self):
        return f"[{self.ds}].[:Measure Names]"

    @property
    def mv(self):
        return f"[{self.ds}].[Multiple Values]"

    def measure_names(self, refs):
        members = "".join(
            f"<groupfilter function='member' level='[:Measure Names]' member={a(chr(34) + r + chr(34))} />"
            for r in refs
        )
        self.filters.append(
            f"<filter class='categorical' column={a(self.mn)}><groupfilter function='union' user:op='manual'>{members}</groupfilter></filter>"
        )
        buckets = "".join(f"<bucket>{'&quot;' + r + '&quot;'}</bucket>" for r in refs)
        self.sorts.append(
            f"<manual-sort column={a(self.mn)} direction='ASC'><dictionary>{buckets}</dictionary></manual-sort>"
        )
        self.slices.append(self.mn)

    def keep(self, ref, value, context=False):
        level = ref.split("].", 1)[1]
        ctx = " context='true'" if context else ""
        self.filters.append(
            f"<filter class='categorical' column={a(ref)}{ctx}><groupfilter function='member' level={a(level)} "
            f"member={a(value)} user:ui-domain='database' user:ui-enumeration='inclusive' user:ui-marker='enumerate' /></filter>"
        )
        self.slices.append(ref)

    def currency(self, context=False):
        self.keep(self.f("Currency Match"), "true", context)

    def at_least(self, ref, lo, hi=None):
        top = f"<max>{hi}</max>" if hi is not None else ""
        self.filters.append(
            f"<filter class='quantitative' column={a(ref)} included-values='in-range'><min>{lo}</min>{top}</filter>"
        )
        self.slices.append(ref)

    def top(self, ref, n, by_formula):
        level = ref.split("].", 1)[1]
        self.filters.append(
            f"<filter class='categorical' column={a(ref)}>"
            f"<groupfilter count='{n}' end='top' function='end' units='records' user:ui-marker='end' user:ui-top-by-field='true'>"
            f"<groupfilter direction='DESC' expression={a(by_formula)} function='order' user:ui-marker='order'>"
            f"<groupfilter function='level-members' level={a(level)} user:ui-enumeration='all' user:ui-marker='enumerate' />"
            f"</groupfilter></groupfilter></filter>"
        )
        self.slices.append(ref)

    def sort_desc(self, ref, using):
        self.sorts.append(f"<computed-sort column={a(ref)} direction='DESC' using={a(using)} />")

    def xml(self, rows="", cols="", mark="Automatic", encodings="", font_size=None):
        deps = "".join(col_def(self.src, c) for c in self.cols) + "".join(self.instances.values())
        dss = f"<datasource caption={a(self.src)} name={a(self.ds)} />"
        param_deps = ""
        if self.uses_param:
            dss += "<datasource name='Parameters' />"
            param_deps = f"<datasource-dependencies datasource='Parameters'>{PARAM_COL}</datasource-dependencies>"
        slices = "".join(f"<column>{s}</column>" for s in self.slices)
        slices = f"<slices>{slices}</slices>" if slices else ""
        style = ""
        if font_size:
            style = f"<style-rule element='cell'><format attr='font-size' value='{font_size}' /></style-rule>"
        return (
            f"<worksheet name={a(self.name)}>"
            f"<layout-options><title><formatted-text><run fontsize='13'>{self.title}</run></formatted-text></title></layout-options>"
            f"<table><view><datasources>{dss}</datasources>{param_deps}"
            f"<datasource-dependencies datasource={a(self.ds)}>{deps}</datasource-dependencies>"
            f"{''.join(self.filters)}{''.join(self.sorts)}{slices}<aggregation value='true' /></view>"
            f"<style>{style}</style>"
            f"<panes><pane selection-relaxation-option='selection-relaxation-allow'><view><breakdown value='auto' /></view>"
            f"<mark class={a(mark)} /><encodings>{encodings}</encodings></pane></panes>"
            f"<rows>{rows}</rows><cols>{cols}</cols></table></worksheet>"
        )


def worksheets():
    out = []

    # ---- Finance (settlement, volume_10min) ----
    s = Sheet("KPIs", "settlement", "Settlement totals")
    s.measure_names([s.f(c, "Sum", "qk") for c in ("gross", "fees", "refunds", "chargebacks", "net")])
    s.currency()
    out.append(s.xml(cols=s.mn, mark="Text", encodings=f"<text column={a(s.mv)} />", font_size=20))

    s = Sheet("Where the money goes", "settlement")
    s.measure_names([s.f(c, "Sum", "qk") for c in ("fees", "refunds", "chargebacks", "net")])
    s.currency()
    out.append(s.xml(rows=s.mv, cols=s.mn, mark="Bar", encodings=f"<color column={a(s.mn)} />"))

    s = Sheet("Top 10 merchants by net", "settlement")
    name, net = s.f("Merchant"), s.f("net", "Sum", "qk")
    s.currency(context=True)
    s.top(name, 10, "SUM([net])")
    s.sort_desc(name, net)
    out.append(s.xml(rows=name, cols=net, mark="Bar", encodings=f"<color column={a(s.f('category'))} />"))

    s = Sheet("Activity over time", "volume_10min", "Captured vs refunded, every 10 minutes (UTC), four-day run")
    t = s.f("time_10min", kind="qk")
    # Before 13:00 UTC on Oct 6 is the earlier test burst (initial load + throughput benchmark), not steady traffic.
    s.at_least(t, "#2026-10-06 13:00:00#", "#2100-01-01 00:00:00#")
    s.measure_names([s.f("captured", "Sum", "qk"), s.f("refunded", "Sum", "qk")])
    s.currency()
    out.append(s.xml(rows=s.mv, cols=t, mark="Line", encodings=f"<color column={a(s.mn)} />"))

    # ---- Risk (merchant_risk, dq_flags) ----
    s = Sheet(
        "Volume vs chargeback rate",
        "merchant_risk",
        "Volume vs chargeback rate (merchants with 20+ payments in 30 days)",
    )
    x, y = s.f("captured_30d", "Sum", "qk"), s.f("chargeback_rate", "Avg", "qk")
    s.at_least(x, 20)  # 1 chargeback out of 2 payments is a 50% rate but says nothing
    enc = (
        f"<color column={a(s.f('current_risk_tier'))} /><lod column={a(s.f('merchant_id'))} />"
        f"<lod column={a(s.f('merchant_name'))} />"
    )
    out.append(s.xml(rows=y, cols=x, mark="Circle", encodings=enc))

    s = Sheet("Flagged records by rule", "dq_flags")
    rule, cnt = s.f("rule_name"), s.f("Number of Records", "Sum", "qk")
    s.sort_desc(rule, cnt)
    out.append(s.xml(rows=rule, cols=cnt, mark="Bar", encodings=f"<color column={a(s.f('table_name'))} />"))

    s = Sheet("Largest merchants above 1%", "merchant_risk", "Largest merchants above 1% chargebacks")
    name, tier = s.f("Merchant"), s.f("current_risk_tier")
    vol = s.f("captured_30d", "Sum", "qk")
    s.measure_names([s.f("chargeback_rate", "Avg", "qk"), vol, s.f("disputes_lost_30d", "Sum", "qk")])
    s.keep(s.f("Above 1pct"), "true", context=True)
    s.top(name, 15, "SUM([captured_30d])")
    s.sort_desc(name, vol)
    out.append(s.xml(rows=f"({name} / {tier})", cols=s.mn, mark="Text", encodings=f"<text column={a(s.mv)} />"))

    # ---- Pipeline health (pipeline_runs, reconciliation_runs, dq_catch_rate) ----
    s = Sheet("Freshness per run", "pipeline_runs", "Minutes from source commit to Databricks, per run")
    t = s.f("run_ts", kind="qk")
    s.measure_names([s.f("latency_p50_min", "Sum", "qk"), s.f("latency_p95_min", "Sum", "qk")])
    s.keep(s.f("is_backfill"), '"False"')
    out.append(s.xml(rows=s.mv, cols=t, mark="Line", encodings=f"<color column={a(s.mn)} />"))

    s = Sheet("Events per run", "pipeline_runs")
    t, ev = s.f("run_ts", kind="qk"), s.f("events_processed", "Sum", "qk")
    s.keep(s.f("is_backfill"), '"False"')
    out.append(s.xml(rows=ev, cols=t, mark="Bar"))

    s = Sheet("Data quality", "dq_catch_rate", "Data quality: injected bad records caught (cumulative)")
    s.measure_names(
        [
            s.f("injected", "Max", "qk"),
            s.f("caught", "Max", "qk"),
            s.f("false_positives", "Max", "qk"),
            s.f("catch_rate", "Min", "qk"),
        ]
    )
    out.append(s.xml(cols=s.mn, mark="Text", encodings=f"<text column={a(s.mv)} />", font_size=16))

    s = Sheet("Reconciliation", "reconciliation_runs", "Reconciliation runs: Postgres vs Databricks")
    ts, mode = s.f("run_ts"), s.f("mode")
    checks = s.f("checks", "Sum", "qk")
    s.measure_names([checks, s.f("mismatches", "Sum", "qk")])
    s.at_least(checks, 1)
    out.append(s.xml(rows=f"({ts} / {mode})", cols=s.mn, mark="Text", encodings=f"<text column={a(s.mv)} />"))
    return out


class Layout:
    """Lays zones out in Tableau's 100000x100000 dashboard coordinate space."""

    def __init__(self):
        self.next_id = 1

    def id(self):
        self.next_id += 1
        return self.next_id

    def zone(self, x, y, w, h, **attrs):
        extra = "".join(f" {k.replace('_', '-')}={a(v)}" for k, v in attrs.items())
        return f"<zone h='{h}' id='{self.id()}' w='{w}' x='{x}' y='{y}'{extra} />"


def dashboard(name, title, rows, param=False):
    """rows: list of (height, [sheet names]) stacked vertically; sheets in a row split the width evenly."""
    L = Layout()
    pad, header = 800, 6000
    zones = [L.zone(pad, pad, 100000 - 2 * pad - (22000 if param else 0), header, type_v2="title")]
    if param:
        zones.append(
            L.zone(100000 - pad - 22000, pad, 22000, header, mode="compact", param=CCY_PARAM, type_v2="paramctrl")
        )
    y = pad + header
    usable = 100000 - y - pad
    total = sum(h for h, _ in rows)
    for i, (h, sheets) in enumerate(rows):
        hh = usable - (y - pad - header) if i == len(rows) - 1 else round(usable * h / total)
        w = (100000 - 2 * pad) // len(sheets)
        for j, sheet in enumerate(sheets):
            zones.append(L.zone(pad + j * w, y, w, hh, name=sheet))
        y += hh
    param_xml = ""
    if param:
        param_xml = (
            f"<datasources><datasource name='Parameters' /></datasources>"
            f"<datasource-dependencies datasource='Parameters'>{PARAM_COL}</datasource-dependencies>"
        )
    return (
        f"<dashboard name={a(name)}>"
        f"<layout-options><title><formatted-text><run fontsize='18'>{title}</run></formatted-text></title></layout-options>"
        f"<style /><size maxheight='850' maxwidth='1200' minheight='850' minwidth='1200' />{param_xml}"
        f"<zones><zone h='100000' id='1' type-v2='layout-basic' w='100000' x='0' y='0'>{''.join(zones)}</zone></zones>"
        f"</dashboard>"
    )


DASHBOARDS = [
    (
        "Finance",
        "PayFlow Finance: settlement by currency",
        [(15, ["KPIs"]), (35, ["Activity over time"]), (50, ["Top 10 merchants by net", "Where the money goes"])],
        True,
    ),
    (
        "Risk",
        "PayFlow Risk: chargebacks and data quality",
        [(50, ["Volume vs chargeback rate"]), (50, ["Flagged records by rule", "Largest merchants above 1%"])],
        False,
    ),
    (
        "Pipeline health",
        "PayFlow Pipeline health: freshness, volume, correctness",
        [(38, ["Freshness per run"]), (30, ["Events per run"]), (32, ["Data quality", "Reconciliation"])],
        False,
    ),
]


PARAM_SHEETS = {"KPIs", "Where the money goes", "Top 10 merchants by net", "Activity over time"}


def windows():
    out = []
    for _name, _, rows, _ in DASHBOARDS:
        for _, sheets in rows:
            for s in sheets:
                right = ""
                if s in PARAM_SHEETS:
                    right = f"<edge name='right'><strip size='160'><card param={a(CCY_PARAM)} type='parameter' /></strip></edge>"
                out.append(
                    f"<window class='worksheet' name={a(s)}><cards>"
                    f"<edge name='left'><strip size='160'><card type='pages' /><card type='filters' /><card type='marks' /></strip></edge>"
                    f"<edge name='top'><strip size='2147483647'><card type='columns' /></strip>"
                    f"<strip size='2147483647'><card type='rows' /></strip><strip size='31'><card type='title' /></strip></edge>"
                    f"{right}</cards></window>"
                )
    for name, _, rows, _ in DASHBOARDS:
        vps = "".join(f"<viewpoint name={a(s)}><zoom type='entire-view' /></viewpoint>" for _, ss in rows for s in ss)
        maximized = " maximized='true'" if name == "Finance" else ""
        out.append(
            f"<window class='dashboard'{maximized} name={a(name)}><viewpoints>{vps}</viewpoints><active id='-1' /></window>"
        )
    return "".join(out)


HYPER_TYPE = {
    "string": SqlType.text(),
    "integer": SqlType.big_int(),
    "real": SqlType.double(),
    "date": SqlType.date(),
    "datetime": SqlType.timestamp(),
}


def build_extracts(out_dir):
    """CSV -> .hyper, one file per source, each holding the table "Extract"."Extract" as Tableau expects."""
    # Hyper writes hyperd.log into the working directory by default; keep it in the temp dir with the extracts.
    log_dir = {"log_dir": str(out_dir.parent)}
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=log_dir) as hyper:
        for src, cols in SOURCES.items():
            table = TableDefinition(
                TableName("Extract", "Extract"),
                [TableDefinition.Column(n, HYPER_TYPE[dt]) for n, dt, _, _ in cols],
            )
            with Connection(hyper.endpoint, out_dir / f"{src}.hyper", CreateMode.CREATE_AND_REPLACE) as conn:
                conn.catalog.create_schema("Extract")
                conn.catalog.create_table(table)
                csv_path = escape_string_literal(str(DATA_DIR / f"{src}.csv"))
                rows = conn.execute_command(f"COPY {table.table_name} FROM {csv_path} WITH (format csv, header)")
            print(f"  {src}.hyper: {rows:,} rows")


def build():
    for src in SOURCES:
        if not (DATA_DIR / f"{src}.csv").exists():
            raise SystemExit(f"missing {DATA_DIR / src}.csv; run `make tableau-export` first")
    param_ds = f"<datasource hasconnection='false' inline='true' name='Parameters' version='18.1'><aliases enabled='yes' />{PARAM_COL}</datasource>"
    xml = (
        "<?xml version='1.0' encoding='utf-8' ?>\n"
        "<workbook source-build='2026.2.3 (20262.26.0912.1023)' source-platform='mac' version='18.1' "
        "xmlns:user='http://www.tableausoftware.com/xml/user'>"
        "<document-format-change-manifest><SortTagCleanup /></document-format-change-manifest>"
        f"<preferences /><datasources>{param_ds}{''.join(datasource(s) for s in SOURCES)}</datasources>"
        f"<worksheets>{''.join(worksheets())}</worksheets>"
        f"<dashboards>{''.join(dashboard(*d) for d in DASHBOARDS)}</dashboards>"
        f"<windows>{windows()}</windows></workbook>\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "Data"
        data.mkdir()
        build_extracts(data)
        (Path(tmp) / "PayFlow.twb").write_text(xml, encoding="utf-8")
        with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(Path(tmp) / "PayFlow.twb", "PayFlow.twb")
            for f in sorted(data.iterdir()):
                z.write(f, f"Data/{f.name}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    build()
