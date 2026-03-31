#!/usr/bin/env python3
"""
PR 데이터 요청 처리 웹앱
- 요청사항 + CSV 업로드 → 자동 파악 → 엑셀 다운로드
- 지표(MAU/DAU/WAU/신규설치), 연령대, 앱 이름 모두 CSV에서 자동 감지
"""
import csv
import re
import io
import base64
from datetime import date
from flask import Flask, request, render_template, send_file, jsonify, make_response
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill, Color
from openpyxl.utils import get_column_letter

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

# ── 스타일 ──
FONT_STYLE = Font(name="맑은 고딕", size=12)
THIN_BORDER = Border(
    top=Side(style="thin"), bottom=Side(style="thin"),
    left=Side(style="thin"), right=Side(style="thin"),
)
HEADER_FILL = PatternFill(patternType="solid", fgColor=Color(theme=3, tint=0.9))
NUM_FMT = '_(* #,##0_);_(* \\(#,##0\\);_(* "-"_);_(@_)'
CENTER = Alignment(horizontal="center", vertical="center")
CENTER_CONT = Alignment(horizontal="centerContinuous", vertical="center")
VCENTER = Alignment(vertical="center")


def apply_style(cell, is_header=False, is_metric=False, is_number=False):
    cell.font = FONT_STYLE
    cell.border = THIN_BORDER
    if is_metric:
        cell.alignment = CENTER_CONT
        cell.fill = HEADER_FILL
    elif is_header:
        cell.alignment = CENTER
    elif is_number:
        cell.alignment = VCENTER
        cell.number_format = NUM_FMT
    else:
        cell.alignment = CENTER


def detect_metric(header_lines):
    text = " ".join(header_lines)
    if "신규 설치" in text or "신규설치" in text:
        label = "신규설치건수"
    elif "사용자 수" in text:
        label = "AU"
    else:
        label = "AU"
    if "월간" in text:
        p = "M"
    elif "주간" in text:
        p = "W"
    elif "일간" in text:
        p = "D"
    else:
        p = "M"
    if label == "AU":
        return f"{p}AU"
    prefixes = {"M": "월간", "W": "주간", "D": "일간"}
    return f"{prefixes[p]} {label}"


def detect_age_group(header_lines):
    text = " ".join(header_lines)
    m = re.search(r"연령:\s*(.+?)\)", text)
    return m.group(1).strip() if m else None


def detect_period_type(dates):
    if not dates:
        return "monthly"
    s = dates[0]
    if re.match(r"^\d{4}-\d{2}$", s):
        return "monthly"
    if "~" in s:
        return "weekly"
    return "daily"


def format_date_label(date_str, period_type):
    if period_type == "monthly":
        m = re.match(r"(\d{4})-(\d{2})", date_str)
        if m:
            return f"{m.group(1)}년 {int(m.group(2))}월"
    return date_str


def parse_csv_content(content):
    lines = content.strip().split("\n")
    header_lines, col_headers, data_rows = [], None, []
    data_started = False
    for line in lines:
        stripped = line.strip().strip('"')
        if not data_started:
            row = list(csv.reader([line]))[0]
            if not row or not row[0].strip():
                header_lines.append(stripped)
                continue
            first = row[0].strip().strip('"')
            if first in ("패키지명", "날짜"):
                data_started = True
                col_headers = [c.strip().strip('"') for c in row]
                continue
            header_lines.append(stripped)
        else:
            row = list(csv.reader([line]))[0]
            if row and len(row) >= 2:
                data_rows.append([c.strip().strip('"') for c in row])
    return header_lines, col_headers, data_rows


def convert(csv_files, media_name):
    """CSV 파일들을 분석하여 자동으로 엑셀 생성. 반환: (BytesIO, warnings)"""
    warnings = []
    # 1) 모든 CSV 파싱
    parsed = []
    for filename, content in csv_files:
        header_lines, col_headers, data_rows = parse_csv_content(content)
        if not col_headers:
            warnings.append(f"'{filename}' 파일에서 데이터를 찾을 수 없어요. 다시 확인해주세요.")
            continue
        if not data_rows:
            warnings.append(f"'{filename}' 파일에 데이터 행이 없어요. 다시 확인해주세요.")
            continue
        metric = detect_metric(header_lines)
        age = detect_age_group(header_lines)
        first_col = col_headers[0]

        # 날짜/앱 컬럼 추출
        if first_col == "패키지명":
            # 단일 앱 형태: 패키지명, 날짜, 값
            dates = [row[1] for row in data_rows]
            app_name = metric  # 단일 값이므로 지표명 사용
            vals = []
            for row in data_rows:
                raw = row[2].replace(",", "") if len(row) > 2 else ""
                vals.append(int(raw) if raw and raw != "-" else None)
            app_columns = [(media_name, vals)]
        else:
            # 비교 분석 형태: 날짜, 앱1, 앱2, ...
            dates = [row[0] for row in data_rows]
            app_columns = []
            for i in range(1, len(col_headers)):
                name = col_headers[i]
                short = name.split("–")[0].split("-")[0].strip()
                vals = []
                for row in data_rows:
                    raw = row[i].replace(",", "") if i < len(row) else ""
                    vals.append(int(raw) if raw and raw != "-" and raw else None)
                app_columns.append((short, vals))

        parsed.append({
            "metric": metric,
            "age": age or "전체",
            "dates": dates,
            "apps": app_columns,
        })

    if not parsed:
        return None, warnings, []

    # 2) 그룹핑: (metric, age) 조합별로 정렬
    age_order = ["전체", "10대 이하", "20대", "30대", "40대", "50대", "60대 이상"]
    metric_order = ["MAU", "WAU", "DAU", "월간 신규설치건수", "일간 신규설치건수"]

    def sort_key(item):
        m = item["metric"]
        mi = metric_order.index(m) if m in metric_order else 99
        a = item["age"]
        ai = age_order.index(a) if a in age_order else 99
        return (mi, ai)

    parsed.sort(key=sort_key)

    # 3) 엑셀 생성
    wb = Workbook()
    ws = wb.active
    ws.title = f"MI-{media_name}"

    col_cursor = 2
    has_ages = len(set(p["age"] for p in parsed)) > 1

    for item in parsed:
        dates = item["dates"]
        apps = item["apps"]
        metric = item["metric"]
        age = item["age"]
        period_type = detect_period_type(dates)
        block_width = 1 + len(apps)

        # 지표 라벨
        label = f"{metric} ({age})" if has_ages else metric

        # 6행: 지표 헤더
        for c in range(block_width):
            apply_style(ws.cell(row=6, column=col_cursor + c), is_metric=True)
        ws.cell(row=6, column=col_cursor).value = label

        # 7행: 날짜 + 앱 헤더
        ws.cell(row=7, column=col_cursor).value = "날짜"
        apply_style(ws.cell(row=7, column=col_cursor), is_header=True)
        ws.column_dimensions[get_column_letter(col_cursor)].width = 23.86

        for idx, (app_name, _) in enumerate(apps):
            vc = col_cursor + 1 + idx
            ws.cell(row=7, column=vc).value = app_name
            apply_style(ws.cell(row=7, column=vc), is_header=True)
            ws.column_dimensions[get_column_letter(vc)].width = 15

        # 8행~: 데이터
        for i, d in enumerate(dates):
            r = 8 + i
            dc = ws.cell(row=r, column=col_cursor)
            dc.value = format_date_label(d, period_type)
            dc.number_format = "@"
            apply_style(dc)
            for idx, (_, vals) in enumerate(apps):
                vc = col_cursor + 1 + idx
                cell = ws.cell(row=r, column=vc)
                if vals[i] is None:
                    cell.value = "-"
                    cell.font = FONT_STYLE
                    cell.border = THIN_BORDER
                    cell.alignment = CENTER
                else:
                    cell.value = vals[i]
                    apply_style(cell, is_number=True)

        col_cursor += block_width + 1

    # 4) 미리보기 데이터 생성
    preview = []
    has_ages = len(set(p["age"] for p in parsed)) > 1
    for item in parsed:
        dates = item["dates"]
        apps = item["apps"]
        metric = item["metric"]
        age = item["age"]
        period_type = detect_period_type(dates)
        label = f"{metric} ({age})" if has_ages else metric
        headers = ["날짜"] + [a[0] for a in apps]
        rows = []
        for i, d in enumerate(dates):
            row = [format_date_label(d, period_type)]
            for _, vals in apps:
                v = vals[i]
                row.append(f"{v:,}" if v is not None else "-")
            rows.append(row)
        preview.append({"label": label, "headers": headers, "rows": rows})

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf, warnings, preview


# ── 라우트 ──
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/convert", methods=["POST"])
def api_convert():
    media_name = request.form.get("media_name", "").strip()
    if not media_name:
        return jsonify({"error": "매체명을 입력해주세요."}), 400

    files = request.files.getlist("csv_files")
    if not files or not files[0].filename:
        return jsonify({"error": "CSV 파일을 업로드해주세요."}), 400

    csv_files = []
    for f in files:
        content = f.read().decode("utf-8-sig")
        csv_files.append((f.filename, content))

    buf, warnings, preview = convert(csv_files, media_name)
    if not buf:
        error_msg = "변환에 실패했습니다. CSV 형식을 확인해주세요."
        return jsonify({"error": error_msg, "warnings": warnings}), 400

    filename = f"MI-{media_name}_요청_데이터.xlsx"
    file_b64 = base64.b64encode(buf.read()).decode("ascii")
    return jsonify({
        "filename": filename,
        "file": file_b64,
        "warnings": warnings,
        "preview": preview,
    })


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
