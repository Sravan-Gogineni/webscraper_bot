import json
import os
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from queue import Empty, Queue
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin, urlparse
import hashlib
from collections import OrderedDict

from requests.adapters import HTTPAdapter

import google.generativeai as genai
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import (
    Blueprint,
    Flask,
    Response,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from sqlalchemy import MetaData, create_engine, func, select, case
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import sqltypes


load_dotenv()

thread_local = threading.local()

LLM_MAX_CONCURRENCY = int(os.environ.get("LLM_MAX_CONCURRENCY", "2"))  # legacy, not used for Gemini

# Simple in-memory LRU cache for LLM prompts to reduce repeated calls
class LRUCache:
    def __init__(self, capacity: int = 128):
        self.capacity = max(8, capacity)
        self.store: OrderedDict[str, str] = OrderedDict()

    def get(self, key: str) -> Optional[str]:
        value = self.store.get(key)
        if value is not None:
            self.store.move_to_end(key)
        return value

    def set(self, key: str, value: str) -> None:
        if key in self.store:
            self.store.move_to_end(key)
        self.store[key] = value
        if len(self.store) > self.capacity:
            self.store.popitem(last=False)

LLM_CACHE = LRUCache(capacity=int(os.environ.get("LLM_CACHE_SIZE", "512")))

admin_bp = Blueprint("admin", __name__)
extract_bp = Blueprint("extract", __name__)
llm_test_bp = Blueprint("llm_test", __name__, url_prefix="/llm-test")
forms_bp = Blueprint("forms", __name__, url_prefix="/forms")
crawler_bp = Blueprint("crawler", __name__, url_prefix="/crawler")

CRAWL_JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

COLLEGE_FIELD_NAMES: List[str] = [
    "CollegeName",
    "LogoPath",
    "Phone",
    "Email",
    "SecondaryEmail",
    "Street1",
    "Street2",
    "County",
    "City",
    "State",
    "Country",
    "ZipCode",
    "WebsiteUrl",
    "AdmissionOfficeUrl",
    "VirtualTourUrl",
    "Facebook",
    "Instagram",
    "Twitter",
    "Youtube",
    "Tiktok",
    "ApplicationFees",
    "TestPolicy",
    "CoursesAndGrades",
    "Recommendations",
    "PersonalEssay",
    "WritingSample",
    "FinancialAidUrl",
    "AdditionalInformation",
    "AdditionalDeadlines",
    "TuitionFees",
    "LinkedIn",
    "NumberOfCampuses",
    "TotalFacultyAvailable",
    "TotalProgramsAvailable",
    "TotalStudentsEnrolled",
    "CollegeSetting",
    "TypeofInstitution",
    "CountriesRepresented",
    "GradAvgTuition",
    "GradInternationalStudents",
    "GradScholarshipHigh",
    "GradScholarshipLow",
    "GradTotalStudents",
    "Student_Faculty",
    "TotalGraduatePrograms",
    "TotalInternationalStudents",
    "TotalStudents",
    "TotalUndergradMajors",
    "UGAvgTuition",
    "UGInternationalStudents",
    "UGScholarshipHigh",
    "UGScholarshipLow",
    "UGTotalStudents",
]

DEPARTMENT_FIELD_NAMES: List[str] = [
    "DepartmentName",
    "Description",
    "City",
    "Country",
    "CountryCode",
    "CountryName",
    "Email",
    "PhoneNumber",
    "PhoneType",
    "State",
    "Street1",
    "Street2",
    "ZipCode",
    "StateName",
    "AdmissionUrl",
    "BuildingName",
]

PROGRAM_FIELD_NAMES: List[str] = [
    "ProgramName",
    "Level",
    "Term",
    "LiveDate",
    "DeadlineDate",
    "Resume",
    "StatementOfPurpose",
    "GreOrGmat",
    "EnglishScore",
    "Requirements",
    "WritingSample",
    "CollegeID",
    "CollegeDepartmentID",
    "IsAnalyticalNotRequired",
    "IsAnalyticalOptional",
    "IsDuoLingoRequired",
    "IsELSRequired",
    "IsGMATOrGreRequired",
    "IsGMATRequired",
    "IsGreRequired",
    "IsIELTSRequired",
    "IsLSATRequired",
    "IsMATRequired",
    "IsMCATRequired",
    "IsPTERequired",
    "IsTOEFLIBRequired",
    "IsTOEFLPBTRequired",
    "IsEnglishNotRequired",
    "IsEnglishOptional",
    "Department",
    "Fees",
    "Concentration",
    "Description",
    "ProgramWebsiteURL",
    "Accreditation",
    "AverageScholarshipAmount",
    "CostPerCredit",
    "IsRecommendationSystemOpted",
    "IsStemProgram",
    "MaxFails",
    "MaxGPA",
    "MinGPA",
    "PreviousYearAcceptanceRates",
    "QsWorldRanking",
    "IsACTRequired",
    "IsSATRequired",
    "MinimumACTScore",
    "MinimumDuoLingoScore",
    "MinimumELSScore",
    "MinimumGMATScore",
    "MinimumGreScore",
    "MinimumIELTSScore",
    "MinimumMATScore",
    "MinimumMCATScore",
    "MinimumPTEScore",
    "MinimumSATScore",
    "MinimumTOEFLScore",
    "ScholarshipAmount",
    "ScholarshipPercentage",
    "ScholarshipType",
    "MinimumLSATScore",
]


def default_worker_count() -> int:
    cpu_count = os.cpu_count() or 4
    return max(2, min(32, cpu_count * 2))


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me")

    default_headers = {
        "User-Agent": os.environ.get(
            "SCRAPER_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ),
        "Accept-Language": os.environ.get("SCRAPER_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    }
    app.config["SCRAPER_HEADERS"] = default_headers

    api_key = os.environ.get("GOOGLE_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)

    engine = None
    metadata: Optional[MetaData] = None
    db_error: Optional[str] = None

    connection_url = build_db_connection_url()
    if connection_url:
        try:
            engine = create_engine(connection_url, pool_pre_ping=True)
            metadata = MetaData()
            metadata.reflect(bind=engine)
        except SQLAlchemyError as exc:
            db_error = str(exc)
            app.logger.error("Failed to connect to database: %s", exc)
    else:
        db_error = (
            "Database configuration is incomplete. "
            "Set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD in your environment."
        )

    table_map = {}
    if metadata:
        table_map = {name.lower(): name for name in metadata.tables.keys()}

    app.config.update(
        DB_ENGINE=engine,
        DB_METADATA=metadata,
        DB_TABLE_MAP=table_map,
        DB_ERROR=db_error,
    )

    app.register_blueprint(admin_bp)
    app.register_blueprint(extract_bp)
    app.register_blueprint(llm_test_bp)
    app.register_blueprint(forms_bp)
    app.register_blueprint(crawler_bp)
    app.add_url_rule("/crawler/progress/<job_id>", "crawler_progress", stream_crawl_progress)

    @app.context_processor
    def inject_globals() -> Dict[str, Any]:
        return {
        "nav_tables": sorted(app.config.get("DB_TABLE_MAP", {}).values()),
        "default_workers": default_worker_count(),
        }

    return app


def build_db_connection_url() -> Optional[str]:
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

    if not all([database, username, password]):
        return None

    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_params)}"


@admin_bp.route("/")
def admin_index():
    metadata = current_app.config.get("DB_METADATA")
    db_error = current_app.config.get("DB_ERROR")
    tables = sorted(current_app.config.get("DB_TABLE_MAP", {}).values()) if metadata else []
    return render_template("admin/index.html", tables=tables, db_error=db_error)


@admin_bp.route("/admin/<table_name>/")
def admin_table(table_name: str):
    table, real_name = resolve_table(table_name)
    engine = get_engine()

    limit = request.args.get("limit", type=int) or 50
    limit = max(1, min(limit, 500))

    with engine.connect() as conn:
        result = conn.execute(select(table).limit(limit))
        rows = [dict(row._mapping) for row in result]

    pk_column = next(iter(table.primary_key.columns)) if table.primary_key.columns else None

    return render_template(
        "admin/table_list.html",
        table_name=real_name,
        columns=list(table.columns),
        rows=rows,
        pk_column=pk_column,
        limit=limit,
    )


@admin_bp.route("/admin/<table_name>/new", methods=["GET", "POST"])
def admin_create_record(table_name: str):
    table, real_name = resolve_table(table_name)
    engine = get_engine()
    fields = build_fields(table)

    form_values = build_form_initial_values(fields, None)

    if request.method == "POST":
        try:
            values = extract_form_values(fields, request.form)
        except ValueError as exc:
            flash(str(exc), "error")
            form_values = build_form_initial_values(fields, request.form)
        else:
            try:
                with engine.begin() as conn:
                    conn.execute(table.insert().values(**values))
                flash(f"{real_name}: record created successfully.", "success")
                return redirect(url_for("admin.admin_table", table_name=real_name))
            except SQLAlchemyError as exc:
                flash(f"Database error: {exc}", "error")
                form_values = build_form_initial_values(fields, request.form)

    return render_template(
        "admin/form.html",
        table_name=real_name,
        fields=fields,
        form_values=form_values,
        action="Create",
        pk_value=None,
    )


@admin_bp.route("/admin/<table_name>/<pk_value>/edit", methods=["GET", "POST"])
def admin_edit_record(table_name: str, pk_value: str):
    table, real_name = resolve_table(table_name)
    engine = get_engine()
    pk_column = get_primary_key_column(table)
    typed_pk = convert_raw_value(pk_column, pk_value)

    with engine.connect() as conn:
        existing = (
            conn.execute(select(table).where(pk_column == typed_pk)).mappings().first()
        )

    if not existing:
        abort(404, f"Record not found for {real_name} with id {pk_value}")

    fields = build_fields(table)
    form_values = build_form_initial_values(fields, existing)

    if request.method == "POST":
        try:
            values = extract_form_values(fields, request.form)
        except ValueError as exc:
            flash(str(exc), "error")
            form_values = build_form_initial_values(fields, request.form)
        else:
            try:
                with engine.begin() as conn:
                    conn.execute(
                        table.update()
                        .where(pk_column == typed_pk)
                        .values(**values)
                    )
                flash(f"{real_name}: record updated successfully.", "success")
                return redirect(url_for("admin.admin_table", table_name=real_name))
            except SQLAlchemyError as exc:
                flash(f"Database error: {exc}", "error")
                form_values = build_form_initial_values(fields, request.form)

    return render_template(
        "admin/form.html",
        table_name=real_name,
        fields=fields,
        form_values=form_values,
        action="Edit",
        pk_value=pk_value,
    )


@admin_bp.route("/admin/<table_name>/<pk_value>/delete", methods=["GET", "POST"])
def admin_delete_record(table_name: str, pk_value: str):
    table, real_name = resolve_table(table_name)
    engine = get_engine()
    pk_column = get_primary_key_column(table)
    typed_pk = convert_raw_value(pk_column, pk_value)

    if request.method == "POST":
        try:
            with engine.begin() as conn:
                conn.execute(table.delete().where(pk_column == typed_pk))
            flash(f"{real_name}: record deleted.", "success")
            return redirect(url_for("admin.admin_table", table_name=real_name))
        except SQLAlchemyError as exc:
            flash(f"Database error: {exc}", "error")

    return render_template(
        "admin/confirm_delete.html",
        table_name=real_name,
        pk_value=pk_value,
    )


@forms_bp.route("/universities/")
def university_list():
    engine = get_engine()
    college_table = fetch_table("College")
    address_table = fetch_table("Address", required=False)
    contact_table = fetch_table("ContactInformation", required=False)

    columns = [
        college_table.c.CollegeID.label("CollegeID"),
        college_table.c.CollegeName.label("CollegeName"),
    ]

    join_from = college_table

    if address_table is not None:
        columns.extend(
            [
                address_table.c.City.label("City"),
                address_table.c.State.label("State"),
            ]
        )
        join_from = join_from.outerjoin(
            address_table, address_table.c.CollegeID == college_table.c.CollegeID
        )

    if contact_table is not None:
        columns.extend(
            [
                contact_table.c.Phone.label("Phone"),
                contact_table.c.Email.label("Email"),
            ]
        )
        join_from = join_from.outerjoin(
            contact_table, contact_table.c.CollegeID == college_table.c.CollegeID
        )

    stmt = select(*columns).select_from(join_from).order_by(college_table.c.CollegeName)

    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]

    return render_template("forms/university_list.html", rows=rows)


@forms_bp.route("/universities/new", methods=["GET", "POST"])
def university_create():
    return handle_university_form(None)


@forms_bp.route("/universities/<int:college_id>/edit", methods=["GET", "POST"])
def university_edit(college_id: int):
    return handle_university_form(college_id)


@forms_bp.route("/universities/<int:college_id>/delete", methods=["POST"])
def university_delete(college_id: int):
    """Delete a university and related records."""
    try:
        engine = get_engine()
    except Exception:
        flash("Database is not configured.", "error")
        return redirect(url_for("forms.university_list"))
    
    try:
        college_table = fetch_table("College")
        address_table = fetch_table("Address", required=False)
        contact_table = fetch_table("ContactInformation", required=False)
        app_req_table = fetch_table("ApplicationRequirements", required=False)
        stats_table = fetch_table("StudentStatistics", required=False)
        social_table = fetch_table("SocialMedia", required=False)
        
        with engine.begin() as conn:
            # Delete related records first (due to foreign key constraints)
            if social_table is not None:
                conn.execute(
                    social_table.delete().where(social_table.c.CollegeID == college_id)
                )
            if stats_table is not None:
                conn.execute(
                    stats_table.delete().where(stats_table.c.CollegeID == college_id)
                )
            if app_req_table is not None:
                conn.execute(
                    app_req_table.delete().where(app_req_table.c.CollegeID == college_id)
                )
            if contact_table is not None:
                conn.execute(
                    contact_table.delete().where(contact_table.c.CollegeID == college_id)
                )
            if address_table is not None:
                conn.execute(
                    address_table.delete().where(address_table.c.CollegeID == college_id)
                )
            # Finally delete the college
            conn.execute(
                college_table.delete().where(college_table.c.CollegeID == college_id)
            )
        
        flash(f"University (ID: {college_id}) and related records deleted successfully.", "success")
    except Exception as exc:
        flash(f"Failed to delete university: {exc}", "error")
    
    return redirect(url_for("forms.university_list"))


def handle_university_form(college_id: Optional[int]):
    sections = build_university_sections()
    all_fields = [field for section in sections for field in section["fields"]]
    social_form_keys = [f"SocialMedia.{platform}" for platform in SOCIAL_MEDIA_PLATFORMS]

    engine = get_engine()
    existing_data: Dict[str, Dict[str, Any]] = {}
    social_existing: Dict[str, Any] = {}

    if college_id is not None:
        existing_data, social_existing = load_university_bundle(engine, college_id)
        if not existing_data.get("College"):
            abort(404, f"College with id {college_id} was not found.")

    form_values = compose_prefixed_initial_values(all_fields, existing_data)
    form_values.update(build_social_form_values(social_existing))

    if request.method == "POST":
        submitted_values = compose_prefixed_request_values(all_fields, request.form)
        submitted_values.update(
            {key: request.form.get(key, "") for key in social_form_keys}
        )
        try:
            table_payloads = extract_prefixed_values(all_fields, request.form)
            social_payloads = extract_social_values(request.form)
        except ValueError as exc:
            flash(str(exc), "error")
            form_values = submitted_values
        else:
            try:
                college_id = persist_university_bundle(engine, college_id, table_payloads, social_payloads)
            except (SQLAlchemyError, ValueError) as exc:
                flash(f"Database error: {exc}", "error")
                form_values = submitted_values
            else:
                flash("University details saved successfully.", "success")
                return redirect(url_for("forms.university_list"))

    form_title = "Create University" if college_id is None else "Edit University"
    submit_label = "Create" if college_id is None else "Update"

    form_hint = (
        "Complete each section to build a welcoming university profile. "
        "You can return later to update details as programs evolve."
    )

    return render_template(
        "forms/university_form.html",
        sections=sections,
        form_values=form_values,
        social_platforms=SOCIAL_MEDIA_PLATFORMS,
        form_title=form_title,
        submit_label=submit_label,
        college_id=college_id,
        form_hint=form_hint,
    )


def build_university_sections() -> List[Dict[str, Any]]:
    mapping = [
        {
            "title": "College Overview",
            "table": "College",
            "columns": [
                "CollegeName",
                "CollegeSetting",
                "TypeofInstitution",
                "Student_Faculty",
                "NumberOfCampuses",
                "TotalFacultyAvailable",
                "TotalProgramsAvailable",
                "TotalStudentsEnrolled",
                "TotalGraduatePrograms",
                "TotalInternationalStudents",
                "TotalStudents",
                "TotalUndergradMajors",
                "CountriesRepresented",
            ],
            "description": "Start with the essential profile information that introduces the institution at a glance.",
        },
        {
            "title": "Primary Location",
            "table": "Address",
            "columns": ["Street1", "Street2", "County", "City", "State", "Country", "ZipCode"],
            "description": "Provide the main campus mailing details so students know where to find you.",
        },
        {
            "title": "Contact & Online Presence",
            "table": "ContactInformation",
            "columns": [
                "LogoPath",
                "Phone",
                "Email",
                "SecondaryEmail",
                "WebsiteUrl",
                "AdmissionOfficeUrl",
                "VirtualTourUrl",
                "FinancialAidUrl",
            ],
            "description": "Make it easy to reach admissions and explore the university online.",
        },
        {
            "title": "Application Snapshot",
            "table": "ApplicationRequirements",
            "columns": [
                "ApplicationFees",
                "TuitionFees",
                "TestPolicy",
                "CoursesAndGrades",
                "Recommendations",
                "PersonalEssay",
                "WritingSample",
                "AdditionalInformation",
                "AdditionalDeadlines",
            ],
            "description": "Outline fees and core application materials so applicants can prepare quickly.",
        },
        {
            "title": "Student Body & Funding",
            "table": "StudentStatistics",
            "columns": [
                "GradAvgTuition",
                "GradInternationalStudents",
                "GradScholarshipHigh",
                "GradScholarshipLow",
                "GradTotalStudents",
                "UGAvgTuition",
                "UGInternationalStudents",
                "UGScholarshipHigh",
                "UGScholarshipLow",
                "UGTotalStudents",
            ],
            "description": "Share tuition trends and scholarship ranges to set expectations for prospective students.",
        },
    ]

    sections: List[Dict[str, Any]] = []
    for item in mapping:
        table = fetch_table(item["table"], required=False)
        fields = build_prefixed_fields(table, item["columns"])
        if fields:
            sections.append(
                {
                    "title": item["title"],
                    "table": table,
                    "fields": fields,
                    "description": item.get("description"),
                }
            )
    return sections


def find_college_by_name(engine, college_name: str) -> Optional[Dict[str, Any]]:
    """Find an existing college by name (case-insensitive)."""
    if not college_name or not college_name.strip():
        return None
    college_table = fetch_table("College")
    search_name = college_name.strip()
    with engine.connect() as conn:
        # Use SQLAlchemy func.upper() for case-insensitive matching (works with SQL Server)
        result = conn.execute(
            select(college_table).where(
                func.upper(college_table.c.CollegeName) == func.upper(search_name)
            )
        ).mappings().first()
        return dict(result) if result else None


def load_university_bundle(engine, college_id: int) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    college_table = fetch_table("College")
    address_table = fetch_table("Address", required=False)
    contact_table = fetch_table("ContactInformation", required=False)
    app_req_table = fetch_table("ApplicationRequirements", required=False)
    stats_table = fetch_table("StudentStatistics", required=False)
    social_table = fetch_table("SocialMedia", required=False)

    bundle: Dict[str, Dict[str, Any]] = {}
    social: Dict[str, Any] = {}

    with engine.connect() as conn:
        college = conn.execute(
            select(college_table).where(college_table.c.CollegeID == college_id)
        ).mappings().first()
        if college:
            bundle["College"] = dict(college)

        if address_table is not None:
            address = conn.execute(
                select(address_table).where(address_table.c.CollegeID == college_id)
            ).mappings().first()
            if address:
                bundle["Address"] = dict(address)

        if contact_table is not None:
            contact = conn.execute(
                select(contact_table).where(contact_table.c.CollegeID == college_id)
            ).mappings().first()
            if contact:
                bundle["ContactInformation"] = dict(contact)

        if app_req_table is not None:
            app_req = conn.execute(
                select(app_req_table).where(app_req_table.c.CollegeID == college_id)
            ).mappings().first()
            if app_req:
                bundle["ApplicationRequirements"] = dict(app_req)

        if stats_table is not None:
            stats = conn.execute(
                select(stats_table).where(stats_table.c.CollegeID == college_id)
            ).mappings().first()
            if stats:
                bundle["StudentStatistics"] = dict(stats)

        if social_table is not None:
            rows = conn.execute(
                select(social_table).where(social_table.c.CollegeID == college_id)
            ).mappings().all()
            social = {row["PlatformName"].lower(): dict(row) for row in rows}

    return bundle, social


def build_social_form_values(social_existing: Dict[str, Any]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for platform in SOCIAL_MEDIA_PLATFORMS:
        row = social_existing.get(platform.lower())
        values[f"SocialMedia.{platform}"] = row.get("URL", "") if row else ""
    return values


def extract_social_values(form_data) -> Dict[str, Optional[str]]:
    values: Dict[str, Optional[str]] = {}
    for platform in SOCIAL_MEDIA_PLATFORMS:
        key = f"SocialMedia.{platform}"
        value = form_data.get(key, "")
        value = value.strip() if value is not None else ""
        values[platform] = value or None
    return values


def persist_university_bundle(
    engine, college_id: Optional[int], table_payloads: Dict[str, Dict[str, Any]], social_payloads: Dict[str, Optional[str]]
) -> int:
    college_table = fetch_table("College")
    address_table = fetch_table("Address", required=False)
    contact_table = fetch_table("ContactInformation", required=False)
    app_req_table = fetch_table("ApplicationRequirements", required=False)
    stats_table = fetch_table("StudentStatistics", required=False)
    social_table = fetch_table("SocialMedia", required=False)

    college_values = table_payloads.get("College", {})
    if not college_values and college_id is None:
        raise ValueError("Provide at least the college name to create a record.")

    with engine.begin() as conn:
        if college_id is None:
            result = conn.execute(college_table.insert().values(**college_values))
            college_id = int(result.inserted_primary_key[0])
        else:
            if college_values:
                conn.execute(
                    college_table.update()
                    .where(college_table.c.CollegeID == college_id)
                    .values(**college_values)
                )

        if address_table is not None:
            payload = table_payloads.get(address_table.name, {})
            upsert_single_row(conn, address_table, address_table.c.CollegeID, college_id, payload)

        if contact_table is not None:
            payload = table_payloads.get(contact_table.name, {})
            upsert_single_row(conn, contact_table, contact_table.c.CollegeID, college_id, payload)

        if app_req_table is not None:
            payload = table_payloads.get(app_req_table.name, {})
            upsert_single_row(conn, app_req_table, app_req_table.c.CollegeID, college_id, payload)

        if stats_table is not None:
            payload = table_payloads.get(stats_table.name, {})
            upsert_single_row(conn, stats_table, stats_table.c.CollegeID, college_id, payload)

        if social_table is not None:
            existing_rows = conn.execute(
                select(social_table).where(social_table.c.CollegeID == college_id)
            ).mappings().all()
            existing_map = {row["PlatformName"].lower(): row for row in existing_rows}

            for platform, url in social_payloads.items():
                platform_lower = platform.lower()
                existing_row = existing_map.get(platform_lower)

                if url:
                    if existing_row:
                        conn.execute(
                            social_table.update()
                            .where(social_table.c.SocialID == existing_row["SocialID"])
                            .values(URL=url)
                        )
                    else:
                        conn.execute(
                            social_table.insert().values(
                                CollegeID=college_id, PlatformName=platform, URL=url
                            )
                        )
                else:
                    if existing_row:
                        conn.execute(
                            social_table.delete().where(
                                social_table.c.SocialID == existing_row["SocialID"]
                            )
                        )

    return college_id


@forms_bp.route("/departments/")
def department_list():
    engine = get_engine()
    department_table = fetch_table("Department")
    college_department_table = fetch_table("CollegeDepartment", required=False)
    college_table = fetch_table("College", required=False)

    # Start from CollegeDepartment to ensure each link is a separate row
    if college_department_table is not None:
        # Select all necessary fields starting from CollegeDepartment
        columns = [
            college_department_table.c.CollegeDepartmentID,
            college_department_table.c.BuildingName,
            college_department_table.c.Street1,
            college_department_table.c.Street2,
            college_department_table.c.City,
            college_department_table.c.State,
            college_department_table.c.ZipCode,
            college_department_table.c.Country,
            college_department_table.c.Email,
            college_department_table.c.PhoneNumber,
            college_department_table.c.AdmissionUrl,
            department_table.c.DepartmentID,
            department_table.c.DepartmentName,
            department_table.c.Description,
        ]

        join_from = college_department_table.join(
            department_table,
            department_table.c.DepartmentID == college_department_table.c.DepartmentID,
        )
        
        if college_table is not None:
            columns.append(college_table.c.CollegeName)
            columns.append(college_table.c.CollegeID)
            join_from = join_from.join(
                college_table,
                college_table.c.CollegeID == college_department_table.c.CollegeID,
            )
    else:
        # Fallback: if no CollegeDepartment table, just show departments
        columns = [
            department_table.c.DepartmentID,
            department_table.c.DepartmentName,
            department_table.c.Description,
        ]
        join_from = department_table

    stmt = select(*columns).select_from(join_from)
    
    # Order by CollegeName first, then DepartmentName
    if college_table is not None and college_department_table is not None:
        stmt = stmt.order_by(college_table.c.CollegeName, department_table.c.DepartmentName)
    else:
        stmt = stmt.order_by(department_table.c.DepartmentName)

    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]

    # Group departments by college name
    # Use CollegeDepartmentID as unique key to prevent duplicates
    grouped_departments = defaultdict(list)
    seen_keys = set()
    for row in rows:
        college_name = row.get("CollegeName") or "Unassigned"
        # Use CollegeDepartmentID as unique key if available, otherwise use DepartmentID
        unique_key = (row.get("CollegeDepartmentID"), row.get("DepartmentID"), college_name)
        if unique_key not in seen_keys:
            seen_keys.add(unique_key)
            grouped_departments[college_name].append(row)
    
    # Sort colleges (put "Unassigned" at the end)
    sorted_colleges = sorted(
        [k for k in grouped_departments.keys() if k != "Unassigned"]
    )
    if "Unassigned" in grouped_departments:
        sorted_colleges.append("Unassigned")
    
    grouped_departments_sorted = {k: grouped_departments[k] for k in sorted_colleges}

    return render_template(
        "forms/department_list.html",
        rows=rows,
        grouped_departments=grouped_departments_sorted
    )


@forms_bp.route("/departments/new", methods=["GET", "POST"])
def department_create():
    return handle_department_form(None)


@forms_bp.route("/departments/<int:department_id>/edit", methods=["GET", "POST"])
def department_edit(department_id: int):
    return handle_department_form(department_id)


@forms_bp.route("/college-departments/<int:college_department_id>/edit", methods=["GET", "POST"])
def college_department_edit(college_department_id: int):
    """Edit a specific college-department link."""
    return handle_college_department_form(college_department_id)


@forms_bp.route("/college-departments/bulk-delete", methods=["POST"])
def bulk_delete_college_departments():
    """Bulk delete selected college-department links."""
    try:
        engine = get_engine()
        if engine is None:
            flash("Database is not configured.", "error")
            return redirect(url_for("forms.department_list"))
        
        college_department_ids = request.form.getlist("college_department_ids")
        
        if not college_department_ids:
            flash("Please select at least one department to delete.", "error")
            return redirect(url_for("forms.department_list"))
        
        college_department_table = fetch_table("CollegeDepartment")
        department_table = fetch_table("Department", required=False)
        program_link_table = fetch_table("ProgramDepartmentLink", required=False)
        
        deleted_count = 0
        errors = []
        deleted_names = []
        
        with engine.begin() as conn:
            for cd_id_str in college_department_ids:
                try:
                    college_department_id = int(cd_id_str)
                    
                    # Get department name for flash message
                    dept_name = f"Department Link #{college_department_id}"
                    if department_table is not None:
                        stmt = (
                            select(department_table.c.DepartmentName)
                            .select_from(
                                college_department_table.join(
                                    department_table,
                                    department_table.c.DepartmentID == college_department_table.c.DepartmentID
                                )
                            )
                            .where(college_department_table.c.CollegeDepartmentID == college_department_id)
                        )
                        dept = conn.execute(stmt).mappings().first()
                        if dept:
                            dept_name = dept["DepartmentName"]
                    
                    # Check if the record exists
                    existing = conn.execute(
                        select(college_department_table).where(
                            college_department_table.c.CollegeDepartmentID == college_department_id
                        )
                    ).mappings().first()
                    
                    if not existing:
                        errors.append(f"College department link (ID: {college_department_id}) not found.")
                        continue
                    
                    # Delete related ProgramDepartmentLink records first (due to foreign key constraint)
                    if program_link_table is not None:
                        conn.execute(
                            program_link_table.delete().where(
                                program_link_table.c.CollegeDepartmentID == college_department_id
                            )
                        )
                    
                    # Delete the college-department link
                    conn.execute(
                        college_department_table.delete().where(
                            college_department_table.c.CollegeDepartmentID == college_department_id
                        )
                    )
                    
                    deleted_count += 1
                    deleted_names.append(dept_name)
                    
                except Exception as e:
                    errors.append(f"College department ID {cd_id_str}: {str(e)}")
        
        if deleted_count > 0:
            if deleted_count == 1:
                flash(f"Successfully deleted department link '{deleted_names[0]}'.", "success")
            else:
                flash(f"Successfully deleted {deleted_count} department link(s).", "success")
        if errors:
            flash(f"Errors deleting {len(errors)} department link(s): {', '.join(errors[:5])}", "error")
        
        return redirect(url_for("forms.department_list"))
        
    except Exception as e:
        flash(f"Error deleting departments: {e}", "error")
        import traceback
        traceback.print_exc()
        return redirect(url_for("forms.department_list"))


@forms_bp.route("/college-departments/<int:college_department_id>/delete", methods=["POST"])
def college_department_delete(college_department_id: int):
    """Delete a specific college-department link."""
    try:
        engine = get_engine()
    except Exception:
        flash("Database is not configured.", "error")
        return redirect(url_for("forms.department_list"))
    
    try:
        college_department_table = fetch_table("CollegeDepartment")
        program_link_table = fetch_table("ProgramDepartmentLink", required=False)
        
        with engine.begin() as conn:
            # Check if the record exists
            existing = conn.execute(
                select(college_department_table).where(
                    college_department_table.c.CollegeDepartmentID == college_department_id
                )
            ).mappings().first()
            
            if not existing:
                flash(f"College department link (ID: {college_department_id}) not found.", "error")
                return redirect(url_for("forms.department_list"))
            
            # Delete related ProgramDepartmentLink records first (due to foreign key constraint)
            if program_link_table is not None:
                conn.execute(
                    program_link_table.delete().where(
                        program_link_table.c.CollegeDepartmentID == college_department_id
                    )
                )
            
            # Delete the college-department link
            conn.execute(
                college_department_table.delete().where(
                    college_department_table.c.CollegeDepartmentID == college_department_id
                )
            )
        
        flash(f"College department link (ID: {college_department_id}) deleted successfully.", "success")
    except Exception as exc:
        flash(f"Failed to delete college department link: {exc}", "error")
    
    return redirect(url_for("forms.department_list"))


def handle_college_department_form(college_department_id: int):
    """Handle form for editing a college-department link."""
    engine = get_engine()
    college_department_table = fetch_table("CollegeDepartment", required=False)
    department_table = fetch_table("Department")
    college_table = fetch_table("College", required=False)
    
    if college_department_table is None:
        abort(404, "CollegeDepartment table is not available.")
    
    # Load existing data
    existing_data: Dict[str, Dict[str, Any]] = {}
    with engine.connect() as conn:
        college_dept = conn.execute(
            select(college_department_table).where(
                college_department_table.c.CollegeDepartmentID == college_department_id
            )
        ).mappings().first()
        
        if not college_dept:
            abort(404, f"College department link (ID: {college_department_id}) not found.")
        
        existing_data["CollegeDepartment"] = dict(college_dept)
        
        # Also load the department info
        department_id = college_dept["DepartmentID"]
        department = conn.execute(
            select(department_table).where(department_table.c.DepartmentID == department_id)
        ).mappings().first()
        if department:
            existing_data["Department"] = dict(department)
    
    # Build form sections
    sections = [
        {
            "title": "Department Information",
            "table": department_table,
            "fields": build_prefixed_fields(
                department_table,
                ["DepartmentName", "Description"],
            ),
            "description": "Department name and description.",
        },
        {
            "title": "College Link & Contact Details",
            "table": college_department_table,
            "fields": build_prefixed_fields(
                college_department_table,
                [
                    "CollegeID",
                    "Email",
                    "PhoneNumber",
                    "PhoneType",
                    "AdmissionUrl",
                    "BuildingName",
                    "Street1",
                    "Street2",
                    "City",
                    "State",
                    "StateName",
                    "ZipCode",
                    "Country",
                    "CountryCode",
                    "CountryName",
                ],
            ),
            "description": "Link this department to a college and provide contact details.",
        },
    ]
    
    # Set up college dropdown
    college_options = get_college_options(engine)
    for section in sections:
        for field in section["fields"]:
            if field["name"] == "CollegeID":
                field["input_type"] = "select"
                field["options"] = college_options
    
    all_fields = [field for section in sections for field in section["fields"]]
    form_values = compose_prefixed_initial_values(all_fields, existing_data)
    
    if request.method == "POST":
        submitted_values = compose_prefixed_request_values(all_fields, request.form)
        try:
            table_payloads = extract_prefixed_values(all_fields, request.form)
        except ValueError as exc:
            flash(str(exc), "error")
            form_values = submitted_values
        else:
            try:
                # Update department if changed
                if "Department" in table_payloads:
                    dept_payload = table_payloads["Department"]
                    if dept_payload:
                        with engine.begin() as conn:
                            conn.execute(
                                department_table.update()
                                .where(department_table.c.DepartmentID == department_id)
                                .values(**dept_payload)
                            )
                
                # Update college department link
                if "CollegeDepartment" in table_payloads:
                    cd_payload = table_payloads["CollegeDepartment"]
                    if cd_payload:
                        with engine.begin() as conn:
                            conn.execute(
                                college_department_table.update()
                                .where(college_department_table.c.CollegeDepartmentID == college_department_id)
                                .values(**cd_payload)
                            )
                
                flash("College department link updated successfully.", "success")
                return redirect(url_for("forms.department_list"))
            except (SQLAlchemyError, ValueError) as exc:
                flash(f"Database error: {exc}", "error")
                form_values = submitted_values
    
    return render_template(
        "forms/department_form.html",
        sections=sections,
        form_values=form_values,
        form_title="Edit Admissions Office",
        submit_label="Update",
        department_id=department_id,
        college_department_id=college_department_id,
        form_hint="Update the admissions office details and its link to a college.",
    )


def handle_department_form(department_id: Optional[int]):
    engine = get_engine()
    sections = build_department_sections(engine)
    all_fields = [field for section in sections for field in section["fields"]]

    existing_data: Dict[str, Dict[str, Any]] = {}
    if department_id is not None:
        existing_data = load_department_bundle(engine, department_id)
        if not existing_data.get("Department"):
            abort(404, f"Department with id {department_id} was not found.")

    form_values = compose_prefixed_initial_values(all_fields, existing_data)

    if request.method == "POST":
        submitted_values = compose_prefixed_request_values(all_fields, request.form)
        try:
            table_payloads = extract_prefixed_values(all_fields, request.form)
        except ValueError as exc:
            flash(str(exc), "error")
            form_values = submitted_values
        else:
            try:
                department_id = persist_department_bundle(engine, department_id, table_payloads)
            except (SQLAlchemyError, ValueError) as exc:
                flash(f"Database error: {exc}", "error")
                form_values = submitted_values
            else:
                flash("Department details saved successfully.", "success")
                return redirect(url_for("forms.department_list"))

    form_title = "Create Department" if department_id is None else "Edit Department"
    submit_label = "Create" if department_id is None else "Update"

    form_hint = (
        "Add or update departmental information so prospective students know who to contact and where to go."
    )

    return render_template(
        "forms/department_form.html",
        sections=sections,
        form_values=form_values,
        form_title=form_title,
        submit_label=submit_label,
        department_id=department_id,
        form_hint=form_hint,
    )


def build_department_sections(engine) -> List[Dict[str, Any]]:
    department_table = fetch_table("Department")
    college_department_table = fetch_table("CollegeDepartment", required=False)

    sections: List[Dict[str, Any]] = [
        {
            "title": "Admissions Office Details",
            "table": department_table,
            "fields": build_prefixed_fields(
                department_table,
                [
                    "DepartmentName",
                    "Description",
                ],
            ),
            "description": "Enter the admissions office name (e.g., Graduate Admissions, Undergraduate Admissions, College of Engineering Admissions) and a concise description.",
        }
    ]

    if college_department_table is not None:
        fields = build_prefixed_fields(
            college_department_table,
            [
                "CollegeID",
                "Email",
                "PhoneNumber",
                "PhoneType",
                "AdmissionUrl",
                "BuildingName",
                "Street1",
                "Street2",
                "City",
                "State",
                "StateName",
                "Country",
                "CountryCode",
                "CountryName",
                "ZipCode",
            ],
        )

        college_options = get_college_options(engine)
        for field in fields:
            if field["name"] == "CollegeID":
                field["input_type"] = "select"
                field["options"] = college_options

        sections.append(
            {
                "title": "Office Location & Contact (per College)",
                "table": college_department_table,
                "fields": fields,
                "description": "Link this admissions office to the college, and provide office address and contact info (email, phone, URL).",
            }
        )

    return sections


def load_department_bundle(engine, department_id: int) -> Dict[str, Dict[str, Any]]:
    department_table = fetch_table("Department")
    college_department_table = fetch_table("CollegeDepartment", required=False)

    bundle: Dict[str, Dict[str, Any]] = {}

    with engine.connect() as conn:
        department = conn.execute(
            select(department_table).where(department_table.c.DepartmentID == department_id)
        ).mappings().first()
        if department:
            bundle["Department"] = dict(department)

        if college_department_table is not None:
            college_dept = conn.execute(
                select(college_department_table).where(
                    college_department_table.c.DepartmentID == department_id
                )
            ).mappings().first()
            if college_dept:
                bundle["CollegeDepartment"] = dict(college_dept)

    return bundle


def persist_department_bundle(
    engine, department_id: Optional[int], table_payloads: Dict[str, Dict[str, Any]]
) -> int:
    department_table = fetch_table("Department")
    college_department_table = fetch_table("CollegeDepartment", required=False)

    department_values = table_payloads.get("Department", {})
    college_department_values = table_payloads.get("CollegeDepartment", {})

    if department_id is None and not department_values:
        raise ValueError("Provide department details to create a record.")

    with engine.begin() as conn:
        if department_id is None:
            result = conn.execute(department_table.insert().values(**department_values))
            department_id = int(result.inserted_primary_key[0])
        else:
            if department_values:
                conn.execute(
                    department_table.update()
                    .where(department_table.c.DepartmentID == department_id)
                    .values(**department_values)
                )

        if college_department_table is not None and college_department_values:
            college_department_values = college_department_values.copy()
            college_department_values["DepartmentID"] = department_id

            existing = conn.execute(
                select(college_department_table).where(
                    college_department_table.c.DepartmentID == department_id
                )
            ).mappings().first()

            if existing:
                conn.execute(
                    college_department_table.update()
                    .where(college_department_table.c.DepartmentID == department_id)
                    .values(**college_department_values)
                )
            else:
                conn.execute(college_department_table.insert().values(**college_department_values))

    return department_id


@forms_bp.route("/programs/")
def program_list():
    engine = get_engine()
    program_table = fetch_table("Program")
    program_link_table = fetch_table("ProgramDepartmentLink", required=False)
    college_table = fetch_table("College", required=False)
    department_table = fetch_table("Department", required=False)
    college_department_table = fetch_table("CollegeDepartment", required=False)

    stmt = select(
        program_table.c.ProgramID,
        program_table.c.ProgramName,
        program_table.c.Level,
        program_table.c.Concentration,
    ).select_from(program_table)

    if program_link_table is not None:
        stmt = stmt.add_columns(program_link_table.c.CollegeDepartmentID)
        stmt = stmt.outerjoin(
            program_link_table,
            program_link_table.c.ProgramID == program_table.c.ProgramID,
        )

        if college_department_table is not None and college_table is not None:
            stmt = stmt.add_columns(college_table.c.CollegeName)
            stmt = stmt.outerjoin(
                college_department_table,
                college_department_table.c.CollegeDepartmentID
                == program_link_table.c.CollegeDepartmentID,
            ).outerjoin(
                college_table,
                college_table.c.CollegeID == college_department_table.c.CollegeID,
            )

        if college_department_table is not None and department_table is not None:
            stmt = stmt.add_columns(department_table.c.DepartmentName)
            stmt = stmt.outerjoin(
                department_table,
                department_table.c.DepartmentID == college_department_table.c.DepartmentID,
            )

    # Order by CollegeName first, then ProgramName
    if college_table is not None:
        stmt = stmt.order_by(college_table.c.CollegeName, program_table.c.ProgramName)
    else:
        stmt = stmt.order_by(program_table.c.ProgramName)

    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(stmt)]

    # Group programs by college name
    grouped_programs = defaultdict(list)
    for row in rows:
        college_name = row.get("CollegeName") or "Unassigned"
        grouped_programs[college_name].append(row)
    
    # Sort colleges (put "Unassigned" at the end)
    sorted_colleges = sorted(
        [k for k in grouped_programs.keys() if k != "Unassigned"]
    )
    if "Unassigned" in grouped_programs:
        sorted_colleges.append("Unassigned")
    
    grouped_programs_sorted = {k: grouped_programs[k] for k in sorted_colleges}

    college_options = get_college_options(engine)

    return render_template("forms/program_list.html", rows=rows, grouped_programs=grouped_programs_sorted, college_options=college_options)


@forms_bp.route("/programs/bulk-update-college", methods=["POST"])
def bulk_update_program_college():
    """Bulk update the university/college for selected programs."""
    try:
        engine = get_engine()
        if engine is None:
            flash("Database is not configured.", "error")
            return redirect(url_for("forms.program_list"))
        
        program_ids = request.form.getlist("program_ids")
        college_id = request.form.get("college_id", type=int)
        
        if not program_ids:
            flash("Please select at least one program to update.", "error")
            return redirect(url_for("forms.program_list"))
        
        if not college_id:
            flash("Please select a university/college.", "error")
            return redirect(url_for("forms.program_list"))
        
        program_term_table = fetch_table("ProgramTermDetails", required=False)
        program_link_table = fetch_table("ProgramDepartmentLink", required=False)
        college_department_table = fetch_table("CollegeDepartment", required=False)
        department_table = fetch_table("Department", required=False)
        
        if program_term_table is None:
            flash("ProgramTermDetails table is not available.", "error")
            return redirect(url_for("forms.program_list"))
        
        updated_count = 0
        errors = []
        
        with engine.begin() as conn:
            for program_id_str in program_ids:
                try:
                    program_id = int(program_id_str)
                    
                    # Update or create ProgramTermDetails
                    existing_term = conn.execute(
                        select(program_term_table)
                        .where(program_term_table.c.ProgramID == program_id)
                        .order_by(program_term_table.c.ProgramTermID)
                    ).mappings().first()
                    
                    if existing_term:
                        # Update existing term details
                        conn.execute(
                            program_term_table.update()
                            .where(program_term_table.c.ProgramTermID == existing_term["ProgramTermID"])
                            .values(CollegeID=college_id)
                        )
                    else:
                        # Create new term details with the selected college
                        conn.execute(
                            program_term_table.insert().values(
                                ProgramID=program_id,
                                CollegeID=college_id,
                                Term="Fall"  # Default term
                            )
                        )
                    
                    # Update or create ProgramDepartmentLink - this is what determines college grouping in the list
                    if program_link_table is not None and college_department_table is not None and department_table is not None:
                        existing_link = conn.execute(
                            select(program_link_table).where(program_link_table.c.ProgramID == program_id)
                        ).mappings().first()
                        
                        # Find a department for this college (prefer "Graduate Admissions" or "Undergraduate Admissions")
                        # If not found, get any department for this college
                        dept_query = (
                            select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                            .select_from(
                                college_department_table.join(
                                    department_table,
                                    department_table.c.DepartmentID == college_department_table.c.DepartmentID
                                )
                            )
                            .where(college_department_table.c.CollegeID == college_id)
                        )
                        
                        # Try to find Graduate or Undergraduate Admissions first
                        dept_candidate = conn.execute(
                            dept_query.where(
                                department_table.c.DepartmentName.ilike("%Graduate Admissions%")
                            )
                        ).mappings().first()
                        
                        if not dept_candidate:
                            dept_candidate = conn.execute(
                                dept_query.where(
                                    department_table.c.DepartmentName.ilike("%Undergraduate Admissions%")
                                )
                            ).mappings().first()
                        
                        # If still not found, get any department for this college
                        if not dept_candidate:
                            dept_candidate = conn.execute(dept_query).mappings().first()
                        
                        if dept_candidate:
                            college_dept_id = dept_candidate["CollegeDepartmentID"]
                            
                            if existing_link:
                                # Update existing link
                                conn.execute(
                                    program_link_table.update()
                                    .where(program_link_table.c.ProgramID == program_id)
                                    .values(
                                        CollegeID=college_id,
                                        CollegeDepartmentID=college_dept_id
                                    )
                                )
                            else:
                                # Create new link
                                conn.execute(
                                    program_link_table.insert().values(
                                        ProgramID=program_id,
                                        CollegeID=college_id,
                                        CollegeDepartmentID=college_dept_id
                                    )
                                )
                        elif not existing_link:
                            # No department found for this college - this is a problem
                            errors.append(f"Program ID {program_id_str}: No department found for selected college. Please create a department first.")
                            continue
                    
                    updated_count += 1
                    
                except Exception as e:
                    errors.append(f"Program ID {program_id_str}: {str(e)}")
        
        if updated_count > 0:
            flash(f"Successfully updated university for {updated_count} program(s).", "success")
        if errors:
            flash(f"Errors updating {len(errors)} program(s): {', '.join(errors[:5])}", "error")
        
        return redirect(url_for("forms.program_list"))
        
    except Exception as e:
        flash(f"Error updating programs: {e}", "error")
        import traceback
        traceback.print_exc()
        return redirect(url_for("forms.program_list"))


@forms_bp.route("/programs/bulk-delete", methods=["POST"])
def bulk_delete_programs():
    """Bulk delete selected programs and all related records."""
    try:
        engine = get_engine()
        if engine is None:
            flash("Database is not configured.", "error")
            return redirect(url_for("forms.program_list"))
        
        program_ids = request.form.getlist("program_ids")
        
        if not program_ids:
            flash("Please select at least one program to delete.", "error")
            return redirect(url_for("forms.program_list"))
        
        program_table = fetch_table("Program")
        program_requirements_table = fetch_table("ProgramRequirements", required=False)
        program_term_table = fetch_table("ProgramTermDetails", required=False)
        program_link_table = fetch_table("ProgramDepartmentLink", required=False)
        program_test_table = fetch_table("ProgramTestScores", required=False)
        
        deleted_count = 0
        errors = []
        deleted_names = []
        
        with engine.begin() as conn:
            for program_id_str in program_ids:
                try:
                    program_id = int(program_id_str)
                    
                    # Get program name for flash message
                    program = conn.execute(
                        select(program_table.c.ProgramName).where(program_table.c.ProgramID == program_id)
                    ).mappings().first()
                    program_name = program["ProgramName"] if program else f"Program #{program_id}"
                    
                    # Delete related records first (due to foreign key constraints)
                    if program_test_table is not None:
                        conn.execute(
                            program_test_table.delete().where(program_test_table.c.ProgramID == program_id)
                        )
                    
                    if program_link_table is not None:
                        conn.execute(
                            program_link_table.delete().where(program_link_table.c.ProgramID == program_id)
                        )
                    
                    if program_term_table is not None:
                        conn.execute(
                            program_term_table.delete().where(program_term_table.c.ProgramID == program_id)
                        )
                    
                    if program_requirements_table is not None:
                        conn.execute(
                            program_requirements_table.delete().where(program_requirements_table.c.ProgramID == program_id)
                        )
                    
                    # Finally delete the program
                    conn.execute(
                        program_table.delete().where(program_table.c.ProgramID == program_id)
                    )
                    
                    deleted_count += 1
                    deleted_names.append(program_name)
                    
                except Exception as e:
                    errors.append(f"Program ID {program_id_str}: {str(e)}")
        
        if deleted_count > 0:
            if deleted_count == 1:
                flash(f"Successfully deleted program '{deleted_names[0]}' and all related records.", "success")
            else:
                flash(f"Successfully deleted {deleted_count} program(s) and all related records.", "success")
        if errors:
            flash(f"Errors deleting {len(errors)} program(s): {', '.join(errors[:5])}", "error")
        
        return redirect(url_for("forms.program_list"))
        
    except Exception as e:
        flash(f"Error deleting programs: {e}", "error")
        import traceback
        traceback.print_exc()
        return redirect(url_for("forms.program_list"))


@forms_bp.route("/programs/<int:program_id>/delete", methods=["POST"])
def program_delete(program_id: int):
    """Delete a program and all related records."""
    try:
        engine = get_engine()
        if engine is None:
            flash("Database is not configured.", "error")
            return redirect(url_for("forms.program_list"))
        
        program_table = fetch_table("Program")
        program_requirements_table = fetch_table("ProgramRequirements", required=False)
        program_term_table = fetch_table("ProgramTermDetails", required=False)
        program_link_table = fetch_table("ProgramDepartmentLink", required=False)
        program_test_table = fetch_table("ProgramTestScores", required=False)
        
        # Get program name for flash message
        with engine.connect() as conn:
            program = conn.execute(
                select(program_table.c.ProgramName).where(program_table.c.ProgramID == program_id)
            ).mappings().first()
            program_name = program["ProgramName"] if program else f"Program #{program_id}"
        
        with engine.begin() as conn:
            # Delete related records first (due to foreign key constraints)
            if program_test_table is not None:
                conn.execute(
                    program_test_table.delete().where(program_test_table.c.ProgramID == program_id)
                )
            
            if program_link_table is not None:
                conn.execute(
                    program_link_table.delete().where(program_link_table.c.ProgramID == program_id)
                )
            
            if program_term_table is not None:
                conn.execute(
                    program_term_table.delete().where(program_term_table.c.ProgramID == program_id)
                )
            
            if program_requirements_table is not None:
                conn.execute(
                    program_requirements_table.delete().where(program_requirements_table.c.ProgramID == program_id)
                )
            
            # Finally delete the program
            conn.execute(
                program_table.delete().where(program_table.c.ProgramID == program_id)
            )
        
        flash(f"Program '{program_name}' and all related records deleted successfully.", "success")
    except Exception as exc:
        flash(f"Failed to delete program: {exc}", "error")
        import traceback
        traceback.print_exc()
    
    return redirect(url_for("forms.program_list"))


@forms_bp.route("/programs/new", methods=["GET", "POST"])
def program_create():
    return handle_program_form(None)


@forms_bp.route("/programs/<int:program_id>/edit", methods=["GET", "POST"])
def program_edit(program_id: int):
    return handle_program_form(program_id)


def handle_program_form(program_id: Optional[int]):
    engine = get_engine()
    sections, college_options, college_department_options = build_program_sections(engine)
    all_fields = [field for section in sections for field in section["fields"]]

    existing_data: Dict[str, Dict[str, Any]] = {}
    if program_id is not None:
        existing_data = load_program_bundle(engine, program_id)
        if not existing_data.get("Program"):
            abort(404, f"Program with id {program_id} was not found.")

    form_values = compose_prefixed_initial_values(all_fields, existing_data)

    if request.method == "POST":
        submitted_values = compose_prefixed_request_values(all_fields, request.form)
        try:
            table_payloads = extract_prefixed_values(all_fields, request.form)
        except ValueError as exc:
            flash(str(exc), "error")
            form_values = submitted_values
        else:
            try:
                program_id = persist_program_bundle(
                    engine,
                    program_id,
                    table_payloads,
                )
            except (SQLAlchemyError, ValueError) as exc:
                flash(f"Database error: {exc}", "error")
                form_values = submitted_values
            else:
                flash("Program details saved successfully.", "success")
                return redirect(url_for("forms.program_list"))

    form_title = "Create Program" if program_id is None else "Edit Program"
    submit_label = "Create" if program_id is None else "Update"

    form_hint = (
        "Walk through the sections to publish a clear, student-friendly overview of this academic program."
    )

    return render_template(
        "forms/program_form.html",
        sections=sections,
        form_values=form_values,
        form_title=form_title,
        submit_label=submit_label,
        program_id=program_id,
        college_options=college_options,
        college_department_options=college_department_options,
        form_hint=form_hint,
    )


def build_program_sections(engine) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], List[Tuple[int, str]]]:
    program_table = fetch_table("Program")
    program_requirements_table = fetch_table("ProgramRequirements", required=False)
    program_term_table = fetch_table("ProgramTermDetails", required=False)
    program_link_table = fetch_table("ProgramDepartmentLink", required=False)
    program_test_table = fetch_table("ProgramTestScores", required=False)

    college_options = get_college_options(engine)
    college_department_options = get_college_department_options(engine)

    sections: List[Dict[str, Any]] = []

    base_sections = [
        {
            "title": "College Assignment",
            "table": program_term_table,
            "columns": [
                "CollegeID",
            ],
            "description": "Select the college/university this program belongs to. This will be used for all term details.",
        },
        {
            "title": "Program Snapshot",
            "table": program_table,
            "columns": [
                "ProgramName",
                "Level",
                "Concentration",
                "Description",
                "ProgramWebsiteURL",
                "Accreditation",
                "QsWorldRanking",
            ],
            "description": "Summarize the core academic details that distinguish this program.",
        },
        {
            "title": "Application Checklist",
            "table": program_requirements_table,
            "columns": [
                "Resume",
                "StatementOfPurpose",
                "GreOrGmat",
                "EnglishScore",
                "Requirements",
                "WritingSample",
                "IsAnalyticalNotRequired",
                "IsAnalyticalOptional",
                "IsDuoLingoRequired",
                "IsELSRequired",
                "IsGMATOrGreRequired",
                "IsGMATRequired",
                "IsGreRequired",
                "IsIELTSRequired",
                "IsLSATRequired",
                "IsMATRequired",
                "IsMCATRequired",
                "IsPTERequired",
                "IsTOEFLIBRequired",
                "IsTOEFLPBTRequired",
                "IsEnglishNotRequired",
                "IsEnglishOptional",
                "IsRecommendationSystemOpted",
                "IsStemProgram",
                "MaxFails",
                "MaxGPA",
                "MinGPA",
                "PreviousYearAcceptanceRates",
            ],
            "description": "Check off the materials and policies applicants must meet to be considered.",
        },
        {
            "title": "Term & Investment",
            "table": program_term_table,
            "columns": [
                "CollegeID",
                "Term",
                "LiveDate",
                "DeadlineDate",
                "Fees",
                "AverageScholarshipAmount",
                "CostPerCredit",
                "ScholarshipAmount",
                "ScholarshipPercentage",
                "ScholarshipType",
            ],
            "description": "Outline which term this information applies to along with key cost figures.",
        },
        {
            "title": "Department Placement",
            "table": program_link_table,
            "columns": [
                "CollegeDepartmentID",
            ],
            "description": "Associate the program with its academic department to power directory listings.",
        },
        {
            "title": "Minimum Test Scores",
            "table": program_test_table,
            "columns": [
                "MinimumACTScore",
                "MinimumDuoLingoScore",
                "MinimumELSScore",
                "MinimumGMATScore",
                "MinimumGreScore",
                "MinimumIELTSScore",
                "MinimumMATScore",
                "MinimumMCATScore",
                "MinimumPTEScore",
                "MinimumSATScore",
                "MinimumTOEFLScore",
                "MinimumLSATScore",
            ],
            "description": "Record target score guidance so counselors have one reference point.",
        },
    ]

    for item in base_sections:
        table_obj = item["table"]
        if table_obj is None:
            continue
        fields = build_prefixed_fields(table_obj, item["columns"])
        if not fields:
            continue

        # Set up CollegeID as select in both College Assignment and Term & Investment sections
        if table_obj.name == "ProgramTermDetails":
            for field in fields:
                if field["name"] == "CollegeID":
                    field["input_type"] = "select"
                    field["options"] = college_options
                    # Make it required in College Assignment section
                    if item["title"] == "College Assignment":
                        field["nullable"] = False

        if table_obj.name == "ProgramDepartmentLink":
            for field in fields:
                if field["name"] == "CollegeDepartmentID":
                    field["input_type"] = "select"
                    field["options"] = college_department_options

        sections.append(
            {
                "title": item["title"],
                "table": table_obj,
                "fields": fields,
                "description": item.get("description"),
            }
        )

    return sections, college_options, college_department_options


def load_program_bundle(engine, program_id: int) -> Dict[str, Dict[str, Any]]:
    program_table = fetch_table("Program")
    program_requirements_table = fetch_table("ProgramRequirements", required=False)
    program_term_table = fetch_table("ProgramTermDetails", required=False)
    program_link_table = fetch_table("ProgramDepartmentLink", required=False)
    program_test_table = fetch_table("ProgramTestScores", required=False)

    bundle: Dict[str, Dict[str, Any]] = {}

    with engine.connect() as conn:
        program = conn.execute(
            select(program_table).where(program_table.c.ProgramID == program_id)
        ).mappings().first()
        if program:
            bundle["Program"] = dict(program)

        if program_requirements_table is not None:
            requirements = conn.execute(
                select(program_requirements_table).where(
                    program_requirements_table.c.ProgramID == program_id
                )
            ).mappings().first()
            if requirements:
                bundle["ProgramRequirements"] = dict(requirements)

        if program_term_table is not None:
            term = conn.execute(
                select(program_term_table)
                .where(program_term_table.c.ProgramID == program_id)
                .order_by(program_term_table.c.ProgramTermID)
            ).mappings().first()
            if term:
                bundle["ProgramTermDetails"] = dict(term)

        if program_link_table is not None:
            link = conn.execute(
                select(program_link_table).where(program_link_table.c.ProgramID == program_id)
            ).mappings().first()
            if link:
                bundle["ProgramDepartmentLink"] = dict(link)

        if program_test_table is not None:
            tests = conn.execute(
                select(program_test_table).where(program_test_table.c.ProgramID == program_id)
            ).mappings().first()
            if tests:
                bundle["ProgramTestScores"] = dict(tests)

    return bundle


def persist_program_bundle(
    engine,
    program_id: Optional[int],
    table_payloads: Dict[str, Dict[str, Any]],
) -> int:
    program_table = fetch_table("Program")
    program_requirements_table = fetch_table("ProgramRequirements", required=False)
    program_term_table = fetch_table("ProgramTermDetails", required=False)
    program_link_table = fetch_table("ProgramDepartmentLink", required=False)
    program_test_table = fetch_table("ProgramTestScores", required=False)

    program_values = table_payloads.get("Program", {})
    if program_id is None and not program_values:
        raise ValueError("Provide program details to create a record.")

    with engine.begin() as conn:
        if program_id is None:
            result = conn.execute(program_table.insert().values(**program_values))
            program_id = int(result.inserted_primary_key[0])
        else:
            if program_values:
                conn.execute(
                    program_table.update()
                    .where(program_table.c.ProgramID == program_id)
                    .values(**program_values)
                )

        term_payload = table_payloads.get("ProgramTermDetails", {})
        selected_college_id = term_payload.get("CollegeID") if term_payload else None

        if program_requirements_table is not None:
            requirements_payload = table_payloads.get("ProgramRequirements", {})
            requirements_payload = requirements_payload.copy()
            requirements_payload["ProgramID"] = program_id

            existing = conn.execute(
                select(program_requirements_table).where(
                    program_requirements_table.c.ProgramID == program_id
                )
            ).mappings().first()

            if existing:
                conn.execute(
                    program_requirements_table.update()
                    .where(program_requirements_table.c.ProgramID == program_id)
                    .values(**requirements_payload)
                )
            else:
                conn.execute(program_requirements_table.insert().values(**requirements_payload))

        if program_term_table is not None and term_payload:
            term_payload = term_payload.copy()
            college_id_value = term_payload.get("CollegeID", selected_college_id)
            if college_id_value is None:
                raise ValueError("Select a college for the term details.")

            term_payload["CollegeID"] = college_id_value
            term_payload["ProgramID"] = program_id
            
            # Validate and parse date fields - set to None if invalid
            for date_field in ["LiveDate", "DeadlineDate"]:
                if date_field in term_payload:
                    parsed_date = parse_date_field(term_payload[date_field])
                    term_payload[date_field] = parsed_date

            existing_term = conn.execute(
                select(program_term_table)
                .where(program_term_table.c.ProgramID == program_id)
                .order_by(program_term_table.c.ProgramTermID)
            ).mappings().first()

            if existing_term:
                conn.execute(
                    program_term_table.update()
                    .where(program_term_table.c.ProgramTermID == existing_term["ProgramTermID"])
                    .values(**term_payload)
                )
            else:
                conn.execute(program_term_table.insert().values(**term_payload))

            selected_college_id = college_id_value

        if program_link_table is not None:
            link_payload = table_payloads.get("ProgramDepartmentLink", {})
            if link_payload:
                link_payload = link_payload.copy()
                if selected_college_id is None:
                    raise ValueError("Select a college before assigning a department.")
                if not link_payload.get("CollegeDepartmentID"):
                    raise ValueError("Select a department for the program.")

                link_payload["CollegeID"] = selected_college_id
                link_payload["ProgramID"] = program_id

                existing_link = conn.execute(
                    select(program_link_table).where(program_link_table.c.ProgramID == program_id)
                ).mappings().first()

                if existing_link:
                    conn.execute(
                        program_link_table.update()
                        .where(program_link_table.c.ProgramID == program_id)
                        .values(**link_payload)
                    )
                else:
                    conn.execute(program_link_table.insert().values(**link_payload))

        if program_test_table is not None:
            test_payload = table_payloads.get("ProgramTestScores", {})
            if test_payload:
                test_payload = test_payload.copy()
                test_payload["ProgramID"] = program_id

                existing_tests = conn.execute(
                    select(program_test_table).where(program_test_table.c.ProgramID == program_id)
                ).mappings().first()

                if existing_tests:
                    conn.execute(
                        program_test_table.update()
                        .where(program_test_table.c.ProgramID == program_id)
                        .values(**test_payload)
                    )
                else:
                    conn.execute(program_test_table.insert().values(**test_payload))

    return program_id


def find_program_by_name(engine, program_name: str) -> Optional[int]:
    """Find a program by name (case-insensitive). Returns ProgramID if found, None otherwise."""
    if not program_name:
        return None
    
    program_table = fetch_table("Program")
    stmt = select(program_table.c.ProgramID).where(
        func.lower(program_table.c.ProgramName) == func.lower(program_name)
    )
    
    with engine.connect() as conn:
        result = conn.execute(stmt).first()
        if result:
            return int(result.ProgramID)
    return None


def get_college_options(engine) -> List[Tuple[int, str]]:
    college_table = fetch_table("College")
    stmt = select(
        college_table.c.CollegeID,
        college_table.c.CollegeName,
    ).order_by(college_table.c.CollegeName)

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    options = []
    for row in rows:
        name = row.CollegeName if row.CollegeName else f"College #{row.CollegeID}"
        options.append((int(row.CollegeID), name))
    return options


def get_college_department_options(engine) -> List[Tuple[int, str]]:
    college_department_table = fetch_table("CollegeDepartment", required=False)
    college_table = fetch_table("College", required=False)
    department_table = fetch_table("Department", required=False)

    if college_department_table is None or college_table is None or department_table is None:
        return []

    stmt = (
        select(
            college_department_table.c.CollegeDepartmentID,
            college_table.c.CollegeName,
            department_table.c.DepartmentName,
        )
        .select_from(
            college_department_table.join(
                college_table,
                college_table.c.CollegeID == college_department_table.c.CollegeID,
            ).join(
                department_table,
                department_table.c.DepartmentID == college_department_table.c.DepartmentID,
            )
        )
        .order_by(college_table.c.CollegeName, department_table.c.DepartmentName)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    options = []
    for row in rows:
        college_name = row.CollegeName or f"College #{row.CollegeDepartmentID}"
        department_name = row.DepartmentName or "Department"
        label = f"{college_name} — {department_name}"
        options.append((int(row.CollegeDepartmentID), label))
    return options




@extract_bp.route("/extract", methods=["GET", "POST"])
def extract_page():
    mode = request.args.get("mode", "").strip()  # Get mode from query parameter
    
    # Initialize context with all required fields
    try:
        engine = get_engine()
        college_options = get_college_options(engine)
    except Exception:
        college_options = []
    
    context = {
        "url": "",
        "prompt": "",
        "extracted_text": "",
        "llm_output": "",
        "primary_title": "",
        "primary_heading": "",
        "mode": mode,
        "college_options": college_options,
        "review_fields": [],
        "matched_college": None,
        "social_links": {},
        "links": [],
    }

    if request.method == "POST":
        url = request.form.get("url", "").strip()
        prompt = request.form.get("prompt", "").strip()
        # Preserve mode from form or query parameter
        mode = request.form.get("mode", request.args.get("mode", "")).strip()
        context.update({"url": url, "prompt": prompt, "mode": mode})

        if not url:
            flash("Please provide a URL.", "error")
            return render_template("extract.html", **context)

        if not is_valid_url(url):
            flash("The URL provided is invalid. Include the scheme (e.g. https://).", "error")
            return render_template("extract.html", **context)

        try:
            headers = current_app.config.get("SCRAPER_HEADERS", {})
            response = requests.get(url, timeout=15, headers=headers)
            response.raise_for_status()
        except requests.RequestException as exc:
            flash(f"Failed to fetch the page: {exc}", "error")
            return render_template("extract.html", **context)

        soup = BeautifulSoup(response.text, "html.parser")
        primary_content = extract_page_content(soup)
        extracted_text = primary_content["text"]
        context["extracted_text"] = extracted_text
        context["primary_title"] = primary_content["title"]
        context["primary_heading"] = primary_content["heading"]
        
        # Extract links and social media links
        try:
            links = collect_links(soup, url, max_links=50)
            context["links"] = links
            
            # Extract social media links
            social_links = {}
            for link in links:
                url_lower = link.get("url", "").lower()
                text_lower = link.get("text", "").lower()
                if "facebook.com" in url_lower or "fb.com" in url_lower:
                    social_links["facebook"] = link["url"]
                elif "instagram.com" in url_lower or "instagr.am" in url_lower:
                    social_links["instagram"] = link["url"]
                elif "twitter.com" in url_lower or "x.com" in url_lower:
                    social_links["twitter"] = link["url"]
                elif "youtube.com" in url_lower:
                    social_links["youtube"] = link["url"]
                elif "tiktok.com" in url_lower:
                    social_links["tiktok"] = link["url"]
                elif "linkedin.com" in url_lower:
                    social_links["linkedin"] = link["url"]
            context["social_links"] = social_links
        except Exception:
            context["links"] = []
            context["social_links"] = {}

        if prompt:
            try:
                llm_output = generate_gemini_response(prompt, url, primary_content)
                context["llm_output"] = llm_output
                
                # Parse JSON and build review fields if LLM output exists
                try:
                    text = llm_output.strip()
                    if text.startswith("```"):
                        lines = text.split("\n")
                        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                    if text.startswith("```json"):
                        lines = text.split("\n")
                        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                    start = text.find("{")
                    end = text.rfind("}")
                    if start >= 0 and end > start:
                        parsed = json.loads(text[start:end+1])
                    else:
                        parsed = json.loads(text)
                    
                    if parsed:
                        # Match college by name
                        college_name = parsed.get("CollegeName", "").strip()
                        matched_college = None
                        if engine and college_name:
                            matched_college = find_college_by_name(engine, college_name)
                        context["matched_college"] = matched_college
                        
                        # Build review fields for college overview
                        college_fields = [
                            "CollegeName", "CollegeSetting", "TypeofInstitution", "Student_Faculty",
                            "NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
                            "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
                            "TotalStudents", "TotalUndergradMajors", "CountriesRepresented"
                        ]
                        
                        review_fields = []
                        if engine and matched_college:
                            college_table = fetch_table("College")
                            with engine.connect() as conn:
                                existing = conn.execute(
                                    select(college_table).where(college_table.c.CollegeID == matched_college["CollegeID"])
                                ).mappings().first()
                                existing_dict = dict(existing) if existing else {}
                        else:
                            existing_dict = {}
                        
                        for field in college_fields:
                            review_fields.append({
                                "column": field,
                                "label": humanize_field_name(field),
                                "llm_value": parsed.get(field, ""),
                                "existing_value": existing_dict.get(field, ""),
                            })
                        context["review_fields"] = review_fields
                except (json.JSONDecodeError, ValueError, Exception):
                    # If parsing fails, leave review_fields empty
                    context["review_fields"] = []
                    
            except RuntimeError as exc:
                flash(str(exc), "error")
            except Exception as exc:
                flash(f"Gemini API error: {exc}", "error")

    return render_template("extract.html", **context)


@extract_bp.route("/extract/college")
def extract_college_link():
    """Quick link to extract page with college mode."""
    return redirect(url_for("extract.extract_page", mode="college"))


@extract_bp.route("/extract/department")
def extract_department_link():
    """Quick link to extract page with department mode."""
    return redirect(url_for("extract.extract_page", mode="department"))


@extract_bp.route("/extract/program")
def extract_program_link():
    """Quick link to extract page with program mode."""
    return redirect(url_for("extract.extract_page", mode="program"))


def build_college_overview_prompt() -> str:
    """Prompt for extracting college overview/basics."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with the fields below.\n"
        "Use ONLY the provided text. If the extraction is unsuccessful or the text is empty, "
        "return all fields as empty strings (DO NOT hallucinate). Do not add extra keys or commentary.\n\n"
        "Rules:\n"
        "- Prefer institution-wide values (not sub-units).\n"
        "- If multiple candidates exist, pick the clearest, most explicit value.\n"
        "- If a field cannot be found verbatim or by strong implication, set it to \"\".\n"
        "- Numbers may include commas; return them as strings.\n"
        "- Output MUST be valid JSON (no markdown fences).\n\n"
        "Return exactly these keys (values as strings):\n"
        "{\n"
        "  \"CollegeName\": \"\",\n"
        "  \"CollegeSetting\": \"\",\n"
        "  \"TypeofInstitution\": \"\",\n"
        "  \"Student_Faculty\": \"\",\n"
        "  \"NumberOfCampuses\": \"\",\n"
        "  \"TotalFacultyAvailable\": \"\",\n"
        "  \"TotalProgramsAvailable\": \"\",\n"
        "  \"TotalStudentsEnrolled\": \"\",\n"
        "  \"TotalGraduatePrograms\": \"\",\n"
        "  \"TotalInternationalStudents\": \"\",\n"
        "  \"TotalStudents\": \"\",\n"
        "  \"TotalUndergradMajors\": \"\",\n"
        "  \"CountriesRepresented\": \"\"\n"
        "}\n\n"
        "Now extract the values from the provided text and return ONLY the JSON object."
    )


def build_address_prompt() -> str:
    """Prompt for extracting address/location details."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with address fields.\n"
        "Use ONLY the provided text. If a field cannot be found, return it as an empty string.\n\n"
        "Extract:\n"
        "- Street1: Primary street address\n"
        "- Street2: Secondary address line (suite, building, etc.)\n"
        "- City: City name\n"
        "- State: State or province (2-letter abbreviation preferred)\n"
        "- ZipCode: Postal/ZIP code\n"
        "- Country: Country name (default to 'USA' if in United States)\n"
        "- County: County name if mentioned\n\n"
        "Return a JSON object with these exact keys (values as strings):\n"
        "{\n"
        "  \"Street1\": \"\",\n"
        "  \"Street2\": \"\",\n"
        "  \"City\": \"\",\n"
        "  \"State\": \"\",\n"
        "  \"ZipCode\": \"\",\n"
        "  \"Country\": \"\",\n"
        "  \"County\": \"\"\n"
        "}\n\n"
        "Now extract the values and return ONLY the JSON object."
    )


def build_contact_prompt() -> str:
    """Prompt for extracting contact information."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with contact fields.\n"
        "Use ONLY the provided text. If a field cannot be found, return it as an empty string.\n\n"
        "Extract:\n"
        "- Phone: Primary phone number\n"
        "- Email: Primary contact email\n"
        "- SecondaryEmail: Secondary email if available\n"
        "- WebsiteUrl: Official website URL\n"
        "- AdmissionOfficeUrl: Admissions office URL\n"
        "- VirtualTourUrl: Virtual tour URL if available\n"
        "- FinancialAidUrl: Financial aid URL if available\n"
        "- LogoPath: Logo image URL if available\n\n"
        "Return a JSON object with these exact keys (values as strings):\n"
        "{\n"
        "  \"Phone\": \"\",\n"
        "  \"Email\": \"\",\n"
        "  \"SecondaryEmail\": \"\",\n"
        "  \"WebsiteUrl\": \"\",\n"
        "  \"AdmissionOfficeUrl\": \"\",\n"
        "  \"VirtualTourUrl\": \"\",\n"
        "  \"FinancialAidUrl\": \"\",\n"
        "  \"LogoPath\": \"\"\n"
        "}\n\n"
        "Now extract the values and return ONLY the JSON object."
    )


def build_appreq_prompt() -> str:
    """Prompt for extracting application requirements."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with application requirement fields.\n"
        "Use ONLY the provided text. If a field cannot be found, return it as an empty string.\n\n"
        "Extract:\n"
        "- ApplicationFees: Application fee amount\n"
        "- TuitionFees: Tuition fees (prioritize international student's per credit cost)\n"
        "- TestPolicy: Summary of standardized test policies\n"
        "- CoursesAndGrades: Information about required courses or grade expectations\n"
        "- Recommendations: Recommendation requirements\n"
        "- PersonalEssay: Summary of personal essay requirements\n"
        "- WritingSample: Writing sample requirements\n"
        "- AdditionalInformation: Other admissions information\n"
        "- AdditionalDeadlines: Any additional application deadlines\n\n"
        "Return a JSON object with these exact keys (values as strings):\n"
        "{\n"
        "  \"ApplicationFees\": \"\",\n"
        "  \"TuitionFees\": \"\",\n"
        "  \"TestPolicy\": \"\",\n"
        "  \"CoursesAndGrades\": \"\",\n"
        "  \"Recommendations\": \"\",\n"
        "  \"PersonalEssay\": \"\",\n"
        "  \"WritingSample\": \"\",\n"
        "  \"AdditionalInformation\": \"\",\n"
        "  \"AdditionalDeadlines\": \"\"\n"
        "}\n\n"
        "Now extract the values and return ONLY the JSON object."
    )


def build_stats_prompt() -> str:
    """Prompt for extracting student statistics and funding."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with student statistics and funding fields.\n"
        "Use ONLY the provided text. If a field cannot be found, return it as an empty string.\n"
        "Numbers may include commas; return them as strings.\n\n"
        "Extract:\n"
        "- GradAvgTuition: Average annual tuition for graduate students\n"
        "- GradInternationalStudents: Number of international graduate students\n"
        "- GradScholarshipHigh: Highest scholarship amount for graduate students\n"
        "- GradScholarshipLow: Lowest scholarship amount for graduate students\n"
        "- GradTotalStudents: Total number of graduate students\n"
        "- UGAvgTuition: Average annual tuition for undergraduate students\n"
        "- UGInternationalStudents: Number of international undergraduate students\n"
        "- UGScholarshipHigh: Highest scholarship amount for undergraduate students\n"
        "- UGScholarshipLow: Lowest scholarship amount for undergraduate students\n"
        "- UGTotalStudents: Total number of undergraduate students\n\n"
        "Return a JSON object with these exact keys (values as strings):\n"
        "{\n"
        "  \"GradAvgTuition\": \"\",\n"
        "  \"GradInternationalStudents\": \"\",\n"
        "  \"GradScholarshipHigh\": \"\",\n"
        "  \"GradScholarshipLow\": \"\",\n"
        "  \"GradTotalStudents\": \"\",\n"
        "  \"UGAvgTuition\": \"\",\n"
        "  \"UGInternationalStudents\": \"\",\n"
        "  \"UGScholarshipHigh\": \"\",\n"
        "  \"UGScholarshipLow\": \"\",\n"
        "  \"UGTotalStudents\": \"\"\n"
        "}\n\n"
        "Now extract the values and return ONLY the JSON object."
    )


def build_social_prompt() -> str:
    """Prompt for extracting social media links."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with social media profile URLs.\n"
        "Use ONLY the provided text. If a field cannot be found, return it as an empty string.\n\n"
        "Extract official social media profile URLs:\n"
        "- Facebook: Facebook page URL\n"
        "- Instagram: Instagram profile URL\n"
        "- Twitter: Twitter/X profile URL\n"
        "- Youtube: YouTube channel URL\n"
        "- Tiktok: TikTok account URL\n"
        "- LinkedIn: LinkedIn page URL\n\n"
        "Return a JSON object with these exact keys (values as strings, full URLs):\n"
        "{\n"
        "  \"Facebook\": \"\",\n"
        "  \"Instagram\": \"\",\n"
        "  \"Twitter\": \"\",\n"
        "  \"Youtube\": \"\",\n"
        "  \"Tiktok\": \"\",\n"
        "  \"LinkedIn\": \"\"\n"
        "}\n\n"
        "Now extract the values and return ONLY the JSON object."
    )


@extract_bp.route("/extract/api/run-college", methods=["POST"])
def api_run_college():
    """API endpoint to run college basics extraction."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    text = data.get("text", "").strip() if data else ""
    url = data.get("url", "").strip() if data else ""
    
    if not text:
        return jsonify({"ok": False, "error": "No text provided."}), 400
    
    try:
        prompt = build_college_overview_prompt()
        primary_content = {"text": text, "title": "", "heading": ""}
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text_output = llm_output.strip()
            if text_output.startswith("```"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            if text_output.startswith("```json"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            start = text_output.find("{")
            end = text_output.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text_output[start:end+1])
            else:
                parsed = json.loads(text_output)
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Match college and build review fields
        matched_college = None
        review_fields = []
        if parsed:
            college_name = parsed.get("CollegeName", "").strip()
            if engine and college_name:
                matched_college = find_college_by_name(engine, college_name)
            
            college_fields = [
                "CollegeName", "CollegeSetting", "TypeofInstitution", "Student_Faculty",
                "NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
                "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
                "TotalStudents", "TotalUndergradMajors", "CountriesRepresented"
            ]
            
            existing_dict = {}
            if engine and matched_college:
                college_table = fetch_table("College")
                with engine.connect() as conn:
                    existing = conn.execute(
                        select(college_table).where(college_table.c.CollegeID == matched_college["CollegeID"])
                    ).mappings().first()
                    existing_dict = dict(existing) if existing else {}
            
            for field in college_fields:
                review_fields.append({
                    "column": field,
                    "label": humanize_field_name(field),
                    "llm_value": parsed.get(field, ""),
                    "existing_value": existing_dict.get(field, ""),
                })
        
        return jsonify({
            "ok": True,
            "llm_output": llm_output,
            "review_fields": review_fields,
            "matched": matched_college
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract/api/run-address", methods=["POST"])
def api_run_address():
    """API endpoint to run address extraction."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    text = data.get("text", "").strip() if data else ""
    url = data.get("url", "").strip() if data else ""
    college_id = None
    if data and "college_id" in data:
        try:
            college_id_val = data.get("college_id")
            college_id = int(college_id_val) if college_id_val else None
        except (ValueError, TypeError):
            college_id = None
    
    if not text:
        return jsonify({"ok": False, "error": "No text provided."}), 400
    
    try:
        prompt = build_address_prompt()
        primary_content = {"text": text, "title": "", "heading": ""}
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text_output = llm_output.strip()
            if text_output.startswith("```"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            start = text_output.find("{")
            end = text_output.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text_output[start:end+1])
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Build review fields
        review_fields = []
        address_fields = ["Street1", "Street2", "City", "State", "ZipCode", "Country", "County"]
        
        existing_dict = {}
        if engine and college_id:
            address_table = fetch_table("Address", required=False)
            if address_table:
                with engine.connect() as conn:
                    existing = conn.execute(
                        select(address_table).where(address_table.c.CollegeID == college_id)
                    ).mappings().first()
                    existing_dict = dict(existing) if existing else {}
        
        for field in address_fields:
            review_fields.append({
                "column": field,
                "label": humanize_field_name(field),
                "llm_value": parsed.get(field, ""),
                "existing_value": existing_dict.get(field, ""),
            })
        
        return jsonify({
            "ok": True,
            "llm_output": llm_output,
            "review_fields": review_fields
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract/api/run-contact", methods=["POST"])
def api_run_contact():
    """API endpoint to run contact extraction."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    text = data.get("text", "").strip() if data else ""
    url = data.get("url", "").strip() if data else ""
    college_id = None
    if data and "college_id" in data:
        try:
            college_id_val = data.get("college_id")
            college_id = int(college_id_val) if college_id_val else None
        except (ValueError, TypeError):
            college_id = None
    
    if not text:
        return jsonify({"ok": False, "error": "No text provided."}), 400
    
    try:
        prompt = build_contact_prompt()
        primary_content = {"text": text, "title": "", "heading": ""}
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text_output = llm_output.strip()
            if text_output.startswith("```"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            start = text_output.find("{")
            end = text_output.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text_output[start:end+1])
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Build review fields
        review_fields = []
        contact_fields = ["Phone", "Email", "SecondaryEmail", "WebsiteUrl", "AdmissionOfficeUrl", 
                         "VirtualTourUrl", "FinancialAidUrl", "LogoPath"]
        
        existing_dict = {}
        if engine and college_id:
            contact_table = fetch_table("ContactInformation", required=False)
            if contact_table:
                with engine.connect() as conn:
                    existing = conn.execute(
                        select(contact_table).where(contact_table.c.CollegeID == college_id)
                    ).mappings().first()
                    existing_dict = dict(existing) if existing else {}
        
        for field in contact_fields:
            review_fields.append({
                "column": field,
                "label": humanize_field_name(field),
                "llm_value": parsed.get(field, ""),
                "existing_value": existing_dict.get(field, ""),
            })
        
        return jsonify({
            "ok": True,
            "llm_output": llm_output,
            "review_fields": review_fields
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract/api/run-appreq", methods=["POST"])
def api_run_appreq():
    """API endpoint to run application requirements extraction."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    text = data.get("text", "").strip() if data else ""
    url = data.get("url", "").strip() if data else ""
    college_id = None
    if data and "college_id" in data:
        try:
            college_id_val = data.get("college_id")
            college_id = int(college_id_val) if college_id_val else None
        except (ValueError, TypeError):
            college_id = None
    
    if not text:
        return jsonify({"ok": False, "error": "No text provided."}), 400
    
    try:
        prompt = build_appreq_prompt()
        primary_content = {"text": text, "title": "", "heading": ""}
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text_output = llm_output.strip()
            if text_output.startswith("```"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            start = text_output.find("{")
            end = text_output.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text_output[start:end+1])
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Build review fields
        review_fields = []
        appreq_fields = ["ApplicationFees", "TuitionFees", "TestPolicy", "CoursesAndGrades",
                        "Recommendations", "PersonalEssay", "WritingSample", "AdditionalInformation",
                        "AdditionalDeadlines"]
        
        existing_dict = {}
        if engine and college_id:
            appreq_table = fetch_table("ApplicationRequirements", required=False)
            if appreq_table:
                with engine.connect() as conn:
                    existing = conn.execute(
                        select(appreq_table).where(appreq_table.c.CollegeID == college_id)
                    ).mappings().first()
                    existing_dict = dict(existing) if existing else {}
        
        for field in appreq_fields:
            review_fields.append({
                "column": field,
                "label": humanize_field_name(field),
                "llm_value": parsed.get(field, ""),
                "existing_value": existing_dict.get(field, ""),
            })
        
        return jsonify({
            "ok": True,
            "llm_output": llm_output,
            "review_fields": review_fields
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract/api/run-stats", methods=["POST"])
def api_run_stats():
    """API endpoint to run student statistics extraction."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    text = data.get("text", "").strip() if data else ""
    url = data.get("url", "").strip() if data else ""
    college_id = None
    if data and "college_id" in data:
        try:
            college_id_val = data.get("college_id")
            college_id = int(college_id_val) if college_id_val else None
        except (ValueError, TypeError):
            college_id = None
    
    if not text:
        return jsonify({"ok": False, "error": "No text provided."}), 400
    
    try:
        prompt = build_stats_prompt()
        primary_content = {"text": text, "title": "", "heading": ""}
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text_output = llm_output.strip()
            if text_output.startswith("```"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            start = text_output.find("{")
            end = text_output.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text_output[start:end+1])
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Build review fields
        review_fields = []
        stats_fields = ["GradAvgTuition", "GradInternationalStudents", "GradScholarshipHigh",
                       "GradScholarshipLow", "GradTotalStudents", "UGAvgTuition",
                       "UGInternationalStudents", "UGScholarshipHigh", "UGScholarshipLow",
                       "UGTotalStudents"]
        
        existing_dict = {}
        if engine and college_id:
            stats_table = fetch_table("StudentStatistics", required=False)
            if stats_table:
                with engine.connect() as conn:
                    existing = conn.execute(
                        select(stats_table).where(stats_table.c.CollegeID == college_id)
                    ).mappings().first()
                    existing_dict = dict(existing) if existing else {}
        
        for field in stats_fields:
            review_fields.append({
                "column": field,
                "label": humanize_field_name(field),
                "llm_value": parsed.get(field, ""),
                "existing_value": existing_dict.get(field, ""),
            })
        
        return jsonify({
            "ok": True,
            "llm_output": llm_output,
            "review_fields": review_fields
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract/api/run-social", methods=["POST"])
def api_run_social():
    """API endpoint to run social media extraction."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    text = data.get("text", "").strip() if data else ""
    url = data.get("url", "").strip() if data else ""
    college_id = None
    if data and "college_id" in data:
        try:
            college_id_val = data.get("college_id")
            college_id = int(college_id_val) if college_id_val else None
        except (ValueError, TypeError):
            college_id = None
    
    if not text:
        return jsonify({"ok": False, "error": "No text provided."}), 400
    
    try:
        prompt = build_social_prompt()
        primary_content = {"text": text, "title": "", "heading": ""}
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text_output = llm_output.strip()
            if text_output.startswith("```"):
                lines = text_output.split("\n")
                text_output = "\n".join(lines[1:-1]) if len(lines) > 2 else text_output
            start = text_output.find("{")
            end = text_output.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text_output[start:end+1])
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Build review fields
        review_fields = []
        social_fields = ["Facebook", "Instagram", "Twitter", "Youtube", "Tiktok", "LinkedIn"]
        
        social_existing = {}
        if engine and college_id:
            social_table = fetch_table("SocialMedia", required=False)
            if social_table:
                with engine.connect() as conn:
                    rows = conn.execute(
                        select(social_table).where(social_table.c.CollegeID == college_id)
                    ).mappings().all()
                    social_existing = {row["PlatformName"].lower(): dict(row) for row in rows}
        
        for field in social_fields:
            existing_row = social_existing.get(field.lower(), {})
            review_fields.append({
                "column": field,
                "label": humanize_field_name(field),
                "llm_value": parsed.get(field, ""),
                "existing_value": existing_row.get("URL", ""),
            })
        
        return jsonify({
            "ok": True,
            "llm_output": llm_output,
            "review_fields": review_fields
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract/confirm-save", methods=["POST"])
def extract_confirm_save():
    """Save extracted college overview data to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    college_id = request.form.get("college_id", type=int) or request.form.get("college_id_select", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Find or create college
    if college_id:
        # Use provided college_id
        pass
    elif college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            # Create new college
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    else:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Collect override values for College table fields
    college_payload = {}
    college_fields = [
        "CollegeName", "CollegeSetting", "TypeofInstitution", "Student_Faculty",
        "NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
        "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
        "TotalStudents", "TotalUndergradMajors", "CountriesRepresented"
    ]
    
    for field in college_fields:
        val = request.form.get(f"override.{field}", "").strip()
        if val:
            # Normalize numeric fields
            if field in ["NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
                        "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
                        "TotalStudents", "TotalUndergradMajors"]:
                val = re.sub(r'[^\d]', '', val)  # Remove non-digits
            college_payload[field] = val
    
    # Save to database
    try:
        college_table = fetch_table("College")
        with engine.begin() as conn:
            if college_payload:
                conn.execute(
                    college_table.update()
                    .where(college_table.c.CollegeID == college_id)
                    .values(**college_payload)
                )
        
        msg = f"College overview saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.extract_page"))
    except (SQLAlchemyError, ValueError) as exc:
        msg = f"Failed to save: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    except Exception as exc:
        msg = f"Error saving: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))


@extract_bp.route("/extract/save-to-db", methods=["POST"])
def extract_save_to_db():
    """Quick save: Save LLM output directly to database without review."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    llm_output = request.form.get("llm_output", "").strip()
    url = request.form.get("url", "").strip()
    
    if not llm_output:
        flash("No LLM output to save.", "error")
        return redirect(url_for("extract.extract_page"))
    
    try:
        # Parse JSON from LLM output
        parsed = {}
        try:
            text = llm_output.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
            if text.startswith("```json"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text[start:end+1])
            else:
                parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            flash("Invalid JSON in LLM output.", "error")
            return redirect(url_for("extract.extract_page"))
        
        if not parsed:
            flash("No data found in LLM output.", "error")
            return redirect(url_for("extract.extract_page"))
        
        # Extract college name
        college_name = parsed.get("CollegeName", "").strip()
        if not college_name:
            flash("College name not found in LLM output.", "error")
            return redirect(url_for("extract.extract_page"))
        
        # Find or create college
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            # Create new college
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
        
        # Collect college fields and normalize
        college_payload = {}
        college_fields = [
            "CollegeName", "CollegeSetting", "TypeofInstitution", "Student_Faculty",
            "NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
            "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
            "TotalStudents", "TotalUndergradMajors", "CountriesRepresented"
        ]
        
        for field in college_fields:
            val = parsed.get(field, "").strip() if isinstance(parsed.get(field), str) else str(parsed.get(field, ""))
            if val:
                # Normalize numeric fields
                if field in ["NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
                            "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
                            "TotalStudents", "TotalUndergradMajors"]:
                    val = re.sub(r'[^\d]', '', val)  # Remove non-digits
                college_payload[field] = val
        
        # Save to database
        if college_payload:
            college_table = fetch_table("College")
            with engine.begin() as conn:
                conn.execute(
                    college_table.update()
                    .where(college_table.c.CollegeID == college_id)
                    .values(**college_payload)
                )
        
        flash(f"College data saved successfully (ID: {college_id}).", "success")
        return redirect(url_for("extract.extract_page"))
        
    except Exception as exc:
        flash(f"Error saving to database: {exc}", "error")
        return redirect(url_for("extract.extract_page"))


def _normalize_payload_value(val: str, field: str) -> str:
    """Normalize a payload value based on field type."""
    if not val:
        return val
    # Remove commas and currency symbols for numeric fields
    if field in ["NumberOfCampuses", "TotalFacultyAvailable", "TotalProgramsAvailable",
                "TotalStudentsEnrolled", "TotalGraduatePrograms", "TotalInternationalStudents",
                "TotalStudents", "TotalUndergradMajors", "GradInternationalStudents",
                "GradTotalStudents", "UGInternationalStudents", "UGTotalStudents"]:
        return re.sub(r'[^\d]', '', val)  # Remove non-digits
    # For decimal/currency fields, keep digits and one decimal point
    if field in ["GradAvgTuition", "GradScholarshipHigh", "GradScholarshipLow",
                "UGAvgTuition", "UGScholarshipHigh", "UGScholarshipLow",
                "ApplicationFees", "TuitionFees"]:
        return re.sub(r'[^\d.]', '', val)  # Keep digits and decimal point
    return val


@extract_bp.route("/extract/confirm-save-address", methods=["POST"])
def extract_confirm_save_address():
    """Save extracted address data to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    college_id = request.form.get("college_id", type=int) or request.form.get("college_id_address_select", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Find or create college
    if not college_id and college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    
    if not college_id:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Collect address fields
    address_payload = {}
    address_fields = ["Street1", "Street2", "City", "State", "ZipCode", "Country", "County"]
    
    for field in address_fields:
        val = request.form.get(f"override_addr.{field}", "").strip()
        if val:
            address_payload[field] = val
    
    # Save to database
    try:
        address_table = fetch_table("Address", required=False)
        if not address_table:
            msg = "Address table not found."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_page"))
        
        with engine.begin() as conn:
            # Check if address exists
            existing = conn.execute(
                select(address_table).where(address_table.c.CollegeID == college_id)
            ).mappings().first()
            
            if existing:
                if address_payload:
                    conn.execute(
                        address_table.update()
                        .where(address_table.c.CollegeID == college_id)
                        .values(**address_payload)
                    )
            else:
                address_payload["CollegeID"] = college_id
                conn.execute(address_table.insert().values(**address_payload))
        
        msg = f"Address saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.extract_page"))
    except Exception as exc:
        msg = f"Failed to save address: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))


@extract_bp.route("/extract/confirm-save-contact", methods=["POST"])
def extract_confirm_save_contact():
    """Save extracted contact data to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    college_id = request.form.get("college_id", type=int) or request.form.get("college_id_contact_select", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Find or create college
    if not college_id and college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    
    if not college_id:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Collect contact fields
    contact_payload = {}
    contact_fields = ["Phone", "Email", "SecondaryEmail", "WebsiteUrl", "AdmissionOfficeUrl",
                     "VirtualTourUrl", "FinancialAidUrl", "LogoPath"]
    
    for field in contact_fields:
        val = request.form.get(f"override_contact.{field}", "").strip()
        if val:
            contact_payload[field] = val
    
    # Save to database
    try:
        contact_table = fetch_table("ContactInformation", required=False)
        if not contact_table:
            msg = "ContactInformation table not found."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_page"))
        
        with engine.begin() as conn:
            existing = conn.execute(
                select(contact_table).where(contact_table.c.CollegeID == college_id)
            ).mappings().first()
            
            if existing:
                if contact_payload:
                    conn.execute(
                        contact_table.update()
                        .where(contact_table.c.CollegeID == college_id)
                        .values(**contact_payload)
                    )
            else:
                contact_payload["CollegeID"] = college_id
                conn.execute(contact_table.insert().values(**contact_payload))
        
        msg = f"Contact information saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.extract_page"))
    except Exception as exc:
        msg = f"Failed to save contact: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))


@extract_bp.route("/extract/confirm-save-appreq", methods=["POST"])
def extract_confirm_save_appreq():
    """Save extracted application requirements data to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    college_id = request.form.get("college_id", type=int) or request.form.get("college_id_appreq_select", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Find or create college
    if not college_id and college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    
    if not college_id:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Collect application requirement fields
    appreq_payload = {}
    appreq_fields = ["ApplicationFees", "TuitionFees", "TestPolicy", "CoursesAndGrades",
                    "Recommendations", "PersonalEssay", "WritingSample", "AdditionalInformation",
                    "AdditionalDeadlines"]
    
    for field in appreq_fields:
        val = request.form.get(f"override_appreq.{field}", "").strip()
        if val:
            appreq_payload[field] = _normalize_payload_value(val, field)
    
    # Save to database
    try:
        appreq_table = fetch_table("ApplicationRequirements", required=False)
        if not appreq_table:
            msg = "ApplicationRequirements table not found."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_page"))
        
        with engine.begin() as conn:
            existing = conn.execute(
                select(appreq_table).where(appreq_table.c.CollegeID == college_id)
            ).mappings().first()
            
            if existing:
                if appreq_payload:
                    conn.execute(
                        appreq_table.update()
                        .where(appreq_table.c.CollegeID == college_id)
                        .values(**appreq_payload)
                    )
            else:
                appreq_payload["CollegeID"] = college_id
                conn.execute(appreq_table.insert().values(**appreq_payload))
        
        msg = f"Application requirements saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.extract_page"))
    except Exception as exc:
        msg = f"Failed to save application requirements: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))


@extract_bp.route("/extract/confirm-save-stats", methods=["POST"])
def extract_confirm_save_stats():
    """Save extracted student statistics data to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    college_id = request.form.get("college_id", type=int) or request.form.get("college_id_stats_select", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Find or create college
    if not college_id and college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    
    if not college_id:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Collect student statistics fields
    stats_payload = {}
    stats_fields = ["GradAvgTuition", "GradInternationalStudents", "GradScholarshipHigh",
                   "GradScholarshipLow", "GradTotalStudents", "UGAvgTuition",
                   "UGInternationalStudents", "UGScholarshipHigh", "UGScholarshipLow",
                   "UGTotalStudents"]
    
    for field in stats_fields:
        val = request.form.get(f"override_stats.{field}", "").strip()
        if val:
            stats_payload[field] = _normalize_payload_value(val, field)
    
    # Save to database
    try:
        stats_table = fetch_table("StudentStatistics", required=False)
        if not stats_table:
            msg = "StudentStatistics table not found."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_page"))
        
        with engine.begin() as conn:
            existing = conn.execute(
                select(stats_table).where(stats_table.c.CollegeID == college_id)
            ).mappings().first()
            
            if existing:
                if stats_payload:
                    conn.execute(
                        stats_table.update()
                        .where(stats_table.c.CollegeID == college_id)
                        .values(**stats_payload)
                    )
            else:
                stats_payload["CollegeID"] = college_id
                conn.execute(stats_table.insert().values(**stats_payload))
        
        msg = f"Student statistics saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.extract_page"))
    except Exception as exc:
        msg = f"Failed to save Student Body & Funding to DB: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))


@extract_bp.route("/extract/confirm-save-social", methods=["POST"])
def extract_confirm_save_social():
    """Save extracted social media data to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    college_id = request.form.get("college_id", type=int) or request.form.get("college_id_social_select", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Find or create college
    if not college_id and college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    
    if not college_id:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))
    
    # Collect social media fields
    social_platforms = ["Facebook", "Instagram", "Twitter", "Youtube", "Tiktok", "LinkedIn"]
    social_payloads = []
    
    for platform in social_platforms:
        url = request.form.get(f"override_social.{platform}", "").strip()
        if url:
            social_payloads.append({
                "CollegeID": college_id,
                "PlatformName": platform,
                "URL": url
            })
    
    # Save to database
    try:
        social_table = fetch_table("SocialMedia", required=False)
        if not social_table:
            msg = "SocialMedia table not found."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_page"))
        
        with engine.begin() as conn:
            # Delete existing social media for this college
            conn.execute(
                social_table.delete().where(social_table.c.CollegeID == college_id)
            )
            
            # Insert new social media entries
            if social_payloads:
                conn.execute(social_table.insert(), social_payloads)
        
        msg = f"Social media saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.extract_page"))
    except Exception as exc:
        msg = f"Failed to save social media: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_page"))


def build_direct_contact_prompt() -> str:
    """Prompt for extracting university address, contact, and social media from a URL directly."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Analyze the provided webpage URL and extract university address, contact information, and social media links.\n"
        "Return a STRICT JSON object with the fields below. Use ONLY the information visible on the page.\n"
        "If a field cannot be found, return it as an empty string (DO NOT hallucinate).\n\n"
        "Extract the following fields:\n\n"
        "ADDRESS FIELDS:\n"
        "- Street1: Primary street address\n"
        "- Street2: Secondary address line (suite, building, etc.)\n"
        "- City: City name\n"
        "- State: State or province (2-letter abbreviation preferred)\n"
        "- ZipCode: Postal/ZIP code\n"
        "- Country: Country name (default to 'USA' if in United States)\n"
        "- County: County name if mentioned\n\n"
        "CONTACT FIELDS:\n"
        "- Phone: Primary phone number\n"
        "- Email: Primary contact email\n"
        "- SecondaryEmail: Secondary email if available\n"
        "- WebsiteUrl: Official website URL\n"
        "- AdmissionOfficeUrl: Admissions office URL\n"
        "- VirtualTourUrl: Virtual tour URL if available\n"
        "- FinancialAidUrl: Financial aid URL if available\n"
        "- LogoPath: Logo image URL if available\n\n"
        "SOCIAL MEDIA FIELDS:\n"
        "- Facebook: Facebook page URL\n"
        "- Instagram: Instagram profile URL\n"
        "- Twitter: Twitter/X profile URL\n"
        "- Youtube: YouTube channel URL\n"
        "- Tiktok: TikTok account URL\n"
        "- LinkedIn: LinkedIn page URL\n\n"
        "OUTPUT FORMAT:\n"
        "Return a JSON object with these exact keys (values as strings):\n"
        "{\n"
        "  \"Street1\": \"\",\n"
        "  \"Street2\": \"\",\n"
        "  \"City\": \"\",\n"
        "  \"State\": \"\",\n"
        "  \"ZipCode\": \"\",\n"
        "  \"Country\": \"\",\n"
        "  \"County\": \"\",\n"
        "  \"Phone\": \"\",\n"
        "  \"Email\": \"\",\n"
        "  \"SecondaryEmail\": \"\",\n"
        "  \"WebsiteUrl\": \"\",\n"
        "  \"AdmissionOfficeUrl\": \"\",\n"
        "  \"VirtualTourUrl\": \"\",\n"
        "  \"FinancialAidUrl\": \"\",\n"
        "  \"LogoPath\": \"\",\n"
        "  \"Facebook\": \"\",\n"
        "  \"Instagram\": \"\",\n"
        "  \"Twitter\": \"\",\n"
        "  \"Youtube\": \"\",\n"
        "  \"Tiktok\": \"\",\n"
        "  \"LinkedIn\": \"\"\n"
        "}\n\n"
        "Now analyze the webpage and return ONLY the JSON object."
    )


@extract_bp.route("/direct-contact", methods=["GET", "POST"])
def direct_contact_page():
    """Page for directly extracting contact and address info from a URL using Gemini."""
    try:
        engine = get_engine()
        context: Dict[str, Any] = {
            "url": "",
            "llm_output": "",
            "review_fields_address": [],
            "review_fields_contact": [],
            "review_fields_social": [],
            "college_options": [],
            "matched_college": None,
        }
        
        context["college_options"] = get_college_options(engine)
    except Exception:
        # Database not configured
        context = {
            "url": "",
            "llm_output": "",
            "review_fields_address": [],
            "review_fields_contact": [],
            "review_fields_social": [],
            "college_options": [],
            "matched_college": None,
        }
    
    if request.method == "POST":
        url = request.form.get("url", "").strip()
        context["url"] = url
        
        if not url:
            flash("Please provide a URL.", "error")
            return render_template("direct_contact.html", **context)
        
        if not is_valid_url(url):
            flash("The URL provided is invalid. Include the scheme (e.g. https://).", "error")
            return render_template("direct_contact.html", **context)
        
        try:
            engine = get_engine()
        except Exception:
            flash("Database is not configured.", "error")
            return render_template("direct_contact.html", **context)
        
        try:
            # Call Gemini directly with the URL (no text extraction first)
            prompt = build_direct_contact_prompt()
            primary_content = {"text": "", "title": "", "heading": ""}
            llm_output = generate_gemini_response(prompt, url, primary_content)
            context["llm_output"] = llm_output
            
            # Parse JSON from LLM output
            parsed = {}
            try:
                # Try to extract JSON from the output (handle code fences)
                text = llm_output.strip()
                if text.startswith("```"):
                    # Remove code fences
                    lines = text.split("\n")
                    text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                if text.startswith("```json"):
                    lines = text.split("\n")
                    text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                # Find JSON object boundaries
                start = text.find("{")
                end = text.rfind("}")
                if start >= 0 and end > start:
                    parsed = json.loads(text[start:end+1])
                else:
                    parsed = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                parsed = {}
            
            if parsed:
                # Build review fields for address (no existing values since college not matched yet)
                address_fields = ["Street1", "Street2", "City", "State", "ZipCode", "Country", "County"]
                review_fields_address = []
                for field in address_fields:
                    review_fields_address.append({
                        "column": field,
                        "label": humanize_field_name(field),
                        "llm_value": parsed.get(field, ""),
                        "existing_value": "",
                    })
                context["review_fields_address"] = review_fields_address
                
                # Build review fields for contact
                contact_fields = ["Phone", "Email", "SecondaryEmail", "WebsiteUrl", "AdmissionOfficeUrl", 
                                 "VirtualTourUrl", "FinancialAidUrl", "LogoPath"]
                review_fields_contact = []
                for field in contact_fields:
                    review_fields_contact.append({
                        "column": field,
                        "label": humanize_field_name(field),
                        "llm_value": parsed.get(field, ""),
                        "existing_value": "",
                    })
                context["review_fields_contact"] = review_fields_contact
                
                # Build review fields for social media
                social_fields = ["Facebook", "Instagram", "Twitter", "Youtube", "Tiktok", "LinkedIn"]
                review_fields_social = []
                for field in social_fields:
                    review_fields_social.append({
                        "column": field,
                        "label": humanize_field_name(field),
                        "llm_value": parsed.get(field, ""),
                        "existing_value": "",
                    })
                context["review_fields_social"] = review_fields_social
                
        except RuntimeError as exc:
            flash(str(exc), "error")
        except Exception as exc:
            flash(f"Error extracting contact information: {exc}", "error")
    
    return render_template("direct_contact.html", **context)


@extract_bp.route("/extract-departments", methods=["GET", "POST"])
def extract_departments_page():
    """Page for extracting department (admissions office) details."""
    try:
        engine = get_engine()
        context: Dict[str, Any] = {
            "url": "",
            "undergrad_url": "",
            "grad_url": "",
            "direct_url": "",
            "custom_prompt": "",
            "college_options": get_college_options(engine),
        }
    except Exception:
        context = {
            "url": "",
            "undergrad_url": "",
            "grad_url": "",
            "direct_url": "",
            "custom_prompt": "",
            "college_options": [],
        }
    
    return render_template("extract_departments.html", **context)


@extract_bp.route("/extract-departments/direct", methods=["POST"])
def extract_department_direct():
    """Direct department extraction endpoint."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    direct_url = data.get("direct_url", "").strip() if data else ""
    custom_prompt = data.get("custom_prompt", "").strip() if data else ""
    
    if not direct_url:
        return jsonify({"ok": False, "error": "Please provide a direct URL."}), 400
    
    if not is_valid_url(direct_url):
        return jsonify({"ok": False, "error": "Invalid URL format."}), 400
    
    try:
        # Fetch the page
        headers = current_app.config.get("SCRAPER_HEADERS", {})
        response = requests.get(direct_url, timeout=15, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        primary_content = extract_page_content(soup)
        extracted_text = primary_content["text"]
        
        # Use custom prompt or default
        if custom_prompt:
            prompt = custom_prompt
        else:
            prompt = (
                "You are an expert higher-education data extractor.\n"
                "Extract the following information for an Undergraduate Admissions or Graduate Admissions office:\n"
                "- DepartmentName: Full name of the admissions office\n"
                "- BuildingName: Building name where the office is located\n"
                "- Street1: Primary street address\n"
                "- Street2: Secondary address (suite, room, etc.)\n"
                "- City: City name\n"
                "- State: State abbreviation\n"
                "- ZipCode: ZIP code\n"
                "- Country: Country name\n"
                "- Email: Contact email address\n"
                "- PhoneNumber: Contact phone number\n"
                "- AdmissionUrl: URL of the admissions page\n"
                "- Description: Brief description of the office's purpose\n\n"
                "Return a JSON object with these fields. If multiple offices are found, return a JSON array."
            )
        
        # Call LLM
        llm_output = generate_gemini_response(prompt, direct_url, primary_content)
        
        # Parse JSON
        parsed = {}
        try:
            text = llm_output.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
            if text.startswith("```json"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                parsed = json.loads(text[start:end+1])
            else:
                parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            parsed = {}
        
        # Handle array response
        if isinstance(parsed, list):
            return jsonify({
                "ok": True,
                "extracted_departments": parsed,
                "matched_college": None
            })
        else:
            return jsonify({
                "ok": True,
                "extracted_department": parsed,
                "matched_college": None
            })
            
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@extract_bp.route("/extract-departments/save", methods=["POST"])
def extract_departments_save():
    """Save extracted departments to database."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_departments_page"))
    
    # This is a placeholder - the actual save logic would need to be implemented
    # based on the form structure in the template
    return jsonify({"ok": True, "message": "Departments saved successfully"})


@extract_bp.route("/extract-departments/api/undergrad", methods=["POST"])
def api_extract_undergrad():
    """Extract undergraduate admissions details."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    url = data.get("url", "").strip() if data else ""
    undergrad_url = data.get("undergrad_url", "").strip() if data else ""
    
    # Use direct URL if provided, otherwise try to find from main URL
    target_url = undergrad_url or url
    
    if not target_url:
        return jsonify({"ok": False, "error": "Please provide a URL."}), 400
    
    # Placeholder - implement actual extraction logic
    return jsonify({
        "ok": True,
        "extracted_department": {
            "DepartmentName": "Undergraduate Admissions",
            "Email": "",
            "PhoneNumber": "",
        },
        "matched_college": None
    })


@extract_bp.route("/extract-departments/api/grad", methods=["POST"])
def api_extract_grad():
    """Extract graduate admissions details."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    url = data.get("url", "").strip() if data else ""
    grad_url = data.get("grad_url", "").strip() if data else ""
    
    target_url = grad_url or url
    
    if not target_url:
        return jsonify({"ok": False, "error": "Please provide a URL."}), 400
    
    # Placeholder - implement actual extraction logic
    return jsonify({
        "ok": True,
        "extracted_department": {
            "DepartmentName": "Graduate Admissions",
            "Email": "",
            "PhoneNumber": "",
        },
        "matched_college": None
    })


@extract_bp.route("/extract-departments/api/all", methods=["POST"])
def api_extract_departments():
    """Extract all departments from a university URL."""
    try:
        engine = get_engine()
    except Exception:
        return jsonify({"ok": False, "error": "Database is not configured."}), 500
    
    data = request.get_json()
    url = data.get("url", "").strip() if data else ""
    
    if not url:
        return jsonify({"ok": False, "error": "Please provide a URL."}), 400
    
    # Placeholder - implement actual extraction logic
    return jsonify({
        "ok": True,
        "extracted_departments": [],
        "matched_college": None
    })


@extract_bp.route("/direct-contact/confirm-save", methods=["POST"])
def direct_contact_confirm_save():
    """Save address, contact, and social media data from direct contact extraction."""
    try:
        engine = get_engine()
    except Exception:
        msg = "Database is not configured."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.direct_contact_page"))
    
    college_id = request.form.get("college_id_direct_select", type=int) or request.form.get("college_id", type=int)
    college_name = request.form.get("college_name", "").strip()
    
    if not college_id and not college_name:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.direct_contact_page"))
    
    # Find or create college
    if college_id:
        # Use provided college_id
        pass
    elif college_name:
        matched_college = find_college_by_name(engine, college_name)
        if not matched_college:
            # Create new college
            college_table = fetch_table("College")
            with engine.begin() as conn:
                result = conn.execute(
                    college_table.insert().values(CollegeName=college_name)
                )
                college_id = int(result.inserted_primary_key[0])
        else:
            college_id = matched_college["CollegeID"]
    else:
        msg = "Please select a college or provide a college name."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(url_for("extract.direct_contact_page"))
    
    # Collect address fields
    address_payload = {}
    address_fields = ["Street1", "Street2", "City", "State", "ZipCode", "Country", "County"]
    for field in address_fields:
        val = request.form.get(f"override_addr.{field}", "").strip()
        if val:
            address_payload[field] = val
    
    # Collect contact fields
    contact_payload = {}
    contact_fields = ["Phone", "Email", "SecondaryEmail", "WebsiteUrl", "AdmissionOfficeUrl", 
                     "VirtualTourUrl", "FinancialAidUrl", "LogoPath"]
    for field in contact_fields:
        val = request.form.get(f"override_contact.{field}", "").strip()
        if val:
            contact_payload[field] = val
    
    # Collect social media fields
    social_payloads = {}
    social_fields = ["Facebook", "Instagram", "Twitter", "Youtube", "Tiktok", "LinkedIn"]
    for field in social_fields:
        val = request.form.get(f"override_social.{field}", "").strip()
        social_payloads[field] = val if val else None
    
    # Save to database
    try:
        table_payloads = {}
        if address_payload:
            table_payloads["Address"] = address_payload
        if contact_payload:
            table_payloads["ContactInformation"] = contact_payload
        
        persist_university_bundle(engine, college_id, table_payloads, social_payloads)
        
        msg = f"Address, Contact & Social Media saved successfully for college ID {college_id}."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": True, "message": msg})
        flash(msg, "success")
        return redirect(url_for("extract.direct_contact_page"))
    except (SQLAlchemyError, ValueError) as exc:
        msg = f"Failed to save: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.direct_contact_page"))
    except Exception as exc:
        msg = f"Error saving: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.direct_contact_page"))


# Program Extraction Routes and Prompts
def build_program_snapshot_prompt() -> str:
    """Prompt for extracting basic program information."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with program snapshot details.\n"
        "Use ONLY the provided text. If the extraction is unsuccessful or the text is empty, "
        "return all fields as empty strings (DO NOT hallucinate). Do not add extra keys or commentary.\n\n"
        "=== EXTRACTION REQUIREMENTS ===\n\n"
        "Extract the following program information:\n\n"
        "1. ProgramName: The full, official name of the program (e.g., 'Master of Science in Data Science', "
        "'Master of Business Administration', 'Bachelor of Arts in Accounting'). Include degree level and field.\n"
        "2. Level: Program level - must be one of: 'Undergraduate', 'Graduate', 'Certificate', 'Doctoral', 'Professional'.\n"
        "3. Concentration: Any specialization, track, or concentration within the program (e.g., 'Finance', 'Marketing Analytics', 'Machine Learning').\n"
        "4. Description: A comprehensive description of the program (2-5 sentences covering what the program is about, "
        "what students learn, career outcomes, etc.).\n"
        "5. ProgramWebsiteURL: The direct URL to the program's official page or catalog entry.\n"
        "6. Accreditation: Any accreditation information (e.g., 'AACSB Accredited', 'ABET Accredited', 'Regional Accreditation').\n"
        "7. QsWorldRanking: QS World University Rankings if mentioned for this specific program.\n\n"
        "=== EXPECTED OUTPUT FORMAT ===\n\n"
        "You MUST return a JSON object with these EXACT keys (all values as strings):\n\n"
        "{\n"
        "  \"ProgramName\": \"\",\n"
        "  \"Level\": \"\",\n"
        "  \"Concentration\": \"\",\n"
        "  \"Description\": \"\",\n"
        "  \"ProgramWebsiteURL\": \"\",\n"
        "  \"Accreditation\": \"\",\n"
        "  \"QsWorldRanking\": \"\"\n"
        "}\n\n"
        "=== EXAMPLE OUTPUT (Complete JSON with Sample Data) ===\n\n"
        "Here is an example of how your response should look with actual data:\n\n"
        "{\n"
        "  \"ProgramName\": \"Master of Science in Data Science\",\n"
        "  \"Level\": \"Graduate\",\n"
        "  \"Concentration\": \"Machine Learning\",\n"
        "  \"Description\": \"The Master of Science in Data Science program prepares students for careers in data analysis, machine learning, and artificial intelligence. Students learn advanced statistical methods, programming languages, and data visualization techniques. Graduates work in industries such as technology, finance, healthcare, and consulting.\",\n"
        "  \"ProgramWebsiteURL\": \"https://university.edu/programs/data-science\",\n"
        "  \"Accreditation\": \"ABET Accredited\",\n"
        "  \"QsWorldRanking\": \"45\"\n"
        "}\n\n"
        "=== IMPORTANT NOTES ===\n"
        "- Return ONLY the JSON object, no markdown code fences, no explanations\n"
        "- If a field cannot be found, use an empty string \"\"\n"
        "- All values must be strings (even numbers like QsWorldRanking)\n"
        "- Do not add any fields beyond those listed above\n\n"
        "Now extract the values from the provided text and return ONLY the JSON object in the exact format shown above."
    )


def build_program_requirements_prompt() -> str:
    """Prompt for extracting program application requirements."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with program application requirements.\n"
        "Use ONLY the provided text. If the extraction is unsuccessful or the text is empty, "
        "return all fields as empty strings or false (DO NOT hallucinate). Do not add extra keys or commentary.\n\n"
        "=== EXTRACTION REQUIREMENTS ===\n\n"
        "Extract ALL application requirements and materials:\n\n"
        "BOOLEAN FIELDS (true/false): Set to true ONLY if explicitly stated as required. Set to false if optional, not mentioned, or not required.\n"
        "- IsGMATRequired: GMAT is required\n"
        "- IsGreRequired: GRE is required\n"
        "- IsGMATOrGreRequired: Either GMAT or GRE is accepted\n"
        "- IsIELTSRequired: IELTS is required\n"
        "- IsTOEFLIBRequired: TOEFL iBT is required\n"
        "- IsTOEFLPBTRequired: TOEFL PBT is required\n"
        "- IsDuoLingoRequired: Duolingo English Test is required\n"
        "- IsELSRequired: ELS is required\n"
        "- IsPTERequired: PTE Academic is required\n"
        "- IsEnglishNotRequired: English proficiency test is NOT required\n"
        "- IsEnglishOptional: English proficiency test is optional\n"
        "- IsLSATRequired: LSAT is required (for law programs)\n"
        "- IsMCATRequired: MCAT is required (for medical programs)\n"
        "- IsMATRequired: MAT is required\n"
        "- IsACTRequired: ACT is required\n"
        "- IsSATRequired: SAT is required\n"
        "- IsAnalyticalNotRequired: Analytical writing section not required\n"
        "- IsAnalyticalOptional: Analytical writing section is optional\n"
        "- IsRecommendationSystemOpted: Recommendation system is used\n"
        "- IsStemProgram: This is a STEM program\n\n"
        "TEXT FIELDS:\n"
        "- Resume: Resume/CV requirements (e.g., 'Required', 'Optional', 'Not required', or specific instructions)\n"
        "- StatementOfPurpose: Statement of Purpose requirements and instructions\n"
        "- GreOrGmat: GRE or GMAT requirements and details\n"
        "- EnglishScore: English proficiency requirements (e.g., 'TOEFL 80', 'IELTS 6.5')\n"
        "- Requirements: General admission requirements (GPA, prerequisites, etc.)\n"
        "- WritingSample: Writing sample requirements\n"
        "- MinGPA: Minimum GPA required (e.g., '3.0', '3.5')\n"
        "- MaxGPA: Maximum GPA mentioned (if any)\n"
        "- MaxFails: Maximum number of failed courses allowed\n"
        "- PreviousYearAcceptanceRates: Acceptance rate or admission statistics\n\n"
        "=== EXPECTED OUTPUT FORMAT ===\n\n"
        "You MUST return a JSON object with these EXACT keys:\n\n"
        "{\n"
        "  \"Resume\": \"\",\n"
        "  \"StatementOfPurpose\": \"\",\n"
        "  \"GreOrGmat\": \"\",\n"
        "  \"EnglishScore\": \"\",\n"
        "  \"Requirements\": \"\",\n"
        "  \"WritingSample\": \"\",\n"
        "  \"IsAnalyticalNotRequired\": false,\n"
        "  \"IsAnalyticalOptional\": false,\n"
        "  \"IsDuoLingoRequired\": false,\n"
        "  \"IsELSRequired\": false,\n"
        "  \"IsGMATOrGreRequired\": false,\n"
        "  \"IsGMATRequired\": false,\n"
        "  \"IsGreRequired\": false,\n"
        "  \"IsIELTSRequired\": false,\n"
        "  \"IsLSATRequired\": false,\n"
        "  \"IsMATRequired\": false,\n"
        "  \"IsMCATRequired\": false,\n"
        "  \"IsPTERequired\": false,\n"
        "  \"IsTOEFLIBRequired\": false,\n"
        "  \"IsTOEFLPBTRequired\": false,\n"
        "  \"IsEnglishNotRequired\": false,\n"
        "  \"IsEnglishOptional\": false,\n"
        "  \"IsRecommendationSystemOpted\": false,\n"
        "  \"IsStemProgram\": false,\n"
        "  \"IsACTRequired\": false,\n"
        "  \"IsSATRequired\": false,\n"
        "  \"MaxFails\": \"\",\n"
        "  \"MaxGPA\": \"\",\n"
        "  \"MinGPA\": \"\",\n"
        "  \"PreviousYearAcceptanceRates\": \"\"\n"
        "}\n\n"
        "=== EXAMPLE OUTPUT (Complete JSON with Sample Data) ===\n\n"
        "Here is an example of how your response should look with actual data:\n\n"
        "{\n"
        "  \"Resume\": \"Required, maximum 2 pages\",\n"
        "  \"StatementOfPurpose\": \"Required, 500-1000 words describing academic and career goals\",\n"
        "  \"GreOrGmat\": \"GRE or GMAT required, minimum GRE 310 or GMAT 600\",\n"
        "  \"EnglishScore\": \"TOEFL iBT 80 or IELTS 6.5 required for international students\",\n"
        "  \"Requirements\": \"Bachelor's degree in related field, minimum GPA 3.0, prerequisite courses in statistics and programming\",\n"
        "  \"WritingSample\": \"Optional, academic writing sample preferred\",\n"
        "  \"IsAnalyticalNotRequired\": false,\n"
        "  \"IsAnalyticalOptional\": true,\n"
        "  \"IsDuoLingoRequired\": false,\n"
        "  \"IsELSRequired\": false,\n"
        "  \"IsGMATOrGreRequired\": true,\n"
        "  \"IsGMATRequired\": false,\n"
        "  \"IsGreRequired\": false,\n"
        "  \"IsIELTSRequired\": true,\n"
        "  \"IsLSATRequired\": false,\n"
        "  \"IsMATRequired\": false,\n"
        "  \"IsMCATRequired\": false,\n"
        "  \"IsPTERequired\": false,\n"
        "  \"IsTOEFLIBRequired\": true,\n"
        "  \"IsTOEFLPBTRequired\": false,\n"
        "  \"IsEnglishNotRequired\": false,\n"
        "  \"IsEnglishOptional\": false,\n"
        "  \"IsRecommendationSystemOpted\": true,\n"
        "  \"IsStemProgram\": true,\n"
        "  \"IsACTRequired\": false,\n"
        "  \"IsSATRequired\": false,\n"
        "  \"MaxFails\": \"2\",\n"
        "  \"MaxGPA\": \"4.0\",\n"
        "  \"MinGPA\": \"3.0\",\n"
        "  \"PreviousYearAcceptanceRates\": \"35%\"\n"
        "}\n\n"
        "=== IMPORTANT NOTES ===\n"
        "- Return ONLY the JSON object, no markdown code fences, no explanations\n"
        "- Boolean fields must be true or false (not strings)\n"
        "- Text fields must be strings (use empty string \"\" if not found)\n"
        "- Set boolean fields to true ONLY if explicitly stated as required\n"
        "- Do not add any fields beyond those listed above\n\n"
        "Now extract the values from the provided text and return ONLY the JSON object in the exact format shown above."
    )


def build_program_term_prompt() -> str:
    """Prompt for extracting program term details, costs, and deadlines."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with program term, cost, and deadline information.\n"
        "Use ONLY the provided text. If the extraction is unsuccessful or the text is empty, "
        "return all fields as empty strings (DO NOT hallucinate). Do not add extra keys or commentary.\n\n"
        "=== EXTRACTION REQUIREMENTS ===\n\n"
        "Extract the following information:\n\n"
        "1. Term: The academic term this information applies to (e.g., 'Fall 2025', 'Spring 2026', 'Academic Year 2025-2026').\n"
        "2. LiveDate: **MANDATORY** - Date when the program information becomes active or application opens for Fall term. "
        "PRIORITIZE Fall application start dates. If multiple terms are mentioned, extract the Fall term date. "
        "Format: YYYY-MM-DD if available (e.g., '2024-09-01' for September 1, 2024). "
        "If only month/day is given, infer the year based on context (typically current or next year for Fall). "
        "If no date is found, use empty string but make every effort to find it.\n"
        "3. DeadlineDate: **MANDATORY** - Application deadline date for Fall term. "
        "PRIORITIZE Fall application deadlines. If multiple deadlines are mentioned, extract the Fall term deadline. "
        "Format: YYYY-MM-DD if available (e.g., '2025-03-15' for March 15, 2025). "
        "If only month/day is given, infer the year based on context. "
        "If deadline is 'Rolling' or similar, include that text. "
        "If no date is found, use empty string but make every effort to find it.\n"
        "4. Fees: Total program fees, application fees, or tuition fees mentioned (include currency if specified).\n"
        "5. CostPerCredit: Cost per credit hour (include currency if specified, e.g., '$1,200 per credit').\n"
        "6. AverageScholarshipAmount: Average scholarship amount awarded (include currency if specified).\n"
        "7. ScholarshipAmount: Specific scholarship amount if mentioned (include currency).\n"
        "8. ScholarshipPercentage: Scholarship percentage if mentioned (e.g., '50%', 'Full tuition').\n"
        "9. ScholarshipType: Type of scholarship (e.g., 'Merit-based', 'Need-based', 'Graduate Assistantship').\n\n"
        "**CRITICAL FOR DATES:**\n"
        "- LiveDate and DeadlineDate are MANDATORY fields - search thoroughly for these dates\n"
        "- Prioritize Fall term dates over other terms (Spring, Summer, etc.)\n"
        "- Look for phrases like 'Fall application opens', 'Fall deadline', 'Fall 2025', 'Fall semester', etc.\n"
        "- If only one set of dates is mentioned, assume it's for Fall unless explicitly stated otherwise\n"
        "- Common Fall application patterns: Opens in September/October, Deadline in January/March/April\n"
        "- Extract dates even if they're embedded in text or in different formats\n\n"
        "=== EXPECTED OUTPUT FORMAT ===\n\n"
        "You MUST return a JSON object with these EXACT keys (all values as strings):\n\n"
        "{\n"
        "  \"Term\": \"\",\n"
        "  \"LiveDate\": \"\",\n"
        "  \"DeadlineDate\": \"\",\n"
        "  \"Fees\": \"\",\n"
        "  \"AverageScholarshipAmount\": \"\",\n"
        "  \"CostPerCredit\": \"\",\n"
        "  \"ScholarshipAmount\": \"\",\n"
        "  \"ScholarshipPercentage\": \"\",\n"
        "  \"ScholarshipType\": \"\"\n"
        "}\n\n"
        "=== EXAMPLE OUTPUT (Complete JSON with Sample Data) ===\n\n"
        "Here is an example of how your response should look with actual data:\n\n"
        "{\n"
        "  \"Term\": \"Fall 2025\",\n"
        "  \"LiveDate\": \"2024-09-01\",\n"
        "  \"DeadlineDate\": \"2025-03-15\",\n"
        "  \"Fees\": \"$45,000 per year\",\n"
        "  \"AverageScholarshipAmount\": \"$15,000\",\n"
        "  \"CostPerCredit\": \"$1,200 per credit hour\",\n"
        "  \"ScholarshipAmount\": \"$20,000\",\n"
        "  \"ScholarshipPercentage\": \"50%\",\n"
        "  \"ScholarshipType\": \"Merit-based\"\n"
        "}\n\n"
        "=== IMPORTANT NOTES ===\n"
        "- Return ONLY the JSON object, no markdown code fences, no explanations\n"
        "- LiveDate and DeadlineDate are MANDATORY - make every effort to find Fall term dates\n"
        "- If dates cannot be found after thorough search, use empty string \"\" but this should be rare\n"
        "- All values must be strings (even dates and numbers)\n"
        "- Date format: YYYY-MM-DD when possible (e.g., '2024-09-01')\n"
        "- Include currency symbols and units in fee fields if mentioned\n"
        "- Prioritize Fall term information over other terms\n"
        "- Do not add any fields beyond those listed above\n\n"
        "Now extract the values from the provided text and return ONLY the JSON object in the exact format shown above. "
        "Pay special attention to finding LiveDate and DeadlineDate for Fall term."
    )


def build_program_test_scores_prompt() -> str:
    """Prompt for extracting minimum test score requirements."""
    return (
        "You are an expert higher-education data extractor.\n"
        "Parse the given webpage text and return a STRICT JSON object with minimum test score requirements.\n"
        "Use ONLY the provided text. If the extraction is unsuccessful or the text is empty, "
        "return all fields as empty strings (DO NOT hallucinate). Do not add extra keys or commentary.\n\n"
        "=== EXTRACTION REQUIREMENTS ===\n\n"
        "Extract minimum test scores. Only include scores if explicitly mentioned as minimum requirements.\n\n"
        "Test Score Fields:\n"
        "- MinimumACTScore: Minimum ACT score required\n"
        "- MinimumSATScore: Minimum SAT score required\n"
        "- MinimumDuoLingoScore: Minimum Duolingo English Test score\n"
        "- MinimumELSScore: Minimum ELS score\n"
        "- MinimumGMATScore: Minimum GMAT score\n"
        "- MinimumGreScore: Minimum GRE score (overall or by section)\n"
        "- MinimumIELTSScore: Minimum IELTS score (overall or by band)\n"
        "- MinimumMATScore: Minimum MAT score\n"
        "- MinimumMCATScore: Minimum MCAT score\n"
        "- MinimumPTEScore: Minimum PTE Academic score\n"
        "- MinimumTOEFLScore: Minimum TOEFL score (iBT or overall)\n"
        "- MinimumLSATScore: Minimum LSAT score\n\n"
        "=== EXPECTED OUTPUT FORMAT ===\n\n"
        "You MUST return a JSON object with these EXACT keys (values as strings with just the score number, e.g., '80', '6.5', '320'):\n\n"
        "{\n"
        "  \"MinimumACTScore\": \"\",\n"
        "  \"MinimumDuoLingoScore\": \"\",\n"
        "  \"MinimumELSScore\": \"\",\n"
        "  \"MinimumGMATScore\": \"\",\n"
        "  \"MinimumGreScore\": \"\",\n"
        "  \"MinimumIELTSScore\": \"\",\n"
        "  \"MinimumMATScore\": \"\",\n"
        "  \"MinimumMCATScore\": \"\",\n"
        "  \"MinimumPTEScore\": \"\",\n"
        "  \"MinimumSATScore\": \"\",\n"
        "  \"MinimumTOEFLScore\": \"\",\n"
        "  \"MinimumLSATScore\": \"\"\n"
        "}\n\n"
        "=== EXAMPLE OUTPUT (Complete JSON with Sample Data) ===\n\n"
        "Here is an example of how your response should look with actual data:\n\n"
        "{\n"
        "  \"MinimumACTScore\": \"28\",\n"
        "  \"MinimumDuoLingoScore\": \"110\",\n"
        "  \"MinimumELSScore\": \"112\",\n"
        "  \"MinimumGMATScore\": \"600\",\n"
        "  \"MinimumGreScore\": \"310\",\n"
        "  \"MinimumIELTSScore\": \"6.5\",\n"
        "  \"MinimumMATScore\": \"400\",\n"
        "  \"MinimumMCATScore\": \"500\",\n"
        "  \"MinimumPTEScore\": \"58\",\n"
        "  \"MinimumSATScore\": \"1200\",\n"
        "  \"MinimumTOEFLScore\": \"80\",\n"
        "  \"MinimumLSATScore\": \"160\"\n"
        "}\n\n"
        "=== IMPORTANT NOTES ===\n"
        "- Return ONLY the JSON object, no markdown code fences, no explanations\n"
        "- If a score cannot be found, use an empty string \"\"\n"
        "- All values must be strings (even numeric scores)\n"
        "- Include only the numeric score value, no units or labels (e.g., '80' not '80 points')\n"
        "- Only extract scores explicitly stated as minimum requirements\n"
        "- Do not add any fields beyond those listed above\n\n"
        "Now extract the values from the provided text and return ONLY the JSON object in the exact format shown above."
    )


@extract_bp.route("/extract-programs", methods=["GET", "POST"])
def extract_programs_page():
    """Page for extracting program details from a URL with multi-step extraction."""
    
    context: Dict[str, Any] = {
        "url": "",
        "extracted_text": "",
        "college_options": [],
        "college_department_options": [],
        "program_snapshot": {},
        "program_requirements": {},
        "program_term": {},
        "program_test_scores": {},
        "matched_college": None,
    }
    
    engine = get_engine()
    college_opts: List[Tuple[int, str]] = []
    college_dept_opts: List[Tuple[int, str]] = []
    if engine:
        try:
            college_opts = get_college_options(engine)
            college_dept_opts = get_college_department_options(engine)
        except Exception:
            pass
    context["college_options"] = college_opts
    context["college_department_options"] = college_dept_opts
    
    if request.method == "GET":
        return render_template("extract_programs.html", **context)
    
    # Handle POST requests for extraction
    url = request.form.get("url", "").strip()
    context["url"] = url
    
    if not url:
        flash("Please provide a URL.", "error")
        return render_template("extract_programs.html", **context)
    
    if not is_valid_url(url):
        flash("Invalid URL. Include the scheme (e.g., https://).", "error")
        return render_template("extract_programs.html", **context)
    
    try:
        headers = current_app.config.get("SCRAPER_HEADERS", {})
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        
        if response.encoding is None or response.encoding == "ISO-8859-1":
            response.encoding = response.apparent_encoding or "utf-8"
        
        try:
            html_content = response.text
        except UnicodeDecodeError:
            html_content = response.content.decode("utf-8", errors="replace")
        
        soup = BeautifulSoup(html_content, "html.parser")
        primary_content = extract_page_content(soup)
        extracted_text = primary_content["text"]
        context["extracted_text"] = extracted_text
        context["primary_title"] = primary_content["title"]
        context["primary_heading"] = primary_content["heading"]
        
        # Try to match college
        if engine:
            college_name_from_page = primary_content.get("title", "")
            for suffix in [" - Home", " | Home", " - Official Site", " | Official Site"]:
                if college_name_from_page.endswith(suffix):
                    college_name_from_page = college_name_from_page[:-len(suffix)]
            matched = find_college_by_name(engine, college_name_from_page)
            if matched:
                context["matched_college"] = {
                    "CollegeID": matched["CollegeID"],
                    "CollegeName": matched["CollegeName"]
                }
        
    except requests.RequestException as exc:
        flash(f"Failed to fetch the page: {exc}", "error")
        return render_template("extract_programs.html", **context)
    except Exception as exc:
        flash(f"Error processing page: {exc}", "error")
        return render_template("extract_programs.html", **context)
    
    return render_template("extract_programs.html", **context)


@extract_bp.route("/extract-programs/run-program-details", methods=["POST"])
def extract_programs_run_program_details():
    """AJAX endpoint to extract complete program details (program-specific information)."""
    
    payload = request.get_json(silent=True) or {}
    url = payload.get("url", "").strip()
    
    if not url:
        return jsonify({"error": "URL is required."}), 400
    
    if not is_valid_url(url):
        return jsonify({"error": "Invalid URL."}), 400
    
    try:
        headers = current_app.config.get("SCRAPER_HEADERS", {})
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        
        if response.encoding is None or response.encoding == "ISO-8859-1":
            response.encoding = response.apparent_encoding or "utf-8"
        
        try:
            html_content = response.text
        except UnicodeDecodeError:
            html_content = response.content.decode("utf-8", errors="replace")
        
        soup = BeautifulSoup(html_content, "html.parser")
        primary_content = extract_page_content(soup)
        extracted_text = primary_content["text"]
        
        # Use snapshot prompt for program details
        prompt = build_program_snapshot_prompt()
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON from LLM output
        parsed = {}
        if llm_output:
            try:
                text = llm_output.strip()
                if text.startswith("```"):
                    lines = text.split("\n")
                    text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                if text.startswith("```json"):
                    lines = text.split("\n")
                    text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                start = text.find("{")
                end = text.rfind("}")
                if start >= 0 and end > start:
                    parsed = json.loads(text[start:end+1])
                else:
                    parsed = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                parsed = {}
        
        return jsonify({
            "ok": True,
            "data": parsed if isinstance(parsed, dict) else {},
            "llm_output": llm_output
        })
        
    except Exception as exc:
        import traceback
        error_trace = traceback.format_exc()
        return jsonify({"error": f"Extraction failed: {exc}", "traceback": error_trace}), 500


@extract_bp.route("/extract-programs/run-shared-requirements", methods=["POST"])
def extract_programs_run_shared_requirements():
    """AJAX endpoint to extract shared requirements/details that apply to multiple programs."""
    
    payload = request.get_json(silent=True) or {}
    url = payload.get("url", "").strip()
    section = payload.get("section", "").strip()  # "requirements", "term", or "test_scores"
    
    if not url or not section:
        return jsonify({"error": "URL and section are required."}), 400
    
    if not is_valid_url(url):
        return jsonify({"error": "Invalid URL."}), 400
    
    try:
        headers = current_app.config.get("SCRAPER_HEADERS", {})
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        
        if response.encoding is None or response.encoding == "ISO-8859-1":
            response.encoding = response.apparent_encoding or "utf-8"
        
        try:
            html_content = response.text
        except UnicodeDecodeError:
            html_content = response.content.decode("utf-8", errors="replace")
        
        soup = BeautifulSoup(html_content, "html.parser")
        primary_content = extract_page_content(soup)
        extracted_text = primary_content["text"]
        
        # Select prompt based on section (only for shared requirements)
        if section == "requirements":
            prompt = build_program_requirements_prompt()
        elif section == "term":
            prompt = build_program_term_prompt()
        elif section == "test_scores":
            prompt = build_program_test_scores_prompt()
        else:
            return jsonify({"error": f"Unknown section: {section}. Use 'requirements', 'term', or 'test_scores'."}), 400
        
        # Call LLM
        llm_output = generate_gemini_response(prompt, url, primary_content)
        
        # Parse JSON from LLM output
        parsed = {}
        if llm_output:
            try:
                text = llm_output.strip()
                # Remove code fences if present
                if text.startswith("```"):
                    lines = text.split("\n")
                    text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                if text.startswith("```json"):
                    lines = text.split("\n")
                    text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
                # Find JSON object in text
                start = text.find("{")
                end = text.rfind("}")
                if start >= 0 and end > start:
                    parsed = json.loads(text[start:end+1])
                else:
                    parsed = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                parsed = {}
        
        return jsonify({
            "ok": True,
            "section": section,
            "data": parsed if isinstance(parsed, dict) else {},
            "llm_output": llm_output
        })
        
    except Exception as exc:
        import traceback
        error_trace = traceback.format_exc()
        return jsonify({"error": f"Extraction failed: {exc}", "traceback": error_trace}), 500


@extract_bp.route("/extract-programs/save", methods=["POST"])
def extract_programs_save():
    """Save extracted program details to database."""
    
    try:
        engine = get_engine()
        if engine is None:
            msg = "Database is not configured."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_programs_page"))
        
        college_id = request.form.get("college_id", type=int) or request.form.get("college_id_program_select", type=int)
        college_department_id = request.form.get("college_department_id", type=int)
        program_id = request.form.get("program_id", type=int)
        
        # Collect program data from form
        program_payload = {}
        for field in ["ProgramName", "Level", "Concentration", "Description", "ProgramWebsiteURL", "Accreditation", "QsWorldRanking"]:
            val = request.form.get(f"Program.{field}", "").strip()
            if val:
                program_payload[field] = val
        
        if not program_payload.get("ProgramName"):
            msg = "ProgramName is required."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 400
            flash(msg, "error")
            return redirect(url_for("extract.extract_programs_page"))
        
        # Collect requirements data
        requirements_payload = {}
        for field in ["Resume", "StatementOfPurpose", "GreOrGmat", "EnglishScore", "Requirements", "WritingSample",
                     "MinGPA", "MaxGPA", "MaxFails", "PreviousYearAcceptanceRates"]:
            val = request.form.get(f"ProgramRequirements.{field}", "").strip()
            if val:
                requirements_payload[field] = val
        
        # Collect boolean fields
        for field in ["IsAnalyticalNotRequired", "IsAnalyticalOptional", "IsDuoLingoRequired", "IsELSRequired",
                     "IsGMATOrGreRequired", "IsGMATRequired", "IsGreRequired", "IsIELTSRequired", "IsLSATRequired",
                     "IsMATRequired", "IsMCATRequired", "IsPTERequired", "IsTOEFLIBRequired", "IsTOEFLPBTRequired",
                     "IsEnglishNotRequired", "IsEnglishOptional", "IsRecommendationSystemOpted", "IsStemProgram",
                     "IsACTRequired", "IsSATRequired"]:
            val = request.form.get(f"ProgramRequirements.{field}")
            requirements_payload[field] = val == "on" or val == "true" or val == "1"
        
        # Collect term details
        term_payload = {}
        if college_id:
            term_payload["CollegeID"] = college_id
        for field in ["Term", "LiveDate", "DeadlineDate", "Fees", "AverageScholarshipAmount", "CostPerCredit",
                     "ScholarshipAmount", "ScholarshipPercentage", "ScholarshipType"]:
            val = request.form.get(f"ProgramTermDetails.{field}", "").strip()
            if val:
                term_payload[field] = val
        
        # Collect test scores
        test_scores_payload = {}
        for field in ["MinimumACTScore", "MinimumDuoLingoScore", "MinimumELSScore", "MinimumGMATScore",
                     "MinimumGreScore", "MinimumIELTSScore", "MinimumMATScore", "MinimumMCATScore",
                     "MinimumPTEScore", "MinimumSATScore", "MinimumTOEFLScore", "MinimumLSATScore"]:
            val = request.form.get(f"ProgramTestScores.{field}", "").strip()
            if val:
                test_scores_payload[field] = val
        
        # Build table payloads
        table_payloads = {}
        if program_payload:
            table_payloads["Program"] = program_payload
        if requirements_payload:
            table_payloads["ProgramRequirements"] = requirements_payload
        if term_payload:
            table_payloads["ProgramTermDetails"] = term_payload
        if test_scores_payload:
            table_payloads["ProgramTestScores"] = test_scores_payload
        
        # Link to department if provided
        if college_department_id:
            table_payloads["ProgramDepartmentLink"] = {
                "CollegeDepartmentID": college_department_id,
                "CollegeID": college_id or 0,
            }
        
        # Save to database
        try:
            saved_program_id = persist_program_bundle(engine, program_id, table_payloads)
            msg = f"Program saved successfully (ID: {saved_program_id})."
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": True, "message": msg, "program_id": saved_program_id})
            flash(msg, "success")
            return redirect(url_for("forms.program_list"))
        except (SQLAlchemyError, ValueError) as exc:
            msg = f"Failed to save program: {exc}"
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok": False, "message": msg}), 500
            flash(msg, "error")
            return redirect(url_for("extract.extract_programs_page"))
            
    except Exception as exc:
        msg = f"Error saving program: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_programs_page"))


def find_college_id_by_name(engine, college_name: str) -> Optional[int]:
    """Find college ID by name (case-insensitive partial match)."""
    college_table = fetch_table("College")
    stmt = select(college_table.c.CollegeID, college_table.c.CollegeName).where(
        college_table.c.CollegeName.ilike(f"%{college_name}%")
    )
    with engine.connect() as conn:
        row = conn.execute(stmt).first()
        return int(row.CollegeID) if row else None


def find_college_department_id_by_name(engine, college_name: str, department_name: str = None) -> Optional[int]:
    """Find college department ID by college name and optional department name."""
    college_department_table = fetch_table("CollegeDepartment", required=False)
    college_table = fetch_table("College", required=False)
    department_table = fetch_table("Department", required=False)
    
    if not all([college_department_table, college_table, department_table]):
        return None
    
    stmt = (
        select(college_department_table.c.CollegeDepartmentID)
        .select_from(
            college_department_table.join(
                college_table,
                college_table.c.CollegeID == college_department_table.c.CollegeID,
            )
        )
        .where(college_table.c.CollegeName.ilike(f"%{college_name}%"))
    )
    
    if department_name:
        stmt = stmt.select_from(
            college_department_table.join(
                college_table,
                college_table.c.CollegeID == college_department_table.c.CollegeID,
            ).join(
                department_table,
                department_table.c.DepartmentID == college_department_table.c.DepartmentID,
            )
        ).where(
            college_table.c.CollegeName.ilike(f"%{college_name}%"),
            department_table.c.DepartmentName.ilike(f"%{department_name}%")
        )
    
    with engine.connect() as conn:
        row = conn.execute(stmt).first()
        return int(row.CollegeDepartmentID) if row else None


def parse_date_field(date_value: Any) -> Optional[str]:
    """Parse a date field value. Returns None if not a valid date format."""
    if date_value is None:
        return None
    
    date_str = str(date_value).strip()
    if not date_str or date_str.lower() in ["null", "none", "n/a", "varies", ""]:
        return None
    
    # Check if it's already a valid date format (YYYY-MM-DD)
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        pass
    
    # Try ISO datetime format (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SSZ)
    try:
        # Try with timezone
        parsed = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        try:
            # Try without timezone
            if 'T' in date_str:
                parsed = datetime.strptime(date_str.split('T')[0], "%Y-%m-%d")
                return parsed.strftime("%Y-%m-%d")
        except ValueError:
            pass
    
    # Try to parse common date formats
    date_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%B %d, %Y",  # October 1, 2024
        "%b %d, %Y",  # Oct 1, 2024
        "%B %d",  # October 1 (no year)
        "%b %d",  # Oct 1 (no year)
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            # If no year in format, use current year
            if "%Y" not in fmt:
                current_year = datetime.now().year
                parsed_date = parsed_date.replace(year=current_year)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    # If contains "varies" or other non-date text, return None
    if any(word in date_str.lower() for word in ["varies", "typically", "example", "check", "see"]):
        return None
    
    return None


def parse_program_json(engine, program_data: Dict[str, Any], override_college_id: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """Parse program JSON data and map it to database structure.
    Supports three formats:
    1. Database format: {"Program": {...}, "ProgramRequirements": {...}, etc.}
    2. Human-readable format: {"Program Snapshot": {...}, "Application Checklist": {...}, etc.}
    3. Structured format: {"ProgramName": "...", "Requirements": {...}, "TermDetails": [...], "TestScores": {...}}
    """
    table_payloads = {}
    
    # Check for structured format (has ProgramName at top level and Requirements/TermDetails/TestScores)
    if "ProgramName" in program_data and ("Requirements" in program_data or "TermDetails" in program_data or "TestScores" in program_data):
        # Structured format
        program_payload = {}
        program_payload["ProgramName"] = program_data.get("ProgramName")
        
        for field in ["Level", "Concentration", "Description", "ProgramWebsiteURL", "Accreditation", "QsWorldRanking"]:
            if field in program_data:
                program_payload[field] = program_data[field]
        
        if program_payload.get("ProgramName"):
            table_payloads["Program"] = program_payload
        
        # Parse Requirements -> ProgramRequirements
        if "Requirements" in program_data:
            reqs = program_data["Requirements"].copy()
            # Convert string boolean values to actual booleans for Is* fields
            for key, value in list(reqs.items()):
                if key.startswith("Is"):
                    if isinstance(value, str):
                        reqs[key] = value.lower() in ["true", "1", "yes", "on"]
                    elif value is None:
                        reqs[key] = False
                    else:
                        reqs[key] = bool(value)
                # Handle string values like "Not Required", "Required", etc. for Resume, StatementOfPurpose, etc.
                elif key in ["Resume", "StatementOfPurpose", "GreOrGmat", "EnglishScore", "WritingSample"]:
                    if isinstance(value, str):
                        value_lower = value.lower()
                        # Convert "Required" to True, "Not Required" to False
                        reqs[key] = value_lower not in ["not required", "false", "0", "no", "n/a", ""]
                    elif value is None:
                        reqs[key] = False
                    else:
                        reqs[key] = bool(value)
            table_payloads["ProgramRequirements"] = reqs
        
        # Parse TermDetails -> ProgramTermDetails (handle array)
        if "TermDetails" in program_data:
            term_details = program_data["TermDetails"]
            if isinstance(term_details, list) and len(term_details) > 0:
                # Take first term detail
                term_data = term_details[0].copy()
                term_payload = {}
                
                # Use override college ID if provided, otherwise find from College field
                if override_college_id:
                    term_payload["CollegeID"] = override_college_id
                else:
                    college_name = program_data.get("College")
                    if college_name:
                        college_id = find_college_id_by_name(engine, college_name)
                        if college_id:
                            term_payload["CollegeID"] = college_id
                
                # Map fields
                if "Term" in term_data:
                    term_payload["Term"] = term_data["Term"]
                
                for field in ["Fees", "AverageScholarshipAmount", "CostPerCredit", "ScholarshipAmount", 
                             "ScholarshipPercentage", "ScholarshipType"]:
                    if field in term_data:
                        term_payload[field] = str(term_data[field]) if term_data[field] is not None else None
                
                # Parse date fields
                if "LiveDate" in term_data:
                    term_payload["LiveDate"] = parse_date_field(term_data["LiveDate"])
                if "DeadlineDate" in term_data:
                    term_payload["DeadlineDate"] = parse_date_field(term_data["DeadlineDate"])
                
                if term_payload.get("CollegeID"):
                    table_payloads["ProgramTermDetails"] = term_payload
        
        # Parse TestScores -> ProgramTestScores
        if "TestScores" in program_data:
            test_scores = program_data["TestScores"].copy()
            test_scores_payload = {}
            for key, value in test_scores.items():
                if value is not None and str(value).upper() != "N/A":
                    test_scores_payload[key] = str(value)
            if test_scores_payload:
                table_payloads["ProgramTestScores"] = test_scores_payload
        
        # Parse Department and College -> ProgramDepartmentLink
        college_name = program_data.get("College")
        department_name = program_data.get("Department")
        
        if college_name and department_name:
            college_id = find_college_id_by_name(engine, college_name)
            if college_id:
                college_dept_id = find_college_department_id_by_name(engine, college_name, department_name)
                if college_dept_id:
                    table_payloads["ProgramDepartmentLink"] = {
                        "CollegeDepartmentID": college_dept_id,
                        "CollegeID": college_id,
                    }
        
        return table_payloads
    
    # Check if data is already in database format (has "Program" key directly)
    if "Program" in program_data:
        # Database format - use data directly
        program_payload = program_data["Program"].copy()
        if program_payload.get("ProgramName"):
            table_payloads["Program"] = program_payload
        
        # ProgramRequirements
        if "ProgramRequirements" in program_data:
            reqs = program_data["ProgramRequirements"].copy()
            # Convert string boolean values to actual booleans for Is* fields
            for key, value in list(reqs.items()):
                if key.startswith("Is") and isinstance(value, str):
                    reqs[key] = value.lower() in ["true", "1", "yes", "on"]
                # Convert None to False for boolean fields
                elif key.startswith("Is") and value is None:
                    reqs[key] = False
            table_payloads["ProgramRequirements"] = reqs
        
        # ProgramTermDetails - handle array or single object
        if "ProgramTermDetails" in program_data:
            term_data = program_data["ProgramTermDetails"]
            if isinstance(term_data, list) and len(term_data) > 0:
                # Take first term detail if it's an array
                term_payload = term_data[0].copy()
                # Remove ProgramID if present (will be set by persist function)
                term_payload.pop("ProgramID", None)
                # Use override college ID if provided, otherwise use from data
                if override_college_id:
                    term_payload["CollegeID"] = override_college_id
                else:
                    # Convert CollegeID to int if it's a string number
                    if "CollegeID" in term_payload:
                        college_id = term_payload["CollegeID"]
                        if isinstance(college_id, str) and college_id.isdigit():
                            term_payload["CollegeID"] = int(college_id)
                        elif not isinstance(college_id, int):
                            term_payload["CollegeID"] = None
                
                # Parse date fields - convert text dates to proper format or None
                if "LiveDate" in term_payload:
                    term_payload["LiveDate"] = parse_date_field(term_payload["LiveDate"])
                if "DeadlineDate" in term_payload:
                    term_payload["DeadlineDate"] = parse_date_field(term_payload["DeadlineDate"])
                
                if term_payload.get("CollegeID"):
                    table_payloads["ProgramTermDetails"] = term_payload
            elif isinstance(term_data, dict):
                term_payload = term_data.copy()
                term_payload.pop("ProgramID", None)
                # Use override college ID if provided, otherwise use from data
                if override_college_id:
                    term_payload["CollegeID"] = override_college_id
                else:
                    # Convert CollegeID to int if it's a string number
                    if "CollegeID" in term_payload:
                        college_id = term_payload["CollegeID"]
                        if isinstance(college_id, str) and college_id.isdigit():
                            term_payload["CollegeID"] = int(college_id)
                        elif not isinstance(college_id, int):
                            term_payload["CollegeID"] = None
                
                # Parse date fields - convert text dates to proper format or None
                if "LiveDate" in term_payload:
                    term_payload["LiveDate"] = parse_date_field(term_payload["LiveDate"])
                if "DeadlineDate" in term_payload:
                    term_payload["DeadlineDate"] = parse_date_field(term_payload["DeadlineDate"])
                
                if term_payload.get("CollegeID"):
                    table_payloads["ProgramTermDetails"] = term_payload
        
        # ProgramTestScores
        if "ProgramTestScores" in program_data:
            test_scores = program_data["ProgramTestScores"].copy()
            table_payloads["ProgramTestScores"] = test_scores
        
        # ProgramDepartmentLink - if provided
        if "ProgramDepartmentLink" in program_data:
            link_data = program_data["ProgramDepartmentLink"].copy()
            link_data.pop("ProgramID", None)
            table_payloads["ProgramDepartmentLink"] = link_data
        
        return table_payloads
    
    # Otherwise, parse human-readable format
    # Parse Program Snapshot - try multiple key formats
    program_snapshot = {}
    for key in ["Program Snapshot", "ProgramSnapshot", "program_snapshot", "programSnapshot"]:
        if key in program_data:
            program_snapshot = program_data[key]
            break
    
    # If still not found, try case-insensitive search
    if not program_snapshot:
        for key in program_data.keys():
            if key.lower().replace("_", " ").replace("-", " ") == "program snapshot":
                program_snapshot = program_data[key]
                break
    
    program_payload = {}
    program_name = program_snapshot.get("Program Name") or program_snapshot.get("ProgramName") or program_snapshot.get("program_name")
    if program_name:
        program_payload["ProgramName"] = program_name
    
    # Add other fields if they exist
    for json_key, db_key in [
        ("Level", "Level"),
        ("Concentration", "Concentration"),
        ("Description", "Description"),
        ("Program Website URL", "ProgramWebsiteURL"),
        ("ProgramWebsiteURL", "ProgramWebsiteURL"),
        ("Accreditation", "Accreditation"),
        ("Qs World Ranking", "QsWorldRanking"),
        ("QsWorldRanking", "QsWorldRanking"),
    ]:
        val = program_snapshot.get(json_key)
        if val is not None:
            program_payload[db_key] = val
    
    # ProgramName is required
    if program_payload.get("ProgramName"):
        table_payloads["Program"] = program_payload
    
    # Parse Application Checklist -> ProgramRequirements
    app_checklist = program_data.get("Application Checklist", {})
    requirements_payload = {}
    
    # Boolean fields
    bool_fields = {
        "Resume": "Resume",
        "Statement Of Purpose": "StatementOfPurpose",
        "Gre Or Gmat": "GreOrGmat",
        "English Score": "EnglishScore",
        "Writing Sample": "WritingSample",
        "Is Analytical Not Required": "IsAnalyticalNotRequired",
        "Is Analytical Optional": "IsAnalyticalOptional",
        "Is Duo Lingo Required": "IsDuoLingoRequired",
        "Is E L S Required": "IsELSRequired",
        "Is G M A T Or Gre Required": "IsGMATOrGreRequired",
        "Is G M A T Required": "IsGMATRequired",
        "Is Gre Required": "IsGreRequired",
        "Is I E L T S Required": "IsIELTSRequired",
        "Is L S A T Required": "IsLSATRequired",
        "Is M A T Required": "IsMATRequired",
        "Is M C A T Required": "IsMCATRequired",
        "Is P T E Required": "IsPTERequired",
        "Is T O E F L I B Required": "IsTOEFLIBRequired",
        "Is T O E F L P B T Required": "IsTOEFLPBTRequired",
        "Is English Not Required": "IsEnglishNotRequired",
        "Is English Optional": "IsEnglishOptional",
        "Is Recommendation System Opted": "IsRecommendationSystemOpted",
        "Is Stem Program": "IsStemProgram",
    }
    
    for json_key, db_key in bool_fields.items():
        val = app_checklist.get(json_key)
        if val is not None:
            requirements_payload[db_key] = bool(val) if isinstance(val, bool) else (str(val).lower() in ["true", "1", "on", "yes"])
    
    # String fields
    if app_checklist.get("Requirements"):
        requirements_payload["Requirements"] = str(app_checklist.get("Requirements"))
    
    # GPA and other numeric fields
    for field in ["Max Fails", "Max G P A", "Min G P A", "Previous Year Acceptance Rates"]:
        val = app_checklist.get(field)
        if val is not None:
            db_key = field.replace(" ", "")
            requirements_payload[db_key] = str(val)
    
    # Set default False for boolean fields that weren't provided
    for db_key in bool_fields.values():
        if db_key not in requirements_payload:
            requirements_payload[db_key] = False
    
    if requirements_payload:
        table_payloads["ProgramRequirements"] = requirements_payload
    
    # Parse Term & Investment -> ProgramTermDetails
    term_investment = program_data.get("Term & Investment", {})
    term_payload = {}
    
    # Use override college ID if provided, otherwise find from data
    if override_college_id:
        term_payload["CollegeID"] = override_college_id
    else:
        college_name = term_investment.get("College I D")
        if college_name:
            college_id = find_college_id_by_name(engine, college_name)
            if college_id:
                term_payload["CollegeID"] = college_id
    
    term_payload["Term"] = term_investment.get("Term")
    
    # Convert Fees to string if it's a number
    fees = term_investment.get("Fees")
    if fees is not None:
        term_payload["Fees"] = str(fees)
    
    for field in ["Average Scholarship Amount", "Cost Per Credit", "Scholarship Amount", 
                  "Scholarship Percentage", "Scholarship Type"]:
        val = term_investment.get(field)
        if val is not None:
            db_key = field.replace(" ", "")
            term_payload[db_key] = str(val)
    
    if term_payload:
        table_payloads["ProgramTermDetails"] = term_payload
    
    # Parse Department Placement -> ProgramDepartmentLink
    dept_placement = program_data.get("Department Placement", {})
    dept_id_str = dept_placement.get("College Department I D", "")
    
    if dept_id_str and college_name and term_payload.get("CollegeID"):
        # Try to parse "College Name — Department Name" format
        dept_name = None
        if "—" in dept_id_str or "–" in dept_id_str or "-" in dept_id_str:
            # Try different dash types
            for separator in ["—", "–", " - "]:
                if separator in dept_id_str:
                    parts = dept_id_str.split(separator)
                    if len(parts) >= 2:
                        dept_name = parts[1].strip()
                        break
        
        college_dept_id = find_college_department_id_by_name(engine, college_name, dept_name)
        
        if college_dept_id:
            table_payloads["ProgramDepartmentLink"] = {
                "CollegeDepartmentID": college_dept_id,
                "CollegeID": term_payload["CollegeID"],
            }
    
    # Parse Minimum Test Scores -> ProgramTestScores
    test_scores = program_data.get("Minimum Test Scores", {})
    test_scores_payload = {}
    
    score_fields = {
        "Minimum A C T Score": "MinimumACTScore",
        "Minimum Duo Lingo Score": "MinimumDuoLingoScore",
        "Minimum E L S Score": "MinimumELSScore",
        "Minimum G M A T Score": "MinimumGMATScore",
        "Minimum Gre Score": "MinimumGreScore",
        "Minimum I E L T S Score": "MinimumIELTSScore",
        "Minimum M A T Score": "MinimumMATScore",
        "Minimum M C A T Score": "MinimumMCATScore",
        "Minimum P T E Score": "MinimumPTEScore",
        "Minimum S A T Score": "MinimumSATScore",
        "Minimum T O E F L Score": "MinimumTOEFLScore",
        "Minimum L S A T Score": "MinimumLSATScore",
    }
    
    for json_key, db_key in score_fields.items():
        val = test_scores.get(json_key)
        if val is not None:
            test_scores_payload[db_key] = str(val)
    
    if test_scores_payload:
        table_payloads["ProgramTestScores"] = test_scores_payload
    
    return table_payloads


@extract_bp.route("/import-programs-json", methods=["GET", "POST"])
def import_programs_json():
    """Import programs from JSON data."""
    if request.method == "GET":
        try:
            engine = get_engine()
            college_options = get_college_options(engine) if engine else []
        except Exception:
            college_options = []
        return render_template("extract_programs_json.html", college_options=college_options)
    
    try:
        engine = get_engine()
        if engine is None:
            flash("Database is not configured.", "error")
            return redirect(url_for("extract.import_programs_json"))
        
        json_data = request.form.get("json_data", "").strip()
        if not json_data:
            flash("Please provide JSON data.", "error")
            return redirect(url_for("extract.import_programs_json"))
        
        try:
            programs_list = json.loads(json_data)
        except json.JSONDecodeError as e:
            flash(f"Invalid JSON format: {e}", "error")
            return redirect(url_for("extract.import_programs_json"))
        
        if not isinstance(programs_list, list):
            flash("JSON data must be a list of program objects.", "error")
            return redirect(url_for("extract.import_programs_json"))
        
        # Get selected college ID if provided
        selected_college_id = request.form.get("college_id", type=int)
        
        success_count = 0
        created_count = 0
        updated_count = 0
        error_count = 0
        errors = []
        
        for idx, program_data in enumerate(programs_list, 1):
            try:
                # Check if data has required structure (support multiple formats)
                has_program = (
                    "Program" in program_data or 
                    "Program Snapshot" in program_data or 
                    "ProgramSnapshot" in program_data or
                    "ProgramName" in program_data  # New structured format
                )
                if not has_program:
                    errors.append(f"Program {idx}: Missing required program data (expected 'Program', 'Program Snapshot', or 'ProgramName')")
                    error_count += 1
                    continue
                
                table_payloads = parse_program_json(engine, program_data, selected_college_id)
                
                if not table_payloads.get("Program"):
                    # Try to get program name for error message
                    program_name = "N/A"
                    if "Program" in program_data:
                        program_name = program_data["Program"].get("ProgramName", "N/A")
                    elif "Program Snapshot" in program_data:
                        program_name = program_data["Program Snapshot"].get("Program Name", "N/A")
                    elif "ProgramName" in program_data:
                        program_name = program_data.get("ProgramName", "N/A")
                    errors.append(f"Program {idx} ({program_name}): Missing or invalid ProgramName")
                    error_count += 1
                    continue
                
                # Check if program already exists by name
                program_name = table_payloads["Program"].get("ProgramName")
                existing_program_id = None
                if program_name:
                    existing_program_id = find_program_by_name(engine, program_name)
                
                # Use existing program ID if found, otherwise create new
                program_id = persist_program_bundle(engine, existing_program_id, table_payloads)
                
                if existing_program_id:
                    updated_count += 1
                    success_count += 1
                else:
                    created_count += 1
                    success_count += 1
                
            except Exception as e:
                error_count += 1
                # Try to get program name for error message
                program_name = f"Program {idx}"
                if "Program" in program_data:
                    program_name = program_data["Program"].get("ProgramName", program_name)
                elif "Program Snapshot" in program_data:
                    program_name = program_data["Program Snapshot"].get("Program Name", program_name)
                elif "ProgramName" in program_data:
                    program_name = program_data.get("ProgramName", program_name)
                errors.append(f"{program_name}: {str(e)}")
        
        if success_count > 0:
            messages = []
            if created_count > 0:
                messages.append(f"created {created_count}")
            if updated_count > 0:
                messages.append(f"updated {updated_count}")
            action_msg = " and ".join(messages) if len(messages) == 2 else messages[0] if messages else "processed"
            flash(f"Successfully {action_msg} {success_count} program(s).", "success")
        if error_count > 0:
            flash(f"Failed to import {error_count} program(s).", "error")
            if errors:
                flash("Errors: " + "; ".join(errors[:5]), "error")  # Show first 5 errors
        
        return redirect(url_for("forms.program_list"))
        
    except Exception as e:
        flash(f"Error importing programs: {e}", "error")
        import traceback
        traceback.print_exc()
        return redirect(url_for("extract.import_programs_json"))
            
    except Exception as exc:
        msg = f"Error saving program: {exc}"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": msg}), 500
        flash(msg, "error")
        return redirect(url_for("extract.extract_programs_page"))


@crawler_bp.route("/", methods=["GET", "POST"])
def crawler_index():
    context = {
        "url": "",
        "max_pages": 30,
        "same_domain": True,
        "per_page_llm": False,
        "results": [],
        "stats": {},
        "entity_results": {},
        "selection_sections": {},
        "college_options": [],
        "college_department_options": [],
        "entity_order": ENTITY_ORDER,
        "job_id": None,
        "job_complete": False,
        "job_error": "",
        "default_workers": default_worker_count(),
    }

    job_id = request.args.get("job_id")

    if request.method == "POST":
        url = request.form.get("url", "").strip()
        max_pages = request.form.get("max_pages", type=int) or 30
        same_domain = request.form.get("same_domain") == "on"
        per_page_llm = request.form.get("per_page_llm") == "on"

        if not url:
            flash("Please provide a starting URL to crawl.", "error")
            return render_template("crawler/crawl.html", **context)

        if not is_valid_url(url):
            flash("The URL provided is invalid. Include the scheme (e.g. https://).", "error")
            return render_template("crawler/crawl.html", **context)

        max_pages = max(1, min(max_pages, 2000))

        job_id = str(uuid.uuid4())
        job_entry = {
            "queue": Queue(),
            "status": "running",
            "result": None,
            "params": {
                "url": url,
                "max_pages": max_pages,
                "same_domain": same_domain,
                "per_page_llm": per_page_llm,
            },
        }
        with JOBS_LOCK:
            CRAWL_JOBS[job_id] = job_entry

        headers = current_app.config.get("SCRAPER_HEADERS", {}).copy()
        app_obj = current_app._get_current_object()
        thread = threading.Thread(
            target=run_crawl_job,
            args=(app_obj, job_id, url, max_pages, same_domain, per_page_llm, headers),
            daemon=True,
        )
        thread.start()

        return redirect(url_for("crawler.crawler_index", job_id=job_id))

    if job_id:
        context["job_id"] = job_id
        with JOBS_LOCK:
            job = CRAWL_JOBS.get(job_id)
        if job:
            params = job.get("params") or {}
            context["url"] = params.get("url", context["url"])
            context["max_pages"] = params.get("max_pages", context["max_pages"])
            context["same_domain"] = params.get("same_domain", context["same_domain"])
            context["per_page_llm"] = params.get("per_page_llm", context["per_page_llm"])

            result_context = job.get("result")
            if result_context:
                if job.get("status") == "finished":
                    if "entity_results" not in result_context and result_context.get("results") is not None:
                        try:
                            result_context["entity_results"] = compute_entity_extraction(
                                result_context.get("results", []),
                                result_context.get("per_page_llm", False),
                            )
                        except Exception as exc:  # noqa: BLE001
                            flash(f"Failed to summarize extracted fields: {exc}", "error")
                            result_context["entity_results"] = {}
                    engine = current_app.config.get("DB_ENGINE")
                    sections, college_opts, college_dept_opts = build_selection_sections(engine)
                    result_context["selection_sections"] = sections
                    result_context["college_options"] = college_opts
                    result_context["college_department_options"] = college_dept_opts
                    inject_linking_options(
                        result_context.get("entity_results", {}),
                        college_opts,
                        college_dept_opts,
                    )
                context.update(result_context)
                context["entity_order"] = ENTITY_ORDER
                context["job_complete"] = True
            else:
                context["job_complete"] = False
        else:
            flash("Crawl job not found. Please start a new crawl.", "error")

    return render_template("crawler/crawl.html", **context)


def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def clean_text(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    filtered_lines = [line for line in lines if line]
    return "\n".join(filtered_lines)


def generate_gemini_response(
    prompt: str, source_url: str, primary_content: Dict[str, str]
) -> str:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("Set GOOGLE_API_KEY in your environment to use the Gemini integration.")

    configured_model = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash-latest").strip()
    base_name = configured_model or "gemini-1.5-flash-latest"
    base_name = base_name.replace("models/", "")
    candidates = []
    if base_name:
        candidates.append(base_name)
        if not base_name.endswith("-latest"):
            candidates.append(f"{base_name}-latest")
    candidates.append("gemini-1.5-flash-latest")

    genai.configure(api_key=api_key)
    model = None
    last_error = None

    for candidate in candidates:
        try:
            model = genai.GenerativeModel(f"models/{candidate}")
            break
        except Exception as exc:
            last_error = exc
            continue

    if model is None:
        return {}

    max_chars = int(os.environ.get("GEMINI_CONTEXT_CHARS", "12000"))
    composed_sections: list[str] = []

    def add_section(title: str, body: str) -> None:
        if not body:
            return
        composed_sections.append(f"=== {title} ===\n{body}")

    primary_text = primary_content.get("text", "") if primary_content else ""
    section_title_parts = ["Primary Page Content", source_url]
    if primary_content.get("title"):
        section_title_parts.append(primary_content["title"])
    elif primary_content.get("heading"):
        section_title_parts.append(primary_content["heading"])
    primary_snippet = primary_text[:max_chars]
    add_section(" - ".join(section_title_parts), primary_snippet)

    composed_prompt = f"{prompt.strip()}\n\n" + "\n\n".join(composed_sections)

    response = model.generate_content(composed_prompt)
    if not response or not response.text:
        raise RuntimeError("Gemini did not return any content.")
    return response.text.strip()


def extract_page_content(soup: BeautifulSoup) -> Dict[str, str]:
    working_soup = BeautifulSoup(str(soup), "html.parser")

    for tag in working_soup(["script", "style", "noscript", "svg", "img", "video", "audio"]):
        tag.decompose()

    for section in working_soup.find_all(["header", "footer", "nav", "form", "aside"]):
        section.decompose()

    title = ""
    if working_soup.title and working_soup.title.string:
        title = working_soup.title.string.strip()

    main_region = (
        working_soup.find("main") or working_soup.find("article") or working_soup.body or working_soup
    )

    heading = ""
    for level in ("h1", "h2", "h3"):
        heading_tag = main_region.find(level)
        if heading_tag:
            heading = heading_tag.get_text(separator=" ", strip=True)
            heading_tag.extract()
            break

    body_text = clean_text(main_region.get_text(separator="\n"))
    if not body_text:
        body_text = clean_text(working_soup.get_text(separator="\n"))

    return {"title": title, "heading": heading, "text": body_text}


def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        return parsed._replace(fragment="").geturl()
    except ValueError:
        return None


def is_static_asset(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    static_ext = (
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".webp",
        ".ico",
        ".css",
        ".js",
        ".pdf",
        ".zip",
        ".rar",
        ".tar",
        ".gz",
        ".mp4",
        ".mp3",
        ".avi",
    )
    return path.endswith(static_ext)


def crawl_site(
    start_url: str,
    headers: Dict[str, str],
    max_pages: int = 30,
    same_domain: bool = True,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Tuple[List[Dict[str, Any]], int, int]:
    normalized_start = normalize_url(start_url)
    if not normalized_start:
        raise ValueError("Unable to normalize start URL.")

    root_netloc = urlparse(normalized_start).netloc
    to_visit: deque[str] = deque([normalized_start])
    seen = {normalized_start}
    visited: set[str] = set()
    results: List[Dict[str, Any]] = []
    errors = 0

    workers_env = os.environ.get("CRAWLER_MAX_WORKERS")
    if workers_env:
        try:
            max_workers = max(1, min(int(workers_env), max_pages))
        except ValueError:
            max_workers = min(default_worker_count(), max_pages)
    else:
        max_workers = min(default_worker_count(), max_pages)

    future_map: Dict[Any, str] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while (to_visit or future_map) and len(visited) < max_pages:
            while (
                to_visit
                and len(future_map) < max_workers
                and len(visited) + len(future_map) < max_pages
            ):
                url = to_visit.popleft()
                future = executor.submit(fetch_page, url, headers, same_domain, root_netloc)
                future_map[future] = url

            if not future_map:
                break

            done, _ = wait(future_map.keys(), return_when=FIRST_COMPLETED)

            for future in done:
                current_url = future_map.pop(future)
                if current_url in visited:
                    continue

                try:
                    data = future.result()
                except Exception as exc:  # noqa: BLE001
                    errors += 1
                    visited.add(current_url)
                    error_result = {
                        "url": current_url,
                        "status": "error",
                        "error": str(exc),
                        "title": "",
                        "heading": "",
                        "text": "",
                        "fields": {},
                        "links": [],
                    }
                    results.append(error_result)
                    if progress_callback:
                        progress_callback(
                            {
                                "type": "error",
                                "url": current_url,
                                "message": str(exc),
                                "visited": len(visited),
                                "limit": max_pages,
                            }
                        )
                    continue

                visited.add(current_url)

                page_result = data.get("result")
                if page_result:
                    results.append(page_result)
                    if progress_callback:
                        progress_callback(
                            {
                                "type": "progress",
                                "url": page_result.get("url"),
                                "status": page_result.get("status"),
                                "visited": len(visited),
                                "limit": max_pages,
                            }
                        )
                if data.get("error"):
                    errors += 1

                for child in data.get("child_urls", []):
                    if len(visited) + len(future_map) >= max_pages:
                        break
                    if child in seen:
                        continue
                    if same_domain and urlparse(child).netloc != root_netloc:
                        continue
                    if is_static_asset(child):
                        continue
                    seen.add(child)
                    to_visit.append(child)

    return results, len(visited), errors


def summarize_text(text: str, limit: int = 1600) -> str:
    if len(text) <= limit:
        return text
    truncated = text[:limit]
    cut = truncated.rfind("\n")
    if cut > 0:
        truncated = truncated[:cut]
    return truncated + "\n..."



FIELD_HINTS: Dict[str, List[str]] = {
    "CollegeName": ["College Name", "Institution Name", "University Name"],
    "CollegeSetting": ["College Setting", "Campus Setting"],
    "TypeofInstitution": ["Type of Institution", "Institution Type"],
    "Student_Faculty": ["Student Faculty Ratio", "Student-Faculty Ratio"],
    "NumberOfCampuses": ["Number of Campuses", "Campuses"],
    "TotalStudentsEnrolled": ["Total Students Enrolled", "Enrollment", "Total Enrollment"],
    "TotalGraduatePrograms": ["Total Graduate Programs", "Graduate Programs"],
    "TotalStudents": ["Total Students"],
    "TotalUndergradMajors": ["Total Undergrad Majors", "Undergraduate Majors"],
    "CountriesRepresented": ["Countries Represented"],
    "ApplicationFees": ["Application Fee", "Application Fees"],
    "TuitionFees": ["Tuition Fee", "Tuition Fees"],
    "GradTotalStudents": ["Graduate Students", "Graduate Student Population"],
    "UGTotalStudents": ["Undergraduate Students", "Undergraduate Student Population"],
    "GradAvgTuition": ["Graduate Average Tuition", "Graduate Tuition"],
    "UGAvgTuition": ["Undergraduate Average Tuition", "Undergraduate Tuition"],
    "FinancialAidUrl": ["Financial Aid URL", "Financial Aid Link"],
}


def humanize_field_name(field: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", " ", field.replace("_", " ")).strip()


def describe_field_for_prompt(field: str, entity_label: str) -> str:
    desc = FIELD_DESCRIPTION_OVERRIDES.get(field)
    if not desc:
        desc = f"{humanize_field_name(field)} for this {entity_label}."
    type_hint = FIELD_TYPE_HINTS.get(field)
    if type_hint:
        return f"{field}: {desc} (Type: {type_hint})"
    return f"{field}: {desc}"


def build_field_prompt_lines(field_names: List[str], entity_label: str) -> str:
    label_text = entity_label.capitalize()
    lines = [
        f"- {describe_field_for_prompt(field, label_text)}"
        for field in field_names
    ]
    return "\n".join(lines)


def build_field_label_map(
    field_names: List[str], extra_hints: Optional[Dict[str, List[str]]] = None
) -> Dict[str, List[str]]:
    label_map: Dict[str, List[str]] = {}
    for field in field_names:
        variants = {
            humanize_field_name(field),
            field.replace("_", " "),
            field,
        }
        if extra_hints and field in extra_hints:
            variants.update(extra_hints[field])
        label_map[field] = sorted({variant.strip() for variant in variants if variant})
    return label_map


COLLEGE_FIELD_LABELS = build_field_label_map(COLLEGE_FIELD_NAMES, FIELD_HINTS)
DEPARTMENT_FIELD_LABELS = build_field_label_map(DEPARTMENT_FIELD_NAMES)
PROGRAM_FIELD_LABELS = build_field_label_map(PROGRAM_FIELD_NAMES)

COLLEGE_FIELD_NAMES: List[str] = [
    "CollegeName",
    "LogoPath",
    "Phone",
    "Email",
    "SecondaryEmail",
    "Street1",
    "Street2",
    "County",
    "City",
    "State",
    "Country",
    "ZipCode",
    "WebsiteUrl",
    "AdmissionOfficeUrl",
    "VirtualTourUrl",
    "Facebook",
    "Instagram",
    "Twitter",
    "Youtube",
    "Tiktok",
    "ApplicationFees",
    "TestPolicy",
    "CoursesAndGrades",
    "Recommendations",
    "PersonalEssay",
    "WritingSample",
    "FinancialAidUrl",
    "AdditionalInformation",
    "AdditionalDeadlines",
    "TuitionFees",
    "LinkedIn",
    "NumberOfCampuses",
    "TotalFacultyAvailable",
    "TotalProgramsAvailable",
    "TotalStudentsEnrolled",
    "CollegeSetting",
    "TypeofInstitution",
    "CountriesRepresented",
    "GradAvgTuition",
    "GradInternationalStudents",
    "GradScholarshipHigh",
    "GradScholarshipLow",
    "GradTotalStudents",
    "Student_Faculty",
    "TotalGraduatePrograms",
    "TotalInternationalStudents",
    "TotalStudents",
    "TotalUndergradMajors",
    "UGAvgTuition",
    "UGInternationalStudents",
    "UGScholarshipHigh",
    "UGScholarshipLow",
    "UGTotalStudents",
]

DEPARTMENT_FIELD_NAMES: List[str] = [
    "DepartmentName",
    "Description",
    "City",
    "Country",
    "CountryCode",
    "CountryName",
    "Email",
    "PhoneNumber",
    "PhoneType",
    "State",
    "Street1",
    "Street2",
    "ZipCode",
    "StateName",
    "AdmissionUrl",
    "BuildingName",
]

PROGRAM_FIELD_NAMES: List[str] = [
    "ProgramName",
    "Level",
    "Term",
    "LiveDate",
    "DeadlineDate",
    "Resume",
    "StatementOfPurpose",
    "GreOrGmat",
    "EnglishScore",
    "Requirements",
    "WritingSample",
    "CollegeID",
    "CollegeDepartmentID",
    "IsAnalyticalNotRequired",
    "IsAnalyticalOptional",
    "IsDuoLingoRequired",
    "IsELSRequired",
    "IsGMATOrGreRequired",
    "IsGMATRequired",
    "IsGreRequired",
    "IsIELTSRequired",
    "IsLSATRequired",
    "IsMATRequired",
    "IsMCATRequired",
    "IsPTERequired",
    "IsTOEFLIBRequired",
    "IsTOEFLPBTRequired",
    "IsEnglishNotRequired",
    "IsEnglishOptional",
    "Department",
    "Fees",
    "Concentration",
    "Description",
    "ProgramWebsiteURL",
    "Accreditation",
    "AverageScholarshipAmount",
    "CostPerCredit",
    "IsRecommendationSystemOpted",
    "IsStemProgram",
    "MaxFails",
    "MaxGPA",
    "MinGPA",
    "PreviousYearAcceptanceRates",
    "QsWorldRanking",
    "IsACTRequired",
    "IsSATRequired",
    "MinimumACTScore",
    "MinimumDuoLingoScore",
    "MinimumELSScore",
    "MinimumGMATScore",
    "MinimumGreScore",
    "MinimumIELTSScore",
    "MinimumMATScore",
    "MinimumMCATScore",
    "MinimumPTEScore",
    "MinimumSATScore",
    "MinimumTOEFLScore",
    "ScholarshipAmount",
    "ScholarshipPercentage",
    "ScholarshipType",
    "MinimumLSATScore",
]


FIELD_DESCRIPTION_OVERRIDES: Dict[str, str] = {
    # College
    "CollegeName": "Official name of the college or university.",
    "LogoPath": "URL or path pointing to the institution's official logo.",
    "Phone": "Primary phone number for the college or admissions.",
    "Email": "Primary admissions or contact email address.",
    "SecondaryEmail": "Secondary admissions or contact email address.",
    "Street1": "Primary street address line for the main campus.",
    "Street2": "Secondary street or suite information for the main campus.",
    "County": "County where the main campus is located.",
    "City": "City of the main campus.",
    "State": "State or province of the main campus.",
    "Country": "Country of the institution.",
    "ZipCode": "Postal or ZIP code of the main campus.",
    "WebsiteUrl": "Official institutional website URL.",
    "AdmissionOfficeUrl": "URL to the admissions office page.",
    "VirtualTourUrl": "URL to any virtual campus tour experience.",
    "Facebook": "Official Facebook page URL.",
    "Instagram": "Official Instagram profile URL.",
    "Twitter": "Official Twitter/X profile URL.",
    "Youtube": "Official YouTube channel URL.",
    "Tiktok": "Official TikTok account URL.",
    "ApplicationFees": "Application fee amount charged per applicant.",
    "TestPolicy": "Summary of standardized test policies.",
    "CoursesAndGrades": "Information about required courses or grade expectations.",
    "Recommendations": "Recommendation requirements for applicants.",
    "PersonalEssay": "Summary of personal essay requirements.",
    "WritingSample": "Writing sample requirements or guidance.",
    "FinancialAidUrl": "URL for financial aid information.",
    "AdditionalInformation": "Other admissions information called out on the site.",
    "AdditionalDeadlines": "Any additional application deadlines mentioned.",
    "TuitionFees": "General tuition fees or ranges for the institution.",
    "LinkedIn": "Official LinkedIn page URL.",
    "NumberOfCampuses": "Total number of campuses operated by the institution.",
    "TotalFacultyAvailable": "Total number of faculty members.",
    "TotalProgramsAvailable": "Total number of academic programs offered.",
    "TotalStudentsEnrolled": "Total number of students currently enrolled.",
    "TotalGraduatePrograms": "Total number of graduate-level programs offered.",
    "TotalInternationalStudents": "Number of international students enrolled.",
    "TotalStudents": "Total number of students across all programs.",
    "TotalUndergradMajors": "Number of undergraduate majors offered.",
    "CountriesRepresented": "Number or list of countries represented within the student body.",
    "CollegeSetting": "Campus setting description (e.g., urban, suburban, rural).",
    "TypeofInstitution": "Institution type (e.g., public university, private college).",
    "GradAvgTuition": "Average annual tuition for graduate students.",
    "GradInternationalStudents": "Number of international graduate students.",
    "GradScholarshipHigh": "Highest scholarship amount available to graduate students.",
    "GradScholarshipLow": "Lowest scholarship amount available to graduate students.",
    "GradTotalStudents": "Total number of graduate students.",
    "Student_Faculty": "Student-to-faculty ratio (e.g., 14:1).",
    "TotalGraduatePrograms": "Total number of graduate programs (duplicate-friendly).",
    "UGAvgTuition": "Average annual tuition for undergraduate students.",
    "UGInternationalStudents": "Number of international undergraduate students.",
    "UGScholarshipHigh": "Highest scholarship amount available to undergraduate students.",
    "UGScholarshipLow": "Lowest scholarship amount available to undergraduate students.",
    "UGTotalStudents": "Total number of undergraduate students.",
    # Department / CollegeDepartment
    "DepartmentName": "Name of the academic department.",
    "Description": "Short description or overview of the department or program.",
    "City": "City location for the department office.",
    "Country": "Country for the department office.",
    "CountryCode": "Two-letter country code for the department office.",
    "CountryName": "Full country name for the department office.",
    "Email": "Primary departmental contact email.",
    "PhoneNumber": "Primary departmental phone number.",
    "PhoneType": "Type of phone number (e.g., office, cell).",
    "State": "State or province for the department address.",
    "StateName": "Full state or province name if abbreviated elsewhere.",
    "Street1": "Primary street address for the department office.",
    "Street2": "Secondary street or suite information for the department office.",
    "ZipCode": "Postal or ZIP code for the department office.",
    "AdmissionUrl": "Department-specific admissions URL.",
    "BuildingName": "Campus building where the department is located.",
    "CollegeID": "Numeric CollegeID this record should link to.",
    # Program & related tables
    "ProgramName": "Name of the academic program.",
    "Level": "Program level (e.g., Undergraduate, Graduate, Certificate).",
    "Term": "Academic term the details apply to (e.g., Fall 2025).",
    "LiveDate": "Date when the program information becomes active.",
    "DeadlineDate": "Application deadline date for the program.",
    "Resume": "Resume or CV requirements for applicants.",
    "StatementOfPurpose": "Statement of purpose requirements.",
    "GreOrGmat": "GRE or GMAT score expectations.",
    "EnglishScore": "English proficiency expectations (IELTS, TOEFL, etc.).",
    "Requirements": "General program admission requirements.",
    "WritingSample": "Writing sample requirements for the program.",
    "CollegeID": "CollegeID this record should reference.",
    "CollegeDepartmentID": "CollegeDepartmentID that the program should link to.",
    "Department": "Name of the department offering the program.",
    "Fees": "Tuition or fee amount for the specified term.",
    "Concentration": "Program concentration or track, if applicable.",
    "Description": "Detailed overview of the program curriculum or focus.",
    "ProgramWebsiteURL": "Program-specific website URL.",
    "Accreditation": "Accreditation status or agency for the program.",
    "CostPerCredit": "Tuition cost per credit hour.",
    "IsRecommendationSystemOpted": "Whether recommendation letters are required (true/false).",
    "IsStemProgram": "Whether the program is STEM-designated (true/false).",
    "MaxFails": "Maximum number of failed courses allowed for applicants.",
    "MaxGPA": "Maximum GPA mentioned in the requirements (if applicable).",
    "MinGPA": "Minimum GPA required for applicants.",
    "PreviousYearAcceptanceRates": "Acceptance rate from the previous admission cycle.",
    "QsWorldRanking": "Program or institution QS World Ranking.",
    "IsAnalyticalNotRequired": "True if analytical writing scores are not required.",
    "IsAnalyticalOptional": "True if analytical writing scores are optional.",
    "IsDuoLingoRequired": "True if Duolingo English test is required.",
    "IsELSRequired": "True if ELS certification is required.",
    "IsGMATOrGreRequired": "True if either GMAT or GRE is required.",
    "IsGMATRequired": "True if GMAT is required.",
    "IsGreRequired": "True if GRE is required.",
    "IsIELTSRequired": "True if IELTS is required.",
    "IsLSATRequired": "True if LSAT is required.",
    "IsMATRequired": "True if MAT is required.",
    "IsMCATRequired": "True if MCAT is required.",
    "IsPTERequired": "True if PTE is required.",
    "IsTOEFLIBRequired": "True if TOEFL iBT is required.",
    "IsTOEFLPBTRequired": "True if TOEFL PBT is required.",
    "IsEnglishNotRequired": "True if English proficiency tests are not required.",
    "IsEnglishOptional": "True if English proficiency tests are optional.",
    "IsACTRequired": "True if ACT scores are required.",
    "IsSATRequired": "True if SAT scores are required.",
    "MinimumACTScore": "Minimum ACT score accepted.",
    "MinimumDuoLingoScore": "Minimum Duolingo English test score accepted.",
    "MinimumELSScore": "Minimum ELS score accepted.",
    "MinimumGMATScore": "Minimum GMAT score accepted.",
    "MinimumGreScore": "Minimum GRE score accepted.",
    "MinimumIELTSScore": "Minimum IELTS band score accepted.",
    "MinimumMATScore": "Minimum MAT score accepted.",
    "MinimumMCATScore": "Minimum MCAT score accepted.",
    "MinimumPTEScore": "Minimum PTE score accepted.",
    "MinimumSATScore": "Minimum SAT score accepted.",
    "MinimumTOEFLScore": "Minimum TOEFL score accepted.",
    "MinimumLSATScore": "Minimum LSAT score accepted.",
    "ScholarshipAmount": "Scholarship amount offered for this program.",
    "ScholarshipPercentage": "Scholarship percentage offered.",
    "ScholarshipType": "Type of scholarship (merit, need-based, etc.).",
    "Resume": "Resume/CV expectation for applicants.",
}


INTEGER_FIELDS = {
    "NumberOfCampuses",
    "TotalFacultyAvailable",
    "TotalProgramsAvailable",
    "TotalStudentsEnrolled",
    "TotalGraduatePrograms",
    "TotalInternationalStudents",
    "TotalStudents",
    "TotalUndergradMajors",
    "GradInternationalStudents",
    "GradScholarshipHigh",
    "GradScholarshipLow",
    "GradTotalStudents",
    "TotalGraduatePrograms",
    "UGInternationalStudents",
    "UGScholarshipHigh",
    "UGScholarshipLow",
    "UGTotalStudents",
    "CollegeID",
    "CollegeDepartmentID",
    "MaxFails",
    "QsWorldRanking",
    "MinimumACTScore",
    "MinimumDuoLingoScore",
    "MinimumELSScore",
    "MinimumGMATScore",
    "MinimumGreScore",
    "MinimumIELTSScore",
    "MinimumMATScore",
    "MinimumMCATScore",
    "MinimumPTEScore",
    "MinimumSATScore",
    "MinimumTOEFLScore",
    "MinimumLSATScore",
}

DECIMAL_FIELDS = {
    "MaxGPA",
    "MinGPA",
}

BOOLEAN_FIELDS = {
    "IsAnalyticalNotRequired",
    "IsAnalyticalOptional",
    "IsDuoLingoRequired",
    "IsELSRequired",
    "IsGMATOrGreRequired",
    "IsGMATRequired",
    "IsGreRequired",
    "IsIELTSRequired",
    "IsLSATRequired",
    "IsMATRequired",
    "IsMCATRequired",
    "IsPTERequired",
    "IsTOEFLIBRequired",
    "IsTOEFLPBTRequired",
    "IsEnglishNotRequired",
    "IsEnglishOptional",
    "IsRecommendationSystemOpted",
    "IsStemProgram",
    "IsACTRequired",
    "IsSATRequired",
}

DATE_FIELDS = {
    "LiveDate",
    "DeadlineDate",
}

URL_FIELDS = {
    "LogoPath",
    "WebsiteUrl",
    "AdmissionOfficeUrl",
    "VirtualTourUrl",
    "Facebook",
    "Instagram",
    "Twitter",
    "Youtube",
    "Tiktok",
    "FinancialAidUrl",
    "LinkedIn",
    "AdmissionUrl",
    "ProgramWebsiteURL",
}

EMAIL_FIELDS = {
    "Email",
    "SecondaryEmail",
}

PHONE_FIELDS = {
    "Phone",
    "PhoneNumber",
}

CURRENCY_FIELDS = {
    "ApplicationFees",
    "TuitionFees",
    "GradAvgTuition",
    "GradScholarshipHigh",
    "GradScholarshipLow",
    "UGAvgTuition",
    "UGScholarshipHigh",
    "UGScholarshipLow",
    "Fees",
    "AverageScholarshipAmount",
    "CostPerCredit",
    "ScholarshipAmount",
}

PERCENT_FIELDS = {
    "ScholarshipPercentage",
    "PreviousYearAcceptanceRates",
}

FIELD_TYPE_HINTS: Dict[str, str] = {}


def _apply_type_hint(fields: Iterable[str], hint: str) -> None:
    for name in fields:
        FIELD_TYPE_HINTS[name] = hint


_apply_type_hint(INTEGER_FIELDS, "integer count (digits only, no commas)")
_apply_type_hint(DECIMAL_FIELDS, "decimal number (use digits with a period as needed)")
_apply_type_hint(CURRENCY_FIELDS, "numeric currency amount (digits only, no symbols)")
_apply_type_hint(URL_FIELDS, "full URL (http or https)")
_apply_type_hint(EMAIL_FIELDS, "email address")
_apply_type_hint(PHONE_FIELDS, "phone number including area code")
_apply_type_hint(DATE_FIELDS, "ISO date (YYYY-MM-DD)")
_apply_type_hint(BOOLEAN_FIELDS, "boolean flag, respond with 'true' or 'false'")
_apply_type_hint(PERCENT_FIELDS, "percentage or rate (digits with optional % sign)")


FIELD_KEYWORDS: Dict[str, List[str]] = {
    "CollegeName": ["university", "college", "institution"],
    "Phone": ["phone", "contact"],
    "Email": ["email", "contact"],
    "ApplicationFees": ["application fee"],
    "TuitionFees": ["tuition"],
    "GradAvgTuition": ["graduate tuition"],
    "UGAvgTuition": ["undergraduate tuition"],
    "TotalStudents": ["total students", "student body"],
    "TotalStudentsEnrolled": ["students enrolled", "enrollment"],
    "GradTotalStudents": ["graduate students"],
    "UGTotalStudents": ["undergraduate students"],
    "CountriesRepresented": ["countries represented"],
    "Student_Faculty": ["student faculty ratio", "student-faculty"],
}


def extract_labeled_fields(text: str, label_map: Dict[str, List[str]]) -> Dict[str, str]:
    matches: Dict[str, str] = {}
    if not text:
        return matches

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for idx, line in enumerate(lines):
        for field, labels in label_map.items():
            if field in matches:
                continue
            for label in labels:
                value = capture_labeled_value(line, label)
                if not value and line.rstrip(":").lower() == label.lower().rstrip(":") and idx + 1 < len(lines):
                    value = clean_extracted_value(lines[idx + 1])
                if value:
                    matches[field] = value
                    break
            if field in matches:
                break

    return matches


def extract_college_fields(text: str) -> Dict[str, str]:
    return extract_labeled_fields(text, COLLEGE_FIELD_LABELS)


def extract_department_fields(text: str) -> Dict[str, str]:
    return extract_labeled_fields(text, DEPARTMENT_FIELD_LABELS)


def extract_program_fields(text: str) -> Dict[str, str]:
    return extract_labeled_fields(text, PROGRAM_FIELD_LABELS)


def capture_labeled_value(line: str, label: str) -> str:
    pattern = re.compile(
        rf"{re.escape(label)}\s*(?:is|are|=|:|-|–|—)?\s*(?P<value>.+)",
        re.IGNORECASE,
    )
    match = pattern.search(line)
    if not match:
        return ""
    value = match.group("value").strip()
    return clean_extracted_value(value)


def clean_extracted_value(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    value = value.split("•")[0]
    value = value.split(" | ")[0]
    value = value.rstrip(".,;")
    if len(value) > 200:
        value = value[:200].rstrip() + "..."
    return value


def clean_url(value: str) -> str:
    if not value:
        return ""
    return value.strip().rstrip(".,);")


def get_http_session() -> requests.Session:
    session = getattr(thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        thread_local.session = session
    return session


def collect_links(soup: BeautifulSoup, base_url: str, max_links: int = 80) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        if len(links) >= max_links:
            break
        href = anchor["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        absolute = urljoin(base_url, href)
        normalized = normalize_url(absolute)
        if not normalized:
            continue
        text = anchor.get_text(" ", strip=True)
        links.append({"url": normalized, "text": text})
    return links


def links_to_text(results: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    seen: set[str] = set()
    for result in results:
        for link in result.get("links") or []:
            url = link.get("url")
            if not url or url in seen:
                continue
            seen.add(url)
            link_text = link.get("text") or ""
            if link_text:
                lines.append(f"{link_text} -> {url}")
            else:
                lines.append(url)
    return "\n".join(lines)


def build_llm_input_text(results: List[Dict[str, Any]]) -> str:
    prioritized_segments: List[str] = []
    used_urls: set[str] = set()

    for field, keywords in FIELD_KEYWORDS.items():
        lower_keywords = [kw.lower() for kw in keywords]
        for result in results:
            snippet = result.get("text") or ""
            if not snippet or result.get("url") in used_urls:
                continue
            lower_snippet = snippet.lower()
            if any(keyword in lower_snippet for keyword in lower_keywords):
                prioritized_segments.append(snippet)
                used_urls.add(result.get("url"))
                break

    for result in results:
        if result.get("url") in used_urls:
            continue
        snippet = result.get("text") or ""
        if snippet:
            prioritized_segments.append(snippet)
        if len("\n\n".join(prioritized_segments)) > 9000:
            break

    link_text = links_to_text(results)
    if link_text:
        prioritized_segments.append("Links discovered:\n" + link_text)

    combined = "\n\n".join(prioritized_segments)
    if len(combined) > 8000:
        combined = combined[:8000]
    return combined


def heuristic_extract_fields(text: str) -> Dict[str, str]:
    if not text:
        return {}

    results: Dict[str, str] = {}
    lower_text = text.lower()

    phone_match = re.search(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}", text)
    if phone_match:
        results["Phone"] = phone_match.group(0)

    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    if emails:
        results["Email"] = emails[0]
        if len(emails) > 1:
            results["SecondaryEmail"] = emails[1]

    url_matches = re.findall(r"https?://[^\s)]+", text)
    for url in url_matches:
        cleaned = clean_url(url)
        lowered = cleaned.lower()
        if "financial" in lowered and "aid" in lowered:
            results.setdefault("FinancialAidUrl", cleaned)
        if "admission" in lowered:
            results.setdefault("AdmissionOfficeUrl", cleaned)
        if "virtual" in lowered and "tour" in lowered:
            results.setdefault("VirtualTourUrl", cleaned)
        if "facebook.com" in lowered:
            results.setdefault("Facebook", cleaned)
        if "instagram.com" in lowered:
            results.setdefault("Instagram", cleaned)
        if "twitter.com" in lowered or "x.com" in lowered:
            results.setdefault("Twitter", cleaned)
        if "youtube.com" in lowered:
            results.setdefault("Youtube", cleaned)
        if "tiktok.com" in lowered:
            results.setdefault("Tiktok", cleaned)
        if "linkedin.com" in lowered:
            results.setdefault("LinkedIn", cleaned)
        if cleaned.endswith((".edu", ".edu/", ".edu)")) or ".edu/" in cleaned:
            results.setdefault("WebsiteUrl", cleaned)

    student_faculty = re.search(r"\b\d+\s*:\s*\d+\b", text)
    if student_faculty:
        results.setdefault("Student_Faculty", student_faculty.group(0))

    number_patterns = [
        ("TotalStudentsEnrolled", r"(?:over|more than|about|approximately)?\s*([\d,]+)\s+(?:students\s+enrolled|enrolled\s+students)"),
        ("TotalStudents", r"(?:over|more than|about|approximately)?\s*([\d,]+)(?:\s*\+?)?(?:\s+\w+){0,4}\s+students"),
        ("GradTotalStudents", r"(?:over|more than|about|approximately)?\s*([\d,]+)\s+(?:graduate|grad)\s+students"),
        ("UGTotalStudents", r"(?:over|more than|about|approximately)?\s*([\d,]+)\s+(?:undergraduate|ug)\s+students"),
        ("NumberOfCampuses", r"(?:over|more than|about|approximately)?\s*([\d,]+)\s+campuses"),
        ("CountriesRepresented", r"(?:over|more than|about|approximately)?\s*([\d,]+)\s+countries"),
    ]
    for field, pattern in number_patterns:
        match = re.search(pattern, lower_text)
        if match:
            results.setdefault(field, match.group(1))

    tuition_patterns = [
        ("GradAvgTuition", r"\$(\d[\d,]*)\s+(?:per\s+year\s+)?graduate\s+tuition"),
        ("UGAvgTuition", r"\$(\d[\d,]*)\s+(?:per\s+year\s+)?undergraduate\s+tuition"),
        ("TuitionFees", r"\$(\d[\d,]*)\s+tuition"),
        ("ApplicationFees", r"\$(\d[\d,]*)\s+application\s+fee"),
    ]
    for field, pattern in tuition_patterns:
        match = re.search(pattern, lower_text)
        if match:
            results.setdefault(field, f"${match.group(1)}")

    college_name_match = re.search(r"([A-Z][A-Za-z&.\s]{3,}\s(?:University|College))", text)
    if college_name_match:
        results.setdefault("CollegeName", college_name_match.group(1).strip())

    county_match = re.search(r"\b([A-Za-z\s]+ County)\b", text)
    if county_match:
        results.setdefault("County", county_match.group(1).strip())

    return results


def project_heuristics_to_entity(entity: str, heuristics: Dict[str, str]) -> Dict[str, str]:
    config = ENTITY_CONFIG.get(entity)
    if not config or not heuristics:
        return {}
    field_set = set(config["field_names"])
    projected: Dict[str, str] = {}
    for field in field_set:
        if field in heuristics:
            projected[field] = heuristics[field]
    alias_map = HEURISTIC_FIELD_ALIASES.get(entity, {})
    for source, target in alias_map.items():
        if target in projected:
            continue
        value = heuristics.get(source)
        if value and target in field_set:
            projected[target] = value
    return projected


def call_vertex_chat_completion(system_prompt: str, user_prompt: str) -> str:
    raise RuntimeError("Vertex AI is disabled. This deployment uses Gemini only.")


def llm_extract_entity_fields(
    text: str,
    field_names: List[str],
    entity_label: str,
) -> Dict[str, str]:
    if not text:
        return {}

    configured_model = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash").strip()
    base_name = configured_model or "gemini-1.5-flash-latest"
    base_name = base_name.replace("models/", "")
    candidates = []
    if base_name:
        candidates.append(base_name)
        if not base_name.endswith("-latest"):
            candidates.append(f"{base_name}-latest")
    candidates.append("gemini-1.5-flash-latest")

    prompt_fields = build_field_prompt_lines(field_names, entity_label)
    instructions = (
        f"You are extracting structured {entity_label} data from university websites.\n"
        "Rules:\n"
        "1. Only emit values that are explicitly present in the text; never guess or hallucinate.\n"
        "2. If a value is missing, return an empty string for that key.\n"
        "3. Output must be valid JSON with exactly the requested keys (no extra keys or commentary).\n"
        "4. Preserve wording and numbers exactly as written, except where type hints require numeric-only digits.\n"
        "5. Type hints: integers/decimals as digits only (strip commas, symbols), booleans as true/false, dates as YYYY-MM-DD, URLs/emails unchanged.\n"
        "6. Address extraction (Street1/Street2/City/State/ZipCode/Country/County/StateName/CountryName/CountryCode):\n"
        "   - Prefer physical campus or admissions-office addresses over mailing boxes when both are present.\n"
        "   - Split Street1/Street2 if lines are clearly separated; otherwise put the single line in Street1 and leave Street2 empty.\n"
        "   - City is a proper noun; State is the two-letter code if shown, otherwise use full name in StateName.\n"
        "   - ZipCode: capture 5-digit (or ZIP+4) exactly as written. CountryCode: use 2-letter code if shown.\n"
        "7. Phone and Email:\n"
        "   - Phone: capture a single canonical phone number (include country code if shown).\n"
        "   - Email: use the most authoritative admissions/contact email on the page; SecondaryEmail is the next best distinct email.\n"
        "8. URLs (WebsiteUrl, AdmissionOfficeUrl, VirtualTourUrl, FinancialAidUrl, ProgramWebsiteURL):\n"
        "   - Use full absolute URLs as written; do not add or remove query parameters.\n"
        "9. Student counts and numeric ranges (e.g., TotalStudents, UGTotalStudents, CountriesRepresented, ApplicationFees, TuitionFees):\n"
        "   - Emit a single numeric value when a clear number is present. If a range is given (e.g., 10–12), emit the first number only.\n"
        "   - Strip units and symbols (e.g., '$', 'USD', 'students') and commas.\n"
        "10. Social links (Facebook, Instagram, Twitter, Youtube, Tiktok, LinkedIn):\n"
        "   - Prefer official/verified institutional profiles. If multiple are present, choose the most official/global account.\n"
        "11. Never infer missing values. If unsure or multiple conflicting values appear without clear primacy, leave the field empty.\n"
    )

    max_chars = int(os.environ.get("MAX_LLM_INPUT_CHARS", "6000"))
    content = text if len(text) <= max_chars else text[:max_chars]

    prompt = f"""{instructions}

Fields:
{prompt_fields}

Text:
\"\"\"{content}\"\"\"
"""

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is required to use Gemini.")
    genai.configure(api_key=api_key)
    model = None
    last_error: Optional[Exception] = None
    for candidate in candidates:
        try:
            model = genai.GenerativeModel(f"models/{candidate}")
            break
        except Exception as exc:  # noqa: PERF203
            last_error = exc
            continue

    if model is None:
        raise RuntimeError(f"Unable to load Gemini model. Last error: {last_error}")

    # LLM cache: avoid repeated calls on the same prompt content
    key_fields = ",".join(sorted(field_names))
    content_hash = hashlib.sha256((entity_label + "|" + key_fields + "|" + content).encode("utf-8")).hexdigest()
    cache_key = f"g::{candidate}::{content_hash}"
    cached_text = LLM_CACHE.get(cache_key)
    if cached_text is None:
        # Retry/backoff for Gemini rate limits
        max_retries = int(os.environ.get("LLM_MAX_RETRIES", "6"))
        backoff_base = float(os.environ.get("LLM_BACKOFF_BASE", "1.5"))
        last_error = None
        response_text = ""
        for attempt in range(max_retries + 1):
            try:
                response = model.generate_content(prompt)
                if not response or not response.text:
                    response_text = ""
                else:
                    response_text = (response.text or "").strip()
                break
            except Exception as exc:  # generic because SDK raises different errors
                last_error = exc
                if attempt < max_retries:
                    time.sleep(backoff_base * (2 ** attempt) + (0.1 * attempt))
                    continue
                raise
        if not response_text:
            return {}
        LLM_CACHE.set(cache_key, response_text)
    else:
        response_text = cached_text.strip()

    raw = response_text.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        return {}

    try:
        payload = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return {}

    if not isinstance(payload, dict):
        return {}

    sanitized: Dict[str, str] = {}
    for field in field_names:
        value = payload.get(field, "")
        if isinstance(value, str):
            sanitized[field] = value.strip()
        else:
            sanitized[field] = ""
    return sanitized


@llm_test_bp.route("/", methods=["GET", "POST"])
def llm_test_index():
    output = ""
    prompt = ""
    model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
    if request.method == "POST":
        prompt = request.form.get("prompt", "")
        if prompt:
            try:
                api_key = os.environ.get("GOOGLE_API_KEY")
                if not api_key:
                    output = "Error: GOOGLE_API_KEY is not set."
                else:
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel(f"models/{model_name.replace('models/','')}")
                    resp = model.generate_content(prompt)
                    output = (resp.text or "").strip() if resp else "(empty response)"
            except Exception as exc:  # noqa: BLE001
                output = f"Error: {exc}"
        else:
            output = "Enter a prompt to test the Gemini API."

    return render_template(
        "llm_test.html",
        prompt=prompt,
        output=output,
        vertex_model="",  # no longer used; template will show 'not set'
    )


def llm_extract_college_fields(text: str) -> Dict[str, str]:
    return llm_extract_entity_fields(text, COLLEGE_FIELD_NAMES, "college")


def llm_extract_department_fields(text: str) -> Dict[str, str]:
    return llm_extract_entity_fields(text, DEPARTMENT_FIELD_NAMES, "department")


def llm_extract_program_fields(text: str) -> Dict[str, str]:
    return llm_extract_entity_fields(text, PROGRAM_FIELD_NAMES, "program")


ENTITY_ORDER: Tuple[str, ...] = ("college", "department", "program")

ENTITY_CONFIG: Dict[str, Dict[str, Any]] = {
    "college": {
        "label": "College",
        "field_names": COLLEGE_FIELD_NAMES,
        "extract_fn": extract_college_fields,
        "llm_fn": llm_extract_college_fields,
        "id_label": "College ID",
        "edit_endpoint": "forms.university_edit",
        "edit_param": "college_id",
        "list_endpoint": "forms.university_list",
    },
    "department": {
        "label": "Admissions Office",
        "field_names": DEPARTMENT_FIELD_NAMES,
        "extract_fn": extract_department_fields,
        "llm_fn": llm_extract_department_fields,
        "id_label": "Department ID",
        "edit_endpoint": "forms.department_edit",
        "edit_param": "department_id",
        "list_endpoint": "forms.department_list",
    },
    "program": {
        "label": "Program",
        "field_names": PROGRAM_FIELD_NAMES,
        "extract_fn": extract_program_fields,
        "llm_fn": llm_extract_program_fields,
        "id_label": "Program ID",
        "edit_endpoint": "forms.program_edit",
        "edit_param": "program_id",
        "list_endpoint": "forms.program_list",
    },
}

HEURISTIC_FIELD_ALIASES: Dict[str, Dict[str, str]] = {
    "department": {
        "Phone": "PhoneNumber",
        "AdmissionOfficeUrl": "AdmissionUrl",
        "WebsiteUrl": "AdmissionUrl",
    },
    "program": {
        "WebsiteUrl": "ProgramWebsiteURL",
        "AdmissionOfficeUrl": "ProgramWebsiteURL",
    },
}


def merge_field_values(*sources: Dict[str, str]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for source in sources:
        for key, value in (source or {}).items():
            if key in merged:
                continue
            if isinstance(value, str) and value.strip():
                merged[key] = value.strip()
    return merged


def compute_entity_extraction(
    results: List[Dict[str, Any]],
    per_page_llm: bool,
) -> Dict[str, Dict[str, Any]]:
    entity_field_candidates: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        entity: defaultdict(list) for entity in ENTITY_ORDER
    }
    literal_fields: Dict[str, Dict[str, str]] = {entity: {} for entity in ENTITY_ORDER}
    heuristic_fields_total: Dict[str, Dict[str, str]] = {entity: {} for entity in ENTITY_ORDER}
    llm_fields_total: Dict[str, Dict[str, str]] = {entity: {} for entity in ENTITY_ORDER}

    def add_candidate(
        entity: str,
        field: str,
        value: Optional[str],
        source_label: str,
        origin: str,
        *,
        page_url: Optional[str] = None,
        snippet: Optional[str] = None,
    ) -> None:
        if entity not in ENTITY_CONFIG:
            return
        if not value or not isinstance(value, str):
            return
        cleaned = value.strip()
        if not cleaned:
            return
        entries = entity_field_candidates[entity][field]
        for entry in entries:
            if (
                entry["value"] == cleaned
                and entry["origin"] == origin
                and entry.get("page_url") == page_url
            ):
                return
        entries.append(
            {
                "value": cleaned,
                "origin": origin,
                "source_label": source_label,
                "page_url": page_url,
                "snippet": (snippet or "").strip()[:320] if snippet else "",
            }
        )

    for result in results:
        text = result.get("text") or ""
        page_url = result.get("url")
        entity_fields: Dict[str, Dict[str, str]] = {}

        for entity in ENTITY_ORDER:
            config = ENTITY_CONFIG[entity]
            extracted = config["extract_fn"](text)
            entity_fields[entity] = extracted
            for field, value in extracted.items():
                if field not in literal_fields[entity]:
                    literal_fields[entity][field] = value
                    add_candidate(
                        entity,
                        field,
                        value,
                        "Literal match",
                        "literal",
                        page_url=page_url,
                        snippet=text,
                    )

        result["entity_fields"] = entity_fields

        if per_page_llm:
            link_lines: List[str] = []
            for link in result.get("links") or []:
                href = link.get("url")
                if not href:
                    continue
                text_value = link.get("text") or ""
                if text_value:
                    link_lines.append(f"{text_value} -> {href}")
                else:
                    link_lines.append(href)
            link_blob = "\n".join(link_lines)

            heur_input = text + ("\n" + link_blob if link_blob else "")
            if heur_input.strip():
                heur_page = heuristic_extract_fields(heur_input)
                if heur_page:
                    entity_heuristics: Dict[str, Dict[str, str]] = {}
                    for entity in ENTITY_ORDER:
                        mapped = project_heuristics_to_entity(entity, heur_page)
                        if not mapped:
                            continue
                        entity_heuristics[entity] = mapped
                        for field, value in mapped.items():
                            if field not in heuristic_fields_total[entity]:
                                heuristic_fields_total[entity][field] = value
                                add_candidate(
                                    entity,
                                    field,
                                    value,
                                    "Heuristic (page)",
                                    "heuristic",
                                    page_url=page_url,
                                    snippet=text,
                                )
                    if entity_heuristics:
                        result["entity_heuristics"] = entity_heuristics

            llm_input = text[:6000]
            if link_blob:
                llm_input = f"{llm_input}\nLinks:\n{link_blob[:4000]}".strip()
            if llm_input.strip():
                entity_llm: Dict[str, Dict[str, str]] = {}
                for entity in ENTITY_ORDER:
                    llm_page = ENTITY_CONFIG[entity]["llm_fn"](llm_input)
                    if not llm_page:
                        continue
                    entity_llm[entity] = llm_page
                    for field, value in llm_page.items():
                        if field not in llm_fields_total[entity] and value:
                            llm_fields_total[entity][field] = value
                            add_candidate(
                                entity,
                                field,
                                value,
                                "Gemini (page)",
                                "llm",
                                page_url=page_url,
                                snippet=text,
                            )
                if entity_llm:
                    result["entity_llm"] = entity_llm

        result["combined_fields"] = merge_field_values(
            entity_fields.get("college"),
            (result.get("entity_heuristics") or {}).get("college"),
            (result.get("entity_llm") or {}).get("college"),
        )

    if not per_page_llm:
        combined_text = "\n\n".join(result.get("text", "") for result in results if result.get("text"))
        combined_links = links_to_text(results)
        heuristics_blob = combined_text + ("\n" + combined_links if combined_links else "")
        heuristics = heuristic_extract_fields(heuristics_blob)
        for entity in ENTITY_ORDER:
            mapped = project_heuristics_to_entity(entity, heuristics)
            for field, value in mapped.items():
                if field not in heuristic_fields_total[entity]:
                    heuristic_fields_total[entity][field] = value
                    add_candidate(entity, field, value, "Heuristic (aggregate)", "heuristic")

        llm_text = build_llm_input_text(results)
        for entity in ENTITY_ORDER:
            llm_fields = ENTITY_CONFIG[entity]["llm_fn"](llm_text)
            for field, value in llm_fields.items():
                if field not in llm_fields_total[entity] and value:
                    llm_fields_total[entity][field] = value
                    add_candidate(entity, field, value, "Gemini (aggregate)", "llm")

    entity_results: Dict[str, Dict[str, Any]] = {}
    for entity in ENTITY_ORDER:
        merged = merge_field_values(
            literal_fields[entity],
            heuristic_fields_total[entity],
            llm_fields_total[entity],
        )
        field_options: Dict[str, List[Dict[str, Any]]] = {}
        field_defaults: Dict[str, str] = {}
        for field in ENTITY_CONFIG[entity]["field_names"]:
            candidates = entity_field_candidates[entity].get(field, [])
            options: List[Dict[str, Any]] = []
            for index, candidate in enumerate(candidates):
                token = f"{candidate['origin']}::{entity}::{field}::{index}"
                option_label = candidate["source_label"]
                if candidate.get("page_url"):
                    option_label = f"{option_label} — {candidate['page_url']}"
                options.append(
                    {
                        "token": token,
                        "value": candidate["value"],
                        "source": option_label,
                        "origin": candidate["origin"],
                        "page_url": candidate.get("page_url"),
                        "snippet": candidate.get("snippet", ""),
                    }
                )
            default_value = merged.get(field, "")
            default_token = ""
            if default_value:
                for option in options:
                    if option["value"] == default_value:
                        default_token = option["token"]
                        break
                else:
                    token = f"merged::{entity}::{field}::{len(options)}"
                    options.append(
                        {
                            "token": token,
                            "value": default_value,
                            "source": "Merged default",
                            "origin": "merged",
                            "page_url": None,
                            "snippet": "",
                        }
                    )
                    default_token = token

            field_options[field] = options
            field_defaults[field] = default_token

        entity_results[entity] = {
            "label": ENTITY_CONFIG[entity]["label"],
            "literal_fields": literal_fields[entity],
            "heuristic_fields": heuristic_fields_total[entity],
            "llm_fields": llm_fields_total[entity],
            "merged_fields": merged,
            "field_options": field_options,
            "field_defaults": field_defaults,
            "selection_overrides": {},
            "ingested_id": None,
            "id_label": ENTITY_CONFIG[entity]["id_label"],
            "edit_endpoint": ENTITY_CONFIG[entity]["edit_endpoint"],
            "edit_param": ENTITY_CONFIG[entity]["edit_param"],
            "list_endpoint": ENTITY_CONFIG[entity]["list_endpoint"],
        }

    return entity_results


def build_selection_sections(
    engine,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Tuple[int, str]], List[Tuple[int, str]]]:
    sections: Dict[str, List[Dict[str, Any]]] = {
        "college": build_university_sections(),
        "department": [],
        "program": [],
    }

    social_fields = [{"name": platform, "label": platform} for platform in SOCIAL_MEDIA_PLATFORMS]
    if social_fields:
        sections["college"] = sections["college"] + [
            {
                "title": "Social Media Profiles",
                "description": "Confirm official social links before publishing.",
                "fields": social_fields,
            }
        ]

    college_options: List[Tuple[int, str]] = []
    college_department_options: List[Tuple[int, str]] = []

    if engine is not None:
        try:
            college_options = get_college_options(engine)
        except Exception:  # noqa: BLE001
            college_options = []

        try:
            sections["department"] = build_department_sections(engine)
        except Exception:  # noqa: BLE001
            sections["department"] = []

        try:
            program_sections, prog_college_opts, prog_cd_opts = build_program_sections(engine)
            sections["program"] = program_sections
            if not college_options:
                college_options = prog_college_opts
            college_department_options = prog_cd_opts
        except Exception:  # noqa: BLE001
            sections["program"] = []

        if not college_department_options and engine is not None:
            try:
                college_department_options = get_college_department_options(engine)
            except Exception:  # noqa: BLE001
                college_department_options = []

    return sections, college_options, college_department_options


def inject_linking_options(
    entity_results: Dict[str, Dict[str, Any]],
    college_options: List[Tuple[int, str]],
    college_department_options: List[Tuple[int, str]],
) -> None:
    if not entity_results:
        return

    def ensure_option(entity: str, field: str, value: Any, label: str) -> None:
        entity_data = entity_results.get(entity)
        if not entity_data:
            return
        options_map = entity_data.setdefault("field_options", {})
        bucket = options_map.setdefault(field, [])
        token = f"db::{entity}::{field}::{value}"
        if any(option["token"] == token for option in bucket):
            return
        bucket.append(
            {
                "token": token,
                "value": str(value),
                "source": label,
                "origin": "system",
                "page_url": None,
                "snippet": "",
            }
        )

    for value, label in college_options:
        ensure_option(
            "department",
            "CollegeID",
            value,
            f"Existing college — {label}",
        )
        ensure_option(
            "program",
            "CollegeID",
            value,
            f"Existing college — {label}",
        )

    for value, label in college_department_options:
        ensure_option(
            "program",
            "CollegeDepartmentID",
            value,
            f"College department — {label}",
        )


def run_crawl_job(
    app: Flask,
    job_id: str,
    url: str,
    max_pages: int,
    same_domain: bool,
    per_page_llm: bool,
    headers: Dict[str, str],
) -> None:
    with app.app_context():
        with JOBS_LOCK:
            job = CRAWL_JOBS.get(job_id)
        if not job:
            return

        queue: Queue = job["queue"]
        engine = current_app.config.get("DB_ENGINE")
        selection_sections, college_options, college_department_options = build_selection_sections(engine)

        def push_event(event: Dict[str, Any]) -> None:
            event.setdefault("job_id", job_id)
            event.setdefault("limit", max_pages)
            try:
                queue.put_nowait(event)
            except Exception:
                pass

        push_event({"type": "start", "visited": 0})

        try:
            results, visited_count, errors = crawl_site(
                url,
                headers=headers,
                max_pages=max_pages,
                same_domain=same_domain,
                progress_callback=push_event,
            )

            push_event({"type": "processing", "stage": "start"})
            entity_results = compute_entity_extraction(results, per_page_llm)
            inject_linking_options(entity_results, college_options, college_department_options)
            push_event({"type": "processing", "stage": "done"})

            stats = {"visited": visited_count, "errors": errors, "limit": max_pages}
            context = {
                "url": url,
                "max_pages": max_pages,
                "same_domain": same_domain,
                "per_page_llm": per_page_llm,
                "results": results,
                "stats": stats,
                "entity_results": entity_results,
                "selection_sections": selection_sections,
                "college_options": college_options,
                "college_department_options": college_department_options,
                "job_complete": True,
                "job_error": "",
                "default_workers": default_worker_count(),
            }

            job["result"] = context
            job["status"] = "finished"
            push_event({"type": "complete", "visited": visited_count, "errors": errors})
        except Exception as exc:  # noqa: BLE001
            push_event({"type": "processing", "stage": "error", "message": str(exc)})
            stats = {"visited": 0, "errors": 1, "limit": max_pages}
            context = {
                "url": url,
                "max_pages": max_pages,
                "same_domain": same_domain,
                "per_page_llm": per_page_llm,
                "results": [],
                "stats": stats,
                "entity_results": {},
                "selection_sections": selection_sections,
                "college_options": college_options,
                "college_department_options": college_department_options,
                "job_complete": True,
                "job_error": str(exc),
                "default_workers": default_worker_count(),
            }
            job["result"] = context
            job["status"] = "failed"
            push_event({"type": "error", "message": str(exc)})


def build_payloads_from_selected_fields(
    entity: str,
    selected_values: Dict[str, str],
    engine,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[Dict[str, Optional[str]]]]:
    if entity == "college":
        sections = build_university_sections()
    elif entity == "department":
        if engine is None:
            raise ValueError("Database connection is required to build department payloads.")
        sections = build_department_sections(engine)
    elif entity == "program":
        if engine is None:
            raise ValueError("Database connection is required to build program payloads.")
        sections, _, _ = build_program_sections(engine)
    else:
        raise ValueError("Unsupported entity for persistence.")

    relevant_names = set(ENTITY_CONFIG[entity]["field_names"]) if entity in ENTITY_CONFIG else set()
    all_fields: List[Dict[str, Any]] = [
        field for section in sections for field in section["fields"] if field.get("name") in relevant_names
    ]

    mock_form: Dict[str, str] = {}
    for field in all_fields:
        field_name = field["name"]
        form_key = field["form_name"]
        mock_form[form_key] = selected_values.get(field_name, "")

    social_payloads: Optional[Dict[str, Optional[str]]] = None
    if entity == "college":
        for platform in SOCIAL_MEDIA_PLATFORMS:
            mock_form[f"SocialMedia.{platform}"] = selected_values.get(platform, "")

    table_payloads = extract_prefixed_values(all_fields, mock_form)
    if entity == "college":
        social_payloads = extract_social_values(mock_form)

    return table_payloads, social_payloads


@crawler_bp.route("/progress/<job_id>")
def stream_crawl_progress(job_id: str):
    def event_stream():
        while True:
            with JOBS_LOCK:
                job = CRAWL_JOBS.get(job_id)
            if not job:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
                break
            queue: Queue = job["queue"]
            try:
                event = queue.get(timeout=1)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in {"complete", "error"}:
                    break
            except Empty:
                if job["status"] != "running":
                    break
        yield "event: done\ndata: {}\n\n"

    response = Response(event_stream(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    return response


@crawler_bp.route("/finalize/<job_id>", methods=["POST"])
def finalize_crawl(job_id: str):
    entity = (request.form.get("entity") or "college").lower()
    if entity not in ENTITY_CONFIG:
        flash("Unknown section submitted. Please try again.", "error")
        return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    with JOBS_LOCK:
        job = CRAWL_JOBS.get(job_id)
    if job is None:
        flash("Crawl job not found. Please start a new crawl.", "error")
        return redirect(url_for("crawler.crawler_index"))

    result = job.get("result")
    if not result:
        flash("Crawl is still running. Wait for completion before finalizing.", "error")
        return redirect(url_for("crawler.crawler_index", job_id=job_id))

    if not result.get("job_complete"):
        flash("Crawl is still running. Wait for completion before finalizing.", "error")
        return redirect(url_for("crawler.crawler_index", job_id=job_id))

    if "entity_results" not in result:
        try:
            result["entity_results"] = compute_entity_extraction(
                result.get("results", []),
                result.get("per_page_llm", False),
            )
        except Exception as exc:  # noqa: BLE001
            flash(f"Failed to prepare selections: {exc}", "error")
            return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    engine = current_app.config.get("DB_ENGINE")
    if engine is None:
        flash("Database connection is not configured.", "error")
        return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    sections, college_opts, college_dept_opts = build_selection_sections(engine)
    result["selection_sections"] = sections
    result["college_options"] = college_opts
    result["college_department_options"] = college_dept_opts
    inject_linking_options(result.get("entity_results", {}), college_opts, college_dept_opts)

    entity_results = result.get("entity_results", {})
    entity_data = entity_results.get(entity)
    if not entity_data:
        flash("No extracted values available for this entity yet.", "error")
        return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    field_options: Dict[str, List[Dict[str, Any]]] = entity_data.get("field_options", {})
    selected_tokens: Dict[str, str] = {}
    overrides: Dict[str, str] = {}
    selected_values: Dict[str, str] = {}

    for field in ENTITY_CONFIG[entity]["field_names"]:
        override_key = f"override_{entity}_{field}"
        choice_key = f"choice_{entity}_{field}"

        override_value = (request.form.get(override_key) or "").strip()
        if override_value:
            overrides[field] = override_value
            selected_tokens[field] = ""
            selected_values[field] = override_value
            continue

        token = (request.form.get(choice_key) or "").strip()
        selected_tokens[field] = token
        if token:
            candidate = next(
                (option for option in field_options.get(field, []) if option["token"] == token),
                None,
            )
            if candidate:
                selected_values[field] = candidate["value"]

    required_field = {
        "college": "CollegeName",
        "department": "DepartmentName",
        "program": "ProgramName",
    }.get(entity)
    if required_field and not selected_values.get(required_field):
        flash(f"Select or enter {required_field} before saving to the database.", "error")
        entity_data["field_defaults"] = selected_tokens
        entity_data["selection_overrides"] = overrides
        with JOBS_LOCK:
            job["result"] = result
        return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    try:
        table_payloads, social_payloads = build_payloads_from_selected_fields(
            entity, selected_values, engine
        )
    except ValueError as exc:
        flash(str(exc), "error")
        entity_data["field_defaults"] = selected_tokens
        entity_data["selection_overrides"] = overrides
        with JOBS_LOCK:
            job["result"] = result
        return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    try:
        if entity == "college":
            college_name = selected_values.get("CollegeName", "").strip()
            existing_college = None
            if college_name:
                existing_college = find_college_by_name(engine, college_name)
            
            if existing_college:
                existing_college_id = existing_college.get("CollegeID")
                existing_bundle, existing_social = load_university_bundle(engine, existing_college_id)
                entity_data["existing_college_id"] = existing_college_id
                entity_data["existing_values"] = existing_bundle
                entity_data["existing_social"] = existing_social
                
                # Check if user explicitly chose to override or skip
                override_choice = request.form.get("override_choice", "").strip()
                if not override_choice:
                    # Fallback to check entity-specific override choice
                    override_choice = request.form.get(f"override_choice_{entity}", "").strip()
                
                # If no override choice was provided, redirect to show the override UI
                if not override_choice:
                    flash(f"College '{college_name}' already exists (ID: {existing_college_id}). Please review and choose to override or skip.", "info")
                    entity_data["field_defaults"] = selected_tokens
                    entity_data["selection_overrides"] = overrides
                    with JOBS_LOCK:
                        job["result"] = result
                    return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")
                
                if override_choice == "override":
                    college_id = persist_university_bundle(engine, existing_college_id, table_payloads, social_payloads or {})
                    entity_data["ingested_id"] = college_id
                    success_message = f"College ID {college_id} updated with new values."
                    # Store college_id for linking departments/programs
                    result["selected_college_id"] = college_id
                else:  # skip
                    # Skip override - use existing college_id for linking but don't update it
                    college_id = existing_college_id
                    entity_data["ingested_id"] = college_id
                    # Store college_id for linking departments/programs
                    result["selected_college_id"] = college_id
                    success_message = f"Using existing College ID {college_id}. No changes were made."
            else:
                college_id = persist_university_bundle(engine, None, table_payloads, social_payloads or {})
                entity_data["ingested_id"] = college_id
                success_message = f"Values ingested successfully. College ID {college_id} created."
            
            # Store college_id for linking departments/programs
            result["selected_college_id"] = entity_data.get("ingested_id")
        elif entity == "department":
            selected_college_id = result.get("selected_college_id")
            if not selected_college_id:
                flash("Please save a college first before adding departments.", "error")
                entity_data["field_defaults"] = selected_tokens
                entity_data["selection_overrides"] = overrides
                with JOBS_LOCK:
                    job["result"] = result
                return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")
            
            # Inject CollegeID into CollegeDepartment payload
            college_dept_payload = table_payloads.get("CollegeDepartment", {})
            college_dept_payload["CollegeID"] = selected_college_id
            table_payloads["CollegeDepartment"] = college_dept_payload
            
            department_id = persist_department_bundle(engine, None, table_payloads)
            entity_data["ingested_id"] = department_id
            extra_ids = entity_data.setdefault("extra_ids", {})
            college_department_table = fetch_table("CollegeDepartment", required=False)
            if college_department_table is not None:
                with engine.connect() as conn:
                    record = conn.execute(
                        select(college_department_table.c.CollegeDepartmentID).where(
                            college_department_table.c.DepartmentID == department_id,
                            college_department_table.c.CollegeID == selected_college_id
                        )
                    ).mappings().first()
                    if record:
                        extra_ids["CollegeDepartmentID"] = int(record["CollegeDepartmentID"])
            result["selected_college_department_id"] = extra_ids.get("CollegeDepartmentID")
            success_message = f"Department saved with Department ID {department_id}."
        else:
            selected_college_id = result.get("selected_college_id")
            selected_college_dept_id = result.get("selected_college_department_id")
            if not selected_college_id:
                flash("Please save a college first before adding programs.", "error")
                entity_data["field_defaults"] = selected_tokens
                entity_data["selection_overrides"] = overrides
                with JOBS_LOCK:
                    job["result"] = result
                return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")
            
            # Inject CollegeID and CollegeDepartmentID into payloads
            term_payload = table_payloads.get("ProgramTermDetails", {})
            term_payload["CollegeID"] = selected_college_id
            table_payloads["ProgramTermDetails"] = term_payload
            
            if selected_college_dept_id:
                link_payload = table_payloads.get("ProgramDepartmentLink", {})
                link_payload["CollegeDepartmentID"] = selected_college_dept_id
                table_payloads["ProgramDepartmentLink"] = link_payload
            
            program_id = persist_program_bundle(engine, None, table_payloads)
            entity_data["ingested_id"] = program_id
            success_message = f"Program saved with Program ID {program_id}."
    except (SQLAlchemyError, ValueError) as exc:
        flash(f"Database error: {exc}", "error")
        entity_data["field_defaults"] = selected_tokens
        entity_data["selection_overrides"] = overrides
        with JOBS_LOCK:
            job["result"] = result
        return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")

    flash(success_message, "success")
    entity_data["field_defaults"] = selected_tokens
    entity_data["selection_overrides"] = overrides
    with JOBS_LOCK:
        job["result"] = result

    return redirect(url_for("crawler.crawler_index", job_id=job_id) + "#finalize")


def fetch_page(
    url: str,
    headers: Dict[str, str],
    same_domain: bool,
    root_netloc: str,
) -> Dict[str, Any]:
    session = get_http_session()
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            return {
                "result": {
                    "url": url,
                    "status": "non-html",
                    "error": "",
                    "title": "",
                    "heading": "",
                    "text": "",
                    "fields": {},
                    "links": [],
                },
                "child_urls": [],
                "error": False,
            }

        soup = BeautifulSoup(response.text, "html.parser")
        links = collect_links(soup, url, max_links=80)
        content = extract_page_content(soup)
        page_fields = extract_college_fields(content["text"])
        snippet = summarize_text(content["text"], 1600)

        child_urls: List[str] = []
        for link in links:
            href = link.get("url")
            if not href:
                continue
            if is_static_asset(href):
                continue
            if same_domain and urlparse(href).netloc != root_netloc:
                continue
            child_urls.append(href)

        result_payload = {
            "url": url,
            "status": response.status_code,
            "error": "",
            "title": content["title"],
            "heading": content["heading"],
            "text": snippet,
            "fields": page_fields,
            "links": links,
        }

        return {"result": result_payload, "child_urls": child_urls, "error": False}
    except requests.RequestException as exc:
        return {
            "result": {
                "url": url,
                "status": "error",
                "error": str(exc),
                "title": "",
                "heading": "",
                "text": "",
                "fields": {},
                "links": [],
            },
            "child_urls": [],
            "error": True,
        }
    except Exception as exc:
        return {
            "result": {
                "url": url,
                "status": "error",
                "error": str(exc),
                "title": "",
                "heading": "",
                "text": "",
                "fields": {},
                "links": [],
            },
            "child_urls": [],
            "error": True,
        }


SOCIAL_MEDIA_PLATFORMS: Tuple[str, ...] = (
    "Facebook",
    "Instagram",
    "Twitter",
    "Youtube",
    "Tiktok",
    "LinkedIn",
)

PROGRAM_TITLE_RE = re.compile(
    r"\b(?:(?:M\.?S\.?|MSc|Master(?:'s)?|B\.?S\.?|BSc|Bachelor(?:'s)?|Ph\.?D\.?|Doctor(?:ate)?|MBA|MPH|MFA|LLM)\b.*|.*\b(?:in|of)\s+[A-Z][A-Za-z&\-/\s]{2,})"
)

def detect_program_titles_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    texts: List[str] = []
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "a", "li"]):
        text = (tag.get_text(" ", strip=True) or "").strip()
        if text:
            texts.append(text)
    candidates: List[str] = []
    seen = set()
    for text in texts:
        if len(text) < 4 or len(text) > 160:
            continue
        if PROGRAM_TITLE_RE.search(text):
            key = text.lower()
            if key not in seen:
                seen.add(key)
                candidates.append(text)
    return candidates[:250]

@crawler_bp.route("/programs/extract", methods=["POST"])
def programs_extract():
    url = (request.form.get("program_start_url") or "").strip()
    if not url:
        flash("Provide a Programs Page URL.", "error")
        return redirect(url_for("crawler.crawler_index") + "#finalize")
    if not is_valid_url(url):
        flash("Invalid Programs Page URL. Include https://", "error")
        return redirect(url_for("crawler.crawler_index") + "#finalize")

    headers = current_app.config.get("SCRAPER_HEADERS", {}).copy()
    try:
        resp = get_http_session().get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        flash(f"Failed to fetch programs page: {exc}", "error")
        return redirect(url_for("crawler.crawler_index") + "#finalize")

    titles = detect_program_titles_from_html(resp.text)
    engine = current_app.config.get("DB_ENGINE")
    sections, college_opts, college_dept_opts = build_selection_sections(engine)

    # Seed a minimal entity_results with ProgramName options
    field_options: Dict[str, List[Dict[str, Any]]] = {}
    options: List[Dict[str, Any]] = []
    for idx, title in enumerate(titles):
        options.append(
            {
                "token": f"program_detect::{idx}",
                "value": title,
                "source": f"Detected — {url}",
                "origin": "program_detect",
                "page_url": url,
                "snippet": title,
            }
        )
    field_options["ProgramName"] = options

    entity_results = {
        "college": None,
        "department": None,
        "program": {
            "label": ENTITY_CONFIG["program"]["label"],
            "literal_fields": {},
            "heuristic_fields": {},
            "llm_fields": {},
            "merged_fields": {},
            "field_options": field_options,
            "field_defaults": {"ProgramName": options[0]["token"] if options else ""},
            "selection_overrides": {},
            "ingested_id": None,
            "id_label": ENTITY_CONFIG["program"]["id_label"],
            "edit_endpoint": ENTITY_CONFIG["program"]["edit_endpoint"],
            "edit_param": ENTITY_CONFIG["program"]["edit_param"],
            "list_endpoint": ENTITY_CONFIG["program"]["list_endpoint"],
        },
    }

    context = {
        "url": "",
        "max_pages": 30,
        "same_domain": True,
        "per_page_llm": False,
        "results": [],
        "stats": {},
        "entity_results": entity_results,
        "selection_sections": sections,
        "college_options": college_opts,
        "college_department_options": college_dept_opts,
        "entity_order": ENTITY_ORDER,
        "job_id": None,
        "job_complete": True,
        "job_error": "",
        "default_workers": default_worker_count(),
        "selected_college_id": None,
    }
    inject_linking_options(entity_results, college_opts, college_dept_opts)
    flash(f"Detected {len(titles)} program title(s) from the page.", "info")
    return render_template("crawler/crawl.html", **context)

def fetch_table(table_name: str, required: bool = True):
    metadata = current_app.config.get("DB_METADATA")
    table_map = current_app.config.get("DB_TABLE_MAP", {})
    if metadata is None:
        abort(500, "Database is not configured. Check the connection settings.")

    real_name = table_map.get(table_name.lower(), table_name)
    table = metadata.tables.get(real_name)
    if table is None and required:
        abort(500, f"Table '{table_name}' is not available in the connected database.")
    return table


def build_prefixed_fields(table, column_names: List[str]) -> List[Dict[str, Any]]:
    if table is None:
        return []
    base_fields = {field["name"]: field for field in build_fields(table)}
    prefixed_fields: List[Dict[str, Any]] = []
    for column_name in column_names:
        base_field = base_fields.get(column_name)
        if not base_field:
            continue
        field_copy = base_field.copy()
        field_copy["form_name"] = f"{table.name}.{base_field['name']}"
        field_copy["table"] = table
        prefixed_fields.append(field_copy)
    return prefixed_fields


def compose_prefixed_initial_values(
    fields: List[Dict[str, Any]], source: Optional[Dict[str, Dict[str, Any]]]
) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for field in fields:
        table_name = field["table"].name
        column_name = field["name"]
        table_source = source.get(table_name, {}) if source else {}
        values[field["form_name"]] = format_value_for_input(field, table_source.get(column_name))
    return values


def compose_prefixed_request_values(fields: List[Dict[str, Any]], form_data) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for field in fields:
        key = field["form_name"]
        if field.get("is_boolean"):
            values[key] = "on" if form_data.get(key) in ("on", "1", "true", "True") else ""
        else:
            values[key] = form_data.get(key, "")
    return values


def extract_prefixed_values(
    fields: List[Dict[str, Any]], form_data
) -> Dict[str, Dict[str, Any]]:
    values: Dict[str, Dict[str, Any]] = defaultdict(dict)

    for field in fields:
        column = field["column"]
        form_key = field["form_name"]
        raw_value = form_data.get(form_key)

        if field.get("is_boolean"):
            raw_value = "1" if form_data.get(form_key) in ("on", "1", "true", "True") else "0"

        if raw_value is None or raw_value == "":
            if field.get("is_boolean"):
                converted = False
            elif column.nullable:
                converted = None
            else:
                raise ValueError(f"{field['label']} is required.")
        else:
            converted = convert_raw_value(column, raw_value)

        table_name = field["table"].name
        values[table_name][field["name"]] = converted

    return values


def upsert_single_row(conn, table, key_column, key_value, payload: Dict[str, Any]) -> None:
    if table is None or not payload:
        return
    existing = (
        conn.execute(
            select(table.c[key_column.name]).where(key_column == key_value)
        ).first()
        is not None
    )

    payload_with_key = payload.copy()
    payload_with_key[key_column.name] = key_value

    if existing:
        conn.execute(table.update().where(key_column == key_value).values(**payload_with_key))
    else:
        conn.execute(table.insert().values(**payload_with_key))


def resolve_table(table_name: str):
    metadata = current_app.config.get("DB_METADATA")
    table_map = current_app.config.get("DB_TABLE_MAP", {})
    if metadata is None or not table_map:
        abort(500, "Database is not configured. Check the connection settings.")

    real_name = table_map.get(table_name.lower())
    if real_name is None:
        abort(404, f"Table '{table_name}' not found.")

    return metadata.tables[real_name], real_name


def get_engine():
    engine = current_app.config.get("DB_ENGINE")
    if engine is None:
        abort(500, "Database engine is not available.")
    return engine


def get_primary_key_column(table):
    pk_columns = list(table.primary_key.columns)
    if not pk_columns:
        abort(400, f"Table '{table.name}' does not have a primary key.")
    if len(pk_columns) > 1:
        abort(400, "Composite primary keys are not supported in this interface.")
    return pk_columns[0]


def build_fields(table) -> list[Dict[str, Any]]:
    fields = []
    for column in table.columns:
        if column.primary_key and column.autoincrement:
            continue

        col_type = column.type
        is_boolean = isinstance(col_type, sqltypes.Boolean)
        is_datetime = isinstance(col_type, sqltypes.DateTime)
        is_date = isinstance(col_type, sqltypes.Date)
        is_numeric = isinstance(
            col_type,
            (
                sqltypes.Integer,
                sqltypes.SmallInteger,
                sqltypes.BigInteger,
                sqltypes.Numeric,
                sqltypes.Float,
            ),
        )
        is_text = isinstance(col_type, sqltypes.Text)
        length = getattr(col_type, "length", None)
        use_textarea = (
            not is_boolean
            and not is_datetime
            and not is_date
            and (is_text or (length is None) or (length and length > 255))
        )

        input_type = "text"
        if is_boolean:
            input_type = "checkbox"
        elif is_datetime:
            input_type = "datetime-local"
        elif is_date:
            input_type = "date"
        elif is_numeric:
            input_type = "number"

        fields.append(
            {
                "name": column.name,
                "form_name": column.name,
                "table": table,
                "label": prettify_label(column.name),
                "nullable": column.nullable,
                "input_type": input_type,
                "use_textarea": use_textarea,
                "is_boolean": is_boolean,
                "is_datetime": is_datetime,
                "is_date": is_date,
                "placeholder": prettify_label(column.name) if not is_boolean else None,
                "column": column,
            }
        )

    return fields


def prettify_label(name: str) -> str:
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", name.replace("_", " ").replace("-", " "))
    return spaced.strip().title()


def build_form_initial_values(fields: list[Dict[str, Any]], source: Any) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for field in fields:
        name = field["name"]
        if source is None:
            value = ""
        elif isinstance(source, dict):
            value = source.get(name, "")
        else:
            value = getattr(source, name, "")
        values[name] = format_value_for_input(field, value)
    return values


def format_value_for_input(field: Dict[str, Any], value: Any) -> str:
    if value is None:
        return ""

    if field["is_boolean"]:
        return "on" if bool(value) else ""

    if field["input_type"] == "datetime-local":
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%dT%H:%M")
        try:
            parsed = datetime.fromisoformat(str(value))
            return parsed.strftime("%Y-%m-%dT%H:%M")
        except ValueError:
            return ""

    if field["input_type"] == "date":
        if isinstance(value, (date, datetime)):
            return value.strftime("%Y-%m-%d")
        try:
            parsed = date.fromisoformat(str(value))
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            return ""

    return str(value)


def extract_form_values(fields: list[Dict[str, Any]], form_data) -> Dict[str, Any]:
    values: Dict[str, Any] = {}

    for field in fields:
        column = field["column"]
        name = field["name"]

        raw_value = form_data.get(name)
        if field["is_boolean"]:
            raw_value = "1" if form_data.get(name) in ("on", "1", "true", "True") else "0"

        if raw_value is None or raw_value == "":
            if field["is_boolean"]:
                values[name] = False
                continue
            if column.nullable:
                values[name] = None
                continue
            raise ValueError(f"{field['label']} is required.")

        try:
            converted = convert_raw_value(column, raw_value)
        except ValueError as exc:
            raise ValueError(f"{field['label']}: {exc}") from exc

        values[name] = converted

    return values


NUMERIC_SANITIZE_RE = re.compile(r"[^0-9eE\+\-\.]")


def sanitize_numeric_string(value: Any) -> str:
    if not isinstance(value, str):
        return str(value)
    stripped = value.strip()
    if not stripped:
        return stripped
    cleaned = stripped.replace(",", "")
    cleaned = NUMERIC_SANITIZE_RE.sub("", cleaned)
    return cleaned


def convert_raw_value(column, raw_value: Any) -> Any:
    col_type = column.type
    if isinstance(raw_value, str):
        if isinstance(col_type, sqltypes.Text):
            value = raw_value
        else:
            value = raw_value.strip()
    else:
        value = raw_value

    if isinstance(col_type, (sqltypes.Integer, sqltypes.SmallInteger, sqltypes.BigInteger)):
        value = sanitize_numeric_string(value)
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ValueError("Enter a whole number.")

    if isinstance(col_type, sqltypes.Numeric):
        value = sanitize_numeric_string(value)
        try:
            return Decimal(value)
        except (InvalidOperation, ValueError):
            raise ValueError("Enter a numeric value.")

    if isinstance(col_type, sqltypes.Float):
        value = sanitize_numeric_string(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValueError("Enter a numeric value.")

    if isinstance(col_type, sqltypes.Boolean):
        return str(value).lower() in {"1", "true", "on", "yes"}

    if isinstance(col_type, sqltypes.DateTime):
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            raise ValueError("Use YYYY-MM-DDTHH:MM format.")

    if isinstance(col_type, sqltypes.Date):
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            raise ValueError("Use YYYY-MM-DD format.")

    return value


if __name__ == "__main__":
    application = create_app()
    application.run(debug=True, host="0.0.0.0", port=5000)

