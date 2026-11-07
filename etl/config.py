"""Settings (from environment variables, with defaults for a local PostgreSQL) and the
list of source tables the pipeline extracts."""
import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"


@dataclass(frozen=True)
class Config:
    crm_dsn: str = field(default_factory=lambda: os.getenv("ETL_CRM_DSN", "dbname=aisleco_crm"))
    dw_dsn: str = field(default_factory=lambda: os.getenv("ETL_DW_DSN", "dbname=aisleco_dw"))
    admin_dsn: str = field(default_factory=lambda: os.getenv("ETL_ADMIN_DSN", "dbname=postgres"))
    landing_dir: Path = field(default_factory=lambda: Path(os.getenv("ETL_LANDING_DIR", ROOT / "data" / "landing")))
    # Re-read this many seconds before the watermark, so a row whose transaction committed
    # late (after a newer row was already extracted) is not missed. Upserts make it harmless.
    overlap_seconds: int = field(default_factory=lambda: int(os.getenv("ETL_OVERLAP_SECONDS", "300")))
    connect_retries: int = field(default_factory=lambda: int(os.getenv("ETL_CONNECT_RETRIES", "4")))
    retry_base_seconds: float = field(default_factory=lambda: float(os.getenv("ETL_RETRY_BASE_SECONDS", "1")))
    extract_workers: int = field(default_factory=lambda: int(os.getenv("ETL_EXTRACT_WORKERS", "4")))
    # The data-quality gate stops the run when more than this share of one table's
    # extracted rows is rejected (and the table delivered at least min_rows_for_rate rows).
    max_reject_rate: float = field(default_factory=lambda: float(os.getenv("ETL_MAX_REJECT_RATE", "0.05")))
    min_rows_for_rate: int = field(default_factory=lambda: int(os.getenv("ETL_MIN_ROWS_FOR_RATE", "20")))
    # Batch cap for incremental runs: at most this many changed transaction rows (Inquiry,
    # ChatMessage, Event, assignments) per run, oldest first, so a run stays inside the
    # 2-minute latency target. The rest waits in the CRM behind the watermark (the backlog
    # queue) and the next run continues. 0 = no cap. Full refreshes are never capped.
    # 100,000 = the cap tested in the stress test (docs/stress_test.txt): about 7 s per run.
    max_rows_per_run: int = field(default_factory=lambda: int(os.getenv("ETL_MAX_ROWS_PER_RUN", "100000")))
    # Test hook: name of a step that raises just before it commits (simulates a crash).
    fail_at: str = field(default_factory=lambda: os.getenv("ETL_FAIL_AT", ""))


@dataclass(frozen=True)
class Source:
    table: str          # same name in the CRM, in stg and in land
    schema: str         # CRM schema
    key: str            # primary key column
    columns: tuple      # columns extracted (personal data and unused columns are never read)
    parents: tuple = () # (column, parent table) pairs, for merge / delete order


# Parents before children. Customer, CustomerContact and CustomerChannelAccount are not
# extracted at all, and CustomerID is not read from Inquiry or Event (Thai PDPA).
SOURCES = (
    Source("MarketingChannel", "master", "ChannelID", ("ChannelID", "ChannelName", "ChannelType", "UpdatedAt")),
    Source("Package", "master", "PackageID",
           ("PackageID", "PackageName", "PackageType", "ServiceFeeType", "EffectiveTo", "UpdatedAt")),
    Source("EmployeeRole", "master", "RoleID", ("RoleID", "RoleName", "UpdatedAt")),
    Source("Employee", "master", "EmployeeID",
           ("EmployeeID", "RoleID", "FirstName", "LastName", "EmploymentType", "HireDate", "StandardWeeklyHours",
            "MaxConcurrentEvents", "Status", "UpdatedAt"), (("RoleID", "EmployeeRole"),)),
    Source("AuspiciousDate", "master", "AuspiciousDateID",
           ("AuspiciousDateID", "CalendarDate", "DateType", "AuspiciousLevel", "UpdatedAt")),
    Source("Inquiry", "transaction", "InquiryID",
           ("InquiryID", "ChannelID", "IndicativePackageID", "InquiryDate", "RequestedEventDate", "BudgetRange",
            "ResponseTimeMinutes", "ContactedDate", "ConsultationDate", "QuotedDate", "QuotedAmount", "ClosedDate",
            "ClosedBy", "Status", "LostReason", "UpdatedAt"),
           (("ChannelID", "MarketingChannel"), ("IndicativePackageID", "Package"))),
    Source("ChatMessage", "transaction", "ChatMessageID",
           ("ChatMessageID", "InquiryID", "SentAt", "Direction", "SenderType", "EmployeeID", "UpdatedAt"),
           (("InquiryID", "Inquiry"), ("EmployeeID", "Employee"))),
    Source("Event", "transaction", "EventID",
           ("EventID", "InquiryID", "PackageID", "EventDate", "GuestCount", "ComplexityLevel", "AgreedPackagePrice",
            "Status", "CreatedDate", "UpdatedAt"),
           (("InquiryID", "Inquiry"), ("PackageID", "Package"))),
    Source("EventEmployeeAssignment", "transaction", "EventEmployeeAssignmentID",
           ("EventEmployeeAssignmentID", "EventID", "EmployeeID", "AssignmentRole", "AssignedDate", "WorkloadHours",
            "Status", "UpdatedAt"),
           (("EventID", "Event"), ("EmployeeID", "Employee"))),
)
SOURCE_BY_TABLE = {s.table: s for s in SOURCES}
