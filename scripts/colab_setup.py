"""Colab setup cells shared by the notebook builders: fetch the project from GitHub, install the
Python packages and PostgreSQL 16 inside the Colab session. Outside Colab they do nothing."""
import nbformat as nbf

REPO = "hhej/aisleco-etl-pipeline"


ETL_TIMING = ("A full Run all takes about 7 minutes on a standard Colab machine: about 2 for the setup, about 3\n"
              "for the automated tests in section 14, and the rest for the demo itself.")


def setup_cells(timing=ETL_TIMING):
    return [
        nbf.v4.new_markdown_cell(f"""### Running this notebook in Google Colab

Choose **Runtime → Run all**. The next two cells fetch the project from GitHub
([`{REPO}`](https://github.com/{REPO})) and install PostgreSQL 16 and the Python driver inside this Colab
session. {timing} Nothing is installed on your own
computer. To run it again from a clean state, use Runtime → Restart session and
run all."""),
        nbf.v4.new_code_cell(f'''# 1. Fetch the project and the Python packages
import os, subprocess, sys
IN_COLAB = "google.colab" in sys.modules
HOME = "/content/aisleco-etl-pipeline"

def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"command failed: {{cmd}}\\n{{r.stdout[-1500:]}}\\n{{r.stderr[-1500:]}}")
    return r.stdout

if IN_COLAB:
    if not os.path.exists(HOME):
        sh("git clone -q https://github.com/{REPO}.git " + HOME)
    os.chdir(HOME)
    sh('pip -q install "psycopg[binary]==3.2.3" "pytest==8.3.4"')
    print("project:", HOME, "| commit", sh("git rev-parse --short HEAD").strip())
else:
    print("local run: using the project folder", os.getcwd())'''),
        nbf.v4.new_code_cell('''# 2. PostgreSQL 16 (the pipeline uses MERGE, which needs version 15 or later)
if IN_COLAB:
    if subprocess.run("pg_lsclusters -h 2>/dev/null | grep -q '^16 '", shell=True).returncode:
        print("installing PostgreSQL 16 from apt.postgresql.org ...")
        sh("apt-get -qq update && apt-get -qq install -y curl ca-certificates > /dev/null")
        sh("install -d /usr/share/postgresql-common/pgdg && curl -fsS -o "
           "/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc")
        sh('. /etc/os-release && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] '
           'https://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main" > /etc/apt/sources.list.d/pgdg.list')
        sh("apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y postgresql-16 > /dev/null")
    sh("pg_ctlcluster 16 main status > /dev/null || pg_ctlcluster 16 main start")
    os.environ["PGPORT"] = sh("pg_lsclusters -h | awk '$1 == 16 {print $3}'").strip()     # used by every connection
    r = subprocess.run(f"su postgres -c \\"psql -p {os.environ['PGPORT']} -c 'CREATE ROLE root SUPERUSER LOGIN'\\"",
                       shell=True, capture_output=True, text=True)
    if r.returncode and "already exists" not in r.stderr:
        raise RuntimeError(r.stderr)
    import psycopg
    with psycopg.connect("dbname=postgres") as c:
        print(c.execute("SELECT version()").fetchone()[0])
else:
    print("local run: using the existing PostgreSQL")'''),
    ]
