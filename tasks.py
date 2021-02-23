import shlex
from pathlib import Path
from platform import python_version

import invoke  # http://www.pyinvoke.org/

PROJECT = "PY"
PACKAGE = "nba_crawler"
REQUIRED_COVERAGE = 100
CONDA_OUTPUT = "build/conda"
DOCS_OUTPUT = "build/docs"


def current_version(ctx):
    return ctx.run("python setup.py --version", hide=True).stdout.split("\n")[-2]


@invoke.task(help={"python": "Set the python version (default: current version)"})
def bootstrap(ctx, python=python_version()):
    """Install required conda packages."""

    def ensure_packages(*packages):
        clean_packages = sorted({shlex.quote(package) for package in sorted(packages)})
        ctx.run("conda install --quiet --yes -c conda-forge " + " ".join(clean_packages), echo=True)

    try:
        import jinja2
        import yaml
    except ModuleNotFoundError:
        ensure_packages("jinja2", "pyyaml")
        import jinja2
        import yaml

    with open("meta.yaml") as file:
        template = jinja2.Template(file.read())

    meta_yaml = yaml.safe_load(template.render(load_setup_py_data=lambda: {}, python=python))
    develop_packages = meta_yaml["requirements"]["develop"]
    build_packages = meta_yaml["requirements"]["build"]
    run_packages = meta_yaml["requirements"]["run"]

    ensure_packages(*develop_packages, *build_packages, *run_packages)


@invoke.task(help={"all": f"Remove {PACKAGE}.egg-info directory too", "n": "Dry-run mode"})
def clean(ctx, all_=False, n=False):
    """Clean unused files."""
    args = ["-d", "-x", "-e .idea", "-e .vscode"]
    if not all_:
        args.append(f"-e {PACKAGE}.egg-info")
    args.append("-n" if n else "-f")
    ctx.run("git clean " + " ".join(args), echo=True)


@invoke.task(
    incrementable=["verbose"],
    help={
        "behavioral": "Run behavioral tests too (default: False)",
        "performance": "Run performance tests too (default: False)",
        "external": "Run external tests too (default: False)",
        "x": "Exit instantly on first error or failed test (default: False)",
        "s": "Disable capture of logging/console output (default: False)",
        "junit-xml": "Create junit-xml style report (default: False)",
        "failed-first": "run all tests but run the last failures first (default: False)",
        "quiet": "Decrease verbosity",
        "verbose": "Increase verbosity (can be repeated)",
    },
)
def test(
    ctx,
    behavioral=False,
    performance=False,
    external=False,
    x=False,
    s=False,
    junit_xml=False,
    failed_first=False,
    quiet=False,
    verbose=0,
):
    """Run tests."""
    markers = []
    if not behavioral:
        markers.append("not behavioral")
    if not performance:
        markers.append("not performance")
    if not external:
        markers.append("not external")
    args = []
    if markers:
        args.append("-m '" + " and ".join(markers) + "'")
    if not behavioral and not performance and not external:
        args.append(f"--cov={PACKAGE}")
        args.append(f"--cov-fail-under={REQUIRED_COVERAGE}")
    if x:
        args.append("-x")
    if s:
        args.append("-s")
    if junit_xml:
        args.append("--junit-xml=junit.xml")
    if failed_first:
        args.append("--failed-first")
    if quiet:
        verbose -= 1
    if verbose < 0:
        args.append("--quiet")
    if verbose > 0:
        args.append("-" + ("v" * verbose))
    ctx.run("pytest tests " + " ".join(args), pty=True, echo=True)


@invoke.task(help={"style": "Check style with flake8, isort, and black", "typing": "Check typing with mypy"})
def check(ctx, style=True, typing=True):
    """Check for style and static typing errors."""
    paths = ["setup.py", "tasks.py", PACKAGE]
    if Path("tests").is_dir():
        paths.append("tests")
    if style:
        ctx.run("flake8 " + " ".join(paths), echo=True)
        ctx.run("isort --diff --check-only " + " ".join(paths), echo=True)
        ctx.run("black --diff --check " + " ".join(paths), echo=True)
    if typing:
        ctx.run(f"mypy --no-incremental --cache-dir=/dev/null {PACKAGE}", echo=True)


@invoke.task(name="format", aliases=["fmt"])
def format_(ctx):
    """Format code to use standard style guidelines."""
    paths = ["setup.py", "tasks.py", PACKAGE]
    if Path("tests").is_dir():
        paths.append("tests")
    autoflake = "autoflake -i --recursive --remove-all-unused-imports --remove-duplicate-keys --remove-unused-variables"
    ctx.run(f"{autoflake} " + " ".join(paths), echo=True)
    ctx.run("isort " + " ".join(paths), echo=True)
    ctx.run("black " + " ".join(paths), echo=True)


@invoke.task
def install(ctx):
    """Install the package."""
    ctx.run("python -m pip install .", echo=True)


@invoke.task
def develop(ctx):
    """Install the package in editable mode."""
    ctx.run("python -m pip install --no-use-pep517 --editable .", echo=True)


@invoke.task(aliases=["undevelop"])
def uninstall(ctx):
    """Uninstall the package."""
    ctx.run(f"python -m pip uninstall --yes {PACKAGE}", echo=True)


@invoke.task(
    help={"python": "Set the python version (default: current version)", "convert": "Convert package to windows too"}
)
def build_conda(ctx, python=python_version(), convert=True):
    """Build conda package(s)."""
    ctx.run(f"rm -rf {CONDA_OUTPUT}")
    ctx.run(f"mkdir -p {CONDA_OUTPUT}")
    ctx.run(
        "conda build --quiet "
        "--no-include-recipe "
        "--no-test "
        f"--output-folder {CONDA_OUTPUT} "
        f"--python {python} "
        ".",
        pty=True,
        echo=True,
    )
    ctx.run(f"chmod 777 {CONDA_OUTPUT}/linux-64/*.tar.bz2", echo=True)
    if convert:
        ctx.run(f"conda convert -p win-64 --output-dir {CONDA_OUTPUT} {CONDA_OUTPUT}/linux-64/*.tar.bz2", echo=True)
        ctx.run(f"chmod 777 {CONDA_OUTPUT}/win-64/*.tar.bz2", echo=True)


@invoke.task(help={"linux": "Verify Linux package", "windows": "Verify Windows package"})
def verify_conda(ctx, linux=True, windows=True):
    """Verify built conda package(s)."""
    version = current_version(ctx)

    def conda_verify(platform):
        assert platform in ("linux-64", "win-64")
        ctx.run(f"tar -jtvf {CONDA_OUTPUT}/{platform}/{PACKAGE}-{version}-*.tar.bz2 | sort -k 6", echo=True)
        ctx.run(f"conda verify {CONDA_OUTPUT}/{platform}/{PACKAGE}-{version}-*.tar.bz2", echo=True)

    if linux:
        conda_verify("linux-64")
    if windows:
        conda_verify("win-64")
