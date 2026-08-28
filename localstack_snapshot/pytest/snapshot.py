import json
import os
from pathlib import Path
from typing import Optional

import pytest
from _pytest.config import Config, PytestPluginManager
from _pytest.config.argparsing import Parser
from _pytest.fixtures import SubRequest
from _pytest.nodes import Item
from _pytest.reports import TestReport
from _pytest.runner import CallInfo
from pluggy import Result

from localstack_snapshot.snapshots import SnapshotAssertionError, SnapshotSession
from localstack_snapshot.snapshots.report import render_report


# TODO: move?
def is_aws():
    return os.environ.get("TEST_TARGET", "") == "AWS_CLOUD"


@pytest.hookimpl
def pytest_configure(config: Config):
    config.addinivalue_line("markers", "skip_snapshot_verify")


@pytest.hookimpl
def pytest_addoption(parser: Parser, pluginmanager: PytestPluginManager):
    parser.addoption("--snapshot-update", action="store_true")
    parser.addoption("--snapshot-raw", action="store_true")
    parser.addoption("--snapshot-skip-all", action="store_true")
    parser.addoption("--snapshot-verify", action="store_true")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: Item, call: CallInfo[None]) -> Optional[TestReport]:
    use_legacy_report = os.environ.get("SNAPSHOT_LEGACY_REPORT", "0") == "1"

    result: Result = yield
    report: TestReport = result.get_result()

    if call.excinfo is not None and isinstance(call.excinfo.value, SnapshotAssertionError):
        err: SnapshotAssertionError = call.excinfo.value

        if use_legacy_report:
            error_report = ""
            for res in err.result:
                if not res:
                    error_report = f"{error_report}Match failed for '{res.key}':\n{json.dumps(json.loads(res.result.to_json()), indent=2)}\n\n"
            report.longrepr = error_report
        else:
            report.longrepr = "\n".join([str(render_report(r)) for r in err.result if not r])
    return report


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item: Item) -> None:
    call: CallInfo = yield  # noqa

    if call.excinfo:
        return

    # TODO: extremely dirty... maybe it would be better to find a way to fail the test itself instead?
    sm = item.funcargs.get("snapshot")

    if sm:
        verify = True
        paths = []

        if not is_aws():  # only skip for local tests
            for m in item.iter_markers(name="skip_snapshot_verify"):
                skip_paths = m.kwargs.get("paths", [])

                skip_condition = m.kwargs.get("condition")
                # can optionally include a condition, when this will be skipped
                # a condition must be a Callable returning something truthy/falsey
                if skip_condition:
                    if not callable(skip_condition):
                        raise ValueError("condition must be a callable")

                    # special case where one of the marks has a skip condition but no paths
                    # since we interpret a missing paths key as "all paths",
                    # this should skip all paths, no matter what the other marks say
                    if skip_condition() and not skip_paths:
                        verify = False
                        paths.clear()  # in case some other marker already added paths
                        break

                    if not skip_condition():
                        continue  # don't skip

                # we skip verification if no condition has been specified
                verify = False
                paths.extend(skip_paths)

        sm._assert_all(verify, paths)


def package_scoped_nodeid(item: Item) -> str:
    """The item's nodeid relative to its enclosing package, independent of pytest's rootdir.

    Snapshot entries are keyed by the pytest nodeid, which is relative to pytest's rootdir.
    The committed ``*.snapshot.json`` / ``*.validation.json`` files assume the rootdir is the
    package containing the test (``tests/...::<test>``), but pytest can be invoked with a
    different rootdir — VS Code's test runner, for example, pins ``--rootdir=<workspace folder>``,
    which prefixes every nodeid with the package directory and makes every lookup of a recorded
    entry miss. Keying by the nodeid relative to the nearest enclosing ``pyproject.toml`` keeps
    the entries stable no matter where pytest was started from.
    """
    path = Path(item.path)
    for parent in path.parents:
        if (parent / "pyproject.toml").is_file():
            _, separator, remainder = item.nodeid.partition("::")
            return path.relative_to(parent).as_posix() + separator + remainder
    return item.nodeid


@pytest.fixture(scope="function")
def _snapshot_session(request: SubRequest):
    update_overwrite = os.environ.get("SNAPSHOT_UPDATE") == "1"
    raw_overwrite = os.environ.get("SNAPSHOT_RAW") == "1"

    sm = SnapshotSession(
        base_file_path=os.path.join(request.fspath.dirname, request.fspath.purebasename),
        scope_key=package_scoped_nodeid(request.node),
        update=update_overwrite or request.config.option.snapshot_update,
        raw=raw_overwrite or request.config.option.snapshot_raw,
        verify=False if request.config.option.snapshot_skip_all else True,
    )

    yield sm

    sm._persist_state()


@pytest.fixture(scope="function")
def snapshot(_snapshot_session):
    return _snapshot_session
