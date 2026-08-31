from pathlib import Path
from types import SimpleNamespace

from localstack_snapshot.pytest.snapshot import package_scoped_nodeid


def _create_package(root: Path) -> Path:
    package = root / "my-package"
    (package / "tests" / "services").mkdir(parents=True)
    (package / "pyproject.toml").touch()
    return package


def test_nodeid_from_a_package_rooted_run_is_unchanged(tmp_path):
    package = _create_package(tmp_path)
    item = SimpleNamespace(
        path=package / "tests" / "services" / "test_thing.py",
        nodeid="tests/services/test_thing.py::TestThing::test_case[param]",
    )

    assert (
        package_scoped_nodeid(item) == "tests/services/test_thing.py::TestThing::test_case[param]"
    )


def test_nodeid_from_a_repo_rooted_run_is_anchored_to_the_package(tmp_path):
    package = _create_package(tmp_path)
    item = SimpleNamespace(
        path=package / "tests" / "services" / "test_thing.py",
        nodeid="my-package/tests/services/test_thing.py::test_case",
    )

    assert package_scoped_nodeid(item) == "tests/services/test_thing.py::test_case"


def test_nearest_pyproject_wins_over_the_workspace_root(tmp_path):
    (tmp_path / "pyproject.toml").touch()
    package = _create_package(tmp_path)
    item = SimpleNamespace(
        path=package / "tests" / "services" / "test_thing.py",
        nodeid="my-package/tests/services/test_thing.py::test_case",
    )

    assert package_scoped_nodeid(item) == "tests/services/test_thing.py::test_case"


def test_nodeid_without_an_enclosing_package_is_unchanged(tmp_path):
    item = SimpleNamespace(
        path=tmp_path / "tests" / "test_thing.py",
        nodeid="tests/test_thing.py::test_case",
    )

    assert package_scoped_nodeid(item) == "tests/test_thing.py::test_case"
