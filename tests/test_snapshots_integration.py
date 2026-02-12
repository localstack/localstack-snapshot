import os
import tempfile

import pytest

from localstack_snapshot.snapshots import SnapshotSession

pytest_plugins = [
    "localstack_snapshot.pytest.snapshot",
]


@pytest.fixture
def snapshot():
    with tempfile.TemporaryDirectory() as temp_dir:
        session = SnapshotSession(
            scope_key="test",
            verify=True,
            base_file_path=os.path.join(temp_dir, "test"),
            update=False,
        )
        yield session


class TestSnapshotIntegration:
    @pytest.mark.skip_snapshot_verify(paths=["$..id"])
    def test_skip_id_field_passes(self, snapshot):
        snapshot.recorded_state = {"user": {"name": "John", "id": "old"}}
        snapshot.match("user", {"name": "John", "id": "new"})

    # HACK(gregfurman): xfail(strict=True) means we expect the test to fail -- where the underlying test failing
    # results in an expected XFAIL, skipping the test. Otherwise, a PASS should trigger a true FAIL.
    @pytest.mark.xfail(strict=True, reason="Should fail because name differs, only ID is skipped")
    @pytest.mark.skip_snapshot_verify(paths=["$..id"])
    def test_skip_id_but_name_differs_fails(self, snapshot):
        snapshot.recorded_state = {"user": {"name": "John", "id": "old"}}
        snapshot.match("user", {"name": "Jane", "id": "new"})

    @pytest.mark.xfail(strict=True, reason="Should fail because name differs, only ID is skipped")
    @pytest.mark.skip_snapshot_verify(["$..id"])
    def test_skip_id_field_passes_args(self, snapshot):
        snapshot.recorded_state = {"user": {"name": "John", "id": "old"}}
        snapshot.match("user", {"name": "Jane", "id": "new"})

    @pytest.mark.xfail(strict=True, reason="Should fail because no fields are skipped")
    def test_no_skip_marker_fails(self, snapshot):
        snapshot.recorded_state = {"user": {"name": "John", "id": "old"}}
        snapshot.match("user", {"name": "John", "id": "new"})

    @pytest.mark.skip_snapshot_verify(paths=["$..id", "$..timestamp"])
    def test_skip_multiple_fields_passes(self, snapshot):
        snapshot.recorded_state = {"event": {"type": "login", "id": "123", "timestamp": "old"}}
        snapshot.match("event", {"type": "login", "id": "456", "timestamp": "new"})

    @pytest.mark.skip_snapshot_verify(condition=lambda: True)
    def test_condition_true_skips_all_verification(self, snapshot):
        snapshot.recorded_state = {"data": "old"}
        snapshot.match("data", "completely_different")

    @pytest.mark.skip_snapshot_verify(condition=lambda: False, paths=["$..id"])
    def test_condition_false_ignores_paths(self, snapshot):
        snapshot.recorded_state = {"user": {"name": "John", "id": "123"}}
        snapshot.match("user", {"name": "John", "id": "123"})

    @pytest.mark.skip_snapshot_verify(["$..id"], lambda: True)
    def test_condition_with_args_skips_all(self, snapshot):
        snapshot.recorded_state = {"data": {"id": "old"}}
        snapshot.match("data", {"id": "new"})

    @pytest.mark.xfail(strict=True, reason="Should fail because condition is False")
    @pytest.mark.skip_snapshot_verify(["$..id"], lambda: False)
    def test_condition_false_with_args_fails(self, snapshot):
        snapshot.recorded_state = {"user": {"name": "John", "id": "old"}}
        snapshot.match("user", {"name": "John", "id": "new"})
