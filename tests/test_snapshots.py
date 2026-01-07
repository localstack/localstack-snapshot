import io
from enum import Enum

import pytest

from localstack_snapshot.snapshots import SnapshotSession
from localstack_snapshot.snapshots.report import _format_json_path
from localstack_snapshot.snapshots.transformer import KeyValueBasedTransformer, SortingTransformer


class TestSnapshotManager:
    def test_simple_diff_nochange(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"a": 3}}
        sm.match("key_a", {"a": 3})
        sm._assert_all()

    def test_simple_diff_change(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"a": 3}}
        sm.match("key_a", {"a": 5})
        with pytest.raises(Exception) as ctx:
            sm._assert_all()
        ctx.match("Parity snapshot failed")

    def test_diff_with_io_stream(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"a": "data"}}
        sm.match("key_a", {"a": io.BytesIO(b"data")})
        sm._assert_all()

    def test_multiple_assertmatch_with_same_key_fail(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"a": 3}}
        sm.match("key_a", {"a": 3})
        with pytest.raises(Exception) as ctx:
            sm.match("key_a", {"a": 3})
        ctx.match("used multiple times in the same test scope")

    def test_context_replacement(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.add_transformer(
            KeyValueBasedTransformer(lambda k, v: v if k == "aaa" else None, replacement="A")
        )
        sm.recorded_state = {"key_a": {"aaa": "<A:1>", "bbb": "<A:1> hello"}}
        sm.match("key_a", {"aaa": "something", "bbb": "something hello"})
        sm._assert_all()

    def test_match_object_nochange(self):
        class CustomObject:
            def __init__(self, name, nested=False):
                self.name = name
                if nested:
                    self.nested = CustomObject(f"nested{name}")
                    self.listed = [CustomObject(f"listed{name}"), "otherobj"]

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {
            "key_a": {
                "name": "myname",
                "nested": {"name": "nestedmyname"},
                "listed": [{"name": "listedmyname"}, "otherobj"],
            }
        }
        sm.match_object("key_a", CustomObject(name="myname", nested=True))
        sm._assert_all()

    def test_match_object_ignore_private_values(self):
        class CustomObject:
            def __init__(self, name):
                self.name = name
                self._internal = "n/a"

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"name": "myname"}}
        sm.match_object("key_a", CustomObject(name="myname"))
        sm._assert_all()

    def test_match_object_lists_and_iterators(self):
        class CustomObject:
            def __init__(self, name):
                self.name = name
                self.my_list = [9, 8, 7, 6, 5]
                self.my_iterator = (x for x in range(5))

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {
            "key_a": {"name": "myname", "my_iterator": [0, 1, 2, 3, 4], "my_list": [9, 8, 7, 6, 5]}
        }
        sm.match_object("key_a", CustomObject(name="myname"))
        sm._assert_all()

    def test_match_object_include_properties(self):
        class CustomObject:
            def __init__(self, name):
                self.name = name
                self._internal = "n/a"

            def some_method(self):
                # method should not be serialized
                return False

            @property
            def some_prop(self):
                # properties should be serialized
                return True

            @property
            def some_iterator(self):
                for i in range(3):
                    yield i

            @property
            def _private_prop(self):
                # private properties should be ignored
                return False

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {
            "key_a": {"name": "myname", "some_prop": True, "some_iterator": [0, 1, 2]}
        }
        sm.match_object("key_a", CustomObject(name="myname"))
        sm._assert_all()

    def test_match_object_enums(self):
        class TestEnum(Enum):
            value1 = "Value 1"
            value2 = "Value 2"

        class CustomObject:
            def __init__(self, name):
                self.name = name
                self.my_enum = TestEnum.value2

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"name": "myname", "my_enum": "Value 2"}}
        sm.match_object("key_a", CustomObject(name="myname"))
        sm._assert_all()

    def test_match_object_with_identity_function(self):
        class CustomObject:
            def __init__(self, name):
                self.name = name

            @property
            def me_myself_and_i(self):
                # This would lead to a RecursionError, so we cannot snapshot this method
                return self

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"name": "myname"}}
        sm.match_object("key_a", CustomObject(name="myname"))
        sm._assert_all()

    def test_match_object_change(self):
        class CustomObject:
            def __init__(self, name):
                self.name = name

        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"name": "myname"}}
        sm.match_object("key_a", CustomObject(name="diffname"))
        with pytest.raises(Exception) as ctx:
            sm._assert_all()
        ctx.match("Parity snapshot failed")

    # def test_context_replacement_no_change(self):
    #     sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
    #     sm.add_transformer(TransformerUtility.key_value("name"))
    #     sm.recorded_state = {"key_a": {"name": ""}}
    #     sm.match("key_a", {"name": ""})
    #     sm._assert_all()

    # def test_match_order_reference_replacement(self):
    #     """tests if the reference-replacement works as expected, e.g., using alphabetical order of keys"""
    #     sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
    #
    #     sm.add_transformer(KeyValueBasedTransformer(_resource_name_transformer, "resource"))
    #
    #     sm.recorded_state = {
    #         "subscription-attributes": {
    #             "Attributes": {
    #                 "ConfirmationWasAuthenticated": "true",
    #                 "Endpoint": "arn:aws:lambda:region:111111111111:function:<resource:1>",
    #                 "Owner": "111111111111",
    #                 "PendingConfirmation": "false",
    #                 "Protocol": "lambda",
    #                 "RawMessageDelivery": "false",
    #                 "RedrivePolicy": {
    #                     "deadLetterTargetArn": "arn:aws:sqs:region:111111111111:<resource:2>"
    #                 },
    #                 "SubscriptionArn": "arn:aws:sns:region:111111111111:<resource:4>:<resource:3>",
    #                 "TopicArn": "arn:aws:sns:region:111111111111:<resource:4>",
    #             },
    #             "ResponseMetadata": {"HTTPHeaders": {}, "HTTPStatusCode": 200},
    #         }
    #     }
    #     sm.match(
    #         "subscription-attributes",
    #         {
    #             "Attributes": {
    #                 "ConfirmationWasAuthenticated": "true",
    #                 "Owner": "111111111111",
    #                 "PendingConfirmation": "false",
    #                 "Protocol": "lambda",
    #                 "RawMessageDelivery": "false",
    #                 "RedrivePolicy": {
    #                     "deadLetterTargetArn": "arn:aws:sqs:region:111111111111:111112222233333"
    #                 },
    #                 "TopicArn": "arn:aws:sns:region:111111111111:rrrrrrrrrrrrrrrrr",
    #                 "SubscriptionArn": "arn:aws:sns:region:111111111111:rrrrrrrrrrrrrrrrr:azazazazazazazaza",
    #                 "Endpoint": "arn:aws:lambda:region:111111111111:function:aaaaabbbbb",
    #             },
    #             "ResponseMetadata": {"HTTPHeaders": {}, "HTTPStatusCode": 200},
    #         },
    #     )
    #     sm._assert_all()

    # def test_reference_replacement_skip_outer_keys(self):
    #     """Test if the reference replacement properly skips the snapshot keys on the outermost level"""
    #     sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
    #     sm.add_transformer(TransformerUtility.key_value("name"))
    #     sm.recorded_state = {"key_a": {"name": "<name:1>"}}
    #     sm.match("key_a", {"name": "key"})
    #     sm._assert_all()

    def test_replacement_key_value(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.add_transformer(
            KeyValueBasedTransformer(
                # returns last two characters of value -> only this should be replaced
                lambda k, v: v[-2:] if k == "aaa" else None,
                replacement="A",
                replace_reference=False,
            )
        )
        sm.recorded_state = {
            "key_a": {"aaa": "hellA", "aab": "this is a test", "b": {"aaa": "another teA"}}
        }
        sm.match("key_a", {"aaa": "helloo", "aab": "this is a test", "b": {"aaa": "another test"}})
        sm._assert_all()

    def test_dot_in_skip_verification_path(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {
            "key_a": {"aaa": "hello", "aab": "this is a test", "b": {"a.aa": "another test"}}
        }
        sm.match(
            "key_a",
            {"aaa": "hello", "aab": "this is a test-fail", "b": {"a.aa": "another test-fail"}},
        )

        with pytest.raises(Exception) as ctx:  # asserts it fail without skipping
            sm._assert_all()
        ctx.match("Parity snapshot failed")

        skip_path = ["$..aab", "$..b.a.aa"]
        with pytest.raises(Exception) as ctx:  # asserts it fails if fields are not escaped
            sm._assert_all(skip_verification_paths=skip_path)
        ctx.match("Parity snapshot failed")

        skip_path_escaped = ["$..aab", "$..b.'a.aa'"]
        sm._assert_all(skip_verification_paths=skip_path_escaped)

    def test_non_homogeneous_list(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key1": [{"key2": "value1"}, "value2", 3]}
        sm.match("key1", [{"key2": "value1"}, "value2", 3])
        sm._assert_all()

    def test_list_as_last_node_in_skip_verification_path(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"aaa": ["item1", "item2", "item3"]}}
        sm.match(
            "key_a",
            {"aaa": ["item1", "different-value"]},
        )

        with pytest.raises(Exception) as ctx:  # asserts it fail without skipping
            sm._assert_all()
        ctx.match("Parity snapshot failed")

        skip_path = ["$..aaa[1]", "$..aaa[2]"]
        sm._assert_all(skip_verification_paths=skip_path)

        skip_path = ["$..aaa.1", "$..aaa.2"]
        sm._assert_all(skip_verification_paths=skip_path)

    def test_list_as_last_node_in_skip_verification_path_complex(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {
            "key_a": {
                "aaa": [
                    {"aab": ["aac", "aad"]},
                    {"aab": ["aac", "aad"]},
                    {"aab": ["aac", "aad"]},
                ]
            }
        }
        sm.match(
            "key_a",
            {
                "aaa": [
                    {"aab": ["aac", "bad-value"], "bbb": "value"},
                    {"aab": ["aac", "aad", "bad-value"]},
                    {"aab": ["bad-value", "aad"]},
                ]
            },
        )

        with pytest.raises(Exception) as ctx:  # asserts it fail without skipping
            sm._assert_all()
        ctx.match("Parity snapshot failed")

        skip_path = [
            "$..aaa[0].aab[1]",
            "$..aaa[0].bbb",
            "$..aaa[1].aab[2]",
            "$..aaa[2].aab[0]",
        ]
        sm._assert_all(skip_verification_paths=skip_path)

        skip_path = [
            "$..aaa.0..aab.1",
            "$..aaa.0..bbb",
            "$..aaa.1..aab.2",
            "$..aaa.2..aab.0",
        ]
        sm._assert_all(skip_verification_paths=skip_path)

    def test_list_as_mid_node_in_skip_verification_path(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {"key_a": {"aaa": [{"aab": "value1"}, {"aab": "value2"}]}}
        sm.match(
            "key_a",
            {"aaa": [{"aab": "value1"}, {"aab": "bad-value"}]},
        )

        with pytest.raises(Exception) as ctx:  # asserts it fail without skipping
            sm._assert_all()
        ctx.match("Parity snapshot failed")

        skip_path = ["$..aaa[1].aab"]
        sm._assert_all(skip_verification_paths=skip_path)

        skip_path = ["$..aaa.1.aab"]
        sm._assert_all(skip_verification_paths=skip_path)

    def test_list_as_last_node_in_skip_verification_path_nested(self):
        sm = SnapshotSession(scope_key="A", verify=True, base_file_path="", update=False)
        sm.recorded_state = {
            "key_a": {
                "aaa": [
                    "bbb",
                    "ccc",
                    [
                        "ddd",
                        "eee",
                        [
                            "fff",
                            "ggg",
                        ],
                    ],
                ]
            }
        }
        sm.match(
            "key_a",
            {
                "aaa": [
                    "bbb",
                    "ccc",
                    [
                        "bad-value",
                        "eee",
                        [
                            "fff",
                            "ggg",
                        ],
                    ],
                ]
            },
        )

        with pytest.raises(Exception) as ctx:  # asserts it fail without skipping
            sm._assert_all()
        ctx.match("Parity snapshot failed")

        skip_path = ["$..aaa[2][0]"]
        sm._assert_all(skip_verification_paths=skip_path)

        skip_path = ["$..aaa.2[0]"]
        sm._assert_all(skip_verification_paths=skip_path)

        # these 2 will actually skip almost everything, as they will match every first element of any list inside `aaa`
        skip_path = ["$..aaa..[0]"]
        sm._assert_all(skip_verification_paths=skip_path)

        skip_path = ["$..aaa..0"]
        sm._assert_all(skip_verification_paths=skip_path)


def test_json_diff_format():
    path = ["Records", 1]
    assert _format_json_path(path) == '"$..Records"'
    path = ["Records", 1, 1, 1]
    assert _format_json_path(path) == '"$..Records"'
    path = ["Records", 1, "SomeKey"]
    assert _format_json_path(path) == '"$..Records..SomeKey"'
    path = ["Records", 1, 1, "SomeKey"]
    assert _format_json_path(path) == '"$..Records..SomeKey"'
    path = ["Records", 1, 1, 0, "SomeKey"]
    assert _format_json_path(path) == '"$..Records..SomeKey"'
    path = ["Records", "SomeKey"]
    assert _format_json_path(path) == '"$..Records.SomeKey"'
    path = []
    assert _format_json_path(path) == '"$.."'
    path = [1, 1, 0, "SomeKey"]
    assert _format_json_path(path) == '"$..SomeKey"'
    path = ["Some:Key"]
    assert _format_json_path(path) == "\"$..'Some:Key'\""
    path = ["Some.Key"]
    assert _format_json_path(path) == "\"$..'Some.Key'\""
    path = ["Some-Key"]
    assert _format_json_path(path) == '"$..Some-Key"'
    path = ["Some0Key"]
    assert _format_json_path(path) == '"$..Some0Key"'


def test_sorting_transformer():
    original_dict = {
        "a": {
            "b": [
                {"name": "c-123"},
                {"name": "a-123"},
                {"name": "b-123"},
            ]
        },
        "a2": {
            "b": [
                {"name": "b-123"},
                {"name": "a-123"},
                {"name": "c-123"},
            ]
        },
    }

    sorted_items = [
        {"name": "a-123"},
        {"name": "b-123"},
        {"name": "c-123"},
    ]

    transformer = SortingTransformer("b", lambda x: x["name"])
    transformed_dict = transformer.transform(original_dict)

    assert transformed_dict["a"]["b"] == sorted_items
    assert transformed_dict["a2"]["b"] == sorted_items
