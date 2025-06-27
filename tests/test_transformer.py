import copy
import json

import pytest

from localstack_snapshot.snapshots.transformer import (
    JsonStringTransformer,
    ResponseMetaDataTransformer,
    SortingTransformer,
    TimestampTransformer,
    TransformContext,
)
from localstack_snapshot.snapshots.transformer_utility import TransformerUtility


class TestTransformer:
    def test_key_value_replacement(self):
        input = {
            "hello": "world",
            "hello2": "again",
            "path": {"to": {"anotherkey": "hi", "inside": {"hello": "inside"}}},
        }

        key_value = TransformerUtility.key_value(
            "hello", "placeholder", reference_replacement=False
        )

        expected_key_value = {
            "hello": "placeholder",
            "hello2": "again",
            "path": {"to": {"anotherkey": "hi", "inside": {"hello": "placeholder"}}},
        }

        copied = copy.deepcopy(input)
        ctx = TransformContext()
        assert key_value.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

        copied = copy.deepcopy(input)
        key_value = TransformerUtility.key_value("hello", "placeholder", reference_replacement=True)
        expected_key_value_reference = {
            "hello": "<placeholder:1>",
            "hello2": "again",
            "path": {"to": {"anotherkey": "hi", "<placeholder:2>": {"hello": "<placeholder:2>"}}},
        }
        assert key_value.transform(copied, ctx=ctx) == copied
        assert len(ctx.serialized_replacements) == 2

        tmp = json.dumps(copied, default=str)
        for sr in ctx.serialized_replacements:
            tmp = sr(tmp)

        assert json.loads(tmp) == expected_key_value_reference

    def test_key_value_replacement_custom_function(self):
        input = {
            "hello": "12characters",
            "hello2": "again",
            "path": {
                "to": {
                    "anotherkey": "hi",
                    "twelvesymbol": {"hello": "twelvesymbol"},
                    "fifteen_symbols": {"hello": "fifteen_symbols"},
                }
            },
        }

        key_value = TransformerUtility.key_value_replacement_function(
            "hello",
            replacement_function=lambda k, v: f"placeholder({len(v)})",
            reference_replacement=False,
        )

        expected_key_value = {
            "hello": "placeholder(12)",
            "hello2": "again",
            "path": {
                "to": {
                    "anotherkey": "hi",
                    "twelvesymbol": {"hello": "placeholder(12)"},
                    "fifteen_symbols": {"hello": "placeholder(15)"},
                }
            },
        }

        ctx = TransformContext()
        assert key_value.transform(input, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    def test_key_value_replacement_custom_function_reference_replacement(self):
        input = {
            "hello": "12characters",
            "hello2": "again",
            "path": {
                "to": {
                    "anotherkey": "hi",
                    "twelvesymbol": {"hello": "twelvesymbol"},
                    "fifteen_symbols": {"hello": "fifteen_symbols"},
                }
            },
        }

        key_value = TransformerUtility.key_value_replacement_function(
            "hello",
            replacement_function=lambda k, v: f"placeholder({len(v)})",
            reference_replacement=True,
        )
        # replacement counters are per replacement key, so it will start from 1 again.
        expected_key_value_reference = {
            "hello": "<placeholder(12):1>",
            "hello2": "again",
            "path": {
                "to": {
                    "anotherkey": "hi",
                    "<placeholder(12):2>": {"hello": "<placeholder(12):2>"},
                    "<placeholder(15):1>": {"hello": "<placeholder(15):1>"},
                }
            },
        }
        ctx = TransformContext()
        assert key_value.transform(input, ctx=ctx) == input
        assert len(ctx.serialized_replacements) == 3

        tmp = json.dumps(input, default=str)
        for sr in ctx.serialized_replacements:
            tmp = sr(tmp)

        assert json.loads(tmp) == expected_key_value_reference

    def test_key_value_replacement_with_falsy_value(self):
        input = {
            "hello": "world",
            "somenumber": 0,
        }

        key_value = TransformerUtility.key_value(
            "somenumber", "placeholder", reference_replacement=False
        )

        expected_key_value = {
            "hello": "world",
            "somenumber": "placeholder",
        }

        copied = copy.deepcopy(input)
        ctx = TransformContext()
        assert key_value.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    @pytest.mark.parametrize("type", ["key_value", "jsonpath"])
    def test_replacement_with_reference(self, type):
        input = {
            "also-me": "b",
            "path": {
                "to": {"anotherkey": "hi", "test": {"hello": "replaceme"}},
                "another": {"key": "this/replaceme/hello"},
            },
            "b": {"a/b/replaceme.again": "bb"},
            "test": {"inside": {"path": {"to": {"test": {"hello": "also-me"}}}}},
        }

        expected = {
            "<MYVALUE:2>": "b",
            "path": {
                "to": {"anotherkey": "hi", "test": {"hello": "<MYVALUE:1>"}},
                "another": {"key": "this/<MYVALUE:1>/hello"},
            },
            "b": {"a/b/<MYVALUE:1>.again": "bb"},
            "test": {"inside": {"path": {"to": {"test": {"hello": "<MYVALUE:2>"}}}}},
        }
        replacement = "MYVALUE"
        if type == "key_value":
            transformer = TransformerUtility.key_value(
                "hello", replacement, reference_replacement=True
            )
        else:
            transformer = TransformerUtility.jsonpath(
                "$..path.to.test.hello", replacement, reference_replacement=True
            )

        copied = copy.deepcopy(input)
        ctx = TransformContext()

        assert transformer.transform(copied, ctx=ctx) == copied
        assert len(ctx.serialized_replacements) == 2

        tmp = json.dumps(copied, default=str)
        for sr in ctx.serialized_replacements:
            tmp = sr(tmp)

        assert json.loads(tmp) == expected

    def test_regex(self):
        input = {
            "hello": "world",
            "hello2": "again",
            "path": {"to": {"anotherkey": "hi", "inside": {"hello": "inside"}}},
        }

        expected = {
            "new-value": "world",
            "new-value2": "again",
            "path": {"to": {"anotherkey": "hi", "inside": {"new-value": "inside"}}},
        }

        transformer = TransformerUtility.regex("hello", "new-value")

        ctx = TransformContext()
        output = transformer.transform(json.dumps(input), ctx=ctx)
        for sr in ctx.serialized_replacements:
            output = sr(output)
        assert json.loads(output) == expected

    # def test_log_stream_name(self):
    #     input = {
    #         "Payload": {
    #             "context": {
    #                 "functionVersion": "$LATEST",
    #                 "functionName": "my-function",
    #                 "memoryLimitInMB": "128",
    #                 "logGroupName": "/aws/lambda/my-function",
    #                 "logStreamName": "2022/05/31/[$LATEST]ced3cafaaf284d8199e02909ac87e2f5",
    #                 "clientContext": {
    #                     "custom": {"foo": "bar"},
    #                     "client": {"snap": ["crackle", "pop"]},
    #                     "env": {"fizz": "buzz"},
    #                 },
    #                 "invokedFunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:my-function",
    #             }
    #         }
    #     }
    #     transformers = TransformerUtility.lambda_api()
    #     ctx = TransformContext()
    #     for t in transformers:
    #         t.transform(input, ctx=ctx)
    #
    #     output = json.dumps(input)
    #     for sr in ctx.serialized_replacements:
    #         output = sr(output)
    #
    #     expected = {
    #         "Payload": {
    #             "context": {
    #                 "functionVersion": "$LATEST",
    #                 "functionName": "<resource:1>",
    #                 "memoryLimitInMB": "128",
    #                 "logGroupName": "/aws/lambda/<resource:1>",
    #                 "logStreamName": "<log-stream-name:1>",
    #                 "clientContext": {
    #                     "custom": {"foo": "bar"},
    #                     "client": {"snap": ["crackle", "pop"]},
    #                     "env": {"fizz": "buzz"},
    #                 },
    #                 "invokedFunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:<resource:1>",
    #             }
    #         }
    #     }
    #     assert expected == json.loads(output)

    def test_nested_sorting_transformer(self):
        input = {
            "subsegments": [
                {
                    "name": "mysubsegment",
                    "subsegments": [
                        {"name": "b"},
                        {"name": "a"},
                    ],
                }
            ],
        }

        expected = {
            "subsegments": [
                {
                    "name": "mysubsegment",
                    "subsegments": [
                        {"name": "a"},
                        {"name": "b"},
                    ],
                }
            ],
        }

        transformer = SortingTransformer("subsegments", lambda s: s["name"])

        ctx = TransformContext()
        output = transformer.transform(input, ctx=ctx)
        assert output == expected

    @pytest.mark.parametrize(
        "value",
        [
            "a+b",
            "question?",
            "amount: $4.00",
            "emoji: ^^",
            "sentence.",
            "others (like so)",
            "special {char}",
        ],
    )
    def test_text(self, value):
        input = {"key": f"some {value} with more text"}

        expected = {"key": "some <value> with more text"}

        transformer = TransformerUtility.text(value, "<value>")

        ctx = TransformContext()
        output = transformer.transform(json.dumps(input), ctx=ctx)
        for sr in ctx.serialized_replacements:
            output = sr(output)
        assert json.loads(output) == expected

    @pytest.mark.parametrize(
        "input_value,transformed_value",
        [
            pytest.param('{"a": "b"}', {"a": "b"}, id="simple_json_object"),
            pytest.param('{\n  "a": "b"\n}', {"a": "b"}, id="formatted_json_object"),
            pytest.param('\n  {"a": "b"}', {"a": "b"}, id="json_with_whitespaces"),
            pytest.param('{"a": 42}malformed', '{"a": 42}malformed', id="malformed_json"),
            pytest.param('["a", "b"]', ["a", "b"], id="simple_json_list"),
            pytest.param('{"a": "{\\"b\\":42}"}', {"a": {"b": 42}}, id="nested_json_object"),
            pytest.param(
                '{"a": "\\n  {\\n  \\"b\\":42}"}',
                {"a": {"b": 42}},
                id="nested_formatted_json_object_with_whitespaces",
            ),
            pytest.param(
                '{"a": "[{\\"b\\":\\"c\\"}]"}', {"a": [{"b": "c"}]}, id="nested_json_list"
            ),
            pytest.param(
                '{"a": "{\\"b\\":42malformed}"}',
                {"a": '{"b":42malformed}'},
                id="malformed_nested_json",
            ),
            pytest.param("[]", [], id="empty_list"),
            pytest.param("{}", {}, id="empty_object"),
            pytest.param("", "", id="empty_string"),
        ],
    )
    def test_json_string(self, input_value, transformed_value):
        key = "key"
        input_data = {key: input_value}
        expected = {key: transformed_value}

        transformer = JsonStringTransformer(key)

        ctx = TransformContext()
        output = transformer.transform(input_data, ctx=ctx)

        assert output == expected

    def test_json_string_in_a_nested_key(self):
        key = "nested-key-in-an-object-hidden-inside-a-list"
        input_data = {"top-level-key": [{key: '{"a": "b"}'}]}
        expected = {"top-level-key": [{key: {"a": "b"}}]}

        transformer = JsonStringTransformer(key)

        ctx = TransformContext()
        output = transformer.transform(input_data, ctx=ctx)

        assert output == expected


class TestTimestampTransformer:
    def test_generic_timestamp_transformer(self):
        # TODO: add more samples

        input = {
            "lambda_": {
                "FunctionName": "lambdafn",
                "LastModified": "2023-10-09T12:49:50.000+0000",
            },
            "cfn": {
                "StackName": "cfnstack",
                "CreationTime": "2023-11-20T18:39:36.014000+00:00",
            },
            "sfn": {
                "name": "statemachine",
                "creationDate": "2023-11-21T07:14:12.243000+01:00",
                "sfninternal": "2023-11-21T07:14:12.243Z",
            },
        }

        expected = {
            "lambda_": {
                "FunctionName": "lambdafn",
                "LastModified": "<timestamp:2022-07-13T13:48:01.000+0000>",
            },
            "cfn": {
                "StackName": "cfnstack",
                "CreationTime": "<timestamp:2022-07-13T13:48:01.000000+00:00>",
            },
            "sfn": {
                "name": "statemachine",
                "creationDate": "<timestamp:2022-07-13T13:48:01.000000+00:00>",
                "sfninternal": "<timestamp:2022-07-13T13:48:01.000Z>",
            },
        }

        transformer = TimestampTransformer()

        ctx = TransformContext()
        output = transformer.transform(input, ctx=ctx)
        assert output == expected


class TestResponseMetaDataTransformer:
    def test_with_headers(self):
        input_data = {"ResponseMetadata": {"HTTPHeaders": {"header1": "value1"}}}

        metadata_transformer = ResponseMetaDataTransformer()

        expected_key_value = {"ResponseMetadata": {"HTTPHeaders": {}}}

        copied = copy.deepcopy(input_data)
        ctx = TransformContext()
        assert metadata_transformer.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    def test_with_headers_and_status_code(self):
        input_data = {
            "ResponseMetadata": {"HTTPHeaders": {"header1": "value1"}, "HTTPStatusCode": 500}
        }

        metadata_transformer = ResponseMetaDataTransformer()

        expected_key_value = {"ResponseMetadata": {"HTTPHeaders": {}, "HTTPStatusCode": 500}}

        copied = copy.deepcopy(input_data)
        ctx = TransformContext()
        assert metadata_transformer.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    def test_with_status_code_only(self):
        input_data = {"ResponseMetadata": {"HTTPStatusCode": 500, "RandomData": "random"}}

        metadata_transformer = ResponseMetaDataTransformer()

        expected_key_value = {"ResponseMetadata": {"HTTPStatusCode": 500, "RandomData": "random"}}

        copied = copy.deepcopy(input_data)
        ctx = TransformContext()
        assert metadata_transformer.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    def test_with_empty_response_metadata(self):
        input_data = {"ResponseMetadata": {"NotHeaders": "data"}}

        metadata_transformer = ResponseMetaDataTransformer()

        expected_key_value = {"ResponseMetadata": {"NotHeaders": "data"}}

        copied = copy.deepcopy(input_data)
        ctx = TransformContext()
        assert metadata_transformer.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    def test_with_headers_wrong_type(self):
        input_data = {"ResponseMetadata": {"HTTPHeaders": "data"}}

        metadata_transformer = ResponseMetaDataTransformer()

        expected_key_value = {"ResponseMetadata": {"HTTPHeaders": "data"}}

        copied = copy.deepcopy(input_data)
        ctx = TransformContext()
        assert metadata_transformer.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []

    def test_headers_filtering(self):
        input_data = {
            "ResponseMetadata": {"HTTPHeaders": {"content_type": "value1", "header1": "value1"}}
        }

        metadata_transformer = ResponseMetaDataTransformer()

        expected_key_value = {"ResponseMetadata": {"HTTPHeaders": {"content_type": "value1"}}}

        copied = copy.deepcopy(input_data)
        ctx = TransformContext()
        assert metadata_transformer.transform(copied, ctx=ctx) == expected_key_value
        assert ctx.serialized_replacements == []
