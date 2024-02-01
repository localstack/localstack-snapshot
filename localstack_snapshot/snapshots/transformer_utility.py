from re import Pattern
from typing import Optional

from localstack_snapshot.snapshots.transformer import (
    JsonpathTransformer,
    KeyValueBasedTransformer,
    RegexTransformer,
)


def _replace_camel_string_with_hyphen(input_string: str):
    return "".join(["-" + char.lower() if char.isupper() else char for char in input_string]).strip(
        "-"
    )


class TransformerUtility:
    @staticmethod
    def key_value(
        key: str, value_replacement: Optional[str] = None, reference_replacement: bool = True
    ):
        """Creates a new KeyValueBasedTransformer. If the key matches, the value will be replaced.

        :param key: the name of the key which should be replaced
        :param value_replacement: the value which will replace the original value.
        By default it is the key-name in lowercase, separated with hyphen
        :param reference_replacement: if False, only the original value for this key will be replaced.
        If True all references of this value will be replaced (using a regex pattern), for the entire test case.
        In this case, the replaced value will be nummerated as well.
        Default: True

        :return: KeyValueBasedTransformer
        """
        return KeyValueBasedTransformer(
            lambda k, v: v if k == key and (v is not None and v != "") else None,
            replacement=value_replacement or _replace_camel_string_with_hyphen(key),
            replace_reference=reference_replacement,
        )

    @staticmethod
    def jsonpath(jsonpath: str, value_replacement: str, reference_replacement: bool = True):
        """Creates a new JsonpathTransformer. If the jsonpath matches, the value will be replaced.

        :param jsonpath: the jsonpath that should be matched
        :param value_replacement: the value which will replace the original value.
        By default it is the key-name in lowercase, separated with hyphen
        :param reference_replacement: if False, only the original value for this key will be replaced.
        If True all references of this value will be replaced (using a regex pattern), for the entire test case.
        In this case, the replaced value will be nummerated as well.
        Default: True

        :return: JsonpathTransformer
        """
        return JsonpathTransformer(
            jsonpath=jsonpath,
            replacement=value_replacement,
            replace_reference=reference_replacement,
        )

    @staticmethod
    def regex(regex: str | Pattern[str], replacement: str):
        """Creates a new RegexTransformer. All matches in the string-converted dict will be replaced.

        :param regex: the regex that should be matched
        :param replacement: the value which will replace the original value.

        :return: RegexTransformer
        """
        return RegexTransformer(regex, replacement)
