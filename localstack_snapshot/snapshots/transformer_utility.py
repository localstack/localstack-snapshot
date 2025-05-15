from re import Pattern
from typing import Any, Callable, Optional

from localstack_snapshot.snapshots.transformer import (
    JsonpathTransformer,
    JsonStringTransformer,
    KeyValueBasedTransformer,
    KeyValueBasedTransformerFunctionReplacement,
    RegexTransformer,
    SortingTransformer,
    TextTransformer,
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
    def key_value_replacement_function(
        key: str,
        replacement_function: Callable[[str, Any], str] = None,
        reference_replacement: bool = True,
    ):
        """Creates a new KeyValueBasedTransformer. If the key matches, the value will be replaced.

        :param key: the name of the key which should be replaced
        :param replacement_function: The function calculating the replacement. Will be passed the key and value of the replaced pair.
        By default it is the key-name in lowercase, separated with hyphen
        :param reference_replacement: if False, only the original value for this key will be replaced.
        If True all references of this value will be replaced (using a regex pattern), for the entire test case.
        In this case, the replaced value will be nummerated as well.
        Default: True

        :return: KeyValueBasedTransformer
        """
        replacement_function = replacement_function or (
            lambda x, y: _replace_camel_string_with_hyphen(key)
        )
        return KeyValueBasedTransformerFunctionReplacement(
            lambda k, v: v if k == key and (v is not None and v != "") else None,
            replacement_function=replacement_function,
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

    @staticmethod
    def text(text: str, replacement: str):
        """Creates a new TextTransformer. All occurrences in the string-converted dict will be replaced.

        Useful if the text contains special characters that would confuse the RegexTransformer, like '+' or '('.

        :param text: the text that should be replaced
        :param replacement: the value which will replace the original value.

        :return: TextTransformer
        """
        return TextTransformer(text, replacement)

    @staticmethod
    def json_string(key: str) -> JsonStringTransformer:
        """Creates a new JsonStringTransformer. If there is a valid JSON text string at specified key
        it will be loaded as a regular object or array.

        :param key: key at which JSON string is expected

        :return: JsonStringTransformer
        """
        return JsonStringTransformer(key)

    @staticmethod
    def sorting(key: str, sorting_fn: Optional[Callable[[...], Any]]) -> SortingTransformer:
        """Creates a new SortingTransformer.

        Sorts a list at `key` with the given `sorting_fn` (argument for `sorted(list, key=sorting_fn)`)

        :param key: key at which the list to sort is expected
        :param sorting_fn: sorting function

        :return: SortingTransformer
        """
        return SortingTransformer(key, sorting_fn)
