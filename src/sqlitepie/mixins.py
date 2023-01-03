# -*- coding: UTF-8 -*-

from abc import ABC
from json import loads, dumps, JSONEncoder


class DictMixin(ABC):

    def as_dict(self) -> dict:
        return self._traverse_dict(self.__dict__)

    def _traverse_dict(self, attributes: dict) -> dict:
        return {
            key: self._traverse(key, value)
            for key, value in attributes.items()
        }

    def _traverse(self, key, value):
        if isinstance(value, DictMixin):
            return value.as_dict()
        elif isinstance(value, dict):
            return self._traverse_dict(value)
        elif isinstance(value, (list, set, tuple)):
            return type(value)([self._traverse(key, item) for item in value])
        # elif hasattr(value, "__dict__"):
        #     return self._traverse_dict(value.__dict__)
        else:
            return value


class JsonMixin(ABC):

    # noinspection PyArgumentList
    # @classmethod
    # def from_json(cls, data):
    #     kwargs = loads(data)
    #     return cls(**kwargs)

    # noinspection PyUnresolvedReferences
    def as_json(self) -> str:
        return dumps(self.as_dict(), cls=CustomJSONEncoder)


class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        try:
            iterable = iter(obj)
        except TypeError:
            return obj.__str__()
        else:
            return list(iterable)

        # Let the base class default method raise the TypeError
        # return JSONEncoder.default(self, obj)


class Mixin(DictMixin, JsonMixin):

    def __str__(self) -> str:
        return self.as_json()
