"""Functions for step level parallelism."""
from __future__ import annotations
from abc import abstractproperty
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, reduce, wraps
import itertools
import math
import re
from typing import (
    Any, Callable, Dict, Iterable, List, Pattern, Set, Tuple, TypedDict, Union
)
from typing_extensions import Literal, NotRequired

from kedro.pipeline.node import Node
from kedro.pipeline import node
from kedro_partitioned.utils.constants import MAX_NODES, MAX_WORKERS
from kedro_partitioned.utils.other import (
    nonefy,
    truthify,
)
from kedro_partitioned.utils.string import (
    get_filepath_without_extension,
)
from kedro_partitioned.utils.iterable import (
    firstorlist,
    partition,
    tolist,
    unique,
    optionaltolist,
)
from kedro.pipeline import Pipeline
from kedro_partitioned.utils.typing import T, Args, IsFunction

_Partitioned = Dict[str, Callable[[], Any]]


class _Template(TypedDict):
    """Configuration for Configurator/partition matching.

    Attributes:
        pattern: A string of the possible partition subpaths containing
            {} as placeholders for the `Configurator` target replacement
        hierarchy: A list of strings of the pattern placeholder declared
            in the order of the most hierarchycal, to the less.
        any: A dictionary of placeholder by its specific regex for `*` targets
    """

    pattern: str
    hierarchy: NotRequired[List[str]]
    any: Dict[str, str]


class _Configurator(TypedDict, total=False):
    """Specifies how a Configurator for multinodes must be declared.

    Attributes:
        target: A list of replacements for the `_Template` pattern.
            Should be declared in the same order as the pattern occurences.
        cached: Whether a partition should not be processed.
        data: Everything that is gonna be passed to the multinode function
    """

    target: List[Union[str, List[str], Literal['*']]]
    cached: NotRequired[bool]
    data: dict


class _Configurators(TypedDict):
    """
    TypedDict for a container of `Configurator`s.

    This describes how a configurator entry must be declared in the
    parameters files.
    """

    template: _Template
    configurators: List[_Configurator]


class ConfiguratorFinder:
    """Finds the best configurator given a list of configurators.

    Given a configurators dict, this class will find the best configurator for
    a given subpath.

    Attributes:
        keys (List[str]): Keys between {} in the template.
        weights (List[int]): Weights of the keys given a hierarchy in
            the template. The higher the weight, the more specific the
            configurator is.
        scores (Dict[int, int]): Scores of the possible configurator targets.
    """

    NOT_FOUND = -1
    ANY = '*'

    def __init__(self, configurators: _Configurators):
        """Initializes the ConfiguratorFinder.

        Args:
            configurators (_Configurators): Configurators to use.
        """
        self._template = configurators['template']
        self._configurators = configurators['configurators']

    @cached_property
    def keys(self) -> List[str]:
        """List of keys between {} in the template.

        Returns:
            List[str]: Keys between {} in the template.
        """
        return re.findall(re.compile(r'\{(.+?)\}'), self._template['pattern'])

    @cached_property
    def weights(self) -> List[int]:
        """Weight for each key in the template.

        Returns:
            List[int]
        """
        keys = self.keys
        hierarchy = self._template.get('hierarchy', keys)
        return [hierarchy.index(key) for key in keys]

    @cached_property
    def scores(self) -> Dict[int, int]:
        """Builds a dict of <hash of a target possibility>: <score>.

        Returns:
            Dict[int, int]: Possible scores for a target possibility

        Note:
            Already sorted in crescent order
        """
        return {
            hash(tuple(reversed(possibility))): score
            for score, possibility in
            enumerate(itertools.product([False, True], repeat=len(self.keys)))
        }

    def _build_regex(self, configurator: _Configurator) -> Pattern:
        """Builds a regex for a configurator target.

        Args:
            configurator (_Configurator): Configurator to build the regex for.

        Returns:
            Pattern: Regex for the configurator target.
        """

        def parse_target(key: str, target: str) -> List[str]:
            if isinstance(target, str):
                if target == self.ANY:
                    return [self._template.get('any', {}).get(key, '.*')]
                else:
                    return [target]
            else:
                return target

        targets = (
            f'({"|".join(parse_target(key, target))})'
            for key, target in zip(self.keys, configurator['target'])
        )
        key_regex = '|'.join([r'\{%s\}' % k for k in self.keys])
        return r'^' + re.sub(
            key_regex, lambda _: next(targets), self._template['pattern']
        ) + r'$'

    def _score_match(self, path: str, configurator: _Configurator) -> int:
        """Computes the score of a configurator if it matches the path.

        Args:
            path (str): Path to match the configurator with.
            configurator (_Configurator): Configurator to score.

        Returns:
            int: Score of the configurator.
        """
        regex = self._build_regex(configurator)
        targets = configurator['target']
        weights = self.weights
        if re.match(regex, path):
            raw_possibility = [group != self.ANY for group in targets]
            possibility = hash(
                tuple(key for _, key in sorted(zip(weights, raw_possibility)))
            )
            return self.scores[possibility]
        else:
            return self.NOT_FOUND

    def __getitem__(self, path: str) -> Union[_Configurator, None]:
        """Finds the best configurator for a given path.

        Args:
            path (str): Path to find the best configurator for.

        Returns:
            Union[_Configurator, None]: Best configurator for the path.
        """
        configurator = max(
            self._configurators, key=lambda x: self._score_match(path, x)
        )
        return None if self._score_match(
            path, configurator
        ) == self.NOT_FOUND else configurator


class _CustomizedFuncNode(Node):

    def __init__(
        self,
        func: Callable,
        inputs: Union[None, str, List[str], Dict[str, str]],
        outputs: Union[None, str, List[str], Dict[str, str]],
        name: str = None,
        tags: Union[str, Iterable[str]] = None,
        confirms: Union[str, List[str]] = None,
        namespace: str = None
    ):
        self._original_func = func
        super().__init__(
            func,
            inputs,
            outputs,
            name=name,
            tags=tags,
            confirms=confirms,
            namespace=namespace
        )

    @abstractproperty
    def func(self) -> Callable:
        pass

    def run(self, inputs: Dict[str, Any] = None) -> Dict[str, Any]:
        self._func = self.func
        out = super().run(inputs)
        self._func = self._original_func
        return out


class _SlicerNode(_CustomizedFuncNode):
    """Splits partitioned datasets partitions and store them in a metadata.

    Example:
        Balance loads

        >>> n = _SlicerNode(2, 'a', 'b', 'x')
        >>> n
        Node(nonefy, ['a'], 'b-slicer', 'x')

        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4}}
        >>> n.run(inputs=dictionary)
        {'b-slicer': [['subpath/a'], ['subpath/b']]}

        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4,
        ...                     'subpath/c.txt': lambda: 5}}
        >>> n.run(inputs=dictionary)
        {'b-slicer': [['subpath/a', 'subpath/b'], ['subpath/c']]}

        >>> n = _SlicerNode(3, 'a', 'b', 'x')
        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4,
        ...                     'subpath/c.txt': lambda: 5}}
        >>> n.run(inputs=dictionary)
        {'b-slicer': [['subpath/a'], ['subpath/b'], ['subpath/c']]}

        With multiple inputs

        >>> n = _SlicerNode(2, ['a', 'b'], 'c', 'x')
        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4},
        ...               'b': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4}}
        >>> n.run(inputs=dictionary)
        {'c-slicer': [['subpath/a'], ['subpath/b']]}

        Intersect partitions

        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4},
        ...               'b': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4,
        ...                     'subpath/c.txt': lambda: 5}}
        >>> n.run(inputs=dictionary)
        {'c-slicer': [['subpath/a'], ['subpath/b']]}

        Using configurators

        >>> n = _SlicerNode(2, ['a', 'b'], 'c', 'x', configurator='params:z')
        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4},
        ...               'b': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4,
        ...                     'subpath/c.txt': lambda: 5},
        ...               'params:z': {
        ...                     'template': {'pattern': 'subpath/{x}'},
        ...                     'configurators': [
        ...                         {'target': ['a'], 'cached': True,
        ...                             'data': 1},
        ...                         {'target': ['*'], 'cached': False,
        ...                             'data': 1}]}}
        >>> n.run(inputs=dictionary)
        {'c-slicer': [['subpath/b'], []]}
    """

    SLICER_SUFFIX = '-slicer'

    def __init__(
        self,
        slice_count: int,
        partitioned_inputs: Union[None, str, List[str]],
        partitioned_outputs: str,
        name: str,
        tags: Union[str, Iterable[str]] = None,
        confirms: Union[str, List[str]] = None,
        namespace: str = None,
        filter: IsFunction[str] = truthify,
        configurator: str = None,
    ):
        self._partitioned_inputs = partitioned_inputs
        self._slice_count = slice_count
        self._original_output = partitioned_outputs
        self._filter = filter
        self._configurator = configurator
        super().__init__(
            func=nonefy,
            inputs=tolist(partitioned_inputs) + optionaltolist(configurator),
            outputs=self._add_slicer_suffix(partitioned_outputs),
            name=name,
            tags=tags,
            confirms=confirms,
            namespace=namespace,
        )

    @property
    def slice_count(self) -> int:
        return self._slice_count

    @property
    def original_output(self) -> str:
        return self._original_output

    @property
    def json_output(self) -> str:
        return self._outputs

    def _copy(self, **overwrite_params: Any) -> _SlicerNode:
        params = {
            'partitioned_inputs': self._partitioned_inputs,
            'partitioned_outputs': self._original_output,
            'slice_count': self._slice_count,
            'name': self._name,
            'namespace': self._namespace,
            'tags': self._tags,
            'confirms': self._confirms,
            'configurator': self._configurator,
            'filter': self._filter,
        }
        params.update(overwrite_params)
        return self.__class__(**params)

    @classmethod
    def _add_slicer_suffix(cls, string: str) -> str:
        """Returns the same string with `-slicer` suffix.

        Args:
            string (str)

        Returns:
            str

        Example:
            >>> _SlicerNode._add_slicer_suffix('test')
            'test-slicer'

            >>> _SlicerNode._add_slicer_suffix(
            ...     'test-slicer')
            'test-slicer'
        """
        return (
            string if string.endswith(cls.SLICER_SUFFIX) else
            f'{string}{cls.SLICER_SUFFIX}'
        )

    def _intersect_partitioneds(self,
                                partitioneds: List[_Partitioned]) -> List[str]:
        """Takes only the matching partitions (required for the input `zip`).

        Args:
            partitioneds (List[Partitioned]): partitioned dicionaries

        Returns:
            List[str]
        """
        partitioned_sets = [{
            get_filepath_without_extension(path)
            for path in partitioned
        } for partitioned in partitioneds]

        return list(
            reduce(
                lambda inter, curr: inter.intersection(curr), partitioned_sets
            )
        )

    def _calc_slice_bound(self, partition_count: int, slice_id: int) -> int:
        """Calculates the bounds of the subset of partitions for node.

        Args:
            partition_count (int): size of the partitions dictionary
            slice_id (int): current slice id

        Returns:
            int
        """
        return math.ceil((partition_count / self._slice_count) * slice_id)

    def _slice_partitions(
        self,
        partitions: List[str],
        slice_id: int,
    ) -> List[str]:
        """Returns a subset of the original partitions.

        Args:
            partitions (List[str])
            slice_id (int)

        Returns:
            List[str]
        """
        partition_count = len(partitions)
        return partitions[self._calc_slice_bound(
            partition_count,
            slice_id,
        ):self._calc_slice_bound(
            partition_count,
            slice_id + 1,
        )]

    def _apply_filter(self, intersection: List[str]) -> List[str]:
        return [p for p in intersection if self._filter(p)]

    @classmethod
    def _extract_args_part(cls, args: tuple,
                           nargs: int) -> Tuple[tuple, tuple]:
        return args[:nargs], args[nargs:]

    def _extract_args(
        self, args: tuple
    ) -> Tuple[List[_Partitioned], _Configurators]:
        if self._configurator is None:
            return args, {}
        else:
            partitioneds, configurators = self._extract_args_part(args, -1)
            return partitioneds, configurators[0]

    def _filter_cached(
        self, configurators: _Configurators, intersection: List[str]
    ) -> List[str]:
        if self._configurator:
            configurator_finder = ConfiguratorFinder(configurators)
            return [
                p for p in intersection
                if not (configurator_finder[p] or {}).get('cached', False)
            ]
        else:
            return intersection

    @property
    def func(self) -> Callable:

        def fn(*args: Any) -> List[List[str]]:
            partitioneds, configurators = self._extract_args(args)

            intersection = self._intersect_partitioneds(partitioneds)
            intersection = self._apply_filter(intersection)
            intersection = self._filter_cached(configurators, intersection)
            intersection = sorted(intersection)

            return [
                self._slice_partitions(intersection, i)
                for i in range(self._slice_count)
            ]

        return fn


class _MultiNode(_CustomizedFuncNode):
    """Node to process a slice of a partitioned dataset.

    Example:
        >>> lbn = _SlicerNode(2, 'a', 'b', 'x')
        >>> dictionary = {'a': {'subpath/a.txt': lambda: 3,
        ...                     'subpath/b.txt': lambda: 4}}
        >>> lb = lbn.run(inputs=dictionary)

        >>> def fn(x: int) -> int: return x+10
        >>> n = _MultiNode(slicer=lbn,
        ...                func=fn,
        ...                partitioned_inputs='a',
        ...                partitioned_outputs='b',
        ...                slice_id=0,
        ...                slice_count=2,
        ...                name='x')
        >>> dictionary['b-slicer'] = [['subpath/a'], ['subpath/b']]
        >>> n.run(inputs=dictionary)
        {'b-slice-0': {'subpath/a': 13}}

        Multiple inputs

        >>> dictionary['b'] = {'subpath/a.txt': lambda: 4,
        ...                    'subpath/b.txt': lambda: 5}
        >>> def fn(x: int, y: int) -> list: return [x+10, y+20]
        >>> n = _MultiNode(slicer=lbn,
        ...                func=fn,
        ...                partitioned_inputs=['a', 'b'],
        ...                partitioned_outputs='c',
        ...                slice_id=0,
        ...                slice_count=2,
        ...                name='x')
        >>> n.run(inputs=dictionary)
        {'c-slice-0': {'subpath/a': [13, 24]}}

        Multiple outputs

        >>> n = _MultiNode(slicer=lbn,
        ...                func=fn,
        ...                partitioned_inputs=['a', 'b'],
        ...                partitioned_outputs=['c', 'd'],
        ...                slice_id=0,
        ...                slice_count=2,
        ...                name='x')
        >>> n.run(inputs=dictionary)
        {'c-slice-0': {'subpath/a': 13}, 'd-slice-0': {'subpath/a': 24}}

        Other inputs

        >>> dictionary['e'] = 100
        >>> def fn(x: int, y: int, e: int) -> list: return [x+e, y+e]
        >>> n = _MultiNode(slicer=lbn,
        ...                func=fn,
        ...                partitioned_inputs=['a', 'b'],
        ...                partitioned_outputs=['c', 'd'],
        ...                other_inputs=['e'],
        ...                slice_id=0,
        ...                slice_count=2,
        ...                name='x')
        >>> n.run(inputs=dictionary)
        {'c-slice-0': {'subpath/a': 103}, 'd-slice-0': {'subpath/a': 104}}

        Configurators

        >>> def fn(x: int, y: int, conf, e: int) -> list:
        ...     return [x+e+conf['add'], y+e+conf['add']]
        >>> n = _MultiNode(slicer=lbn,
        ...                func=fn,
        ...                partitioned_inputs=['a', 'b'],
        ...                partitioned_outputs=['c', 'd'],
        ...                other_inputs=['e'],
        ...                slice_id=0,
        ...                slice_count=1,
        ...                name='x',
        ...                configurator='param:conf')
        >>> dictionary['param:conf'] = {
        ...     'template': {'pattern': 'subpath/{ab}'},
        ...     'configurators': [{'target': ['a'], 'data': {'add': 100}},
        ...                       {'target': ['*'], 'data': {'add': 20}}]}
        >>> n.run(inputs=dictionary)
        {'c-slice-0': {'subpath/a': 203}, 'd-slice-0': {'subpath/a': 204}}
    """

    SLICE_SUFFIX = '-slice-'

    def __init__(
        self,
        slicer: _SlicerNode,
        func: Callable,
        name: str,
        partitioned_inputs: Union[str, List[str]],
        partitioned_outputs: Union[None, str, List[str]],
        slice_id: int,
        slice_count: int,
        other_inputs: Union[None, str, List[str]] = [],
        tags: Union[str, Iterable[str]] = None,
        confirms: Union[str, List[str]] = None,
        namespace: str = None,
        previous_nodes: List[_MultiNode] = [],
        configurator: str = None,
    ):
        self._slicer = slicer

        self._partitioned_inputs = partitioned_inputs

        self._other_inputs = other_inputs
        self._partitioned_outputs = partitioned_outputs

        self._slice_id = slice_id
        self._slice_count = slice_count

        self._configurator = configurator

        self._point_to_matches(previous_nodes)

        super().__init__(
            func=func,
            inputs=([self.slicer_output] + tolist(partitioned_inputs)
                    + optionaltolist(configurator) + tolist(other_inputs)),
            outputs=self.partitioned_outputs,
            name=self._add_slice_suffix(name),
            tags=tags,
            confirms=confirms,
            namespace=namespace,
        )

    def _calc_match_index(self, input_count: int) -> int:
        """Assings a previous output copy to an input of this layer.

        Args:
            input_count (int): number of slices for a previous output

        Returns:
            int
        """
        return math.ceil(self.slice_id * (input_count / self.slice_count))

    def _point_to_matches(self, previous_nodes: List[_MultiNode]) -> List[str]:
        """Points to the partitioned output copies from previous nodes.

        Args:
            previous_nodes (List[_MultiNode])

        Returns:
            List[str]

        Note:
            This is not necessary, it is only made for visualization purposes.
            i.e. all nodes can point to the slice 0 if a previous node outputs
            an input
        """
        inputs = Counter(
            inp for n in previous_nodes
            for inp in n.original_partitioned_outputs
        )

        self._partitioned_inputs = [
            self.add_slice_suffix(
                input, self._calc_match_index(inputs[input])
            ) if inputs[input] > 0 else input
            for input in self.original_partitioned_inputs
        ]

    # required for inheritance
    def _copy(self, **overwrite_params: Any) -> _MultiNode:
        params = {
            'slicer': self._slicer,
            'func': self._func,
            'partitioned_inputs': self._partitioned_inputs,
            'other_inputs': self._other_inputs,
            'partitioned_outputs': self._partitioned_outputs,
            'slice_id': self._slice_id,
            'slice_count': self._slice_count,
            'name': self._name,
            'namespace': self._namespace,
            'tags': self._tags,
            'confirms': self._confirms,
            'configurator': self._configurator,
        }
        params.update(overwrite_params)
        return self.__class__(**params)

    def _validate_inputs(self, func: Any, inputs: Any):
        try:
            super()._validate_inputs(func, inputs)
        except TypeError as e:
            expected, passed = re.findall(r'(\[.*?\])', str(e))
            if len(expected) > len(passed):
                raise e

    @property
    def slicer_output(self) -> str:
        """Returns the load balancer json output.

        Returns:
            str
        """
        return self._slicer.json_output

    @classmethod
    def add_slice_suffix(cls, string: Union[str, List[str]],
                         slice_id: int) -> Union[str, List[str]]:
        """Adds a `{SLICE_SUFFIX}{slice_id}` at the end of a string.

        Args:
            string (Union[str, List[str]])
            slice_id (int)

        Returns:
            str

        Example:
            >>> _MultiNode.add_slice_suffix('test', 1)
            'test-slice-1'

            >>> _MultiNode.add_slice_suffix('test-slice-1', 1)
            'test-slice-1'
        """
        return firstorlist([
            el if re.search(rf'{cls.SLICE_SUFFIX}\d+$', el) else
            f'{el}{cls.SLICE_SUFFIX}{slice_id}' for el in tolist(string)
        ])

    def _add_slice_suffix(
        self,
        string: Union[str, List[str]],
    ) -> Union[str, List[str]]:
        return self.add_slice_suffix(string, self.slice_id)

    @property
    def slice_id(self) -> int:
        """Index of the current multinode.

        Returns:
            int
        """
        return self._slice_id

    @property
    def slice_count(self) -> int:
        """Size of this multinode set.

        Returns:
            int
        """
        return self._slice_count

    @property
    def original_partitioned_inputs(self) -> List[str]:
        """Partitioned inputs as list.

        Returns:
            List[str]
        """
        return tolist(self._partitioned_inputs)

    @property
    def partitioned_inputs(self) -> List[str]:
        return tolist(self._partitioned_inputs)

    @property
    def other_inputs(self) -> List[str]:
        """Regular inputs (provided to all multinodes).

        Returns:
            List[str]
        """
        return tolist(self._other_inputs or [])

    @property
    def original_partitioned_outputs(self) -> List[str]:
        """Partitioned outputs passed in init.

        Returns:
            List[str]
        """
        return tolist(self._partitioned_outputs)

    @property
    def partitioned_outputs(self) -> List[str]:
        """Partitioned outputs adding slice suffix.

        Returns:
            List[str]
        """
        return [
            self._add_slice_suffix(output)
            for output in self.original_partitioned_outputs
        ]

    @property
    def outputs(self) -> List[str]:
        """Node outputs adding slice suffixes.

        Returns:
            List[str]
        """
        return [
            self._add_slice_suffix(output)
            if output in self.original_partitioned_outputs else output
            for output in super().outputs
        ]

    def _intersect_partitioneds(
        self, slice: Set[str], partitioneds: List[_Partitioned]
    ) -> List[_Partitioned]:
        """Takes only the matching partitions (required for the input `zip`).

        Args:
            partitioneds (List[Partitioned]): partitioned dicionaries

        Returns:
            List[Partitioned]
        """
        return [{
            path: partitioned[path]
            for path in partitioned
            if get_filepath_without_extension(path) in slice
        } for partitioned in partitioneds]

    def _get_slice(self, slices: List[List[str]]) -> Set[str]:
        return set(slices[self.slice_id])

    def _slice_inputs(
        self, slices: List[List[str]], partitioneds: List[_Partitioned]
    ) -> List[_Partitioned]:
        """Returns the partitioned dictionaries sliced for this node.

        Args:
            partitioneds (List[Partitioned]): original partitioned dictionaries

        Returns:
            List[Partitioned]
        """
        slice = self._get_slice(slices)
        return self._intersect_partitioneds(slice, partitioneds)

    @classmethod
    def _extract_args_part(cls, args: List[Any],
                           nargs: int) -> Tuple[List[Any], List[Any]]:
        return args[:nargs], args[nargs:]

    def _extract_slices(self,
                        args: List[Any]) -> Tuple[List[List[str]], List[Any]]:
        slices, args = self._extract_args_part(args, 1)
        return slices[0], args

    def _extract_partitioneds(
        self, args: List[Any]
    ) -> Tuple[List[_Partitioned], List[Any]]:
        return self._extract_args_part(args, len(self.partitioned_inputs))

    def _extract_configurators(
        self, args: List[Any]
    ) -> Tuple[_Configurators, List[Any]]:
        if self._configurator is None:
            return {}, args
        else:
            configurator, rest = self._extract_args_part(args, 1)
            return configurator[0], rest

    def _extract_args(
        self, args: List[Any]
    ) -> Tuple[List[List[str]],
               List[_Partitioned],
               Union[None, Dict[str, _Configurator]],
               List[Any],
               ]:
        slices, args = self._extract_slices(args)
        partitioneds, args = self._extract_partitioneds(args)
        configurators, args = self._extract_configurators(args)
        other_inputs = args
        return slices, partitioneds, configurators, other_inputs

    @property
    def func(self) -> Callable:
        """Original `func`, but adding the partition loop.

        Returns:
            Callable
        """

        @wraps(self._func)
        def fn(*args: Any) -> Any:
            slices, partitioneds, configurators, other_inputs =\
                self._extract_args(args)

            partitioneds = self._slice_inputs(slices, partitioneds)

            if self._configurator:
                configurator_finder = ConfiguratorFinder(configurators)

            outputs = [dict() for _ in range(len(self.partitioned_outputs))]
            if partitioneds[0]:
                for partitions in zip(
                    *[partition.items() for partition in partitioneds]
                ):
                    # partitions[i][j]
                    # i = partitioned partitions
                    # j = key == 0, value == 1
                    partition = get_filepath_without_extension(
                        partitions[0][0]
                    )
                    self._logger.info(
                        f'Processing "{partition}" on "{self.name}"'
                    )

                    configurator = []
                    if self._configurator:
                        possible_configurator = configurator_finder[partition]

                        if possible_configurator is None:
                            self._logger.warning(
                                f'No configurator found for "{partition}"'
                            )
                        else:
                            target = possible_configurator['target']
                            configurator = [possible_configurator['data']]
                            self._logger.info(
                                f'Using configurator "{target}" for '
                                f'"{partition}"'
                            )

                    with ThreadPoolExecutor() as pool:
                        inputs = pool.map(lambda p: p[1](), partitions)

                    fn_return = self._original_func(
                        *inputs, *configurator, *other_inputs
                    )

                    if len(self.partitioned_outputs) > 1:
                        for i, _ in enumerate(self.partitioned_outputs):
                            outputs[i][partition] = fn_return[i]
                    else:
                        outputs[0][partition] = fn_return

            return outputs

        return fn


class _SynchronizationNode(_CustomizedFuncNode):
    """Barrier node to prevent multinode dependants to run out of order.

    Example:
        >>> lbn = _SlicerNode(2, 'a', 'b', 'x')
        >>> def fn(x: int) -> int: x + 10
        >>> mns = [_MultiNode(slicer=lbn, func=fn,
        ...                   partitioned_inputs='a', slice_count=2,
        ...                   partitioned_outputs='b', slice_id=i, name='x')
        ...        for i in range(2)]
        >>> n = _SynchronizationNode(multinodes=mns, name='x',
        ...                          partitioned_outputs='b',)
        >>> dictionary = {mn.outputs[0]: {'subpath/a': lambda: 13,
        ...                'subpath/b': lambda: 14,
        ...                'subpath/c': lambda: 15} for mn in mns}
        >>> n.run(inputs=dictionary)
        {'b': {}}
    """

    SYNCHRONIZATION_SUFFIX = '-synchronization'

    def __init__(
        self,
        multinodes: List[_MultiNode],
        partitioned_outputs: Union[str, List[str]],
        name: str,
        tags: Union[str, Iterable[str]] = None,
        confirms: Union[str, List[str]] = None,
        namespace: str = None,
    ):
        self._multinodes = multinodes
        self._partitioned_outputs = partitioned_outputs

        super().__init__(
            func=nonefy,
            inputs=self._extract_inputs(multinodes),
            outputs=tolist(partitioned_outputs),
            name=self._add_synchronization_suffix(name),
            tags=tags,
            confirms=confirms,
            namespace=namespace,
        )

    def _add_synchronization_suffix(self, string: str) -> str:
        if not string.endswith(self.SYNCHRONIZATION_SUFFIX):
            return f'{string}{self.SYNCHRONIZATION_SUFFIX}'
        else:
            return string

    # required for inheritance
    def _copy(self, **overwrite_params: Any) -> _SynchronizationNode:
        params = {
            'multinodes': self._multinodes,
            'partitioned_outputs': self._partitioned_outputs,
            'name': self._name,
            'namespace': self._namespace,
            'tags': self._tags,
            'confirms': self._confirms,
        }
        params.update(overwrite_params)
        return self.__class__(**params)

    @classmethod
    def _extract_inputs(cls, nodes: List[_MultiNode]) -> List[str]:
        return [output for node in nodes for output in node.outputs]

    @property
    def func(self) -> Callable:

        def fn(*args: Any) -> List[dict]:
            return [dict() for _ in range(len(self.outputs))]

        return fn


def _treat_optional_one_or_many(arg: Union[None, T, List[str]]) -> List[T]:
    """Treats argument that can be None, List, or another type as list.

    Args:
        arg (Union[None, T, List[str]])

    Returns:
        List[T]

    Example:
        >>> _treat_optional_one_or_many(3)
        [3]

        >>> _treat_optional_one_or_many([3])
        [3]

        >>> _treat_optional_one_or_many(None)
        []
    """
    if arg is None:
        return []
    if isinstance(arg, list):
        return arg
    else:
        return [arg]


def _sortnodes(nodes: Iterable[Node]) -> List[Node]:
    """Sorts a list of nodes by its name.

    Args:
        nodes (List[Node])

    Returns:
        List[Node]

    Example:
        >>> _sortnodes([node(min, 'a', 'b', name='def'),
        ...             node(max, 'b', 'c', name='abc')])
        [Node(max, 'b', 'c', 'abc'), Node(min, 'a', 'b', 'def')]
    """
    return sorted(nodes, key=lambda x: x.name)


def multipipeline(
    pipe: Pipeline,
    partitioned_input: Union[str, List[str]],
    name: str,
    configurator: str = None,
    tags: Union[str, Iterable[str]] = None,
    confirms: Union[str, List[str]] = None,
    namespace: str = None,
    n_slices: int = MAX_NODES * MAX_WORKERS,
    max_simultaneous_steps: int = None,
    filter: IsFunction[str] = truthify,
) -> Pipeline:
    """Creates multiple pipelines to process partitioned data.

    Multipipelines are the same as multinode, but instead of adding a
    synhcronization node for each step, it creates small pipelines that work
    like a multinode, with a synchronization only at the end of the pipeline.
    This enables to process data in parallel, but without the need of waiting
    for a multinode layer to finish its work.

    See also:
        :py:func:`kedro_partitioned.multinode.multinode`

    Args:
        pipe (Pipeline): Pipeline to be parallelized by multiple nodes.
        partitioned_input (Union[str, List[str]]):
            Name of the `PartitionedDataSet` used as input. If a list is
            provided, it will work like a zip(partitions_a, ..., partitions_n)
        configurator (str, optional): Name of partitioned parameters used as
            input. e.g. 'param:configurators'
        name (str, optional): Name prefix for the multiple nodes, and the name
            of the post node. Defaults to None.
        tags (Union[str, Iterable[str]], optional): List of tags for all nodes
            generated by this function. Defaults to None.
        confirms (Union[str, List[str]], optional): List of DataSets that
            should be confirmed before the execution of the nodes.
            Defaults to None.
        namespace (str, optional): Namespace the nodes belong to.
            Defaults to None.
        n_slices (int): Number of multinodes to build.
            Defaults to MAX_WORKERS + MAX_NODES
        max_simultaneous_steps (int): Maximum number of slices created for
            each branch. Defaults to None.
        filter (IsFunction[str]): A function applied to each partition of
            the partitioned inputs. If the function returns False, the
            parttition won't be used.

    Returns:
        Pipeline

    Example:
        >>> sortnodes = lambda pipe: sorted(pipe.nodes, key=lambda n: n.name)
        >>> funcpipe = Pipeline([node(min, ['a', 'b'], 'c', name='abc'),
        ...                      node(max, ['c', 'd'], ['e'], name='def')])
        >>> sortnodes(multipipeline(
        ...     funcpipe,
        ...     ['a'],
        ...     'x',
        ...     n_slices=2)) # doctest: +NORMALIZE_WHITESPACE
        [Node(min, ['c-slicer', 'a', 'b'], ['c-slice-0'], 'abc-slice-0'),
         Node(min, ['c-slicer', 'a', 'b'], ['c-slice-1'], 'abc-slice-1'),
         Node(max, ['c-slicer', 'c-slice-0', 'd'], ['e-slice-0'],\
            'def-slice-0'),
         Node(max, ['c-slicer', 'c-slice-1', 'd'], ['e-slice-1'],\
            'def-slice-1'),
         Node(nonefy, ['a'], 'c-slicer', 'x'),
         Node(nonefy, ['e-slice-0', 'e-slice-1'], ['c', 'e'],\
            'x-synchronization')]

    Max Simultaneous Steps:

        This configuration defines the maximum number of steps per branch.
        Check the example bellow for more details:

        .. code-block:: python

            max_simultaneous_steps = None
            n_slices = 2
            func = pipe([A->B, B->C, [C, D]->E])
                            B->D
            output = pipe(A->B0, B0->C0, [C0, D0] -> E0)
                            A->B1  B1->C1  [C1, D1] -> D1
                                    B0->D0
                                    B1->D1

            max_simultaneous_steps = 2
            output: pipe(A->B0, B0->C0, [C0, D0] -> E0)
                                B0->D0

    Warning:
        every function must me declared considering partitioned inputs are
        the first arguments of the function, the configurator (if present)
        is the following argument, and the rest of the arguments are other
        outputs.

    Warning:
        The configurator syntax prioritizes more specific targets rather
        than generalist ones. However, if you have ambiguity
        between configurators, a wrong configurator may be used e.g. if
        you have a configurator with target ['a', 'b'], and another with
        target ['a'], the first match will be used i.e. order is random or
        list instance order driven.
    """
    # sorts just to keep output consistency
    partitioned_output = sorted(list(pipe.all_outputs()))
    if max_simultaneous_steps is not None:
        n_slices = max(
            1,
            math.floor(
                max_simultaneous_steps
                / max(len(layer) for layer in pipe.grouped_nodes)
            )
        )

    # because a multinode becomes multiple nodes
    confirms = unique(_treat_optional_one_or_many(confirms))
    tags = unique(_treat_optional_one_or_many(tags) + [name])

    slicer = Pipeline([
        _SlicerNode(
            slice_count=n_slices,
            partitioned_inputs=partitioned_input,
            partitioned_outputs=tolist(partitioned_output)[0],
            name=name,
            tags=tags,
            confirms=confirms,
            namespace=namespace,
            filter=filter,
            configurator=configurator
        )
    ])

    sources = set(tolist(partitioned_input) + tolist(partitioned_output))

    multinodes = Pipeline([])

    for layer in pipe.grouped_nodes:
        multinode_layer: List[_MultiNode] = []
        for lnode in layer:
            assert lnode._name, f'"{lnode}" name not defined'

            partitioned, other = partition(
                lambda x: x in sources, lnode.inputs
            )

            possible_configurator, other = partition(
                lambda x: x == configurator, other
            )
            node_configurator = (
                possible_configurator[0] if possible_configurator else None
            )

            for i in range(n_slices):
                multinode_layer.append(
                    _MultiNode(
                        name=lnode.name,
                        func=lnode._func,
                        namespace=namespace,
                        other_inputs=other,
                        partitioned_inputs=partitioned,
                        partitioned_outputs=lnode.outputs,
                        slice_count=n_slices,
                        slice_id=i,
                        slicer=slicer.nodes[0],
                        confirms=unique(lnode.confirms + confirms),
                        tags=unique(list(lnode.tags) + tags),
                        previous_nodes=multinodes._nodes,
                        configurator=node_configurator,
                    )
                )

        multinodes = multinodes + Pipeline(multinode_layer)

    synchronization = Pipeline([
        _SynchronizationNode(
            multinodes=_sortnodes(multinodes.grouped_nodes[-1]),
            partitioned_outputs=partitioned_output,
            name=name,
            tags=tags,
            confirms=confirms,
            namespace=namespace,
        )
    ])

    return slicer + multinodes + synchronization


def multinode(
    func: Callable[[Args[Any]], Union[Any, List[Any]]],
    partitioned_input: Union[str, List[str]],
    partitioned_output: Union[str, List[str]],
    name: str,
    configurator: str = None,
    other_inputs: List[str] = [],
    tags: Union[str, Iterable[str]] = None,
    confirms: Union[str, List[str]] = None,
    namespace: str = None,
    n_slices: int = MAX_NODES * MAX_WORKERS,
    filter: IsFunction[str] = truthify,
) -> Pipeline:
    """Creates multiple nodes to process partitioned data.

    Multinodes are a way to implement step level parallelism. It is useful
    for processing independent data in parallel, managed by pipeline runners.
    For example, in Kedro, running the pipeline with ParallelRunner would
    enable the steps generated by multinode to be run using multiple cpus. At
    the same time, if you run this pipeline in a distributed context, you
    could rely on a "DistributedRunner" or in another pipeline manager like
    AzureML or Kubeflow, without having to change the code.

    Multinodes work like the following flowchart:
        .. code-block:: text

            +--------------+
            | Configurator |
            | parameter    |-+
            +--------------+ |
                        |    |
                        v    |
            +-------------+  +->+------------+     +--------------------+
            | Slicer Node |--+->| Slice-0    |--+->| Synchronization    |
            | my          |  +->| my-slice-0 |  |  | my-synchronization |
            +-------------+  |  +------------+  |  +--------------------+
                        ^    |          ...     |       |
                        |    +->+------------+  |       v
            +-------------+  +->| Slice-1    |--|   +-------------+
            | Partitioned |--+->| my-slice-1 |  |   | Partitioned |
            | input-ds    |  |  +------------+  |   | output-ds   |
            +-------------+  +->+------------+  |   +-------------+
            Start            +->| Slice-n    |--+   End
                             +->| my-slice-n |
                                +------------+

    Nodes specification:
        Slicer Node:
            name: multinode name
            inputs: partitioned inputs and configurator cached flags
            outputs: json with a list of partitions for each slice

        Slice:
            name: multinode name + slice id
            inputs: partitioned inputs, slicer json, configurator data
            outputs: subset of the partitioned outputs

        Synchronization:
            name: multinode name + synchronization
            inputs: subset of the partitioned outputs
            outputs: partitioned outputs without data (synchronization only)

    Args:
        func (Callable[[Args[Any]], Union[str, List[Any]]]): Function executed
            by each of the n-nodes. It takes n positional arguments, being the
            first of them one partition from the `partitioned_input`
        partitioned_input (Union[str, List[str]]):
            Name of the `PartitionedDataSet` used as input. If a list is
            provided, it will work like a zip(partitions_a, ..., partitions_n)
        partitioned_output (Union[str,  List[str]]):
            Name of the `PartitionedDataSet` used as output by func nodes
        configurator (str, optional): An input name of a partitioned parameter
            dict. e.g. 'params:config'
        name (str, optional): Name prefix for the multiple nodes, and the name
            of the post node. Defaults to None.
        other_inputs (List[str], optional):
            Name of other inputs for func. Defaults to [].
        tags (Union[str, Iterable[str]], optional): List of tags for all nodes
            generated by this function. Defaults to None.
        confirms (Union[str, List[str]], optional): List of DataSets that
            should be confirmed before the execution of the nodes.
            Defaults to None.
        namespace (str, optional): Namespace the nodes belong to.
            Defaults to None.
        n_slices (int): Number of multinodes to build.
            Defaults to MAX_WORKERS + MAX_NODES
        filter (IsFunction[str], optional): Function to filter input partitions

    Returns:
        Pipeline

    Example:
        >>> sortnodes = lambda pipe: sorted(pipe.nodes, key=lambda n: n.name)
        >>> sortnodes(multinode(
        ...     func=max,
        ...     partitioned_input='a',
        ...     partitioned_output='b',
        ...     other_inputs=['d'],
        ...     n_slices=2,
        ...     name='x',)) # doctest: +NORMALIZE_WHITESPACE
        [Node(nonefy, ['a'], 'b-slicer', 'x'),
         Node(max, ['b-slicer', 'a', 'd'], ['b-slice-0'], 'x-slice-0'),
         Node(max, ['b-slicer', 'a', 'd'], ['b-slice-1'], 'x-slice-1'),
         Node(nonefy, ['b-slice-0', 'b-slice-1'], ['b'], 'x-synchronization')]

        Accepts multiple inputs (works like zip(*partitioneds)):

        >>> sortnodes(multinode(
        ...     func=max,
        ...     partitioned_input=['a', 'b'],
        ...     partitioned_output='c',
        ...     n_slices=2,
        ...     other_inputs=['d'],
        ...     name='x')) # doctest: +NORMALIZE_WHITESPACE
        [Node(nonefy, ['a', 'b'], 'c-slicer', 'x'),
         Node(max, ['c-slicer', 'a', 'b', 'd'], ['c-slice-0'], 'x-slice-0'),
         Node(max, ['c-slicer', 'a', 'b', 'd'], ['c-slice-1'], 'x-slice-1'),
         Node(nonefy, ['c-slice-0', 'c-slice-1'], ['c'], 'x-synchronization')]

        Accepts multiple outputs:

        >>> sortnodes(multinode(
        ...     func=max,
        ...     partitioned_input='a',
        ...     partitioned_output=['b', 'c'],
        ...     n_slices=2,
        ...     other_inputs=['d'],
        ...     name='x')) # doctest: +NORMALIZE_WHITESPACE
        [Node(nonefy, ['a'], 'b-slicer', 'x'),
         Node(max, ['b-slicer', 'a', 'd'], ['b-slice-0', 'c-slice-0'],\
            'x-slice-0'),
         Node(max, ['b-slicer', 'a', 'd'], ['b-slice-1', 'c-slice-1'],\
            'x-slice-1'),
         Node(nonefy, ['b-slice-0', 'c-slice-0', 'b-slice-1', 'c-slice-1'],\
            ['b', 'c'], 'x-synchronization')]

        Tags and namespaces are allowed:

        >>> mn = multinode(
        ...     func=max,
        ...     partitioned_input='a',
        ...     partitioned_output=['b', 'c'],
        ...     name='x',
        ...     n_slices=2,
        ...     tags=['test_tag'],
        ...     namespace='namespace',)
        >>> sortnodes(mn) # doctest: +NORMALIZE_WHITESPACE
        [Node(nonefy, ['a'], 'b-slicer', 'x'),
         Node(max, ['b-slicer', 'a'], ['b-slice-0', 'c-slice-0'], 'x-slice-0'),
         Node(max, ['b-slicer', 'a'], ['b-slice-1', 'c-slice-1'], 'x-slice-1'),
         Node(nonefy, ['b-slice-0', 'c-slice-0', 'b-slice-1', 'c-slice-1'],\
            ['b', 'c'], 'x-synchronization')]
        >>> all([n.tags == {'x', 'test_tag'} for n in mn.nodes])
        True
        >>> all([n.namespace == 'namespace' for n in mn.nodes])
        True

    Configurators

        A configurator is a dict parameter declared in parameters yamls that
        contains two sections: 'template' and 'configurators'. The 'template'
        section is a dict that contains the partitions subpath pattern, and
        its configurations. The 'configurators' section is a list of
        configurators. Each configurator is a dict that contains a 'target'
        list specifying replacements for the pattern, and a data entry that
        is going to be inputted to the multinode.

        .. code-block:: yaml

            config:
              template:
                pattern: 'a-part-{part}'

              #    optional, overwrites '.*' as the regex when a
              # v  target is set to '*'
              # any:
              #  part: '(a|b|c|d)'

              #    optional, specifies priority of each target if left
              # v  to right order is not correct
              # hierarchy:
              #  - part

              configurators:
                -
                   target: # replaces pattern's {} from left to right order
                     -
                       - a
                       - b
                   # or a regex alternate syntax
                   # - a|b
                   cached: true # will not run
                   data:
                     setting_a: 'foo'
                     setting_b: 2
                -
                   target:
                     - c
                   data:
                     setting_a: 'zzz'
                     setting_b: 4
                -
                   target:
                     - '*'
                   data:
                     setting_a: 'bar'
                     setting_b: 1

        In the example above, target ['a', 'b'] will be the configurator of the
        partition 'a-part-a' and 'a-part-b', the configurator with target 'c'
        will be the configurator of the partition 'a-part-c', and the
        configurator with target '*' will be the configurator of all other
        partitions.

    Example:

        >>> mn = multinode(
        ...     func=max,
        ...     partitioned_input='a',
        ...     partitioned_output=['b', 'c'],
        ...     name='x',
        ...     n_slices=2,
        ...     tags=['test_tag'],
        ...     namespace='namespace',
        ...     configurator='params:config')
        >>> sortnodes(mn) # doctest: +NORMALIZE_WHITESPACE
        [Node(nonefy, ['a', 'params:config'], 'b-slicer', 'x'),
         Node(max, ['b-slicer', 'a', 'params:config'],\
            ['b-slice-0', 'c-slice-0'], 'x-slice-0'),
         Node(max, ['b-slicer', 'a', 'params:config'],\
            ['b-slice-1', 'c-slice-1'], 'x-slice-1'),
         Node(nonefy, ['b-slice-0', 'c-slice-0', 'b-slice-1', 'c-slice-1'],\
            ['b', 'c'], 'x-synchronization')]

    Note:
        the multinode name is also added as a tag into all nodes in order
        to allow running the multinode with `kedro run --tag`.

    Warning:
        every function must me declared considering partitioned inputs are
        the first arguments of the function, the configurator (if present)
        is the following argument, and the rest of the arguments are other
        outputs.

    Warning:
        The configurator syntax prioritizes more specific targets rather
        than generalist ones. However, if you have ambiguity
        between configurators, a wrong configurator may be used e.g. if
        you have a configurator with target ['a', 'b'], and another with
        target ['a'], the first match will be used i.e. order is random or
        list instance order driven.
    """
    pipe = Pipeline([
        node(
            func=func,
            inputs=(
                tolist(partitioned_input) + optionaltolist(configurator)
                + tolist(other_inputs)
            ),
            outputs=partitioned_output,
            name=name,
        )
    ])

    return multipipeline(
        pipe=pipe,
        partitioned_input=partitioned_input,
        configurator=configurator,
        confirms=confirms,
        filter=filter,
        max_simultaneous_steps=None,
        n_slices=n_slices,
        name=name,
        namespace=namespace,
        tags=tags,
    )
