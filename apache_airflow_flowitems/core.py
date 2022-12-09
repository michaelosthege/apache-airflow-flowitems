import abc
import copy
import inspect
from typing import Any, Callable, Dict, List, Mapping, Sequence, Union

from airflow.models import BaseOperator

try:
    from airflow.models import Operator
except:
    Operator = BaseOperator
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator


class FlowItem:
    def __init__(self, *args: Sequence[Union[Operator, XComArg]], **kwargs: Mapping[str, Any]) -> None:
        """Creates a FlowItem.

        Parameters
        ----------
        *args
            Upstream tasks (Operator instances) to depend on.
        *kwargs
            Named parameters of this tasks.
            The values can be constants,
            or upstream tasks (Operator instances) from which
            to xcom_pull the return values.
        """
        self.upstream_tasks: List[Operator] = []
        for iv, v in enumerate(args):
            if isinstance(v, BaseOperator):
                self.upstream_tasks.append(v)
            elif isinstance(v, XComArg):
                self.upstream_tasks.append(v.operator)
            else:
                raise NotImplementedError(f"Unsupported upstream dependency type {type(v)} in position {iv}.")

        self.kwargs_xcom: Dict[str, XComArg] = {}
        self.kwargs_static: Dict[str, Any] = {}

        for k, v in kwargs.items():
            if isinstance(v, BaseOperator):
                self.kwargs_xcom[k] = v.output
                self.upstream_tasks.append(v)
            elif isinstance(v, XComArg):
                self.kwargs_xcom[k] = v
                self.upstream_tasks.append(v.operator)
            else:
                self.kwargs_static[k] = v
        super().__init__()

    @abc.abstractmethod
    def __call__(self, **kwargs: Mapping[str, Any]) -> Operator:
        """Creates an operator instance for a `FlowItem`.

        Parameters
        ----------
        **kwargs
            Additional kwargs for the operator.
            For example: ``task_id``.

        Returns
        -------
        task : Operator
            Instance of the created operator.
        """
        raise NotImplementedError()


class PythonItem(FlowItem):
    def __init__(
        self,
        python_callable: Callable,
        *args: Sequence[Union[Operator, XComArg]],
        **kwargs: Mapping[str, Any],
    ) -> None:
        self.python_callable = python_callable
        super().__init__(*args, **kwargs)

    def _run(self, **context: Mapping[str, Any]) -> str:
        """Executes the callable with args & kwargs.

        Parameters
        ----------
        **context
            The context dictionary of the task instance.

        Returns
        -------
        result_json : str
            JSON-encoded return value of the callable.
        """
        # assemble the kwargs in a new dict
        kwargs = copy.copy(self.kwargs_static)

        for k, xarg in self.kwargs_xcom.items():
            kwargs[k] = xarg.resolve(context)

        # Forward the context dict if the callable takes a "context" parameter
        spec = inspect.getfullargspec(self.python_callable)
        if "context" in spec.args or "context" in spec.kwonlyargs:
            result = self.python_callable(context=context, **kwargs)
        else:
            result = self.python_callable(**kwargs)
        return result

    def __call__(self, **kwargs: Mapping[str, Any]) -> PythonOperator:
        """Creates a ``PythonOperator`` for the execution of this flow item.

        Parameters
        ----------
        **kwargs
            Additional kwargs for the ``PythonOperator``.
            For example: ``task_id``.

        Returns
        -------
        task : PythonOperator
            Instance of the created operator.
        """
        if "task_id" not in kwargs:
            kwargs["task_id"] = self.python_callable.__name__
        t = PythonOperator(python_callable=self._run, **kwargs)
        t.set_upstream(self.upstream_tasks)
        return t
