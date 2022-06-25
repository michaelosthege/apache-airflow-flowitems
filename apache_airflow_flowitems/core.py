import abc
import copy
import inspect
import json
from typing import Any, Callable, Mapping, Sequence

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator


class FlowItem:
    def __init__(self, *args: Sequence[BaseOperator], **kwargs: Mapping[str, Any]) -> None:
        """Creates a FlowItem.

        Parameters
        ----------
        *args
            Upstream tasks (BaseOperator instances) to depend on.
        *kwargs
            Named parameters of this tasks.
            The values can be constants,
            or upstream tasks (BaseOperator instances) from which
            to xcom_pull the return values.
        """
        self.upstream_tasks = list(args)
        self.kwargs_upstream = {}
        self.kwargs_static = {}

        for k, v in kwargs.items():
            if isinstance(v, BaseOperator):
                self.kwargs_upstream[k] = v.task_id
                self.upstream_tasks.append(v)
            else:
                self.kwargs_static[k] = v
        super().__init__()

    @abc.abstractmethod
    def __call__(self, **kwargs: Mapping[str, Any]) -> BaseOperator:
        """Creates an operator instance for a `FlowItem`.

        Parameters
        ----------
        **kwargs
            Additional kwargs for the operator.
            For example: ``task_id``.

        Returns
        -------
        task : PythonOperator
            Instance of the created operator.
        """
        raise NotImplementedError()


class PythonItem(FlowItem):
    def __init__(
        self, python_callable: Callable, *args: Sequence[BaseOperator], **kwargs: Mapping[str, Any]
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
        task_instance = context["task_instance"]

        # assemble the kwargs in a new dict
        kwargs = copy(self.kwargs_static)

        for k, taskid in self.kwargs_upstream.items():
            upstream_result = task_instance.xcom_pull(task_ids=[taskid])[0]
            kwargs[k] = json.loads(upstream_result)

        # Forward the context dict if the callable takes a "context" parameter
        spec = inspect.getfullargspec(self.callable)
        if "context" in spec.args or "context" in spec.kwonlyargs:
            result = self.callable(context=context, **kwargs)
        else:
            result = self.callable(**kwargs)
        return json.dumps(result)

    def __call__(self, **kwargs: Mapping[str, Any]) -> PythonOperator:
        """Creates a PythonOperator for the execution of this flow item.

        Parameters
        ----------
        **kwargs
            Additional kwargs for the PythonOperator.
            For example: ``task_id``.

        Returns
        -------
        task : PythonOperator
            Instance of the created operator.
        """
        if "task_id" not in kwargs:
            kwargs["task_id"] = self.callable.__name__
        t = PythonOperator(python_callable=self._run, **kwargs)
        t.set_upstream(self.upstream_tasks)
        return t
