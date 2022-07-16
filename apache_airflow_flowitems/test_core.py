from datetime import datetime
from unittest.mock import MagicMock

import pytest
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from . import core


class TestFlowItem:
    def test_init(self):
        up1 = BashOperator(bash_command="echo 1", task_id="bash_1")
        up2 = BashOperator(bash_command="echo 2", task_id="bash_2")
        fi = core.FlowItem(up1, kw1="one", kw2=up2)
        assert up1 in fi.upstream_tasks
        assert up2 in fi.upstream_tasks
        assert fi.kwargs_static.get("kw1") == "one"
        assert fi.kwargs_xcom.get("kw2") == up2.output
        pass

    def test_exceptions(self):
        fi = core.FlowItem()
        with pytest.raises(NotImplementedError):
            fi()

        with pytest.raises(NotImplementedError, match="dependency type"):
            core.FlowItem("not a task")
        pass

    def test_xcom_args(self):
        up1 = BashOperator(bash_command="echo 1", task_id="bash_1").output
        fi = core.FlowItem(up1, up1)
        assert not fi.kwargs_static
        assert not fi.kwargs_xcom
        assert up1.operator in fi.upstream_tasks
        pass

    def test_xcom_kwargs(self):
        up1 = BashOperator(bash_command="echo 1", task_id="bash_1").output
        fi = core.FlowItem(up1, kw1=up1)
        assert not fi.kwargs_static
        assert fi.kwargs_xcom["kw1"] == up1
        assert up1.operator in fi.upstream_tasks
        pass


class TestPythonItem:
    @staticmethod
    def fun_one():
        return 1

    @staticmethod
    def fun_dict(**kwargs):
        return {"first": "one", "second": "two", **kwargs}

    def test_init(self):
        up1 = BashOperator(bash_command="echo 1", task_id="bash_1")
        up2 = BashOperator(bash_command="echo 2", task_id="bash_2")
        pi = core.PythonItem(TestPythonItem.fun_dict, up1, kw1=up2)
        assert pi.python_callable == TestPythonItem.fun_dict
        assert up1 in pi.upstream_tasks
        assert up2 in pi.upstream_tasks
        assert not pi.kwargs_static
        assert pi.kwargs_xcom["kw1"] == up2.output
        pass

    @dag("testdag", start_date=datetime.utcnow())
    def test_call(self):
        up1 = BashOperator(bash_command="echo 1", task_id="bash_1")
        up2 = BashOperator(bash_command="echo 2", task_id="bash_2")
        pi = core.PythonItem(TestPythonItem.fun_dict, up1, kw1=up2)

        pi_task = pi()
        assert isinstance(pi_task, PythonOperator)
        assert pi_task.task_id == "fun_dict"
        assert up1 in pi_task.upstream_list
        assert up2 in pi_task.upstream_list

        pi_task_named = pi(task_id="into_dict")
        assert pi_task_named.task_id == "into_dict"
        pass

    def test_run(self):
        x1 = BashOperator(bash_command="echo 1", task_id="bash_1").output
        pi = core.PythonItem(TestPythonItem.fun_dict, a=x1)
        assert not pi.kwargs_static
        assert pi.kwargs_xcom["a"] == x1

        x1.resolve = MagicMock(return_value="hello")
        context = {"task_instance": None}
        result = pi._run(**context)
        x1.resolve.assert_called_once_with(context)
        assert isinstance(result, dict)
        assert result["a"] == "hello"
        pass

    def test_run_autocontext(self):
        def fun_with_context(context):
            return context["run_id"]

        pi = core.PythonItem(fun_with_context)

        context = {"run_id": "manual__123"}
        result = pi._run(**context)
        assert result == "manual__123"
        pass
