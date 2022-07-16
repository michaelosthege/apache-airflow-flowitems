from datetime import datetime

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
        assert fi.kwargs_upstream.get("kw2") == "bash_2"
        pass

    def test_exceptions(self):
        fi = core.FlowItem()
        with pytest.raises(NotImplementedError):
            fi()
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
        assert pi.kwargs_upstream["kw1"] == "bash_2"
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
