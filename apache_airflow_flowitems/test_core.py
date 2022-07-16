import pytest
from airflow.operators.bash import BashOperator

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
