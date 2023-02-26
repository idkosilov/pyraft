from time import sleep
from unittest.mock import MagicMock

import pytest


def test_election_timer_not_called_if_run_time_above_election_timeout_lower(election_timer):
    on_election_timeout_mock = MagicMock()
    election_timer.set_on_election_timeout(on_election_timeout_mock)

    election_timer.start()

    sleep(0.1)

    election_timer.stop()
    election_timer.join()

    on_election_timeout_mock.assert_not_called()


def test_election_timer_not_called_if_run_time_under_election_timeout_upper(election_timer):
    on_election_timeout_mock = MagicMock()
    election_timer.set_on_election_timeout(on_election_timeout_mock)

    election_timer.start()

    sleep(1.5)

    election_timer.stop()
    election_timer.join()

    on_election_timeout_mock.assert_called()


def test_election_timer_not_called_on_cancel(election_timer):
    on_election_timeout_mock = MagicMock()
    election_timer.set_on_election_timeout(on_election_timeout_mock)

    election_timer.start()

    sleep(0.4)
    election_timer.cancel()
    sleep(0.4)
    election_timer.stop()
    election_timer.join()

    on_election_timeout_mock.assert_not_called()


@pytest.mark.parametrize("run_time, on_heartbeat_callback_calls_count", [
    (0.05, 0),
    (0.15, 1),
    (0.25, 2)
])
def test_heartbeat_timer(heartbeat_timer, run_time, on_heartbeat_callback_calls_count):
    on_heartbeat_callback_mock = MagicMock()
    heartbeat_timer.set_on_heartbeat_callback(on_heartbeat_callback_mock)

    heartbeat_timer.start()

    sleep(run_time)

    heartbeat_timer.stop()
    heartbeat_timer.join()

    assert on_heartbeat_callback_mock.call_count == on_heartbeat_callback_calls_count
