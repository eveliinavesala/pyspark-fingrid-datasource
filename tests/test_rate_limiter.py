"""Tests for pyspark_fingrid.rate_limiter.RateLimiter."""

from unittest.mock import patch

from pyspark_fingrid.rate_limiter import RateLimiter


class TestRateLimiter:
    def test_waits_when_called_too_soon(self, monkeypatch):
        limiter = RateLimiter(min_interval_seconds=2.0)
        monkeypatch.setattr(limiter, "_last_call_time", 100.0)

        with (
            patch("pyspark_fingrid.rate_limiter.time.monotonic", return_value=100.5),
            patch("pyspark_fingrid.rate_limiter.time.sleep") as mock_sleep,
        ):
            limiter.wait()

        mock_sleep.assert_called_once()
        waited = mock_sleep.call_args[0][0]
        assert waited == 1.5  # needed 2.0s total, only 0.5s had passed

    def test_does_not_wait_when_interval_already_elapsed(self, monkeypatch):
        limiter = RateLimiter(min_interval_seconds=2.0)
        monkeypatch.setattr(limiter, "_last_call_time", 100.0)

        with (
            patch("pyspark_fingrid.rate_limiter.time.monotonic", return_value=105.0),
            patch("pyspark_fingrid.rate_limiter.time.sleep") as mock_sleep,
        ):
            limiter.wait()

        mock_sleep.assert_not_called()

    def test_zero_interval_never_waits(self):
        limiter = RateLimiter(min_interval_seconds=0.0)
        with patch("pyspark_fingrid.rate_limiter.time.sleep") as mock_sleep:
            limiter.wait()
            limiter.wait()

        mock_sleep.assert_not_called()

    def test_two_instances_do_not_share_state(self):
        """Different clients/keys must not share one clock."""
        a = RateLimiter(min_interval_seconds=2.0)
        b = RateLimiter(min_interval_seconds=2.0)

        with (
            patch("pyspark_fingrid.rate_limiter.time.monotonic", return_value=100.0),
            patch("pyspark_fingrid.rate_limiter.time.sleep") as mock_sleep,
        ):
            a.wait()  # a's first call; nothing to wait on yet
            a.wait()  # a's second call at the "same" instant; must wait
            assert mock_sleep.call_count == 1

            b.wait()  # b's first call; must NOT wait despite a having just waited
            assert mock_sleep.call_count == 1
