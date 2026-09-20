import unittest

from ai_gateway.limits import RateLimiter


class RateLimitTests(unittest.TestCase):
    def test_disabled_limit_allows_requests(self) -> None:
        limiter = RateLimiter(0)
        self.assertTrue(limiter.allow("client"))
        self.assertTrue(limiter.allow("client"))

    def test_limit_is_scoped_to_identity(self) -> None:
        limiter = RateLimiter(1)
        self.assertTrue(limiter.allow("one"))
        self.assertFalse(limiter.allow("one"))
        self.assertTrue(limiter.allow("two"))

    def test_managed_key_identities_can_be_limited_independently(self) -> None:
        limiter = RateLimiter(1)
        self.assertTrue(limiter.allow("api-key:1"))
        self.assertTrue(limiter.allow("api-key:2"))
        self.assertFalse(limiter.allow("api-key:1"))
